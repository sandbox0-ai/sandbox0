package main

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/infra/manager/pkg/generated/clientset/versioned"
	"github.com/sandbox0-ai/infra/manager/pkg/generated/informers/externalversions"
	httpserver "github.com/sandbox0-ai/infra/manager/pkg/http"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/manager/pkg/webhook"
	"github.com/sandbox0-ai/infra/pkg/clock"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
)

func main() {
	// Load configuration
	cfg := config.LoadManagerConfig()

	// Initialize logger
	logger := initLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("Starting Manager",
		zap.String("version", "v0.1.0"),
		zap.Int("httpPort", cfg.HTTPPort),
		zap.Int("metricsPort", cfg.MetricsPort),
	)

	// Create context that cancels on signal
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Create Kubernetes client
	k8sConfig, err := buildKubeConfig(cfg.KubeConfig)
	if err != nil {
		logger.Fatal("Failed to build Kubernetes config", zap.Error(err))
	}

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("Failed to create Kubernetes client", zap.Error(err))
	}

	// Add SandboxTemplate to scheme
	if err := v1alpha1.AddToScheme(scheme.Scheme); err != nil {
		logger.Fatal("Failed to add SandboxTemplate to scheme", zap.Error(err))
	}

	// Create generated CRD clientset
	crdClient, err := clientset.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("Failed to create CRD clientset", zap.Error(err))
	}

	// Initialize database and clock if DATABASE_URL is provided
	var pool *pgxpool.Pool
	var clk *clock.Clock
	if cfg.DatabaseURL != "" {
		pool, err = initDatabase(ctx, cfg.DatabaseURL, logger)
		if err != nil {
			logger.Fatal("Failed to connect to database", zap.Error(err))
		}
		defer pool.Close()

		// Initialize clock for cross-cluster time synchronization
		clk, err = clock.New(ctx, &pgxPoolAdapter{pool: pool},
			clock.WithSyncInterval(30*time.Second),
			clock.WithLogger(&zapClockLogger{logger: logger}),
		)
		if err != nil {
			logger.Fatal("Failed to initialize clock", zap.Error(err))
		}
		defer clk.Close()

		logger.Info("Clock initialized for cross-cluster time synchronization",
			zap.Int64("offset_ms", clk.Offset().Milliseconds()),
			zap.Int64("rtt_ms", clk.LastRTT().Milliseconds()),
		)
	} else {
		logger.Warn("DATABASE_URL not set, using local time (may cause cross-cluster inconsistencies)")
	}

	// Create informers
	informerFactory := informers.NewSharedInformerFactory(k8sClient, cfg.ResyncPeriod)
	podInformer := informerFactory.Core().V1().Pods().Informer()
	nodeInformer := informerFactory.Core().V1().Nodes().Informer()

	// Create CRD informer factory using generated clientset
	crdInformerFactory := externalversions.NewSharedInformerFactoryWithOptions(
		crdClient,
		cfg.ResyncPeriod,
		externalversions.WithNamespace(cfg.DefaultTemplateNamespace),
	)

	// Get SandboxTemplate informer from the factory
	templateInformer := crdInformerFactory.Sandbox0().V1alpha1().SandboxTemplates().Informer()

	// Create event recorder
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: k8sClient.CoreV1().Events(""),
	})
	eventSource := corev1.EventSource{Component: "manager"}
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, eventSource)

	// Create operator
	operator := controller.NewOperator(
		k8sClient,
		podInformer,
		templateInformer,
		recorder,
		clk,
		logger,
	)
	if pool != nil {
		operator.SetTemplateStatsPublisher(controller.NewPGTemplateStatsPublisher(pool, cfg.DefaultClusterId, clk, logger))
	}

	// Create listers
	podLister := informerFactory.Core().V1().Pods().Lister()
	nodeLister := informerFactory.Core().V1().Nodes().Lister()

	// Create network policy service for building policy annotations
	networkPolicyService := service.NewNetworkPolicyService(logger)

	// Initialize internal auth generator for procd communication
	var internalTokenGenerator service.TokenGenerator
	var procdTokenGenerator service.TokenGenerator
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		logger.Warn("Failed to load internal auth private key, pause/resume will not work",
			zap.String("path", internalauth.DefaultInternalJWTPrivateKeyPath),
			zap.Error(err),
		)
	} else {
		internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "manager",
			PrivateKey: privateKey,
			TTL:        30 * time.Second,
		})
		internalTokenGenerator = service.NewInternalTokenGenerator(internalAuthGen)
		procdTokenGenerator = service.NewProcdTokenGenerator(internalAuthGen)
		logger.Info("Internal auth generators initialized for procd communication")
	}

	// Create services
	sandboxService := service.NewSandboxService(
		k8sClient,
		podLister,
		operator.GetTemplateLister(),
		networkPolicyService,
		internalTokenGenerator,
		procdTokenGenerator,
		clk,
		cfg.DefaultSandboxTTL,
		logger,
	)

	templateService := service.NewTemplateService(
		crdClient,
		operator.GetTemplateLister(),
		logger,
	)

	// Create cluster service (for scheduler)
	clusterService := service.NewClusterService(
		k8sClient,
		podLister,
		nodeLister,
		operator.GetTemplateLister(),
		logger,
	)

	// Create cleanup controller
	cleanupController := controller.NewCleanupController(
		k8sClient,
		podLister,
		operator.GetTemplateLister(),
		recorder,
		clk,
		sandboxService,
		logger,
		cfg.CleanupInterval,
	)

	// Initialize internal auth validator
	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth public key",
			zap.String("path", internalauth.DefaultInternalJWTPublicKeyPath),
			zap.Error(err),
		)
	}

	validatorConfig := internalauth.DefaultValidatorConfig("manager", publicKey)
	validatorConfig.AllowedCallers = []string{"internal-gateway"}
	authValidator := internalauth.NewValidator(validatorConfig)

	logger.Info("Internal authentication enabled",
		zap.String("target", "manager"),
		zap.Strings("allowed_callers", validatorConfig.AllowedCallers),
	)

	// Create HTTP server
	httpServer := httpserver.NewServer(
		sandboxService,
		templateService,
		clusterService,
		authValidator,
		logger,
		cfg.HTTPPort,
	)

	// Start metrics server
	go startMetricsServer(cfg.MetricsPort, logger)

	// Start webhook server
	webhookServer := webhook.NewServer(
		cfg.WebhookPort,
		cfg.WebhookCertPath,
		cfg.WebhookKeyPath,
		logger,
	)
	go func() {
		if err := webhookServer.Start(ctx); err != nil {
			// In development, we might not have certs, so just log error but don't crash
			// In production, this should probably be fatal if webhook is expected
			logger.Error("Webhook server failed", zap.Error(err))
		}
	}()

	// Start informers
	logger.Info("Starting informers")
	informerFactory.Start(ctx.Done())
	crdInformerFactory.Start(ctx.Done())

	// Wait for cache sync
	logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced, nodeInformer.HasSynced, templateInformer.HasSynced) {
		logger.Fatal("Failed to sync informer caches")
	}

	// Wait for CRD cache sync
	syncResult := crdInformerFactory.WaitForCacheSync(ctx.Done())
	for typ, synced := range syncResult {
		if !synced {
			logger.Warn("CRD informer cache not synced", zap.String("type", typ.String()))
		} else {
			logger.Info("CRD informer cache synced", zap.String("type", typ.String()))
		}
	}

	// Start operator
	go func() {
		if err := operator.Run(ctx, 2); err != nil {
			logger.Fatal("Operator failed", zap.Error(err))
		}
	}()

	// Start cleanup controller
	go func() {
		if err := cleanupController.Start(ctx); err != nil && err != context.Canceled {
			logger.Error("Cleanup controller failed", zap.Error(err))
		}
	}()

	// Start HTTP server
	go func() {
		if err := httpServer.Start(ctx); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	logger.Info("Manager is running")

	// Wait for termination signal
	<-ctx.Done()
	logger.Info("Shutting down gracefully")

	// Give components time to shut down
	time.Sleep(2 * time.Second)

	logger.Info("Manager stopped")
}

// initLogger initializes the logger
func initLogger(logLevel string) *zap.Logger {
	level := zapcore.InfoLevel
	switch logLevel {
	case "debug":
		level = zapcore.DebugLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := cfg.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	return logger
}

// buildKubeConfig builds Kubernetes config
func buildKubeConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

// startMetricsServer starts the Prometheus metrics server
func startMetricsServer(port int, logger *zap.Logger) {
	http.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%d", port)
	logger.Info("Starting metrics server", zap.String("addr", addr))

	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("Metrics server failed", zap.Error(err))
	}
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, databaseURL string, logger *zap.Logger) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	// Configure pool
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	logger.Info("Database connection established",
		zap.Int32("max_conns", poolConfig.MaxConns),
		zap.Int32("min_conns", poolConfig.MinConns),
	)

	return pool, nil
}

// pgxPoolAdapter adapts pgxpool.Pool to clock.DB interface
type pgxPoolAdapter struct {
	pool *pgxpool.Pool
}

type pgxRowAdapter struct {
	row interface {
		Scan(dest ...any) error
	}
}

func (r *pgxRowAdapter) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

func (a *pgxPoolAdapter) QueryRow(ctx context.Context, sql string, args ...any) clock.Row {
	return &pgxRowAdapter{row: a.pool.QueryRow(ctx, sql, args...)}
}

// zapClockLogger adapts zap.Logger to clock.Logger interface
type zapClockLogger struct {
	logger *zap.Logger
}

func (z *zapClockLogger) Info(msg string, keysAndValues ...any) {
	z.logger.Info(msg, toZapFields(keysAndValues)...)
}

func (z *zapClockLogger) Warn(msg string, keysAndValues ...any) {
	z.logger.Warn(msg, toZapFields(keysAndValues)...)
}

func (z *zapClockLogger) Error(msg string, keysAndValues ...any) {
	z.logger.Error(msg, toZapFields(keysAndValues)...)
}

// toZapFields converts key-value pairs to zap fields
func toZapFields(keysAndValues []any) []zap.Field {
	if len(keysAndValues)%2 != 0 {
		return []zap.Field{zap.Any("args", keysAndValues)}
	}

	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			continue
		}
		fields = append(fields, zap.Any(key, keysAndValues[i+1]))
	}
	return fields
}
