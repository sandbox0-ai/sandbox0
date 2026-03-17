package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/generated/informers/externalversions"
	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	managermetering "github.com/sandbox0-ai/sandbox0/manager/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	registryprovider "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	templmigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	templreconciler "github.com/sandbox0-ai/sandbox0/pkg/template/reconciler"
	templstorepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
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

	// Initialize observability provider
	obsProvider, err := observability.New(observability.Config{
		ServiceName: "manager",
		Logger:      logger,
		TraceExporter: observability.TraceExporterConfig{
			Type:     os.Getenv("OTEL_EXPORTER_TYPE"),
			Endpoint: os.Getenv("OTEL_EXPORTER_ENDPOINT"),
		},
	})
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)

	managerMetrics := obsmetrics.NewManager(obsProvider.MetricsRegistryOrNil())

	// Create Kubernetes client
	k8sConfig, err := buildKubeConfig(cfg.KubeConfig)
	if err != nil {
		logger.Fatal("Failed to build Kubernetes config", zap.Error(err))
	}

	// Wrap K8s config with observability
	obsProvider.K8s.WrapConfig(k8sConfig)

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

	// Initialize database (required for template store)
	if cfg.DatabaseURL == "" {
		logger.Fatal("DATABASE_URL is required for template store")
	}

	pool, err := initDatabase(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConns, cfg.DatabaseMinConns, logger, obsProvider)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	if err := runTemplateMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run template migrations", zap.Error(err))
	}
	if err := runMeteringMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run metering migrations", zap.Error(err))
	}
	if err := runEgressAuthMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run egress auth migrations", zap.Error(err))
	}

	// Initialize clock for cross-cluster time synchronization
	var clk *clock.Clock
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

	// Create informers
	informerFactory := informers.NewSharedInformerFactory(k8sClient, cfg.ResyncPeriod.Duration)
	podInformer := informerFactory.Core().V1().Pods()
	nodeInformer := informerFactory.Core().V1().Nodes().Informer()
	secretInformer := informerFactory.Core().V1().Secrets().Informer()
	_ = informerFactory.Core().V1().Namespaces().Informer()
	replicaSetInformer := informerFactory.Apps().V1().ReplicaSets().Informer()

	// Create CRD informer factory using generated clientset
	crdInformerFactory := externalversions.NewSharedInformerFactory(
		crdClient,
		cfg.ResyncPeriod.Duration,
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
		podInformer.Informer(),
		replicaSetInformer,
		secretInformer,
		templateInformer,
		recorder,
		clk,
		logger,
		managerMetrics,
		cfg.Autoscaler,
	)
	if pool != nil {
		operator.SetTemplateStatsPublisher(controller.NewPGTemplateStatsPublisher(pool, cfg.DefaultClusterId, clk, logger))
	}

	// Create listers
	podLister := informerFactory.Core().V1().Pods().Lister()
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	secretLister := informerFactory.Core().V1().Secrets().Lister()
	namespaceLister := informerFactory.Core().V1().Namespaces().Lister()

	sandboxIndex := service.NewSandboxIndex()
	podInformer.Informer().AddEventHandler(sandboxIndex.ResourceEventHandler())
	meteringRepo := metering.NewRepository(pool)
	lifecycleProjector := managermetering.NewLifecycleProjector(managermetering.NewStore(meteringRepo), cfg.RegionID, cfg.DefaultClusterId)
	lifecycleProjector.SetLogger(logger)
	lifecycleProjector.SetMetrics(managerMetrics)
	podInformer.Informer().AddEventHandler(lifecycleProjector.ResourceEventHandler())

	// Create network policy service for building policy annotations
	networkPolicyService := service.NewNetworkPolicyService(logger)

	networkProviderName := strings.TrimSpace(strings.ToLower(cfg.NetworkPolicyProvider))
	networkProvider := network.NewNoopProvider()
	switch networkProviderName {
	case "", "noop":
		logger.Info("Network provider set to noop")
	case "netd":
		networkProvider = network.NewNetdProvider(podInformer, podLister, network.NetdProviderConfig{
			ApplyTimeout: cfg.NetdPolicyApplyTimeout.Duration,
			PollInterval: cfg.NetdPolicyApplyPollInterval.Duration,
		}, logger)
		logger.Info("Network provider set to netd",
			zap.Duration("applyTimeout", cfg.NetdPolicyApplyTimeout.Duration),
			zap.Duration("pollInterval", cfg.NetdPolicyApplyPollInterval.Duration),
		)
	default:
		logger.Warn("Unknown network policy provider, falling back to noop",
			zap.String("provider", cfg.NetworkPolicyProvider),
		)
	}

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

	// Parse ratios
	pauseMemoryBufferRatio, err := strconv.ParseFloat(cfg.PauseMemoryBufferRatio, 64)
	if err != nil {
		logger.Warn("Failed to parse PauseMemoryBufferRatio, using default 1.1", zap.String("value", cfg.PauseMemoryBufferRatio), zap.Error(err))
		pauseMemoryBufferRatio = 1.1
	}

	// Create services
	cfgForSandbox := service.SandboxServiceConfig{
		DefaultTTL:             cfg.DefaultSandboxTTL.Duration,
		PauseMinMemoryRequest:  cfg.PauseMinMemoryRequest,
		PauseMinMemoryLimit:    cfg.PauseMinMemoryLimit,
		PauseMemoryBufferRatio: pauseMemoryBufferRatio,
		PauseMinCPU:            cfg.PauseMinCPU,
		ProcdPort:              cfg.ProcdConfig.HTTPPort,
		ProcdClientTimeout:     cfg.ProcdClientTimeout.Duration,
		ProcdInitTimeout:       cfg.ProcdInitTimeout.Duration,
	}

	sandboxService := service.NewSandboxService(
		k8sClient,
		podLister,
		sandboxIndex,
		secretLister,
		operator.GetTemplateLister(),
		networkPolicyService,
		networkProvider,
		internalTokenGenerator,
		procdTokenGenerator,
		clk,
		cfgForSandbox,
		logger,
		managerMetrics,
	)
	sandboxService.SetCredentialStore(egressauth.NewRepository(pool))

	templateService := service.NewTemplateService(
		k8sClient,
		crdClient,
		operator.GetTemplateLister(),
		namespaceLister,
		networkProvider,
		cfg.Registry,
		logger,
	)

	registryProvider, err := registryprovider.NewProvider(cfg.Registry, secretLister, logger)
	if err != nil {
		logger.Warn("Registry provider disabled", zap.Error(err))
	}
	registryService := service.NewRegistryService(registryProvider, logger)
	var templateStore *templstorepg.Store
	var templateReconciler *templreconciler.SingleClusterReconciler
	if cfg.TemplateStoreEnabled {
		templateStore = templstorepg.NewStore(pool)
		templateApplier := service.NewTemplateApplier(templateService)
		reconcileInterval := cfg.ResyncPeriod.Duration
		if reconcileInterval == 0 {
			reconcileInterval = 30 * time.Second
		}
		templateReconciler = templreconciler.NewSingleClusterReconciler(
			templateStore,
			templateApplier,
			cfg.DefaultClusterId,
			reconcileInterval,
			clk,
			logger,
		)
		go templateReconciler.Start(ctx)
	} else {
		logger.Info("Template store disabled; manager will apply templates directly")
	}

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
		sandboxService,
		logger,
		cfg.CleanupInterval.Duration,
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
		registryService,
		templateStore,
		templateReconciler,
		cfg.TemplateStoreEnabled,
		clusterService,
		authValidator,
		logger,
		cfg.HTTPPort,
		obsProvider,
		cfg.PublicRootDomain,
		cfg.PublicRegionID,
	)

	// Start metrics server
	go startMetricsServer(cfg.MetricsPort, logger)

	// Start informers
	logger.Info("Starting informers")
	informerFactory.Start(ctx.Done())
	crdInformerFactory.Start(ctx.Done())

	// Wait for cache sync
	logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.Informer().HasSynced, nodeInformer.HasSynced, secretInformer.HasSynced, replicaSetInformer.HasSynced, templateInformer.HasSynced) {
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

func runTemplateMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running template migrations")

	migrateLogger := &zapMigrateLogger{logger: logger}
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(templmigrations.FS),
		migrate.WithLogger(migrateLogger),
		migrate.WithSchema("sched"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}

	logger.Info("Template migrations completed successfully")
	return nil
}

func runMeteringMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running metering migrations")

	migrateLogger := &zapMigrateLogger{logger: logger}
	if err := metering.RunMigrations(ctx, pool, migrateLogger); err != nil {
		return fmt.Errorf("metering migrations: %w", err)
	}

	logger.Info("Metering migrations completed successfully")
	return nil
}

func runEgressAuthMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running egress auth migrations")

	migrateLogger := &zapMigrateLogger{logger: logger}
	if err := egressauth.RunMigrations(ctx, pool, migrateLogger); err != nil {
		return fmt.Errorf("egress auth migrations: %w", err)
	}

	logger.Info("Egress auth migrations completed successfully")
	return nil
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, databaseURL string, maxConns, minConns int32, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: databaseURL,
		MaxConns:    maxConns,
		MinConns:    minConns,
		Schema:      "sched",
	})
	if err != nil {
		return nil, err
	}

	// Wrap pool with observability
	obsProvider.Pgx.WrapPool(pool)

	logger.Info("Database connection established",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)

	return pool, nil
}

// zapMigrateLogger adapts zap.Logger to migrate.Logger interface.
type zapMigrateLogger struct {
	logger *zap.Logger
}

func (z *zapMigrateLogger) Printf(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapMigrateLogger) Fatalf(format string, args ...any) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
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
