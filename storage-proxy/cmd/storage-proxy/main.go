package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/pkg/dbpool"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/k8s"
	"github.com/sandbox0-ai/infra/pkg/migrate"
	"github.com/sandbox0-ai/infra/pkg/observability"
	spmigrations "github.com/sandbox0-ai/infra/storage-proxy/migrations"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/coordinator"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
	grpcserver "github.com/sandbox0-ai/infra/storage-proxy/pkg/grpc"
	httpserver "github.com/sandbox0-ai/infra/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/juicefs"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/volume"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/watcher"
	pb "github.com/sandbox0-ai/infra/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Setup logger (logrus for compatibility)
	logrusLogger := logrus.New()
	logrusLogger.SetFormatter(&logrus.JSONFormatter{})
	logrusLogger.SetOutput(os.Stdout)

	// Load configuration
	cfg := config.LoadStorageProxyConfig()

	if err := cfg.Validate(); err != nil {
		logrusLogger.WithError(err).Fatal("Invalid configuration")
	}

	// Set log level
	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		logrusLogger.WithError(err).Warn("Invalid log level, using info")
		level = logrus.InfoLevel
	}
	logrusLogger.SetLevel(level)

	// Setup zap logger for new components
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	if cfg.LogLevel == "debug" {
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}
	zapLogger, err := zapConfig.Build()
	if err != nil {
		logrusLogger.WithError(err).Fatal("Failed to create zap logger")
	}
	defer zapLogger.Sync()

	zapLogger.Info("Starting storage-proxy",
		zap.Int("grpc_port", cfg.GRPCPort),
		zap.Int("http_port", cfg.HTTPPort),
		zap.String("log_level", cfg.LogLevel),
		zap.String("cache_dir", cfg.CacheDir),
	)

	// Initialize observability provider
	obsProvider, err := observability.New(observability.Config{
		ServiceName: "storage-proxy",
		Logger:      zapLogger,
		TraceExporter: observability.TraceExporterConfig{
			Type:     os.Getenv("OTEL_EXPORTER_TYPE"),
			Endpoint: os.Getenv("OTEL_EXPORTER_ENDPOINT"),
		},
	})
	if err != nil {
		zapLogger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(context.Background())

	// Initialize database connection pool
	var repo *db.Repository
	var pool *pgxpool.Pool
	if cfg.DatabaseURL != "" {
		pool, err = initDatabase(context.Background(), cfg.DatabaseURL, cfg, zapLogger, obsProvider)
		if err != nil {
			zapLogger.Fatal("Failed to connect to database", zap.Error(err))
		}
		defer pool.Close()

		// Run database migrations
		if err := runMigrations(context.Background(), pool, cfg.DatabaseSchema, zapLogger); err != nil {
			zapLogger.Fatal("Failed to run database migrations", zap.Error(err))
		}

		repo = db.NewRepository(pool)
	} else {
		zapLogger.Warn("DATABASE_URL not set, running without database persistence")
	}

	// Initialize JuiceFS filesystem if not already initialized
	if err := initializeJuiceFS(cfg, zapLogger); err != nil {
		zapLogger.Fatal("Failed to initialize JuiceFS", zap.Error(err))
	}

	// Create volume manager
	volMgr := volume.NewManager(logrusLogger, cfg)

	// Create watch event hub
	var eventHub *notify.Hub
	var eventBroadcaster notify.Broadcaster
	if cfg.WatchEventsEnabled {
		eventHub = notify.NewHub(logrusLogger, cfg.WatchEventQueueSize)
		eventBroadcaster = notify.NewLocalBroadcaster(eventHub)
	}

	// Create and start coordinator for distributed flush coordination
	var coord *coordinator.Coordinator
	if pool != nil && repo != nil {
		// Create volume provider adapter for coordinator
		volProvider := &volumeProviderAdapter{volMgr: volMgr}
		coord = coordinator.NewCoordinator(pool, repo, volProvider, eventHub, cfg, logrusLogger)

		// Set coordinator as mount registrar for volume manager
		volMgr.SetMountRegistrar(coord)

		// Start coordinator
		if err := coord.Start(context.Background()); err != nil {
			zapLogger.Fatal("Failed to start coordinator", zap.Error(err))
		}
		defer coord.Stop(context.Background())

		zapLogger.Info("Distributed flush coordinator started",
			zap.String("instance_id", coord.GetInstanceID()),
		)

		if eventHub != nil {
			eventBroadcaster = coord
		}
	}

	// Create Kubernetes client for pod watching
	k8sClient, err := k8s.NewClientWithObservability(cfg.KubeconfigPath, obsProvider)
	if err != nil {
		zapLogger.Warn("Failed to create Kubernetes client, pod watcher disabled",
			zap.Error(err),
		)
	} else {
		// Create and start sandbox watcher
		podWatcher := watcher.NewWatcher(
			k8sClient,
			"", // Watch all namespaces
			10*time.Minute,
			zapLogger,
		)

		// Set up delete handler to auto-unmount volumes
		podWatcher.SetPodDeleteHandler(func(info *watcher.SandboxInfo) {
			zapLogger.Info("Sandbox pod deleted, unmounting volumes",
				zap.String("sandbox_id", info.SandboxID),
			)
			if errs := volMgr.UnmountSandboxVolumes(context.Background(), info.SandboxID); errs != nil {
				zapLogger.Error("Errors unmounting sandbox volumes",
					zap.String("sandbox_id", info.SandboxID),
					zap.Int("error_count", len(errs)),
				)
			}
		})

		// Start watcher in background
		go func() {
			if err := podWatcher.Start(context.Background()); err != nil {
				zapLogger.Error("Watcher failed", zap.Error(err))
			}
		}()

		zapLogger.Info("Sandbox watcher started")
	}

	// Create authenticator based on config
	var grpcInterceptor grpc.UnaryServerInterceptor
	var httpAuthenticator *auth.HTTPAuthenticator
	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		zapLogger.Fatal("Failed to load internal auth public key",
			zap.String("path", internalauth.DefaultInternalJWTPublicKeyPath),
			zap.Error(err),
		)
	}

	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:                 "storage-proxy",
		PublicKey:              publicKey,
		AllowedCallers:         []string{"internal-gateway", "manager", "procd"},
		ClockSkewTolerance:     5 * time.Second,
		ReplayDetectionEnabled: false, // Disable for high-throughput scenarios
	})

	authenticator := auth.NewGRPCAuthenticator(validator, zapLogger)
	grpcInterceptor = authenticator.UnaryInterceptor()

	httpAuthenticator = auth.NewHTTPAuthenticator(validator, zapLogger)

	zapLogger.Info("Using internalauth validator for gRPC and HTTP authentication")

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcInterceptor),
	)

	// Register FileSystem service
	fsServer := grpcserver.NewFileSystemServer(volMgr, eventHub, eventBroadcaster, logrusLogger)
	pb.RegisterFileSystemServer(grpcServer, fsServer)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for grpcurl
	reflection.Register(grpcServer)

	// Start gRPC server
	grpcAddr := fmt.Sprintf("%s:%d", cfg.GRPCAddr, cfg.GRPCPort)
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logrusLogger.WithError(err).Fatal("Failed to listen for gRPC")
	}

	go func() {
		logrusLogger.WithField("address", grpcAddr).Info("Starting gRPC server")
		if err := grpcServer.Serve(grpcListener); err != nil {
			logrusLogger.WithError(err).Fatal("Failed to serve gRPC")
		}
	}()

	// Create snapshot manager
	snapshotMgr := snapshot.NewManager(repo, volMgr, cfg, logrusLogger)

	// Set coordinator for snapshot manager (for distributed flush)
	if coord != nil {
		snapshotMgr.SetFlushCoordinator(coord)
	}

	// Create HTTP server
	httpSrv := httpserver.NewServer(logrusLogger, repo, httpAuthenticator, snapshotMgr)
	httpAddr := fmt.Sprintf("%s:%d", cfg.HTTPAddr, cfg.HTTPPort)

	readTimeout, _ := time.ParseDuration(cfg.HTTPReadTimeout)
	if readTimeout == 0 {
		readTimeout = 15 * time.Second
	}
	writeTimeout, _ := time.ParseDuration(cfg.HTTPWriteTimeout)
	if writeTimeout == 0 {
		writeTimeout = 15 * time.Second
	}
	idleTimeout, _ := time.ParseDuration(cfg.HTTPIdleTimeout)
	if idleTimeout == 0 {
		idleTimeout = 60 * time.Second
	}

	httpServer := &http.Server{
		Addr:         httpAddr,
		Handler:      httpSrv,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	go func() {
		logrusLogger.WithField("address", httpAddr).Info("Starting HTTP server")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrusLogger.WithError(err).Fatal("Failed to serve HTTP")
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	zapLogger.Info("Shutting down gracefully...")

	// Shutdown HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		zapLogger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// Stop gRPC server
	grpcServer.GracefulStop()

	// Unmount all volumes
	for _, volumeID := range volMgr.ListVolumes() {
		zapLogger.Info("Unmounting volume", zap.String("volume_id", volumeID))
		if err := volMgr.UnmountVolume(context.Background(), volumeID); err != nil {
			zapLogger.Error("Failed to unmount volume",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
		}
	}

	zapLogger.Info("Shutdown complete")
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, databaseURL string, cfg *config.StorageProxyConfig, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}

	schema := cfg.DatabaseSchema
	if schema == "" {
		schema = "sp"
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     databaseURL,
		MaxConns:        int32(cfg.DatabaseMaxConns),
		MinConns:        int32(cfg.DatabaseMinConns),
		DefaultMaxConns: 30,
		DefaultMinConns: 5,
		Schema:          schema,
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

// runMigrations runs database migrations on startup
func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, logger *zap.Logger) error {
	logger.Info("Running database migrations")

	if schema == "" {
		schema = "sp"
	}

	// Create a migration logger that writes to zap
	migrateLogger := &zapLogger{logger: logger}

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(spmigrations.FS),
		migrate.WithLogger(migrateLogger),
		migrate.WithSchema(schema),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}

	logger.Info("Database migrations completed successfully")
	return nil
}

// zapLogger adapts zap.Logger to migrate.Logger interface
type zapLogger struct {
	logger *zap.Logger
}

func (z *zapLogger) Printf(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapLogger) Fatalf(format string, args ...any) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
}

// volumeProviderAdapter adapts volume.Manager to coordinator.VolumeProvider interface
type volumeProviderAdapter struct {
	volMgr *volume.Manager
}

func (a *volumeProviderAdapter) GetVolume(volumeID string) (coordinator.VolumeContext, error) {
	volCtx, err := a.volMgr.GetVolume(volumeID)
	if err != nil {
		return nil, err
	}
	return &volumeContextAdapter{vfs: volCtx.VFS}, nil
}

func (a *volumeProviderAdapter) ListVolumes() []string {
	return a.volMgr.ListVolumes()
}

// volumeContextAdapter adapts VFS to coordinator.VolumeContext interface
type volumeContextAdapter struct {
	vfs interface{ FlushAll(string) error }
}

func (a *volumeContextAdapter) FlushAll(path string) error {
	return a.vfs.FlushAll(path)
}

// initializeJuiceFS initializes the JuiceFS filesystem if not already initialized
func initializeJuiceFS(cfg *config.StorageProxyConfig, logger *zap.Logger) error {
	logger.Info("Checking JuiceFS initialization status")

	// Skip if essential config is missing
	if cfg.MetaURL == "" || cfg.S3Bucket == "" {
		logger.Warn("JuiceFS config incomplete, skipping initialization",
			zap.String("meta_url", cfg.MetaURL),
			zap.String("s3_bucket", cfg.S3Bucket))
		return nil
	}

	initConfig := &juicefs.InitConfig{
		Name:           cfg.JuiceFSName,
		MetaURL:        cfg.MetaURL,
		S3Bucket:       cfg.S3Bucket,
		S3Region:       cfg.S3Region,
		S3Endpoint:     cfg.S3Endpoint,
		S3AccessKey:    cfg.S3AccessKey,
		S3SecretKey:    cfg.S3SecretKey,
		S3SessionToken: cfg.S3SessionToken,
		BlockSize:      cfg.JuiceFSBlockSize,
		Compression:    cfg.JuiceFSCompression,
		TrashDays:      cfg.JuiceFSTrashDays,
		MetaRetries:    cfg.JuiceFSMetaRetries,
	}

	initializer := juicefs.NewInitializer(initConfig, logger)

	if err := initializer.Initialize(); err != nil {
		return fmt.Errorf("initialize juicefs: %w", err)
	}

	return nil
}
