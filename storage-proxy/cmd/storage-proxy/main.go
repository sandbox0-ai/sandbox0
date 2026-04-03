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
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	spmigrations "github.com/sandbox0-ai/sandbox0/storage-proxy/migrations"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/coordinator"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	grpcserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/grpc"
	httpserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/juicefs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volumelock"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/watcher"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
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

	storageProxyMetrics := obsmetrics.NewStorageProxy(obsProvider.MetricsRegistryOrNil())

	// Initialize database connection pool
	var repo *db.Repository
	var meteringRepo *metering.Repository
	var pool *pgxpool.Pool
	var sharedClock *clock.Clock
	if cfg.DatabaseURL != "" {
		pool, err = initDatabase(context.Background(), cfg.DatabaseURL, cfg, zapLogger, obsProvider)
		if err != nil {
			zapLogger.Fatal("Failed to connect to database", zap.Error(err))
		}
		defer pool.Close()

		sharedClock, err = clock.New(context.Background(), &pgxPoolClockAdapter{pool: pool},
			clock.WithLogger(&zapClockLogger{logger: zapLogger}),
		)
		if err != nil {
			zapLogger.Fatal("Failed to initialize shared clock", zap.Error(err))
		}
		defer sharedClock.Close()

		// Run database migrations
		if err := runMigrations(context.Background(), pool, cfg.DatabaseSchema, zapLogger); err != nil {
			zapLogger.Fatal("Failed to run database migrations", zap.Error(err))
		}
		if err := metering.RunMigrations(context.Background(), pool, &zapLoggerAdapter{logger: zapLogger}); err != nil {
			zapLogger.Fatal("Failed to run metering migrations", zap.Error(err))
		}

		repo = db.NewRepository(pool)
		meteringRepo = metering.NewRepository(pool)
	} else {
		zapLogger.Warn("DATABASE_URL not set, running without database persistence")
	}

	// Initialize JuiceFS filesystem if not already initialized
	if err := initializeJuiceFS(cfg, zapLogger); err != nil {
		zapLogger.Fatal("Failed to initialize JuiceFS", zap.Error(err))
	}

	// Create volume manager
	volMgr := volume.NewManager(logrusLogger, cfg)
	directVolumeFileIdleTTL := buildDirectVolumeFileIdleTTL(cfg)
	directVolumeFileCleanupInterval := buildDirectVolumeFileCleanupInterval(cfg, directVolumeFileIdleTTL)
	var syncSvc *volsync.Service
	var syncMaintenance *volsync.Maintenance
	var volumeBarrier *volumelock.Locker
	syncMaintenanceCfg := buildSyncMaintenanceConfig(cfg)
	if pool != nil {
		volumeBarrier = volumelock.New(pool)
	}
	if repo != nil {
		syncSvc = volsync.NewService(repo, logrusLogger)
		if sharedClock != nil {
			syncSvc.SetNowFunc(sharedClock.Now)
		}
		syncSvc.SetMetrics(storageProxyMetrics)
		syncSvc.SetConflictArtifactWriter(volsync.NewConflictArtifactWriter(volMgr, logrusLogger))
		syncSvc.SetReplicaChangeApplier(volsync.NewVolumeChangeApplier(volMgr, logrusLogger))
		replayPayloadStore, err := volsync.NewS3ReplayPayloadStore(cfg)
		if err != nil {
			zapLogger.Fatal("Failed to initialize replay payload store", zap.Error(err))
		}
		syncSvc.SetReplayPayloadStore(replayPayloadStore)
		syncSvc.SetVolumeMutationBarrier(volumeBarrier)
		syncMaintenance = volsync.NewMaintenance(repo, syncSvc, logrusLogger, syncMaintenanceCfg)
		if sharedClock != nil {
			syncMaintenance.SetNowFunc(sharedClock.Now)
		}
		syncMaintenance.SetMetrics(storageProxyMetrics)
	}

	var syncMaintenanceCancel context.CancelFunc
	if syncMaintenance != nil && syncMaintenance.Enabled() {
		var maintenanceCtx context.Context
		maintenanceCtx, syncMaintenanceCancel = context.WithCancel(context.Background())
		go syncMaintenance.Run(maintenanceCtx)
		zapLogger.Info("Volume sync maintenance started",
			zap.Duration("compaction_interval", syncMaintenanceCfg.CompactionInterval),
			zap.Int64("journal_retain_entries", syncMaintenanceCfg.JournalRetainEntries),
			zap.Duration("request_retention", syncMaintenanceCfg.RequestRetention),
		)
	}

	// Create watch event hub
	var eventHub *notify.Hub
	var eventBroadcaster notify.Broadcaster
	if cfg.WatchEventsEnabled {
		eventHub = notify.NewHub(logrusLogger, cfg.WatchEventQueueSize)
		eventBroadcaster = notify.NewLocalBroadcaster(eventHub)
	}

	// Create Kubernetes client (used by coordinator orphan cleanup and pod watcher)
	k8sClient, err := k8s.NewClientWithObservability(cfg.KubeconfigPath, obsProvider)
	if err != nil {
		zapLogger.Warn("Failed to create Kubernetes client",
			zap.Error(err),
		)
		k8sClient = nil
	}

	// Create and start coordinator for distributed flush coordination
	var coord *coordinator.Coordinator
	if pool != nil && repo != nil {
		// Create volume provider adapter for coordinator
		volProvider := &volumeProviderAdapter{volMgr: volMgr}
		coord = coordinator.NewCoordinator(pool, repo, volProvider, eventHub, k8sClient, cfg, logrusLogger, storageProxyMetrics)

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

	directMountCleanupCtx, directMountCleanupCancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(directVolumeFileCleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-directMountCleanupCtx.Done():
				return
			case <-ticker.C:
				if errs := volMgr.CleanupIdleDirectVolumeFileMounts(context.Background(), directVolumeFileIdleTTL); len(errs) > 0 {
					zapLogger.Warn("Idle direct volume file cleanup reported errors",
						zap.Int("error_count", len(errs)),
					)
				}
			}
		}
	}()
	zapLogger.Info("Direct volume file idle cleanup started",
		zap.Duration("idle_ttl", directVolumeFileIdleTTL),
		zap.Duration("cleanup_interval", directVolumeFileCleanupInterval),
	)

	// Create and start pod watcher
	if k8sClient != nil {
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
	} else {
		zapLogger.Warn("Pod watcher disabled because Kubernetes client is unavailable")
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
		AllowedCallers:         []string{"cluster-gateway", "manager", "procd"},
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
	fsServer := grpcserver.NewFileSystemServer(volMgr, repo, eventHub, eventBroadcaster, logrusLogger, syncSvc, volumeBarrier)
	if sharedClock != nil {
		fsServer.SetNowFunc(sharedClock.Now)
	}
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
	snapshotMgr, err := snapshot.NewManager(repo, volMgr, cfg, logrusLogger, storageProxyMetrics)
	if err != nil {
		zapLogger.Fatal("Failed to initialize snapshot manager", zap.Error(err))
	}
	snapshotMgr.SetMeteringRepository(meteringRepo)
	if eventBroadcaster != nil {
		snapshotMgr.SetEventPublisher(eventBroadcaster)
	}

	// Set coordinator for snapshot manager (for distributed flush)
	if coord != nil {
		snapshotMgr.SetFlushCoordinator(coord)
	}

	// Create HTTP server
	httpSrv := httpserver.NewServer(logrusLogger, repo, meteringRepo, cfg.RegionID, httpAuthenticator, snapshotMgr, syncSvc, volumeBarrier, volMgr, fsServer, eventHub)
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

	httpHandler := httpobs.ServerMiddleware(httpobs.ServerConfig{
		Tracer: obsProvider.Tracer(),
	})(httpSrv)

	httpServer := &http.Server{
		Addr:         httpAddr,
		Handler:      httpHandler,
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

	if syncMaintenanceCancel != nil {
		syncMaintenanceCancel()
	}
	directMountCleanupCancel()

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
		for _, sessionID := range volMgr.ListMountSessions(volumeID) {
			if err := volMgr.UnmountVolume(context.Background(), volumeID, sessionID); err != nil {
				zapLogger.Error("Failed to unmount volume",
					zap.String("volume_id", volumeID),
					zap.Error(err),
				)
			}
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
		schema = "storage_proxy"
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

type zapLoggerAdapter struct {
	logger *zap.Logger
}

func (z *zapLoggerAdapter) Printf(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *zapLoggerAdapter) Fatalf(format string, args ...any) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
}

// runMigrations runs database migrations on startup
func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, logger *zap.Logger) error {
	logger.Info("Running database migrations")

	if schema == "" {
		schema = "storage_proxy"
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

type pgxPoolClockAdapter struct {
	pool *pgxpool.Pool
}

type pgxClockRowAdapter struct {
	row interface {
		Scan(dest ...any) error
	}
}

func (r *pgxClockRowAdapter) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

func (a *pgxPoolClockAdapter) QueryRow(ctx context.Context, sql string, args ...any) clock.Row {
	return &pgxClockRowAdapter{row: a.pool.QueryRow(ctx, sql, args...)}
}

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
		Name:                 cfg.JuiceFSName,
		MetaURL:              cfg.MetaURL,
		S3Bucket:             cfg.S3Bucket,
		S3Region:             cfg.S3Region,
		S3Endpoint:           cfg.S3Endpoint,
		S3AccessKey:          cfg.S3AccessKey,
		S3SecretKey:          cfg.S3SecretKey,
		S3SessionToken:       cfg.S3SessionToken,
		BlockSize:            cfg.JuiceFSBlockSize,
		Compression:          cfg.JuiceFSCompression,
		TrashDays:            cfg.JuiceFSTrashDays,
		MetaRetries:          cfg.JuiceFSMetaRetries,
		EncryptionEnabled:    cfg.JuiceFSEncryptionEnabled,
		EncryptionKeyPath:    cfg.JuiceFSEncryptionKeyPath,
		EncryptionPassphrase: cfg.JuiceFSEncryptionPassphrase,
		EncryptionAlgo:       cfg.JuiceFSEncryptionAlgo,
	}

	initializer := juicefs.NewInitializer(initConfig, logger)

	if err := initializer.Initialize(); err != nil {
		return fmt.Errorf("initialize juicefs: %w", err)
	}

	return nil
}

func buildSyncMaintenanceConfig(cfg *config.StorageProxyConfig) volsync.MaintenanceConfig {
	compactionInterval, _ := time.ParseDuration(cfg.SyncCompactionInterval)
	if compactionInterval == 0 {
		compactionInterval = 10 * time.Minute
	}

	journalRetainEntries := cfg.SyncJournalRetainEntries
	if journalRetainEntries == 0 {
		journalRetainEntries = 10000
	}

	requestRetention, _ := time.ParseDuration(cfg.SyncRequestRetention)
	if requestRetention == 0 {
		requestRetention = 24 * time.Hour
	}

	return volsync.MaintenanceConfig{
		CompactionInterval:   compactionInterval,
		JournalRetainEntries: journalRetainEntries,
		RequestRetention:     requestRetention,
	}
}

func buildDirectVolumeFileIdleTTL(cfg *config.StorageProxyConfig) time.Duration {
	idleTTL, _ := time.ParseDuration(cfg.DirectVolumeFileIdleTTL)
	if idleTTL <= 0 {
		idleTTL = 30 * time.Second
	}
	return idleTTL
}

func buildDirectVolumeFileCleanupInterval(cfg *config.StorageProxyConfig, idleTTL time.Duration) time.Duration {
	cleanupInterval, _ := time.ParseDuration(cfg.CleanupInterval)
	if cleanupInterval <= 0 {
		cleanupInterval = 15 * time.Second
	}
	if idleTTL > 0 && cleanupInterval > idleTTL {
		cleanupInterval = idleTTL
	}
	if cleanupInterval < time.Second {
		cleanupInterval = time.Second
	}
	return cleanupInterval
}
