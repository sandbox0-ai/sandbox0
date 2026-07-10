package main

import (
	"context"
	"database/sql"
	"fmt"
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
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	spmigrations "github.com/sandbox0-ai/sandbox0/storage-proxy/migrations"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/coordinator"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	fsserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsserver"
	httpserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volumelock"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
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

	// Setup zap logger for new components.
	zapLogger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "storage-proxy",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		logrusLogger.WithError(err).Fatal("Failed to create zap logger")
	}
	defer zapLogger.Sync()

	zapLogger.Info("Starting storage-proxy",
		zap.Int("http_port", cfg.HTTPPort),
		zap.String("log_level", cfg.LogLevel),
		zap.String("cache_dir", cfg.CacheDir),
	)

	// Initialize observability provider
	obsProvider, err := observability.New(observability.ConfigFromEnv("storage-proxy", zapLogger))
	if err != nil {
		zapLogger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(context.Background())

	storageProxyMetrics := obsmetrics.NewStorageProxy(obsProvider.MetricsRegistryOrNil())

	// Initialize database connection pool
	var repo *db.Repository
	var meteringRepo *meteringclickhouse.Repository
	var quotaRepo *quota.Repository
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
		if cfg.Metering.Enabled {
			if err := quota.RunMigrations(context.Background(), pool, observability.NewMigrateLogger(zapLogger)); err != nil {
				zapLogger.Fatal("Failed to run quota migrations", zap.Error(err))
			}
		}

		repo = db.NewRepository(pool)
		if cfg.Metering.Enabled {
			quotaRepo = quota.NewRepository(pool)
		}
	} else {
		zapLogger.Warn("DATABASE_URL not set, running without database persistence")
	}
	meteringDB, meteringRepo, err := initMetering(context.Background(), cfg, zapLogger)
	if err != nil {
		zapLogger.Fatal("Failed to initialize metering backend", zap.Error(err))
	}
	if meteringDB != nil {
		defer meteringDB.Close()
	}
	if quotaRepo != nil {
		quotaRepo.SetUsageStore(meteringRepo)
	}

	// Create volume manager
	volMgr := volume.NewManager(logrusLogger, cfg, repo)
	volMgr.SetMetrics(storageProxyMetrics)
	directVolumeFileIdleTTL := buildDirectVolumeFileIdleTTL(cfg)
	directVolumeFileCleanupInterval := buildDirectVolumeFileCleanupInterval(cfg, directVolumeFileIdleTTL)
	var volumeBarrier *volumelock.Locker
	if pool != nil {
		volumeBarrier = volumelock.New(pool)
	}

	// Create watch event hub
	var eventHub *notify.Hub
	var eventBroadcaster notify.Broadcaster
	if cfg.WatchEventsEnabled {
		eventHub = notify.NewHub(logrusLogger, cfg.WatchEventQueueSize)
		eventBroadcaster = notify.NewLocalBroadcaster(eventHub)
	}

	// Create Kubernetes client used by coordinator orphan cleanup.
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

	// Create authenticators based on config.
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
		AllowedCallers:         []string{"cluster-gateway", "manager"},
		ClockSkewTolerance:     5 * time.Second,
		ReplayDetectionEnabled: false, // Disable for high-throughput scenarios
	})

	httpAuthenticator = auth.NewHTTPAuthenticator(validator, zapLogger)

	zapLogger.Info("Using internalauth validator for HTTP authentication")

	fsServer := fsserver.NewFileSystemServer(volMgr, repo, eventHub, eventBroadcaster, logrusLogger, volumeBarrier)
	if sharedClock != nil {
		fsServer.SetNowFunc(sharedClock.Now)
	}

	// Create snapshot manager
	snapshotMgr, err := snapshot.NewManager(repo, volMgr, cfg, logrusLogger, storageProxyMetrics)
	if err != nil {
		zapLogger.Fatal("Failed to initialize snapshot manager", zap.Error(err))
	}
	snapshotMgr.SetMeteringRepository(meteringRepo)
	snapshotMgr.SetQuotaRepository(quotaRepo)
	volMgr.SetStorageObserver(snapshotMgr)
	if eventBroadcaster != nil {
		snapshotMgr.SetEventPublisher(eventBroadcaster)
	}

	storageMeteringCtx, storageMeteringCancel := context.WithCancel(context.Background())
	defer storageMeteringCancel()
	if meteringRepo != nil {
		go runStorageMeteringFlushLoop(storageMeteringCtx, meteringRepo, cfg.RegionID, zapLogger)
	}

	// Set coordinator for snapshot manager (for distributed flush)
	if coord != nil {
		snapshotMgr.SetFlushCoordinator(coord)
	}

	// Create HTTP server
	httpSrv := httpserver.NewServer(logrusLogger, cfg, k8sClient, repo, meteringRepo, cfg.RegionID, httpAuthenticator, snapshotMgr, volumeBarrier, volMgr, fsServer, eventHub)
	httpSrv.SetQuotaRepository(quotaRepo)
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

	httpHandler := httpobs.ServerMiddleware(obsProvider.HTTPServerConfig(nil))(httpSrv)

	httpServer := &http.Server{
		Addr:         httpAddr,
		Handler:      httpHandler,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}
	httpServer.ConnState = httpobs.NewConnStateTracker(obsProvider.HTTPServerConfig(nil)).Wrap(httpServer.ConnState)

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

	directMountCleanupCancel()

	// Shutdown HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		zapLogger.Error("HTTP server shutdown error", zap.Error(err))
	}

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
		ConfigModifier:  obsProvider.Pgx.ConfigModifier(),
	})
	if err != nil {
		return nil, err
	}

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
		schema = "storage_proxy"
	}

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(spmigrations.FS),
		migrate.WithLogger(observability.NewMigrateLogger(logger)),
		migrate.WithSchema(schema),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}

	logger.Info("Database migrations completed successfully")
	return nil
}

func initMetering(ctx context.Context, cfg *config.StorageProxyConfig, logger *zap.Logger) (*sql.DB, *meteringclickhouse.Repository, error) {
	if cfg == nil || !cfg.Metering.Enabled {
		return nil, nil, nil
	}
	ch := cfg.Metering.ClickHouse
	timeout := ch.ConnectTimeout.Duration
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	db, repo, err := meteringclickhouse.Open(connectCtx, meteringclickhouse.OpenConfig{
		DSN: ch.DSN,
		Schema: meteringclickhouse.Config{
			Database:          ch.Database,
			EventsTable:       ch.EventsTable,
			WindowsTable:      ch.WindowsTable,
			WatermarksTable:   ch.WatermarksTable,
			SandboxStateTable: ch.SandboxStateTable,
			StorageStateTable: ch.StorageStateTable,
		},
		Migrate: !ch.SkipSchemaMigration,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("initialize clickhouse metering backend: %w", err)
	}
	logger.Info("Metering ClickHouse backend initialized",
		zap.String("database", ch.Database),
		zap.String("events_table", ch.EventsTable),
		zap.String("windows_table", ch.WindowsTable),
		zap.Bool("schema_migration", !ch.SkipSchemaMigration),
	)
	return db, repo, nil
}

func runStorageMeteringFlushLoop(ctx context.Context, repo *meteringclickhouse.Repository, regionID string, logger *zap.Logger) {
	const (
		flushInterval = time.Minute
		flushBatch    = 500
	)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			before := time.Now().UTC()
			for {
				processed, err := repo.FlushStorageProjectionWindows(ctx, before, flushBatch)
				if err != nil {
					logger.Warn("Failed to flush storage metering windows", zap.Error(err))
					break
				}
				if err := repo.UpsertProducerWatermark(ctx, meteringpkg.ProducerStorage, regionID, before); err != nil {
					logger.Warn("Failed to update storage metering watermark", zap.Error(err))
					break
				}
				if processed < flushBatch {
					break
				}
			}
		}
	}
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
	return volCtx, nil
}

func (a *volumeProviderAdapter) ListVolumes() []string {
	return a.volMgr.ListVolumes()
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
