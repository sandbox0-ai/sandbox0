// Package runtime assembles and runs the storage-proxy service. It is shared by
// the standalone storage-proxy command and processes embedding the service.
package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
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
	"k8s.io/client-go/kubernetes"
)

const defaultShutdownTimeout = 30 * time.Second

// Options provides process-owned dependencies to a storage-proxy Runtime.
// The runtime owns resources it creates, but never closes the supplied loggers,
// observability provider, or Kubernetes client.
type Options struct {
	Config        *config.StorageProxyConfig
	Logger        *zap.Logger
	LogrusLogger  *logrus.Logger
	Observability *observability.Provider
	K8sClient     kubernetes.Interface
	Listen        func(network, address string) (net.Listener, error)
}

// Runtime owns one storage-proxy HTTP endpoint and all of its background
// workers. Runtime is safe to embed in another process.
type Runtime struct {
	cfg           *config.StorageProxyConfig
	logger        *zap.Logger
	logrusLogger  *logrus.Logger
	observability *observability.Provider
	k8sClient     kubernetes.Interface
	listen        func(network, address string) (net.Listener, error)

	pool               *pgxpool.Pool
	sharedClock        *clock.Clock
	meteringDB         *sql.DB
	meteringRepo       *meteringoutbox.Repository
	volMgr             *volume.Manager
	coordinator        *coordinator.Coordinator
	httpHandler        http.Handler
	httpServer         *http.Server
	listener           net.Listener
	runCancel          context.CancelFunc
	coordinatorStarted bool

	mu           sync.RWMutex
	started      bool
	stopped      bool
	workers      sync.WaitGroup
	internalHTTP sync.WaitGroup
	shutdownOnce sync.Once
	shutdownErr  error
	errors       chan error
	done         chan struct{}
}

// New initializes the storage subsystem without accepting traffic. Call Start
// after the parent process has finished assembling its own dependencies.
func New(ctx context.Context, opts Options) (_ *Runtime, retErr error) {
	if opts.Config == nil {
		return nil, fmt.Errorf("storage-proxy config is required")
	}
	if err := opts.Config.Validate(); err != nil {
		return nil, fmt.Errorf("validate storage-proxy config: %w", err)
	}
	if opts.Logger == nil {
		return nil, fmt.Errorf("storage-proxy zap logger is required")
	}
	if opts.LogrusLogger == nil {
		return nil, fmt.Errorf("storage-proxy logrus logger is required")
	}
	listen := opts.Listen
	if listen == nil {
		listen = net.Listen
	}

	r := &Runtime{
		cfg:           opts.Config,
		logger:        opts.Logger,
		logrusLogger:  opts.LogrusLogger,
		observability: opts.Observability,
		k8sClient:     opts.K8sClient,
		listen:        listen,
		errors:        make(chan error, 1),
		done:          make(chan struct{}),
	}
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cancel()
			_ = r.closeResources(cleanupCtx)
		}
	}()

	storageProxyMetrics := obsmetrics.NewStorageProxy(metricsRegisterer(opts.Observability))

	var repo *db.Repository
	var meteringRepo *meteringoutbox.Repository
	var meteringSink *meteringclickhouse.Repository
	var quotaRepo *quota.Repository
	if r.cfg.DatabaseURL != "" {
		pool, err := initDatabase(ctx, r.cfg.DatabaseURL, r.cfg, r.logger, r.observability)
		if err != nil {
			return nil, fmt.Errorf("connect storage-proxy database: %w", err)
		}
		r.pool = pool

		sharedClock, err := clock.New(ctx, &pgxPoolClockAdapter{pool: pool},
			clock.WithLogger(&zapClockLogger{logger: r.logger}),
		)
		if err != nil {
			return nil, fmt.Errorf("initialize storage-proxy shared clock: %w", err)
		}
		r.sharedClock = sharedClock

		if err := runMigrations(ctx, pool, r.cfg.DatabaseSchema, r.logger); err != nil {
			return nil, fmt.Errorf("run storage-proxy migrations: %w", err)
		}
		if r.cfg.Metering.Enabled {
			if err := quota.RunMigrations(ctx, pool, observability.NewMigrateLogger(r.logger)); err != nil {
				return nil, fmt.Errorf("run storage quota migrations: %w", err)
			}
			if err := meteringoutbox.RunMigrations(ctx, pool, observability.NewMigrateLogger(r.logger)); err != nil {
				return nil, fmt.Errorf("run storage metering outbox migrations: %w", err)
			}
		}

		repo = db.NewRepository(pool)
		if r.cfg.Metering.Enabled {
			quotaRepo = quota.NewRepository(pool)
			meteringRepo = meteringoutbox.NewRepository(pool)
		}
	} else {
		r.logger.Warn("DATABASE_URL not set, running storage-proxy without database persistence")
	}
	if r.cfg.Metering.Enabled && r.pool == nil {
		return nil, fmt.Errorf("DATABASE_URL is required when storage metering is enabled")
	}

	meteringDB, sink, sinkReady, err := initMetering(ctx, r.cfg, r.logger)
	if err != nil {
		return nil, fmt.Errorf("initialize storage metering backend: %w", err)
	}
	r.meteringDB = meteringDB
	meteringSink = sink
	if meteringRepo != nil && meteringSink != nil {
		bootstrapCompleted, err := meteringRepo.ProjectionBootstrapCompleted(ctx)
		if err != nil {
			return nil, fmt.Errorf("inspect storage metering projection bootstrap: %w", err)
		}
		if !sinkReady && !bootstrapCompleted {
			return nil, fmt.Errorf("ClickHouse must be reachable for the initial metering projection bootstrap")
		}
		if sinkReady {
			bootstrap, err := meteringRepo.BootstrapProjectionStates(ctx, meteringSink)
			if err != nil {
				return nil, fmt.Errorf("bootstrap storage metering projection state: %w", err)
			}
			r.logger.Info("Storage metering projection state bootstrapped",
				zap.Int64("sandbox_states", bootstrap.SandboxStates),
				zap.Int64("storage_states", bootstrap.StorageStates),
			)
		} else {
			r.logger.Warn("Starting storage runtime with deferred ClickHouse delivery; projection bootstrap is complete")
		}
	}
	if quotaRepo != nil {
		quotaRepo.SetUsageStore(meteringSink)
	}

	r.volMgr = volume.NewManager(r.logrusLogger, r.cfg, repo)
	r.volMgr.SetMetrics(storageProxyMetrics)
	var volumeBarrier *volumelock.Locker
	if r.pool != nil {
		volumeBarrier = volumelock.New(r.pool)
	}

	var eventHub *notify.Hub
	var eventBroadcaster notify.Broadcaster
	if r.cfg.WatchEventsEnabled {
		eventHub = notify.NewHub(r.logrusLogger, r.cfg.WatchEventQueueSize)
		eventBroadcaster = notify.NewLocalBroadcaster(eventHub)
	}

	if r.k8sClient == nil {
		client, err := k8s.NewClientWithObservability(r.cfg.KubeconfigPath, r.observability)
		if err != nil {
			r.logger.Warn("Failed to create storage-proxy Kubernetes client", zap.Error(err))
		} else {
			r.k8sClient = client
		}
	}

	if r.pool != nil && repo != nil {
		volProvider := &volumeProviderAdapter{volMgr: r.volMgr}
		r.coordinator = coordinator.NewCoordinator(r.pool, repo, volProvider, eventHub, r.k8sClient, r.cfg, r.logrusLogger, storageProxyMetrics)
		r.volMgr.SetMountRegistrar(r.coordinator)
		if eventHub != nil {
			eventBroadcaster = r.coordinator
		}
	}

	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load storage-proxy internal auth public key from %s: %w", internalauth.DefaultInternalJWTPublicKeyPath, err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:                 internalauth.ServiceManagerStorage,
		AdditionalTargets:      []string{internalauth.ServiceStorageProxy},
		PublicKey:              publicKey,
		AllowedCallers:         []string{internalauth.ServiceClusterGateway, internalauth.ServiceManager},
		ClockSkewTolerance:     5 * time.Second,
		ReplayDetectionEnabled: false,
	})
	httpAuthenticator := auth.NewHTTPAuthenticator(validator, r.logger)

	fsServer := fsserver.NewFileSystemServer(r.volMgr, repo, eventHub, eventBroadcaster, r.logrusLogger, volumeBarrier)
	if r.sharedClock != nil {
		fsServer.SetNowFunc(r.sharedClock.Now)
	}

	snapshotMgr, err := snapshot.NewManager(repo, r.volMgr, r.cfg, r.logrusLogger, storageProxyMetrics)
	if err != nil {
		return nil, fmt.Errorf("initialize snapshot manager: %w", err)
	}
	snapshotMgr.SetMeteringRepository(meteringRepo)
	snapshotMgr.SetQuotaRepository(quotaRepo)
	r.volMgr.SetStorageObserver(snapshotMgr)
	if eventBroadcaster != nil {
		snapshotMgr.SetEventPublisher(eventBroadcaster)
	}
	if r.coordinator != nil {
		snapshotMgr.SetFlushCoordinator(r.coordinator)
	}

	storageHTTP := httpserver.NewServer(r.logrusLogger, r.cfg, r.k8sClient, repo, meteringRepo, r.cfg.RegionID, httpAuthenticator, snapshotMgr, volumeBarrier, r.volMgr, fsServer, eventHub)
	storageHTTP.SetQuotaRepository(quotaRepo)
	r.httpHandler = storageHTTP
	if r.observability != nil {
		r.httpHandler = httpobs.ServerMiddleware(r.observability.HTTPServerConfig(nil))(r.httpHandler)
	}
	r.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", r.cfg.HTTPAddr, r.cfg.HTTPPort),
		Handler:      r.httpHandler,
		ReadTimeout:  durationOrDefault(r.cfg.HTTPReadTimeout, 15*time.Second),
		WriteTimeout: durationOrDefault(r.cfg.HTTPWriteTimeout, 15*time.Second),
		IdleTimeout:  durationOrDefault(r.cfg.HTTPIdleTimeout, 60*time.Second),
	}
	if r.observability != nil {
		r.httpServer.ConnState = httpobs.NewConnStateTracker(r.observability.HTTPServerConfig(nil)).Wrap(r.httpServer.ConnState)
	}
	r.meteringRepo = meteringRepo

	return r, nil
}

// Start begins accepting HTTP traffic and runs background workers until ctx is
// canceled or Shutdown is called.
func (r *Runtime) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return fmt.Errorf("storage-proxy runtime is stopped")
	}
	if r.started {
		r.mu.Unlock()
		return nil
	}
	listener, err := r.listen("tcp", r.httpServer.Addr)
	if err != nil {
		r.mu.Unlock()
		return fmt.Errorf("listen on storage-proxy address %s: %w", r.httpServer.Addr, err)
	}
	r.listener = listener
	runCtx, runCancel := context.WithCancel(ctx)
	r.runCancel = runCancel

	if r.coordinator != nil {
		if err := r.coordinator.Start(runCtx); err != nil {
			runCancel()
			_ = listener.Close()
			r.stopped = true
			r.mu.Unlock()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cancel()
			_ = r.Shutdown(shutdownCtx)
			return fmt.Errorf("start storage-proxy coordinator: %w", err)
		}
		r.coordinatorStarted = true
		r.logger.Info("Distributed storage coordinator started", zap.String("instance_id", r.coordinator.GetInstanceID()))
	}
	if r.volMgr != nil {
		r.workers.Add(1)
	}
	if r.meteringRepo != nil {
		r.workers.Add(1)
	}
	r.started = true
	r.mu.Unlock()

	directVolumeFileIdleTTL := buildDirectVolumeFileIdleTTL(r.cfg)
	directVolumeFileCleanupInterval := buildDirectVolumeFileCleanupInterval(r.cfg, directVolumeFileIdleTTL)
	if r.volMgr != nil {
		go func() {
			defer r.workers.Done()
			r.runDirectMountCleanup(runCtx, directVolumeFileIdleTTL, directVolumeFileCleanupInterval)
		}()
	}
	if r.meteringRepo != nil {
		go func() {
			defer r.workers.Done()
			runStorageMeteringFlushLoop(runCtx, r.meteringRepo, r.cfg.RegionID, r.logger)
		}()
	}

	go func() {
		err := r.httpServer.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			wrapped := fmt.Errorf("serve storage-proxy HTTP: %w", err)
			select {
			case r.errors <- wrapped:
			default:
			}
			shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cancel()
			_ = r.Shutdown(shutdownCtx)
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cancel()
			_ = r.Shutdown(shutdownCtx)
		case <-r.done:
		}
	}()

	r.logger.Info("Storage-proxy runtime started", zap.String("address", listener.Addr().String()))
	return nil
}

// Handler returns the authenticated storage-proxy HTTP handler. It is useful
// for trusted in-process callers that still need the exact wire semantics.
func (r *Runtime) Handler() http.Handler {
	if r == nil {
		return nil
	}
	return r.httpHandler
}

// InternalHTTPClient returns an in-process HTTP client backed by Handler. The
// client is intended for manager-owned volume calls and does not open sockets.
func (r *Runtime) InternalHTTPClient() *http.Client {
	return &http.Client{Transport: &handlerRoundTripper{runtime: r}}
}

// Errors reports fatal serving errors. Normal context cancellation and server
// shutdown are not reported.
func (r *Runtime) Errors() <-chan error {
	return r.errors
}

// Done is closed after all runtime-owned resources have been released.
func (r *Runtime) Done() <-chan struct{} {
	return r.done
}

// Address returns the bound address after Start.
func (r *Runtime) Address() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.listener == nil {
		return ""
	}
	return r.listener.Addr().String()
}

// Shutdown drains HTTP requests, stops background workers, unmounts local
// volumes, and closes runtime-owned database resources.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.shutdownOnce.Do(func() {
		r.shutdownErr = r.closeResources(ctx)
		close(r.done)
	})
	return r.shutdownErr
}

func (r *Runtime) closeResources(ctx context.Context) error {
	var errs []error
	r.mu.Lock()
	r.stopped = true
	runCancel := r.runCancel
	httpServer := r.httpServer
	listener := r.listener
	coord := r.coordinator
	coordinatorStarted := r.coordinatorStarted
	volMgr := r.volMgr
	sharedClock := r.sharedClock
	meteringDB := r.meteringDB
	pool := r.pool
	r.mu.Unlock()

	if runCancel != nil {
		runCancel()
	}
	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("shutdown HTTP server: %w", err))
		}
	}
	if listener != nil {
		_ = listener.Close()
	}
	if err := r.waitForInternalHTTP(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := r.waitForWorkers(ctx); err != nil {
		errs = append(errs, err)
	}
	if volMgr != nil {
		for _, volumeID := range volMgr.ListVolumes() {
			for _, sessionID := range volMgr.ListMountSessions(volumeID) {
				if err := volMgr.UnmountVolume(ctx, volumeID, sessionID); err != nil {
					errs = append(errs, fmt.Errorf("unmount volume %s session %s: %w", volumeID, sessionID, err))
				}
			}
		}
	}
	if coord != nil && coordinatorStarted {
		if err := coord.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop coordinator: %w", err))
		}
	}
	if sharedClock != nil {
		sharedClock.Close()
	}
	if meteringDB != nil {
		if err := meteringDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close metering database: %w", err))
		}
	}
	if pool != nil {
		pool.Close()
	}
	return errors.Join(errs...)
}

func (r *Runtime) waitForInternalHTTP(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.internalHTTP.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for storage-proxy internal HTTP requests: %w", ctx.Err())
	}
}

func (r *Runtime) waitForWorkers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for storage-proxy workers: %w", ctx.Err())
	}
}

func (r *Runtime) runDirectMountCleanup(ctx context.Context, idleTTL, cleanupInterval time.Duration) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if errs := r.volMgr.CleanupIdleDirectVolumeFileMounts(ctx, idleTTL); len(errs) > 0 {
				r.logger.Warn("Idle direct volume file cleanup reported errors", zap.Int("error_count", len(errs)))
			}
		}
	}
}

func metricsRegisterer(provider *observability.Provider) prometheus.Registerer {
	if provider == nil {
		return nil
	}
	return provider.MetricsRegistryOrNil()
}

func initDatabase(ctx context.Context, databaseURL string, cfg *config.StorageProxyConfig, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}
	schema := cfg.DatabaseSchema
	if schema == "" {
		schema = "storage_proxy"
	}
	options := dbpool.Options{
		DatabaseURL:     databaseURL,
		MaxConns:        int32(cfg.DatabaseMaxConns),
		MinConns:        int32(cfg.DatabaseMinConns),
		DefaultMaxConns: 30,
		DefaultMinConns: 5,
		Schema:          schema,
	}
	if obsProvider != nil {
		options.ConfigModifier = obsProvider.Pgx.ConfigModifier()
	}
	pool, err := dbpool.New(ctx, options)
	if err != nil {
		return nil, err
	}
	logger.Info("Storage-proxy database connection established",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
	)
	return pool, nil
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, logger *zap.Logger) error {
	if schema == "" {
		schema = "storage_proxy"
	}
	return migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(spmigrations.FS),
		migrate.WithLogger(observability.NewMigrateLogger(logger)),
		migrate.WithSchema(schema),
	)
}

func initMetering(ctx context.Context, cfg *config.StorageProxyConfig, logger *zap.Logger) (*sql.DB, *meteringclickhouse.Repository, bool, error) {
	if cfg == nil || !cfg.Metering.Enabled {
		return nil, nil, false, nil
	}
	ch := cfg.Metering.ClickHouse
	timeout := ch.ConnectTimeout.Duration
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	openConfig := meteringclickhouse.OpenConfig{
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
	}
	database, repo, err := meteringclickhouse.Open(connectCtx, openConfig)
	if err != nil {
		deferredDB, deferredRepo, deferredErr := meteringclickhouse.OpenDeferred(openConfig)
		if deferredErr != nil {
			return nil, nil, false, fmt.Errorf("initialize deferred ClickHouse metering backend after %v: %w", err, deferredErr)
		}
		logger.Warn("Metering ClickHouse backend is unavailable; delivery will retry from PostgreSQL", zap.Error(err))
		return deferredDB, deferredRepo, false, nil
	}
	return database, repo, true, nil
}

func runStorageMeteringFlushLoop(ctx context.Context, repo *meteringoutbox.Repository, regionID string, logger *zap.Logger) {
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

func durationOrDefault(raw string, fallback time.Duration) time.Duration {
	parsed, _ := time.ParseDuration(raw)
	if parsed <= 0 {
		return fallback
	}
	return parsed
}

func buildDirectVolumeFileIdleTTL(cfg *config.StorageProxyConfig) time.Duration {
	return durationOrDefault(cfg.DirectVolumeFileIdleTTL, 30*time.Second)
}

func buildDirectVolumeFileCleanupInterval(cfg *config.StorageProxyConfig, idleTTL time.Duration) time.Duration {
	cleanupInterval := durationOrDefault(cfg.CleanupInterval, 15*time.Second)
	if idleTTL > 0 && cleanupInterval > idleTTL {
		cleanupInterval = idleTTL
	}
	if cleanupInterval < time.Second {
		cleanupInterval = time.Second
	}
	return cleanupInterval
}

type pgxPoolClockAdapter struct{ pool *pgxpool.Pool }

func (a *pgxPoolClockAdapter) QueryRow(ctx context.Context, query string, args ...any) clock.Row {
	return &pgxClockRowAdapter{row: a.pool.QueryRow(ctx, query, args...)}
}

type pgxClockRowAdapter struct {
	row interface{ Scan(dest ...any) error }
}

func (r *pgxClockRowAdapter) Scan(dest ...any) error { return r.row.Scan(dest...) }

type zapClockLogger struct{ logger *zap.Logger }

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
		if ok {
			fields = append(fields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	return fields
}

type volumeProviderAdapter struct{ volMgr *volume.Manager }

func (a *volumeProviderAdapter) GetVolume(volumeID string) (coordinator.VolumeContext, error) {
	return a.volMgr.GetVolume(volumeID)
}
func (a *volumeProviderAdapter) ListVolumes() []string { return a.volMgr.ListVolumes() }

type handlerRoundTripper struct{ runtime *Runtime }

func (t *handlerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.runtime == nil || t.runtime.Handler() == nil {
		return nil, fmt.Errorf("storage-proxy in-process handler is not configured")
	}
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	t.runtime.mu.Lock()
	if !t.runtime.started || t.runtime.stopped {
		t.runtime.mu.Unlock()
		return nil, fmt.Errorf("storage-proxy runtime is not accepting internal requests")
	}
	t.runtime.internalHTTP.Add(1)
	t.runtime.mu.Unlock()
	defer t.runtime.internalHTTP.Done()

	handlerRequest := req.Clone(req.Context())
	recorder := &responseRecorder{header: make(http.Header), status: http.StatusOK}
	t.runtime.Handler().ServeHTTP(recorder, handlerRequest)
	body := recorder.body.Bytes()
	return &http.Response{
		StatusCode:    recorder.status,
		Status:        fmt.Sprintf("%d %s", recorder.status, http.StatusText(recorder.status)),
		Header:        recorder.header.Clone(),
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       handlerRequest,
	}, nil
}

type responseRecorder struct {
	header      http.Header
	body        bytes.Buffer
	status      int
	wroteHeader bool
}

func (w *responseRecorder) Header() http.Header { return w.header }
func (w *responseRecorder) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
}
func (w *responseRecorder) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(data)
}
func (w *responseRecorder) Flush() {}
