package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialsource"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/deletionwebhook"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	managermetering "github.com/sandbox0-ai/sandbox0/manager/pkg/metering"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaintenance"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templatebuild"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsobjectstore"
	s0template "github.com/sandbox0-ai/sandbox0/pkg/template"
	templmigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	templstorepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
	"go.uber.org/zap"
)

func main() {
	cfg := config.LoadManagerConfig()

	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "manager",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Manager",
		zap.String("version", "v0.1.0"),
		zap.Int("httpPort", cfg.HTTPPort),
		zap.Int("metricsPort", cfg.MetricsPort),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	obsProvider, err := observability.New(observability.ConfigFromEnv("manager", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)
	managerMetrics := obsmetrics.NewManager(obsProvider.MetricsRegistryOrNil())

	if cfg.DatabaseURL == "" {
		logger.Fatal("DATABASE_URL is required")
	}
	pool, err := initDatabase(
		ctx,
		cfg.DatabaseURL,
		cfg.DatabaseMaxConns,
		cfg.DatabaseMinConns,
		true,
		logger,
		obsProvider,
	)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	if err := runTemplateMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run template migrations", zap.Error(err))
	}
	if err := runQuotaMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run quota migrations", zap.Error(err))
	}
	if err := runEgressAuthMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run egress auth migrations", zap.Error(err))
	}
	if cfg.Metering.Enabled {
		if err := meteringoutbox.RunMigrations(ctx, pool, observability.NewMigrateLogger(logger)); err != nil {
			logger.Fatal("Failed to run metering outbox migrations", zap.Error(err))
		}
	}
	if err := runSandboxStoreMigrations(ctx, pool, logger); err != nil {
		logger.Fatal("Failed to run sandbox store migrations", zap.Error(err))
	}
	credentialStore, err := buildCredentialStore(ctx, pool, cfg, logger)
	if err != nil {
		logger.Fatal("Failed to configure credential store", zap.Error(err))
	}

	clk, err := clock.NewPGX(
		ctx,
		pool,
		clock.WithSyncInterval(30*time.Second),
		clock.WithZapLogger(logger),
	)
	if err != nil {
		logger.Fatal("Failed to initialize clock", zap.Error(err))
	}
	defer clk.Close()
	logger.Info("Clock initialized for cross-cluster time synchronization",
		zap.Int64("offset_ms", clk.Offset().Milliseconds()),
		zap.Int64("rtt_ms", clk.LastRTT().Milliseconds()),
	)

	meteringDB, meteringSink, meteringSinkReady, err := initMetering(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize metering backend", zap.Error(err))
	}
	if meteringDB != nil {
		defer meteringDB.Close()
	}

	sandboxStore := sandboxstore.NewPGSandboxStore(pool)
	managerNodeAuthority, err := buildManagerNodeAuthority(cfg, sandboxStore)
	if err != nil {
		logger.Fatal("Failed to configure manager node authority", zap.Error(err))
	}

	sandboxDeletionWebhookDispatcher := deletionwebhook.NewSandboxDeletionWebhookDispatcher(
		deletionwebhook.NewSandboxDeletionWebhookOutbox(pool),
		obsProvider.HTTP.NewClient(httpobs.Config{Timeout: 5 * time.Second}),
		deletionwebhook.SandboxDeletionWebhookDispatcherConfig{},
		logger,
	)
	sandboxDeletionWebhookDispatcher.SetMetrics(managerMetrics)
	go func() {
		if err := sandboxDeletionWebhookDispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Sandbox deletion webhook dispatcher stopped", zap.Error(err))
		}
	}()

	var meteringRepo *meteringoutbox.Repository
	if cfg.Metering.Enabled {
		meteringRepo = meteringoutbox.NewRepository(pool)
	}
	if meteringRepo != nil && meteringSink != nil {
		bootstrapCompleted, err := meteringRepo.ProjectionBootstrapCompleted(ctx)
		if err != nil {
			logger.Fatal("Failed to inspect metering projection bootstrap", zap.Error(err))
		}
		if !meteringSinkReady && !bootstrapCompleted {
			logger.Fatal("ClickHouse must be reachable for the initial metering projection bootstrap")
		}
		if meteringSinkReady {
			bootstrap, err := meteringRepo.BootstrapProjectionStates(ctx, meteringSink)
			if err != nil {
				logger.Fatal("Failed to bootstrap metering projection state", zap.Error(err))
			}
			logger.Info("Metering projection state bootstrapped",
				zap.Int64("sandbox_states", bootstrap.SandboxStates),
				zap.Int64("storage_states", bootstrap.StorageStates),
			)
		} else {
			logger.Warn("Starting with deferred ClickHouse metering delivery; projection bootstrap is already complete")
		}
		projector := meteringoutbox.NewProjector(meteringRepo, meteringSink, meteringoutbox.ProjectorConfig{}, logger)
		projector.SetMetrics(managerMetrics)
		go func() {
			if err := projector.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("Metering outbox projector stopped", zap.Error(err))
			}
		}()
		logger.Info("Metering PostgreSQL outbox projector started")
	}

	objectStoreRequestMeter := startManagerObjectStoreRequestMetering(
		ctx,
		meteringRepo,
		cfg.RegionID,
		cfg.DefaultClusterId,
		logger,
	)
	defer flushObjectStoreRequestMetering(objectStoreRequestMeter, logger)

	networkPolicyService := buildNomadManagerNetworkComponents(logger).policyService
	internalAuth := buildManagerInternalAuth(logger)
	internalTokenGenerator := internalAuth.procdTokenGenerator

	procdHTTPClient := obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.ProcdClientTimeout.Duration})
	procdClient := procdapi.NewProcdClientWithHTTPClient(procdHTTPClient)

	var meteringQuotaUsageStore quota.UsageStore
	if meteringSink != nil {
		meteringQuotaUsageStore = meteringSink
	}
	quotaUsageStore := quota.UsageStore(&managerQuotaUsageStore{
		activeSandboxes: sandboxStore,
		metering:        meteringQuotaUsageStore,
	})
	quotaRepo, err := buildQuotaRepository(ctx, pool, cfg, quotaUsageStore)
	if err != nil {
		logger.Fatal("Invalid quota configuration", zap.Error(err))
	}
	templateStore := templstorepg.NewStore(pool)
	runtimeClasses, err := nomadclaim.LoadRuntimeClassCatalog(cfg.NodeAuthority.Claim.ClassCatalogFile)
	if err != nil {
		logger.Fatal("Failed to load the Nomad runtime class catalog", zap.Error(err))
	}

	rootFSObjectStore, err := buildRootFSObjectStore(cfg, objectStoreRequestMeter)
	if err != nil {
		logger.Fatal("Nomad RootFS object store is unavailable", zap.Error(err))
	}
	rootFSCompositeMaterializer, err := configureRootFSCompositeMaterializer(
		cfg,
		sandboxStore,
		rootFSObjectStore,
	)
	if err != nil {
		logger.Fatal("Nomad RootFS composite materializer is unavailable", zap.Error(err))
	}
	rootFSImportWorker, err := configureRootFSImportWorker(cfg, sandboxStore, rootFSObjectStore)
	if err != nil {
		logger.Fatal("Durable RootFS importer is unavailable", zap.Error(err))
	}
	rootFSImportDiscovery, err := configureRootFSImportDiscovery(
		cfg,
		templateStore,
		sandboxStore,
		runtimeClasses.ArtifactPlatforms(),
	)
	if err != nil {
		logger.Fatal("Template RootFS import discovery is unavailable", zap.Error(err))
	}
	sandboxClaimReconciler, err := configureSandboxClaimReconciler(cfg, sandboxStore)
	if err != nil {
		logger.Fatal("Nomad abandoned sandbox claim reconciler is unavailable", zap.Error(err))
	}

	staticAuth := make([]egressauthservice.StaticAuthConfig, 0, len(cfg.EgressAuthStaticAuth))
	for _, entry := range cfg.EgressAuthStaticAuth {
		staticAuth = append(staticAuth, egressauthservice.StaticAuthConfig{
			AuthRef: entry.AuthRef,
			Headers: entry.Headers,
			TTL:     entry.TTL.Duration,
		})
	}
	egressAuthService := egressauthservice.NewEgressAuthService(egressauthservice.EgressAuthServiceConfig{
		DefaultResolveTTL: cfg.EgressAuthDefaultResolveTTL.Duration,
		StaticAuth:        staticAuth,
	}, credentialStore, logger)
	credentialSourceService := credentialsource.NewCredentialSourceService(credentialStore, logger)

	templateResourcePolicy := s0template.NewResourcePolicy(cfg.TeamTemplateMemoryPerCPU, cfg.SandboxMaxMemory)
	sandboxRuntime, err := buildSandboxRuntime(cfg, sandboxRuntimeBackendDependencies{
		nodeAuthority:   managerNodeAuthority,
		store:           sandboxStore,
		quotaLimits:     quotaRepo,
		templates:       templateStore,
		networkPolicies: networkPolicyService,
		resourcePolicy:  templateResourcePolicy,
		prober:          procdClient,
		tokenGenerator:  internalTokenGenerator,
		observer:        runtimeslotclaim.NewPrometheusObserver(obsProvider.MetricsRegistryOrNil()),
		defaultTTL:      cfg.DefaultSandboxTTL.Duration,
		now:             clk.Now,
		logger:          logger,
		runtimeClasses:  runtimeClasses,
	})
	if err != nil {
		logger.Fatal("Failed to configure Nomad sandbox runtime", zap.Error(err))
	}

	if meteringRepo != nil {
		lifecycleProjector, err := managermetering.NewNomadLifecycleProjector(
			meteringRepo,
			cfg.RegionID,
			cfg.DefaultClusterId,
			managermetering.NomadLifecycleProjectorConfig{},
		)
		if err != nil {
			logger.Fatal("Failed to configure Nomad lifecycle metering", zap.Error(err))
		}
		lifecycleProjector.SetLogger(logger)
		lifecycleProjector.SetMetrics(managerMetrics)
		go func() {
			if err := lifecycleProjector.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("Nomad lifecycle metering projector stopped", zap.Error(err))
			}
		}()
		logger.Info("Nomad lifecycle metering projector started")
	}

	sandboxReader, err := service.NewNomadSandboxReader(sandboxStore)
	if err != nil {
		logger.Fatal("Failed to configure Nomad sandbox query service", zap.Error(err))
	}
	sandboxUpdater, err := service.NewNomadSandboxUpdater(sandboxStore, cfg.DefaultSandboxTTL.Duration, clk.Now)
	if err != nil {
		logger.Fatal("Failed to configure Nomad sandbox mutation service", zap.Error(err))
	}
	sandboxRootFS, err := service.NewNomadSandboxRootFSService(sandboxStore, clk.Now)
	if err != nil {
		logger.Fatal("Failed to configure Nomad sandbox RootFS service", zap.Error(err))
	}
	nomadSandboxNetworkPolicy, err := service.NewNomadSandboxNetworkPolicyService(
		sandboxStore,
		networkPolicyService,
		managerNodeAuthority,
	)
	if err != nil {
		logger.Fatal("Failed to configure Nomad sandbox network policy service", zap.Error(err))
	}

	pressurePauser, ok := sandboxRuntime.(rootfswriterauthority.PressurePauser)
	if !ok {
		logger.Fatal("Nomad sandbox runtime lacks exact RootFS pressure pause authority")
	}
	managerNodeAuthority.SetWriterPressurePauser(pressurePauser)

	sandboxPauseController := service.NewSandboxPauseController(sandboxStore, sandboxRuntime, logger)
	sandboxRuntime.SetPauseEnqueuer(sandboxPauseController)
	hardExpiryTerminator, ok := sandboxRuntime.(service.SandboxHardExpiryTerminator)
	if !ok {
		logger.Fatal("Nomad sandbox runtime lacks exact hard-expiry termination")
	}
	sandboxTTLController, err := service.NewSandboxTTLController(
		sandboxStore,
		sandboxRuntime,
		hardExpiryTerminator,
		service.SandboxTTLControllerConfig{Interval: cfg.CleanupInterval.Duration},
		clk.Now,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to configure Nomad sandbox TTL controller", zap.Error(err))
	}

	forkReconciler, _ := sandboxRuntime.(service.SandboxForkReconciler)
	rebaseReconciler, _ := sandboxRuntime.(service.SandboxRootFSRebaseReconciler)
	rootFSRebaser, _ := sandboxRuntime.(service.SandboxRootFSRebaser)
	var sandboxRootFSController *service.SandboxRootFSController
	if forkReconciler != nil || rebaseReconciler != nil {
		sandboxRootFSController = service.NewSandboxRootFSController(
			sandboxStore,
			forkReconciler,
			rebaseReconciler,
			logger,
		)
	}
	sandboxNetworkMutationController := service.NewSandboxNetworkMutationController(
		sandboxStore,
		nomadSandboxNetworkPolicy,
		logger,
	)
	nomadSandboxNetworkPolicy.SetMutationEnqueuer(sandboxNetworkMutationController)

	var templateBuildWorker *templatebuild.TemplateBuildWorker
	capturer, ok := sandboxRuntime.(templatebuild.Capturer)
	if !ok {
		logger.Fatal("Nomad sandbox runtime cannot capture RootFS generations")
	}
	templateBuildWorker, err = templatebuild.NewTemplateBuildWorker(
		templateStore,
		capturer,
		templatebuild.TemplateBuildWorkerConfig{
			ClusterID: naming.ClusterIDOrDefault(&cfg.DefaultClusterId),
		},
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to configure Nomad block-COW template build worker", zap.Error(err))
	}
	sandboxSourceResolver, err := service.NewNomadSandboxTemplateSourceResolver(
		sandboxStore,
		true,
		clk.Now,
	)
	if err != nil {
		logger.Fatal("Failed to configure Nomad sandbox template source resolver", zap.Error(err))
	}

	publicKey, err := internalauth.LoadEd25519PublicKeyFromFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		logger.Fatal("Failed to load internal auth public key",
			zap.String("path", internalauth.DefaultInternalJWTPublicKeyPath),
			zap.Error(err),
		)
	}
	validatorConfig := internalauth.DefaultValidatorConfig(internalauth.ServiceManager, publicKey)
	validatorConfig.AllowedCallers = internalauth.ManagerAllowedCallers()
	authValidator := internalauth.NewValidator(validatorConfig)
	logger.Info("Internal authentication enabled",
		zap.String("target", internalauth.ServiceManager),
		zap.Strings("allowed_callers", validatorConfig.AllowedCallers),
	)

	httpServer := httpserver.NewServerWithDependencies(httpserver.ServerDependencies{
		SandboxReader:           sandboxReader,
		SandboxUpdater:          sandboxUpdater,
		SandboxNetworkPolicy:    nomadSandboxNetworkPolicy,
		SandboxRootFS:           sandboxRootFS,
		SandboxSourceResolver:   sandboxSourceResolver,
		SandboxClaimer:          sandboxRuntime,
		SandboxTerminator:       sandboxRuntime,
		SandboxPauser:           sandboxRuntime,
		SandboxResumer:          sandboxRuntime,
		SandboxForker:           sandboxRuntime,
		SandboxRootFSRebaser:    rootFSRebaser,
		EgressAuthService:       egressAuthService,
		CredentialSourceService: credentialSourceService,
		PrivateRegistryHosts:    managerPrivateRegistryHosts(cfg.Registry),
		TemplateStore:           templateStore,
		TemplateStoreEnabled:    cfg.TemplateStoreEnabled,
		TemplateResourcePolicy:  templateResourcePolicy,
		QuotaRepository:         quotaRepo,
		AuthValidator:           authValidator,
		Logger:                  logger,
		Port:                    cfg.HTTPPort,
		ObservabilityProvider:   obsProvider,
		PublicRootDomain:        cfg.PublicRootDomain,
		PublicRegionID:          cfg.PublicRegionID,
		ReadinessProbe:          pool.Ping,
	})

	controllers := &managerControllerSet{
		cfg:                              cfg,
		clock:                            clk,
		logger:                           logger,
		sandboxPauseController:           sandboxPauseController,
		sandboxTTLController:             sandboxTTLController,
		sandboxRootFSController:          sandboxRootFSController,
		sandboxNetworkMutationController: sandboxNetworkMutationController,
		templateBuildWorker:              templateBuildWorker,
		sandboxStore:                     sandboxStore,
		rootFSObjectStore:                rootFSObjectStore,
		meteringRepo:                     meteringRepo,
		managerMetrics:                   managerMetrics,
	}

	app := &managerApp{
		ctx:                    ctx,
		cancel:                 cancel,
		logger:                 logger,
		httpServer:             httpServer,
		nodeAuthority:          managerNodeAuthority,
		rootFSMaterializer:     rootFSCompositeMaterializer,
		rootFSImportDiscovery:  rootFSImportDiscovery,
		rootFSImporter:         rootFSImportWorker,
		sandboxClaimReconciler: sandboxClaimReconciler,
		metricsPort:            cfg.MetricsPort,
		startControllers:       controllers.Start,
	}
	app.Run()
}

func managerPrivateRegistryHosts(registry config.RegistryConfig) []string {
	hosts := make([]string, 0, 3)
	seen := make(map[string]struct{}, 3)
	for _, value := range []string{registry.PushRegistry, registry.PullRegistry, registry.InternalRegistry} {
		host := strings.TrimSpace(value)
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		hosts = append(hosts, host)
	}
	return hosts
}

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

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(templmigrations.FS),
		migrate.WithLogger(observability.NewMigrateLogger(logger)),
		migrate.WithSchema("scheduler"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}

	logger.Info("Template migrations completed successfully")
	return nil
}

func runQuotaMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running quota migrations")

	if err := quota.RunMigrations(ctx, pool, observability.NewMigrateLogger(logger)); err != nil {
		return fmt.Errorf("quota migrations: %w", err)
	}

	logger.Info("Quota migrations completed successfully")
	return nil
}

func runEgressAuthMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running egress auth migrations")

	if err := egressauthstore.RunMigrations(ctx, pool, observability.NewMigrateLogger(logger)); err != nil {
		return fmt.Errorf("egress auth migrations: %w", err)
	}

	logger.Info("Egress auth migrations completed successfully")
	return nil
}

func initMetering(ctx context.Context, cfg *config.ManagerConfig, logger *zap.Logger) (*sql.DB, *meteringclickhouse.Repository, bool, error) {
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
		DSN: strings.TrimSpace(ch.DSN),
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
	db, repo, err := meteringclickhouse.Open(connectCtx, openConfig)
	if err != nil {
		deferredDB, deferredRepo, deferredErr := meteringclickhouse.OpenDeferred(openConfig)
		if deferredErr != nil {
			return nil, nil, false, fmt.Errorf("initialize deferred clickhouse metering backend after %v: %w", err, deferredErr)
		}
		logger.Warn("Metering ClickHouse backend is unavailable; delivery will retry from PostgreSQL", zap.Error(err))
		return deferredDB, deferredRepo, false, nil
	}
	logger.Info("Metering ClickHouse backend initialized",
		zap.String("database", ch.Database),
		zap.String("events_table", ch.EventsTable),
		zap.String("windows_table", ch.WindowsTable),
		zap.Bool("schema_migration", !ch.SkipSchemaMigration),
	)
	return db, repo, true, nil
}

func buildQuotaRepository(ctx context.Context, pool *pgxpool.Pool, cfg *config.ManagerConfig, usageStore quota.UsageStore) (*quota.Repository, error) {
	repo := quota.NewRepository(pool)
	if repo == nil {
		return nil, nil
	}
	if err := repo.SyncDefaultPolicies(ctx, "manager_config", defaultTeamQuotaLimits(cfg)); err != nil {
		return nil, err
	}
	if usageStore != nil {
		repo.SetUsageStore(usageStore)
	}
	policyStore, err := quota.NewCachedPolicyStore(ctx, pool, repo, quota.DefaultPolicyCacheTTL)
	if err != nil {
		return nil, err
	}
	repo.SetLimitPolicyStore(policyStore)
	return repo, nil
}

func defaultTeamQuotaLimits(cfg *config.ManagerConfig) []quota.DefaultLimit {
	if cfg == nil || len(cfg.DefaultTeamQuotas) == 0 {
		return nil
	}
	limits := make([]quota.DefaultLimit, 0, len(cfg.DefaultTeamQuotas))
	for _, limit := range cfg.DefaultTeamQuotas {
		intervalMS := limit.IntervalMS
		burstValue := limit.BurstValue
		if quota.KindForDimension(quota.Dimension(limit.Dimension)) == quota.KindRate {
			if intervalMS == 0 {
				intervalMS = int64(time.Second / time.Millisecond)
			}
			if burstValue == 0 {
				burstValue = limit.LimitValue
			}
		}
		limits = append(limits, quota.DefaultLimit{
			Dimension:  quota.Dimension(limit.Dimension),
			LimitValue: limit.LimitValue,
			IntervalMS: intervalMS,
			BurstValue: burstValue,
		})
	}
	return limits
}

func buildRootFSObjectStore(
	cfg *config.ManagerConfig,
	requestObserver objectstore.RequestObserver,
) (objectstore.Store, error) {
	if cfg == nil {
		return nil, nil
	}
	objectStorageCfg := cfg.RootFSObjectStorage
	if strings.TrimSpace(objectStorageCfg.Type) == "" && strings.TrimSpace(objectStorageCfg.Bucket) == "" {
		return nil, nil
	}
	return rootfsobjectstore.Create(objectStorageCfg, requestObserver)
}

type rootFSObjectStoreInspector struct {
	store objectstore.Store
}

func (i rootFSObjectStoreInspector) StatRootFSObject(key string) (sandboxstore.RootFSObjectInfo, error) {
	info, err := i.store.Head(key)
	if err != nil {
		return sandboxstore.RootFSObjectInfo{}, err
	}
	return sandboxstore.RootFSObjectInfo{
		Key:      info.Key,
		Size:     info.Size,
		Modified: info.Modified,
	}, nil
}

func rootFSMaintenanceControllerConfig(cfg *config.ManagerConfig) rootfsmaintenance.Config {
	if cfg == nil {
		return rootfsmaintenance.Config{}
	}
	return rootfsmaintenance.Config{
		Interval:         cfg.RootFSMaintenance.Interval.Duration,
		BatchSize:        cfg.RootFSMaintenance.BatchSize,
		MaxBatchesPerRun: cfg.RootFSMaintenance.MaxBatchesPerRun,
		Workers:          cfg.RootFSMaintenance.Workers,
		DeleteOptions: sandboxstore.DeletePendingRootFSObjectsOptions{
			ClaimTTL:    cfg.RootFSMaintenance.ObjectDeleteClaimTTL.Duration,
			BackoffBase: cfg.RootFSMaintenance.ObjectDeleteBackoffBase.Duration,
			BackoffMax:  cfg.RootFSMaintenance.ObjectDeleteBackoffMax.Duration,
			MaxAttempts: cfg.RootFSMaintenance.ObjectDeleteMaxAttempts,
		},
	}
}

func buildCredentialStore(ctx context.Context, pool *pgxpool.Pool, cfg *config.ManagerConfig, logger *zap.Logger) (*egressauthstore.Repository, error) {
	if cfg == nil {
		cfg = &config.ManagerConfig{}
	}
	storeCfg := cfg.CredentialStore
	if strings.TrimSpace(storeCfg.DefaultStorageKind) == "" {
		storeCfg.DefaultStorageKind = egressauthstore.CredentialSourceStorageKindEncryptedPG
	}

	var codec egressauthstore.SecretCodec
	if storeCfg.EncryptedPG.KeyFile != "" || storeCfg.EncryptedPG.Key != "" {
		key, err := loadCredentialEncryptionKey(storeCfg.EncryptedPG)
		if err != nil {
			return nil, err
		}
		keyID := strings.TrimSpace(storeCfg.EncryptedPG.KeyID)
		if keyID == "" {
			keyID = "default"
		}
		codec, err = egressauthstore.NewAESGCMCodec(keyID, map[string][]byte{keyID: key})
		if err != nil {
			return nil, err
		}
	}

	vaultConfigs := make([]egressauthstore.VaultConnectionConfig, 0, len(storeCfg.Vault.Connections))
	for _, conn := range storeCfg.Vault.Connections {
		vaultConfigs = append(vaultConfigs, egressauthstore.VaultConnectionConfig{
			Name:                conn.Name,
			Provider:            conn.Provider,
			Address:             conn.Address,
			TokenFile:           conn.TokenFile,
			CACertFile:          conn.CACertFile,
			Namespace:           conn.Namespace,
			DefaultMount:        conn.DefaultMount,
			KVVersion:           conn.KVVersion,
			SkipTLSVerify:       conn.SkipTLSVerify,
			AllowedPathPrefixes: conn.AllowedPathPrefixes,
		})
	}
	vaultResolver, err := egressauthstore.NewVaultResolver(vaultConfigs)
	if err != nil {
		return nil, err
	}

	repo := egressauthstore.NewRepository(
		pool,
		egressauthstore.WithDefaultStorageKind(storeCfg.DefaultStorageKind),
		egressauthstore.WithSecretCodec(codec),
		egressauthstore.WithVaultResolver(vaultResolver),
	)
	return repo, nil
}

func loadCredentialEncryptionKey(cfg config.CredentialEncryptedPGConfig) ([]byte, error) {
	if strings.TrimSpace(cfg.KeyFile) != "" {
		data, err := os.ReadFile(cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read credential encryption key file: %w", err)
		}
		return data, nil
	}
	if strings.TrimSpace(cfg.Key) != "" {
		return []byte(cfg.Key), nil
	}
	return nil, fmt.Errorf("credential encrypted_pg key_file or key is required")
}

func runSandboxStoreMigrations(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	logger.Info("Running sandbox store migrations")

	if err := sandboxstore.RunSandboxStoreMigrations(ctx, pool, observability.NewMigrateLogger(logger)); err != nil {
		return fmt.Errorf("sandbox store migrations: %w", err)
	}
	logger.Info("Sandbox store migrations completed successfully")
	return nil
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, databaseURL string, maxConns, minConns int32, requirePrimary bool, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	options := managerDatabaseOptions(databaseURL, maxConns, minConns, requirePrimary)
	options.ConfigModifier = obsProvider.Pgx.ConfigModifier()
	pool, err := dbpool.New(ctx, options)
	if err != nil {
		return nil, err
	}

	logger.Info("Database connection established",
		zap.Int32("max_conns", pool.Config().MaxConns),
		zap.Int32("min_conns", pool.Config().MinConns),
		zap.Bool("require_primary", requirePrimary),
	)

	return pool, nil
}

func managerDatabaseOptions(databaseURL string, maxConns, minConns int32, requirePrimary bool) dbpool.Options {
	return dbpool.Options{
		DatabaseURL:    databaseURL,
		MaxConns:       maxConns,
		MinConns:       minConns,
		Schema:         "scheduler",
		RequirePrimary: requirePrimary,
	}
}
