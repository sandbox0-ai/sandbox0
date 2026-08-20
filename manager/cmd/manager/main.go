package main

import (
	"context"
	"database/sql"
	"errors"
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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/clusterservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialsource"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/deletionwebhook"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyvolumemigrations"
	managermetering "github.com/sandbox0-ai/sandbox0/manager/pkg/metering"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/registryservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaintenance"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templatebuild"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateservice"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	s0k8s "github.com/sandbox0-ai/sandbox0/pkg/k8s"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	registryprovider "github.com/sandbox0-ai/sandbox0/pkg/registry"
	s0template "github.com/sandbox0-ai/sandbox0/pkg/template"
	templmigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	templreconciler "github.com/sandbox0-ai/sandbox0/pkg/template/reconciler"
	templstorepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
	"go.uber.org/zap"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
)

func main() {

	// Load configuration
	cfg := config.LoadManagerConfig()

	// Initialize logger
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

	// Create context that cancels on signal
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	templateReconcilerQuiesceSignals := make(chan os.Signal, 1)
	signal.Notify(templateReconcilerQuiesceSignals, syscall.SIGUSR1)
	defer signal.Stop(templateReconcilerQuiesceSignals)

	// Initialize observability provider
	obsProvider, err := observability.New(observability.ConfigFromEnv("manager", logger))
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer obsProvider.Shutdown(ctx)

	managerMetrics := obsmetrics.NewManager(obsProvider.MetricsRegistryOrNil())

	kubernetesClients, err := buildManagerKubernetesClients(cfg, obsProvider, managerMetrics, logger)
	if err != nil {
		logger.Fatal("Failed to initialize Kubernetes clients", zap.Error(err))
	}
	k8sClient := kubernetesClients.client
	hotClaimK8sClient := kubernetesClients.hotClaimClient
	crdClient := kubernetesClients.crdClient

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

	// Initialize clock for cross-cluster time synchronization
	var clk *clock.Clock
	clk, err = clock.NewPGX(ctx, pool,
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

	informerRuntime, err := buildManagerInformerRuntime(
		kubernetesClients,
		cfg,
		pool,
		clk,
		managerMetrics,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to initialize Kubernetes informers", zap.Error(err))
	}
	informerFactory := informerRuntime.factory
	crdInformerFactory := informerRuntime.crdFactory
	podInformer := informerRuntime.podInformer
	podLister := informerRuntime.podLister
	nodeLister := informerRuntime.nodeLister
	secretLister := informerRuntime.secretLister
	namespaceLister := informerRuntime.namespaceLister
	serviceAccountLister := informerRuntime.serviceAccountLister
	networkPolicyLister := informerRuntime.networkPolicyLister
	operator := informerRuntime.operator
	recorder := informerRuntime.recorder
	sandboxIndex := informerRuntime.sandboxIndex
	teardownCoordinator := informerRuntime.teardownCoordinator
	autoscalerAnnotationKeys := informerRuntime.autoscalerAnnotationKeys
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
	if meteringRepo != nil {
		lifecycleProjector := managermetering.NewLifecycleProjector(managermetering.NewStore(meteringRepo), cfg.RegionID, cfg.DefaultClusterId)
		lifecycleProjector.SetLogger(logger)
		lifecycleProjector.SetMetrics(managerMetrics)
		lifecycleProjector.SetRuntimePauseLookup(func(ctx context.Context, info managermetering.RuntimeDeletionInfo) (bool, error) {
			record, err := sandboxStore.GetSandbox(ctx, info.SandboxID)
			if err != nil || record == nil {
				return false, err
			}
			return service.SandboxRecordDeletionIsRuntimeOnly(record, info.Namespace, info.PodName, info.RuntimeGeneration), nil
		})
		podInformer.Informer().AddEventHandler(lifecycleProjector.ResourceEventHandler())
	}

	networkComponents := buildManagerNetworkComponents(
		cfg,
		k8sClient,
		podInformer,
		podLister,
		networkPolicyLister,
		logger,
	)
	networkPolicyService := networkComponents.policyService
	templateNamespacePolicy := networkComponents.namespacePolicy
	networkProvider := networkComponents.provider

	internalAuth := buildManagerInternalAuth(logger)
	internalAuthGen := internalAuth.generator
	internalTokenGenerator := internalAuth.procdTokenGenerator

	// Parse ratios
	pauseMemoryBufferRatio, err := strconv.ParseFloat(cfg.PauseMemoryBufferRatio, 64)
	if err != nil {
		logger.Warn("Failed to parse PauseMemoryBufferRatio, using default 1.1", zap.String("value", cfg.PauseMemoryBufferRatio), zap.Error(err))
		pauseMemoryBufferRatio = 1.1
	}

	// Create services
	procdHTTPClient := obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.ProcdClientTimeout.Duration})
	cfgForSandbox := service.SandboxServiceConfig{
		ClusterID:                           naming.ClusterIDOrDefault(&cfg.DefaultClusterId),
		DefaultTTL:                          cfg.DefaultSandboxTTL.Duration,
		SandboxMemoryPerCPU:                 cfg.TeamTemplateMemoryPerCPU,
		SandboxMaxMemory:                    cfg.SandboxMaxMemory,
		PauseMinMemoryRequest:               cfg.PauseMinMemoryRequest,
		PauseMinMemoryLimit:                 cfg.PauseMinMemoryLimit,
		PauseMemoryBufferRatio:              pauseMemoryBufferRatio,
		PauseMinCPU:                         cfg.PauseMinCPU,
		CtldEnabled:                         cfg.CtldEnabled,
		CtldPort:                            cfg.CtldPort,
		CtldClientTimeout:                   cfg.CtldClientTimeout.Duration,
		CtldHTTPClient:                      obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.CtldClientTimeout.Duration}),
		ProcdPort:                           cfg.ProcdConfig.HTTPPort,
		ProcdClientTimeout:                  cfg.ProcdClientTimeout.Duration,
		ProcdHTTPClient:                     procdHTTPClient,
		RuntimeReadyTimeout:                 cfg.RuntimeReadyTimeout.Duration,
		AllowColdStartWithoutReadyDataPlane: cfg.AllowColdStartWithoutReadyDataPlane,
		PreferredNodeSelector:               cfg.SandboxPodPlacement.PreferredNodeSelector,
		RootFSSquashDisabled:                cfg.RootFSMaintenance.SquashDisabled,
		RootFSSquashMaxChainDepth:           cfg.RootFSMaintenance.SquashMaxChainDepth,
		RootFSSquashMaxChainBytes:           cfg.RootFSMaintenance.SquashMaxChainBytes,
		PublicRootDomain:                    cfg.PublicRootDomain,
		PublicRegionID:                      cfg.PublicRegionID,
		AutoscalerSafeToEvictAnnotationKeys: autoscalerAnnotationKeys,
	}
	procdClient := procdapi.NewProcdClientWithHTTPClient(procdHTTPClient)

	var quotaUsageStore quota.UsageStore
	if meteringSink != nil {
		quotaUsageStore = meteringSink
	}
	quotaRepo, err := buildQuotaRepository(ctx, pool, cfg, quotaUsageStore)
	if err != nil {
		logger.Fatal("Invalid quota configuration", zap.Error(err))
	}
	hotClaimReservationController := service.NewHotClaimReservationController(
		k8sClient,
		podLister,
		sandboxStore,
		logger,
	)
	rootFSObjectStore, rootFSObjectStoreErr := buildRootFSObjectStore(cfg, objectStoreRequestMeter)
	if rootFSObjectStoreErr != nil {
		logger.Warn("Rootfs object cleanup disabled; object store is not configured", zap.Error(rootFSObjectStoreErr))
	}
	var rootFSCompositeMaterializerErr error
	var rootFSCompositeMaterializer *rootfsmaterializer.Worker
	if rootFSObjectStoreErr == nil {
		rootFSCompositeMaterializer, rootFSCompositeMaterializerErr = configureRootFSCompositeMaterializer(
			cfg, sandboxStore, rootFSObjectStore,
		)
	} else {
		rootFSCompositeMaterializerErr = rootFSObjectStoreErr
	}
	if rootFSCompositeMaterializerErr != nil {
		if cfg.SandboxRuntimeBackend == config.SandboxRuntimeBackendNomad {
			logger.Fatal("Nomad RootFS composite materializer is unavailable", zap.Error(rootFSCompositeMaterializerErr))
		}
		logger.Warn("Rootfs composite materializer disabled", zap.Error(rootFSCompositeMaterializerErr))
	}
	sandboxClaimReconciler, err := configureSandboxClaimReconciler(cfg, sandboxStore)
	if err != nil {
		logger.Fatal("Nomad abandoned sandbox claim reconciler is unavailable", zap.Error(err))
	}
	sandboxService := service.NewSandboxServiceWithDependencies(service.SandboxServiceDependencies{
		K8sClient:                   k8sClient,
		HotClaimK8sClient:           hotClaimK8sClient,
		PodLister:                   podLister,
		NodeLister:                  nodeLister,
		SandboxIndex:                sandboxIndex,
		SecretLister:                secretLister,
		TemplateLister:              operator.GetTemplateLister(),
		NetworkPolicyService:        networkPolicyService,
		NetworkProvider:             networkProvider,
		InternalTokenGenerator:      internalTokenGenerator,
		Clock:                       clk,
		Config:                      cfgForSandbox,
		Logger:                      logger,
		Metrics:                     managerMetrics,
		ProcdClient:                 procdClient,
		HotClaimReservationEnqueuer: hotClaimReservationController,
		CredentialStore:             credentialStore,
		QuotaStore:                  quotaRepo,
		SandboxStore:                sandboxStore,
		RootFSObjectDeleter:         rootFSObjectStore,
	})
	sandboxService.SetTemplateImageBuildAvailable(false)
	podInformer.Informer().AddEventHandler(sandboxService.PodEventHandler())
	podInformer.Informer().AddEventHandler(hotClaimReservationController.ResourceEventHandler())
	sandboxLifecycleController := service.NewSandboxLifecycleController(k8sClient, podLister, sandboxService, logger)
	sandboxLifecycleController.SetMetrics(managerMetrics)
	podInformer.Informer().AddEventHandler(sandboxLifecycleController.ResourceEventHandler())
	sandboxCrashLogCollector := service.NewSandboxCrashLogCollector(k8sClient, logger, managerMetrics)
	podInformer.Informer().AddEventHandler(sandboxCrashLogCollector.ResourceEventHandler())
	sandboxCrashRecoveryController := service.NewSandboxCrashRecoveryController(k8sClient, podLister, sandboxService, logger)
	podInformer.Informer().AddEventHandler(sandboxCrashRecoveryController.ResourceEventHandler())
	sandboxRuntimeReconciler := service.NewSandboxRuntimeReconciler(
		naming.ClusterIDOrDefault(&cfg.DefaultClusterId),
		sandboxStore,
		podLister,
		sandboxService,
		logger,
	)
	podInformer.Informer().AddEventHandler(sandboxRuntimeReconciler.ResourceEventHandler())
	sandboxLogWorker := buildSandboxObservabilityLogWorker(cfg, internalAuthGen, obsProvider, logger)
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

	templateService := templateservice.New(templateservice.Dependencies{
		KubernetesClient: k8sClient,
		CRDClient:        crdClient,
		Templates:        operator.GetTemplateLister(),
		Namespaces:       namespaceLister,
		Pods:             podLister,
		Secrets:          secretLister,
		ServiceAccounts:  serviceAccountLister,
		Network:          networkProvider,
		Registry:         cfg.Registry,
		Logger:           logger,
	})
	templateService.SetNamespacePolicyReconciler(templateNamespacePolicy)
	operator.SetNamespacePolicyReconciler(templateNamespacePolicy)

	registryProvider, err := registryprovider.NewProvider(cfg.Registry, secretLister, logger)
	if err != nil {
		logger.Warn("Registry provider disabled", zap.Error(err))
	}
	registryService := registryservice.NewRegistryService(registryProvider, logger)
	templateStore := templstorepg.NewStore(pool)
	templateResourcePolicy := s0template.NewResourcePolicy(cfg.TeamTemplateMemoryPerCPU, cfg.SandboxMaxMemory)
	sandboxBackend, err := buildSandboxRuntimeBackend(cfg, sandboxRuntimeBackendDependencies{
		kubernetes: sandboxService, nodeAuthority: managerNodeAuthority,
		store: sandboxStore, quotaLimits: quotaRepo,
		templates: templateStore, networkPolicies: networkPolicyService,
		resourcePolicy: templateResourcePolicy, prober: procdClient,
		tokenGenerator: internalTokenGenerator,
		observer:       runtimeslotclaim.NewPrometheusObserver(obsProvider.MetricsRegistryOrNil()),
		defaultTTL:     cfg.DefaultSandboxTTL.Duration, now: clk.Now, logger: logger,
	})
	if err != nil {
		logger.Fatal("Failed to configure sandbox runtime backend", zap.Error(err))
	}
	sandboxPauseController := service.NewSandboxPauseController(sandboxStore, sandboxBackend, logger)
	sandboxBackend.SetPauseEnqueuer(sandboxPauseController)
	var sandboxForkController *service.SandboxForkController
	if forkReconciler, ok := sandboxBackend.(service.SandboxForkReconciler); ok {
		sandboxForkController = service.NewSandboxForkController(sandboxStore, forkReconciler, logger)
	}
	var templateReconciler *templreconciler.SingleClusterReconciler
	if cfg.TemplateStoreEnabled {
		templateApplier := templateservice.NewTemplateApplier(templateService)
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
	} else {
		logger.Info("Template reconciliation disabled; durable template build queue remains enabled")
	}
	var templateBuildWorker *templatebuild.TemplateBuildWorker
	switch {
	case registryProvider == nil:
		logger.Warn("Template image build worker disabled; registry provider is not configured")
	case rootFSObjectStoreErr != nil:
		logger.Warn("Template image build worker disabled; rootfs object store is unavailable", zap.Error(rootFSObjectStoreErr))
	case rootFSObjectStore == nil:
		logger.Warn("Template image build worker disabled; rootfs object store is not configured")
	default:
		imagePublisher, publisherErr := templateimage.NewPublisher(rootFSObjectStore, registryProvider, cfg.Registry)
		if publisherErr != nil {
			logger.Warn("Template image build worker disabled", zap.Error(publisherErr))
			break
		}
		templateBuildWorker, err = templatebuild.NewTemplateBuildWorker(
			templateStore,
			sandboxService,
			imagePublisher,
			rootFSObjectStore,
			templatebuild.TemplateBuildWorkerConfig{
				ClusterID: naming.ClusterIDOrDefault(&cfg.DefaultClusterId),
			},
			logger,
		)
		if err != nil {
			logger.Warn("Template image build worker disabled", zap.Error(err))
		} else {
			sandboxService.SetTemplateImageBuildAvailable(true)
		}
	}
	go serveTemplateReconcilerQuiesceSignals(
		ctx,
		templateReconcilerQuiesceSignals,
		templateReconciler,
		defaultTemplateReconcilerQuiesceSupportedMarkerPath,
		defaultTemplateReconcilerQuiescedMarkerPath,
		logger,
	)

	// Create cluster service (for scheduler)
	clusterService := clusterservice.NewClusterService(
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
		sandboxBackend,
		sandboxBackend,
		logger,
		cfg.CleanupInterval.Duration,
	)
	cleanupController.SetHardExpiredSandboxLister(sandboxService)
	cleanupController.SetPodTeardownCoordinator(teardownCoordinator)

	// Initialize internal auth validator
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

	// Create HTTP server
	httpServer := httpserver.NewServerWithDependencies(httpserver.ServerDependencies{
		SandboxService:          sandboxService,
		SandboxClaimer:          sandboxBackend,
		SandboxTerminator:       sandboxBackend,
		SandboxPauser:           sandboxBackend,
		SandboxResumer:          sandboxBackend,
		SandboxForker:           sandboxBackend,
		EgressAuthService:       egressAuthService,
		CredentialSourceService: credentialSourceService,
		TemplateService:         templateService,
		RegistryService:         registryService,
		TemplateStore:           templateStore,
		TemplateReconciler:      templateReconciler,
		TemplateStoreEnabled:    cfg.TemplateStoreEnabled,
		TemplateResourcePolicy:  templateResourcePolicy,
		ClusterService:          clusterService,
		QuotaRepository:         quotaRepo,
		AuthValidator:           authValidator,
		Logger:                  logger,
		Port:                    cfg.HTTPPort,
		ObservabilityProvider:   obsProvider,
		PublicRootDomain:        cfg.PublicRootDomain,
		PublicRegionID:          cfg.PublicRegionID,
	})

	controllers := &managerControllerSet{
		cfg:                            cfg,
		k8sClient:                      k8sClient,
		podLister:                      podLister,
		clock:                          clk,
		logger:                         logger,
		operator:                       operator,
		cleanupController:              cleanupController,
		sandboxService:                 sandboxService,
		sandboxLifecycleController:     sandboxLifecycleController,
		sandboxCrashLogCollector:       sandboxCrashLogCollector,
		sandboxCrashRecoveryController: sandboxCrashRecoveryController,
		sandboxRuntimeReconciler:       sandboxRuntimeReconciler,
		hotClaimReservationController:  hotClaimReservationController,
		sandboxPauseController:         sandboxPauseController,
		sandboxForkController:          sandboxForkController,
		templateReconciler:             templateReconciler,
		templateBuildWorker:            templateBuildWorker,
		sandboxLogWorker:               sandboxLogWorker,
		sandboxStore:                   sandboxStore,
		rootFSObjectStore:              rootFSObjectStore,
		rootFSObjectStoreErr:           rootFSObjectStoreErr,
		meteringRepo:                   meteringRepo,
		managerMetrics:                 managerMetrics,
	}

	app := &managerApp{
		ctx:                    ctx,
		cancel:                 cancel,
		logger:                 logger,
		k8sClient:              k8sClient,
		httpServer:             httpServer,
		nodeAuthority:          managerNodeAuthority,
		rootFSMaterializer:     rootFSCompositeMaterializer,
		sandboxClaimReconciler: sandboxClaimReconciler,
		informerFactory:        informerFactory,
		crdInformerFactory:     crdInformerFactory,
		metricsPort:            cfg.MetricsPort,
		leaderElectionEnabled:  cfg.LeaderElection,
		startControllers:       controllers.Start,
		cacheSyncs:             informerRuntime.cacheSyncs(),
	}
	app.Run()
}

type templateReconcilerQuiescer interface {
	Quiesce(context.Context) error
}

const (
	defaultTemplateReconcilerQuiesceSupportedMarkerPath = "/tmp/sandbox0-manager-template-reconciler-quiesce-supported"
	defaultTemplateReconcilerQuiescedMarkerPath         = "/tmp/sandbox0-manager-template-reconciler-quiesced"
)

// serveTemplateReconcilerQuiesceSignals exposes a process-local maintenance
// barrier without stopping the manager APIs needed to drain active sandboxes.
func serveTemplateReconcilerQuiesceSignals(
	ctx context.Context,
	signals <-chan os.Signal,
	reconciler templateReconcilerQuiescer,
	supportedMarkerPath string,
	quiescedMarkerPath string,
	logger *zap.Logger,
) {
	if err := os.Remove(quiescedMarkerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error("Failed to clear stale template reconciliation quiesce acknowledgement", zap.Error(err))
		return
	}
	if err := os.WriteFile(supportedMarkerPath, nil, 0o600); err != nil {
		logger.Error("Failed to publish template reconciliation quiesce support", zap.Error(err))
	} else {
		logger.Info("Template reconciliation quiesce signal enabled")
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-signals:
			if reconciler != nil {
				quiesceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := reconciler.Quiesce(quiesceCtx)
				cancel()
				if err != nil {
					logger.Error("Failed to quiesce template reconciliation", zap.Error(err))
					continue
				}
			}
			if err := os.WriteFile(quiescedMarkerPath, nil, 0o600); err != nil {
				logger.Error("Failed to acknowledge quiesced template reconciliation", zap.Error(err))
				continue
			}
			logger.Info("Template reconciliation quiesced")
		}
	}
}

// buildKubeConfig builds Kubernetes config
func buildKubeConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func configureK8sClientRateLimiter(restConfig *rest.Config, qps int, burst int) {
	if restConfig == nil {
		return
	}
	rate := float32(qps)
	if rate <= 0 {
		rate = s0k8s.DefaultClientQPS
	}
	if burst <= 0 {
		burst = s0k8s.DefaultClientBurst
	}
	restConfig.QPS = rate
	restConfig.Burst = burst
	restConfig.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(rate, burst)
}

func isolatedK8sClientConfig(base *rest.Config) *rest.Config {
	if base == nil {
		return nil
	}
	isolated := rest.CopyConfig(base)
	rate := effectiveK8sClientQPS(base)
	burst := effectiveK8sClientBurst(base)
	isolated.QPS = rate
	isolated.Burst = burst
	isolated.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(rate, burst)
	return isolated
}

func observeK8sClientRateLimit(metrics *obsmetrics.ManagerMetrics, restConfig *rest.Config) {
	if metrics == nil || metrics.K8sClientRateLimit == nil || restConfig == nil {
		return
	}
	metrics.K8sClientRateLimit.WithLabelValues("qps").Set(float64(effectiveK8sClientQPS(restConfig)))
	metrics.K8sClientRateLimit.WithLabelValues("burst").Set(float64(effectiveK8sClientBurst(restConfig)))
}

func observeHotClaimK8sClientRateLimit(metrics *obsmetrics.ManagerMetrics, restConfig *rest.Config) {
	if metrics == nil || metrics.K8sClientRateLimit == nil || restConfig == nil {
		return
	}
	metrics.K8sClientRateLimit.WithLabelValues("hot_claim_qps").Set(float64(effectiveK8sClientQPS(restConfig)))
	metrics.K8sClientRateLimit.WithLabelValues("hot_claim_burst").Set(float64(effectiveK8sClientBurst(restConfig)))
}

func effectiveK8sClientQPS(restConfig *rest.Config) float32 {
	if restConfig == nil || restConfig.QPS <= 0 {
		return s0k8s.DefaultClientQPS
	}
	return restConfig.QPS
}

func effectiveK8sClientBurst(restConfig *rest.Config) int {
	if restConfig == nil || restConfig.Burst <= 0 {
		return s0k8s.DefaultClientBurst
	}
	return restConfig.Burst
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
	store, err := objectstore.Create(objectstore.Config{
		Type:            objectStorageCfg.Type,
		Bucket:          objectStorageCfg.Bucket,
		Region:          objectStorageCfg.Region,
		Endpoint:        objectStorageCfg.Endpoint,
		AccessKey:       objectStorageCfg.AccessKey,
		SecretKey:       objectStorageCfg.SecretKey,
		SessionToken:    objectStorageCfg.SessionToken,
		RequestObserver: requestObserver,
	})
	if err != nil {
		return nil, err
	}
	return wrapRootFSObjectStoreEncryption(store, objectStorageCfg)
}

func wrapRootFSObjectStoreEncryption(store objectstore.Store, objectStorageCfg config.RootFSObjectStorageConfig) (objectstore.Store, error) {
	if store == nil || !objectStorageCfg.ObjectEncryptionEnabled {
		return store, nil
	}
	keyPEM, err := objectstore.LoadEncryptionKey(objectStorageCfg.ObjectEncryptionKeyPath)
	if err != nil {
		return nil, err
	}
	keyEncryptor, err := objectstore.NewKeyEncryptor(keyPEM, objectStorageCfg.ObjectEncryptionPassphrase)
	if err != nil {
		return nil, err
	}
	return objectstore.Encrypting(store, objectstore.EncryptionConfig{
		Enabled:      true,
		Algorithm:    objectStorageCfg.ObjectEncryptionAlgo,
		KeyEncryptor: keyEncryptor,
	}), nil
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
	if err := legacyvolumemigrations.Run(ctx, pool, observability.NewMigrateLogger(logger)); err != nil {
		return fmt.Errorf("legacy volume migrations: %w", err)
	}

	logger.Info("Sandbox store migrations completed successfully")
	return nil
}

// initDatabase initializes the database connection pool
func initDatabase(ctx context.Context, databaseURL string, maxConns, minConns int32, logger *zap.Logger, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:    databaseURL,
		MaxConns:       maxConns,
		MinConns:       minConns,
		Schema:         "scheduler",
		ConfigModifier: obsProvider.Pgx.ConfigModifier(),
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
