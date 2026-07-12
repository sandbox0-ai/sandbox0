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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/generated/informers/externalversions"
	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	managermetering "github.com/sandbox0-ai/sandbox0/manager/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/namespacepolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	registryprovider "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/startlimiter"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	egressauthruntime "github.com/sandbox0-ai/sandbox0/pkg/egressauth/runtime"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	s0k8s "github.com/sandbox0-ai/sandbox0/pkg/k8s"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	templmigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	templreconciler "github.com/sandbox0-ai/sandbox0/pkg/template/reconciler"
	templstorepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
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

	// Initialize observability provider
	obsProvider, err := observability.New(observability.ConfigFromEnv("manager", logger))
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
	configureK8sClientRateLimiter(k8sConfig, cfg.K8sClientQPS, cfg.K8sClientBurst)
	observeK8sClientRateLimit(managerMetrics, k8sConfig)
	logger.Info("Kubernetes client rate limit configured",
		zap.Float32("qps", effectiveK8sClientQPS(k8sConfig)),
		zap.Int("burst", effectiveK8sClientBurst(k8sConfig)),
	)

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
	namespaceInformer := informerFactory.Core().V1().Namespaces().Informer()
	serviceAccountInformer := informerFactory.Core().V1().ServiceAccounts().Informer()
	replicaSetInformer := informerFactory.Apps().V1().ReplicaSets().Informer()
	networkPolicyInformer := informerFactory.Networking().V1().NetworkPolicies().Informer()

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

	// Create listers
	podLister := informerFactory.Core().V1().Pods().Lister()
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	secretLister := informerFactory.Core().V1().Secrets().Lister()
	namespaceLister := informerFactory.Core().V1().Namespaces().Lister()
	serviceAccountLister := informerFactory.Core().V1().ServiceAccounts().Lister()
	networkPolicyLister := informerFactory.Networking().V1().NetworkPolicies().Lister()
	replicaSetLister := informerFactory.Apps().V1().ReplicaSets().Lister()

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
	)
	if pool != nil {
		operator.SetTemplateStatsPublisher(controller.NewPGTemplateStatsPublisher(pool, cfg.DefaultClusterId, clk, logger))
	}
	claimStartLimiter, err := startlimiter.New(ctx, startlimiter.Config{
		ClusterID:        cfg.DefaultClusterId,
		K8sClient:        k8sClient,
		NodeLister:       nodeLister,
		PodLister:        podLister,
		ReplicaSetLister: replicaSetLister,
		PGPool:           pool,
		Redis: rediscache.Config{
			URL:       cfg.RedisURL,
			KeyPrefix: cfg.RedisKeyPrefix,
			Timeout:   cfg.RedisTimeout.Duration,
			FailOpen:  false,
		},
		PerSandboxNode:      cfg.ClaimStartLimiter.PerSandboxNode,
		MaxLimit:            cfg.ClaimStartLimiter.MaxLimit,
		LockTTL:             cfg.ClaimStartLimiter.LockTTL.Duration,
		AcquireTimeout:      cfg.ClaimStartLimiter.AcquireTimeout.Duration,
		SandboxNodeSelector: cfg.SandboxPodPlacement.NodeSelector,
		SandboxTolerations:  cfg.SandboxPodPlacement.Tolerations,
		Logger:              logger,
	})
	if err != nil {
		logger.Fatal("Failed to initialize claim start limiter", zap.Error(err))
	}
	operator.SetClaimStartLimiter(claimStartLimiter)
	logger.Info("Claim start limiter enabled",
		zap.String("backend", claimStartLimiter.Backend()),
		zap.Int32("perSandboxNode", cfg.ClaimStartLimiter.PerSandboxNode),
		zap.Int32("maxLimit", cfg.ClaimStartLimiter.MaxLimit),
	)

	sandboxIndex := service.NewSandboxIndex()
	podInformer.Informer().AddEventHandler(sandboxIndex.ResourceEventHandler())
	meteringDB, meteringSink, meteringSinkReady, err := initMetering(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize metering backend", zap.Error(err))
	}
	if meteringDB != nil {
		defer meteringDB.Close()
	}
	sandboxStore := service.NewPGSandboxStore(pool)
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

	// Create network policy service for building policy annotations
	networkPolicyService := service.NewNetworkPolicyService(logger)
	var templateNamespacePolicy namespacepolicy.TemplateNamespaceReconciler
	baselineReconciler, err := namespacepolicy.NewReconciler(k8sClient, networkPolicyLister, namespacepolicy.Config{
		SystemNamespace: cfg.NetdMITMCASecretNamespace,
		ProcdPort:       cfg.ProcdConfig.HTTPPort,
	}, logger)
	if err != nil {
		logger.Warn("Template namespace ingress baseline disabled", zap.Error(err))
	} else {
		templateNamespacePolicy = baselineReconciler
		logger.Info("Template namespace ingress baseline enabled",
			zap.String("systemNamespace", cfg.NetdMITMCASecretNamespace),
			zap.Int("procdPort", cfg.ProcdConfig.HTTPPort),
		)
	}

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
	var storageProxyAdminTokenGenerator service.TokenGenerator
	var internalAuthGen *internalauth.Generator
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		logger.Warn("Failed to load internal auth private key, procd and storage-proxy calls will not work",
			zap.String("path", internalauth.DefaultInternalJWTPrivateKeyPath),
			zap.Error(err),
		)
	} else {
		internalAuthGen = internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "manager",
			PrivateKey: privateKey,
			TTL:        30 * time.Second,
		})
		internalTokenGenerator = service.NewInternalTokenGenerator(internalAuthGen)
		storageProxyAdminTokenGenerator = service.NewStorageProxyAdminTokenGenerator(internalAuthGen)
		logger.Info("Internal auth generators initialized for procd and storage-proxy communication")
	}

	// Parse ratios
	pauseMemoryBufferRatio, err := strconv.ParseFloat(cfg.PauseMemoryBufferRatio, 64)
	if err != nil {
		logger.Warn("Failed to parse PauseMemoryBufferRatio, using default 1.1", zap.String("value", cfg.PauseMemoryBufferRatio), zap.Error(err))
		pauseMemoryBufferRatio = 1.1
	}

	// Create services
	cfgForSandbox := service.SandboxServiceConfig{
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
		ProcdHTTPClient:                     obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.ProcdClientTimeout.Duration}),
		ProcdInitTimeout:                    cfg.ProcdInitTimeout.Duration,
		AllowColdStartWithoutReadyDataPlane: cfg.AllowColdStartWithoutReadyDataPlane,
		RootFSSquashDisabled:                cfg.RootFSMaintenance.SquashDisabled,
		RootFSSquashMaxChainDepth:           cfg.RootFSMaintenance.SquashMaxChainDepth,
		RootFSSquashMaxChainBytes:           cfg.RootFSMaintenance.SquashMaxChainBytes,
		PublicRootDomain:                    cfg.PublicRootDomain,
		PublicRegionID:                      cfg.PublicRegionID,
	}

	sandboxService := service.NewSandboxService(
		k8sClient,
		podLister,
		nodeLister,
		sandboxIndex,
		secretLister,
		operator.GetTemplateLister(),
		networkPolicyService,
		networkProvider,
		internalTokenGenerator,
		clk,
		cfgForSandbox,
		logger,
		managerMetrics,
	)
	var quotaUsageStore quota.UsageStore
	if meteringSink != nil {
		quotaUsageStore = meteringSink
	}
	quotaRepo, err := buildQuotaRepository(pool, cfg, quotaUsageStore)
	if err != nil {
		logger.Fatal("Invalid quota configuration", zap.Error(err))
	}
	sandboxService.SetCredentialStore(credentialStore)
	sandboxService.SetQuotaStore(quotaRepo)
	sandboxService.SetSandboxStore(sandboxStore)
	sandboxService.SetClaimStartLimiter(claimStartLimiter)
	rootFSObjectStore, rootFSObjectStoreErr := buildRootFSObjectStore(cfg)
	if rootFSObjectStoreErr != nil {
		logger.Warn("Rootfs object cleanup disabled; object store is not configured", zap.Error(rootFSObjectStoreErr))
	} else if rootFSObjectStore != nil {
		sandboxService.SetRootFSObjectDeleter(rootFSObjectStore)
	}
	if cfg.StorageProxyBaseURL != "" && cfg.StorageProxyHTTPPort > 0 && storageProxyAdminTokenGenerator != nil {
		storageProxyBaseURL := fmt.Sprintf("http://%s:%d", strings.TrimSpace(cfg.StorageProxyBaseURL), cfg.StorageProxyHTTPPort)
		sandboxService.SetWebhookStateVolumeClient(service.NewStorageProxyVolumeClient(service.StorageProxyVolumeClientConfig{
			BaseURL:        storageProxyBaseURL,
			HTTPClient:     obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.ProcdClientTimeout.Duration}),
			TokenGenerator: storageProxyAdminTokenGenerator,
			ClusterID:      cfg.DefaultClusterId,
		}))
		logger.Info("Webhook state volumes enabled", zap.String("storageProxyBaseURL", storageProxyBaseURL))
	} else {
		logger.Warn("Webhook state volumes disabled; sandbox claims with webhooks will fail until storage-proxy is configured")
	}
	sandboxService.SetDeletionWebhookEmitter(service.NewHTTPSandboxDeletionWebhookEmitter(obsProvider.HTTP.NewClient(httpobs.Config{Timeout: cfg.ProcdClientTimeout.Duration})))
	podInformer.Informer().AddEventHandler(sandboxService.PodEventHandler())
	sandboxLifecycleController := service.NewSandboxLifecycleController(k8sClient, podLister, sandboxService, logger)
	sandboxLifecycleController.SetMetrics(managerMetrics)
	podInformer.Informer().AddEventHandler(sandboxLifecycleController.ResourceEventHandler())
	sandboxPauseController := service.NewSandboxPauseController(sandboxService, logger)
	sandboxService.SetPauseEnqueuer(sandboxPauseController)
	operator.SetSandboxProbeRunner(sandboxService)
	sandboxLogWorker := buildSandboxObservabilityLogWorker(cfg, internalAuthGen, obsProvider, logger)
	staticAuth := make([]egressauthruntime.StaticAuthConfig, 0, len(cfg.EgressAuthStaticAuth))
	for _, entry := range cfg.EgressAuthStaticAuth {
		staticAuth = append(staticAuth, egressauthruntime.StaticAuthConfig{
			AuthRef: entry.AuthRef,
			Headers: entry.Headers,
			TTL:     entry.TTL.Duration,
		})
	}
	egressAuthService := service.NewEgressAuthService(service.EgressAuthServiceConfig{
		DefaultResolveTTL: cfg.EgressAuthDefaultResolveTTL.Duration,
		StaticAuth:        staticAuth,
	}, credentialStore, logger)
	credentialSourceService := service.NewCredentialSourceService(credentialStore, logger)

	templateService := service.NewTemplateService(
		k8sClient,
		crdClient,
		operator.GetTemplateLister(),
		namespaceLister,
		podLister,
		secretLister,
		serviceAccountLister,
		networkProvider,
		cfg.Registry,
		logger,
	)
	templateService.SetNamespacePolicyReconciler(templateNamespacePolicy)
	operator.SetNamespacePolicyReconciler(templateNamespacePolicy)

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
	clusterService.SetClaimStartLimiter(claimStartLimiter)

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

	validatorConfig := internalauth.DefaultValidatorConfig(internalauth.ServiceManager, publicKey)
	validatorConfig.AllowedCallers = internalauth.ManagerAllowedCallers()
	authValidator := internalauth.NewValidator(validatorConfig)

	logger.Info("Internal authentication enabled",
		zap.String("target", internalauth.ServiceManager),
		zap.Strings("allowed_callers", validatorConfig.AllowedCallers),
	)

	// Create HTTP server
	httpServer := httpserver.NewServer(
		sandboxService,
		egressAuthService,
		credentialSourceService,
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
	httpServer.SetQuotaRepository(quotaRepo)

	// Start metrics server
	go startMetricsServer(cfg.MetricsPort, logger)

	// Start informers
	logger.Info("Starting informers")
	informerFactory.Start(ctx.Done())
	crdInformerFactory.Start(ctx.Done())

	// Wait for cache sync
	logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.Informer().HasSynced, nodeInformer.HasSynced, secretInformer.HasSynced, namespaceInformer.HasSynced, serviceAccountInformer.HasSynced, replicaSetInformer.HasSynced, networkPolicyInformer.HasSynced, templateInformer.HasSynced) {
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

	startSandboxObservabilityLogProducer(ctx, cfg, k8sClient, podLister, sandboxLogWorker, logger, clk)

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

	go func() {
		if err := sandboxLifecycleController.Run(ctx, 2); err != nil && err != context.Canceled {
			logger.Error("Sandbox lifecycle controller failed", zap.Error(err))
		}
	}()

	go func() {
		if err := sandboxPauseController.Run(ctx, 2); err != nil && err != context.Canceled {
			logger.Error("Sandbox pause controller failed", zap.Error(err))
		}
	}()
	if !cfg.RootFSMaintenance.Disabled {
		if rootFSObjectStoreErr != nil {
			logger.Warn("Rootfs maintenance disabled; object store is not configured", zap.Error(rootFSObjectStoreErr))
		} else if rootFSObjectStore == nil {
			logger.Warn("Rootfs maintenance disabled; object store is not configured")
		} else {
			rootFSMaintenanceController := service.NewRootFSMaintenanceController(
				sandboxStore,
				rootFSObjectStore,
				rootFSMaintenanceControllerConfig(cfg),
				logger,
				managerMetrics,
			)
			rootFSMaintenanceController.SetObjectInspector(rootFSObjectStoreInspector{store: rootFSObjectStore})
			if meteringRepo != nil {
				rootFSMaintenanceController.SetStorageMeteringRecorder(meteringRepo)
			}
			go func() {
				if err := rootFSMaintenanceController.Run(ctx); err != nil && err != context.Canceled {
					logger.Error("Rootfs maintenance controller failed", zap.Error(err))
				}
			}()
		}
	} else {
		logger.Info("Rootfs maintenance controller disabled by config")
	}

	go sandboxService.StartSystemVolumeReconciler(ctx, cfg.ResyncPeriod.Duration)

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

func observeK8sClientRateLimit(metrics *obsmetrics.ManagerMetrics, restConfig *rest.Config) {
	if metrics == nil || metrics.K8sClientRateLimit == nil || restConfig == nil {
		return
	}
	metrics.K8sClientRateLimit.WithLabelValues("qps").Set(float64(effectiveK8sClientQPS(restConfig)))
	metrics.K8sClientRateLimit.WithLabelValues("burst").Set(float64(effectiveK8sClientBurst(restConfig)))
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

	if err := egressauth.RunMigrations(ctx, pool, observability.NewMigrateLogger(logger)); err != nil {
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

func buildQuotaRepository(pool *pgxpool.Pool, cfg *config.ManagerConfig, usageStore quota.UsageStore) (*quota.Repository, error) {
	repo, err := quota.NewRepositoryWithDefaults(pool, defaultTeamQuotaLimits(cfg))
	if err != nil || repo == nil {
		return repo, err
	}
	if usageStore != nil {
		repo.SetUsageStore(usageStore)
	}
	return repo, nil
}

func defaultTeamQuotaLimits(cfg *config.ManagerConfig) []quota.DefaultLimit {
	if cfg == nil || len(cfg.DefaultTeamQuotas) == 0 {
		return nil
	}
	limits := make([]quota.DefaultLimit, 0, len(cfg.DefaultTeamQuotas))
	for _, limit := range cfg.DefaultTeamQuotas {
		limits = append(limits, quota.DefaultLimit{
			Dimension:  quota.Dimension(limit.Dimension),
			LimitValue: limit.LimitValue,
		})
	}
	return limits
}

func buildRootFSObjectStore(cfg *config.ManagerConfig) (objectstore.Store, error) {
	if cfg == nil {
		return nil, nil
	}
	storageCfg := cfg.RootFSObjectStorage
	if strings.TrimSpace(storageCfg.Type) == "" && strings.TrimSpace(storageCfg.Bucket) == "" {
		return nil, nil
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:         storageCfg.Type,
		Bucket:       storageCfg.Bucket,
		Region:       storageCfg.Region,
		Endpoint:     storageCfg.Endpoint,
		AccessKey:    storageCfg.AccessKey,
		SecretKey:    storageCfg.SecretKey,
		SessionToken: storageCfg.SessionToken,
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

type rootFSObjectStoreInspector struct {
	store objectstore.Store
}

func (i rootFSObjectStoreInspector) StatRootFSObject(key string) (service.RootFSObjectInfo, error) {
	info, err := i.store.Head(key)
	if err != nil {
		return service.RootFSObjectInfo{}, err
	}
	return service.RootFSObjectInfo{
		Key:      info.Key,
		Size:     info.Size,
		Modified: info.Modified,
	}, nil
}

func rootFSMaintenanceControllerConfig(cfg *config.ManagerConfig) service.RootFSMaintenanceControllerConfig {
	if cfg == nil {
		return service.RootFSMaintenanceControllerConfig{}
	}
	return service.RootFSMaintenanceControllerConfig{
		Interval:         cfg.RootFSMaintenance.Interval.Duration,
		BatchSize:        cfg.RootFSMaintenance.BatchSize,
		MaxBatchesPerRun: cfg.RootFSMaintenance.MaxBatchesPerRun,
		Workers:          cfg.RootFSMaintenance.Workers,
		DeleteOptions: service.DeletePendingRootFSObjectsOptions{
			ClaimTTL:    cfg.RootFSMaintenance.ObjectDeleteClaimTTL.Duration,
			BackoffBase: cfg.RootFSMaintenance.ObjectDeleteBackoffBase.Duration,
			BackoffMax:  cfg.RootFSMaintenance.ObjectDeleteBackoffMax.Duration,
			MaxAttempts: cfg.RootFSMaintenance.ObjectDeleteMaxAttempts,
		},
	}
}

func buildCredentialStore(ctx context.Context, pool *pgxpool.Pool, cfg *config.ManagerConfig, logger *zap.Logger) (*egressauth.Repository, error) {
	if cfg == nil {
		cfg = &config.ManagerConfig{}
	}
	storeCfg := cfg.CredentialStore
	if strings.TrimSpace(storeCfg.DefaultStorageKind) == "" {
		storeCfg.DefaultStorageKind = egressauth.CredentialSourceStorageKindEncryptedPG
	}

	var codec egressauth.SecretCodec
	if storeCfg.EncryptedPG.KeyFile != "" || storeCfg.EncryptedPG.Key != "" {
		key, err := loadCredentialEncryptionKey(storeCfg.EncryptedPG)
		if err != nil {
			return nil, err
		}
		keyID := strings.TrimSpace(storeCfg.EncryptedPG.KeyID)
		if keyID == "" {
			keyID = "default"
		}
		codec, err = egressauth.NewAESGCMCodec(keyID, map[string][]byte{keyID: key})
		if err != nil {
			return nil, err
		}
	}

	vaultConfigs := make([]egressauth.VaultConnectionConfig, 0, len(storeCfg.Vault.Connections))
	for _, conn := range storeCfg.Vault.Connections {
		vaultConfigs = append(vaultConfigs, egressauth.VaultConnectionConfig{
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
	vaultResolver, err := egressauth.NewVaultResolver(vaultConfigs)
	if err != nil {
		return nil, err
	}

	repo := egressauth.NewRepository(
		pool,
		egressauth.WithDefaultStorageKind(storeCfg.DefaultStorageKind),
		egressauth.WithSecretCodec(codec),
		egressauth.WithVaultResolver(vaultResolver),
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

	if err := service.RunSandboxStoreMigrations(ctx, pool, observability.NewMigrateLogger(logger)); err != nil {
		return fmt.Errorf("sandbox store migrations: %w", err)
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
