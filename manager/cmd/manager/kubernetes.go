package main

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/generated/informers/externalversions"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxindex"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

type managerKubernetesClients struct {
	client         kubernetes.Interface
	hotClaimClient kubernetes.Interface
	crdClient      clientset.Interface
}

func managerUsesKubernetesSandboxRuntime(cfg *config.ManagerConfig) bool {
	return cfg == nil || cfg.SandboxRuntimeBackend == "" ||
		cfg.SandboxRuntimeBackend == config.SandboxRuntimeBackendKubernetes
}

func buildManagerKubernetesClients(
	cfg *config.ManagerConfig,
	obsProvider *observability.Provider,
	metrics *obsmetrics.ManagerMetrics,
	logger *zap.Logger,
) (*managerKubernetesClients, error) {
	k8sConfig, err := buildKubeConfig(cfg.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("build Kubernetes config: %w", err)
	}
	configureK8sClientRateLimiter(k8sConfig, cfg.K8sClientQPS, cfg.K8sClientBurst)
	observeK8sClientRateLimit(metrics, k8sConfig)
	logger.Info("Kubernetes client rate limit configured",
		zap.Float32("qps", effectiveK8sClientQPS(k8sConfig)),
		zap.Int("burst", effectiveK8sClientBurst(k8sConfig)),
	)
	obsProvider.K8s.WrapConfig(k8sConfig)

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	var hotClaimClient kubernetes.Interface
	if managerUsesKubernetesSandboxRuntime(cfg) {
		hotClaimConfig := isolatedK8sClientConfig(k8sConfig)
		observeHotClaimK8sClientRateLimit(metrics, hotClaimConfig)
		logger.Info("Hot claim Kubernetes client rate limit configured",
			zap.Float32("qps", effectiveK8sClientQPS(hotClaimConfig)),
			zap.Int("burst", effectiveK8sClientBurst(hotClaimConfig)),
		)
		hotClaimClient, err = kubernetes.NewForConfig(hotClaimConfig)
		if err != nil {
			return nil, fmt.Errorf("create hot claim Kubernetes client: %w", err)
		}
	}
	var crdClient clientset.Interface
	if managerUsesKubernetesSandboxRuntime(cfg) {
		if err := v1alpha1.AddToScheme(scheme.Scheme); err != nil {
			return nil, fmt.Errorf("add SandboxTemplate to scheme: %w", err)
		}
		crdClient, err = clientset.NewForConfig(k8sConfig)
		if err != nil {
			return nil, fmt.Errorf("create CRD clientset: %w", err)
		}
	}
	return &managerKubernetesClients{
		client:         k8sClient,
		hotClaimClient: hotClaimClient,
		crdClient:      crdClient,
	}, nil
}

type managerInformerRuntime struct {
	factory                  informers.SharedInformerFactory
	crdFactory               externalversions.SharedInformerFactory
	podInformer              coreinformers.PodInformer
	nodeInformer             cache.SharedIndexInformer
	secretInformer           cache.SharedIndexInformer
	namespaceInformer        cache.SharedIndexInformer
	serviceAccountInformer   cache.SharedIndexInformer
	replicaSetInformer       cache.SharedIndexInformer
	networkPolicyInformer    cache.SharedIndexInformer
	templateInformer         cache.SharedIndexInformer
	podLister                corelisters.PodLister
	nodeLister               corelisters.NodeLister
	secretLister             corelisters.SecretLister
	namespaceLister          corelisters.NamespaceLister
	serviceAccountLister     corelisters.ServiceAccountLister
	networkPolicyLister      networkinglisters.NetworkPolicyLister
	templateLister           controller.TemplateLister
	operator                 *controller.Operator
	recorder                 record.EventRecorder
	sandboxIndex             *sandboxindex.SandboxIndex
	teardownCoordinator      *controller.PodTeardownCoordinator
	autoscalerAnnotationKeys []string
	syncs                    []cache.InformerSynced
}

func buildManagerInformerRuntime(
	clients *managerKubernetesClients,
	cfg *config.ManagerConfig,
	pool *pgxpool.Pool,
	clk *clock.Clock,
	metrics *obsmetrics.ManagerMetrics,
	logger *zap.Logger,
) (*managerInformerRuntime, error) {
	factory := informers.NewSharedInformerFactory(clients.client, cfg.ResyncPeriod.Duration)
	if !managerUsesKubernetesSandboxRuntime(cfg) {
		secretInformer := factory.Core().V1().Secrets().Informer()
		return &managerInformerRuntime{
			factory:        factory,
			secretInformer: secretInformer,
			secretLister:   factory.Core().V1().Secrets().Lister(),
			syncs:          []cache.InformerSynced{secretInformer.HasSynced},
		}, nil
	}
	podInformer := factory.Core().V1().Pods()
	nodeInformer := factory.Core().V1().Nodes().Informer()
	secretInformer := factory.Core().V1().Secrets().Informer()
	namespaceInformer := factory.Core().V1().Namespaces().Informer()
	serviceAccountInformer := factory.Core().V1().ServiceAccounts().Informer()
	replicaSetInformer := factory.Apps().V1().ReplicaSets().Informer()
	networkPolicyInformer := factory.Networking().V1().NetworkPolicies().Informer()

	crdFactory := externalversions.NewSharedInformerFactory(clients.crdClient, cfg.ResyncPeriod.Duration)
	templateInformer := crdFactory.Sandbox0().V1alpha1().SandboxTemplates().Informer()

	podLister := factory.Core().V1().Pods().Lister()
	nodeLister := factory.Core().V1().Nodes().Lister()
	secretLister := factory.Core().V1().Secrets().Lister()
	namespaceLister := factory.Core().V1().Namespaces().Lister()
	serviceAccountLister := factory.Core().V1().ServiceAccounts().Lister()
	networkPolicyLister := factory.Networking().V1().NetworkPolicies().Lister()
	templateLister := controller.NewTemplateLister(templateInformer.GetIndexer())
	var recorder record.EventRecorder
	var operator *controller.Operator
	var teardownCoordinator *controller.PodTeardownCoordinator
	var index *sandboxindex.SandboxIndex
	var autoscalerKeys []string
	if managerUsesKubernetesSandboxRuntime(cfg) {
		eventBroadcaster := record.NewBroadcaster()
		eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
			Interface: clients.client.CoreV1().Events(""),
		})
		recorder = eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "manager"})
		var err error
		autoscalerKeys, err = controller.NormalizeAutoscalerSafeToEvictAnnotationKeys(cfg.AutoscalerSafeToEvictAnnotationKeys)
		if err != nil {
			return nil, fmt.Errorf("normalize autoscaler safe-to-evict annotation keys: %w", err)
		}
		teardownCoordinator = controller.NewPodTeardownCoordinator(
			podLister,
			nodeLister,
			cfg.PodTeardown,
			cfg.RuntimeReadyTimeout.Duration,
			metrics,
			logger,
		)
		operator = controller.NewOperator(
			clients.client,
			clients.crdClient,
			podInformer.Informer(),
			replicaSetInformer,
			secretInformer,
			templateInformer,
			recorder,
			clk,
			logger,
			metrics,
			teardownCoordinator,
			autoscalerKeys,
		)
		templateLister = operator.GetTemplateLister()
		if pool != nil {
			operator.SetTemplateStatsPublisher(controller.NewPGTemplateStatsPublisher(pool, cfg.DefaultClusterId, clk, logger))
		}
		index = sandboxindex.NewSandboxIndex()
		podInformer.Informer().AddEventHandler(index.ResourceEventHandler())
	}

	return &managerInformerRuntime{
		factory:                  factory,
		crdFactory:               crdFactory,
		podInformer:              podInformer,
		nodeInformer:             nodeInformer,
		secretInformer:           secretInformer,
		namespaceInformer:        namespaceInformer,
		serviceAccountInformer:   serviceAccountInformer,
		replicaSetInformer:       replicaSetInformer,
		networkPolicyInformer:    networkPolicyInformer,
		templateInformer:         templateInformer,
		podLister:                podLister,
		nodeLister:               nodeLister,
		secretLister:             secretLister,
		namespaceLister:          namespaceLister,
		serviceAccountLister:     serviceAccountLister,
		networkPolicyLister:      networkPolicyLister,
		templateLister:           templateLister,
		operator:                 operator,
		recorder:                 recorder,
		sandboxIndex:             index,
		teardownCoordinator:      teardownCoordinator,
		autoscalerAnnotationKeys: autoscalerKeys,
		syncs: []cache.InformerSynced{
			podInformer.Informer().HasSynced,
			nodeInformer.HasSynced,
			secretInformer.HasSynced,
			namespaceInformer.HasSynced,
			serviceAccountInformer.HasSynced,
			replicaSetInformer.HasSynced,
			networkPolicyInformer.HasSynced,
			templateInformer.HasSynced,
		},
	}, nil
}

func (r *managerInformerRuntime) cacheSyncs() []cache.InformerSynced {
	if r == nil {
		return nil
	}
	return append([]cache.InformerSynced(nil), r.syncs...)
}
