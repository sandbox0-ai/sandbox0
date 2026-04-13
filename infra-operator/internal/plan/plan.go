package plan

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

const (
	defaultManagerHTTPPort         = 8080
	defaultClusterGatewayHTTPPort  = 8443
	defaultClusterGatewayAuthMode  = "internal"
	defaultRegionalGatewayAuthMode = "self_hosted"
	defaultSSHGatewayPort          = 2222
	defaultStorageProxyHTTPPort    = 8081
	registryCredentialsPath        = "/etc/sandbox0/registry/.dockerconfigjson"
)

type InfraPlan struct {
	Scope           common.ObjectScope
	Components      ComponentPlan
	Services        ServicePlan
	Scheduler       SchedulerPlan
	Manager         ManagerPlan
	Netd            NetdPlan
	RegionalGateway RegionalGatewayPlan
	Enterprise      EnterpriseLicensePlan
	Validation      ValidationPlan
	Cleanup         CleanupPlan
	Status          StatusPlan
	Workflow        WorkflowPlan
	infra           *infrav1alpha1.Sandbox0Infra
}

type ComponentPlan struct {
	EnableGlobalGateway       bool
	HasControlPlane           bool
	HasDataPlane              bool
	EnableRegionalGateway     bool
	EnableSSHGateway          bool
	EnableScheduler           bool
	EnableClusterGateway      bool
	EnableManager             bool
	EnableStorageProxy        bool
	EnableFusePlugin          bool
	EnableNetd                bool
	EnableInternalAuth        bool
	EnableDatabase            bool
	EnableStorage             bool
	EnableRegistry            bool
	EnableInitUser            bool
	EnableClusterRegistration bool
}

type ValidationPlan struct {
	FatalErrors                  []string
	RequireControlPlanePublicKey bool
}

type CleanupPlan struct {
	CleanupBuiltinDatabase bool
	CleanupBuiltinStorage  bool
	CleanupBuiltinRegistry bool
	DeleteNamespaced       []ResourceRef
	DeleteClusterScoped    []ResourceRef
}

type ResourceRef struct {
	Kind      string
	Name      string
	Namespace string
}

type ServicePlan struct {
	Manager        ServiceReference
	Scheduler      ServiceReference
	ClusterGateway ServiceReference
	StorageProxy   ServiceReference
}

type ServiceReference struct {
	Name string
	Port int32
	URL  string
}

type SchedulerPlan struct {
	Enabled       bool
	Replicas      int32
	Resources     *corev1.ResourceRequirements
	ServiceConfig *infrav1alpha1.ServiceNetworkConfig
	Config        *apiconfig.SchedulerConfig
	HomeCluster   *template.Cluster
}

type ManagerPlan struct {
	Enabled               bool
	Replicas              int32
	Resources             *corev1.ResourceRequirements
	ServiceConfig         *infrav1alpha1.ServiceNetworkConfig
	Config                *apiconfig.ManagerConfig
	TemplateStoreEnabled  bool
	NetworkPolicyProvider string
	SandboxPodPlacement   apiconfig.SandboxPodPlacementConfig
	DefaultClusterID      string
	RegionID              string
}

type NetdPlan struct {
	Enabled               bool
	RuntimeClassName      *string
	Config                *apiconfig.NetdConfig
	EgressAuthResolverURL string
	RegionID              string
	ClusterID             string
	NodeSelector          map[string]string
	Tolerations           []corev1.Toleration
}

type RegionalGatewayPlan struct {
	Enabled                  bool
	Replicas                 int32
	Resources                *corev1.ResourceRequirements
	ServiceConfig            *infrav1alpha1.ServiceNetworkConfig
	IngressConfig            *infrav1alpha1.IngressConfig
	Config                   *apiconfig.RegionalGatewayConfig
	DefaultClusterGatewayURL string
}

type EnterpriseLicensePlan struct {
	Scheduler       bool
	RegionalGateway bool
	GlobalGateway   bool
	ClusterGateway  bool
}

type StatusPlan struct {
	ExpectedConditions []string
	Endpoints          EndpointStatusPlan
	Cluster            ClusterStatusPlan
	RetainedResources  []infrav1alpha1.RetainedResourceStatus
}

type EndpointStatusPlan struct {
	GlobalGateway           string
	RegionalGateway         string
	RegionalGatewayInternal string
	ClusterGateway          string
}

type ClusterStatusPlan struct {
	Present bool
	ID      string
}

type WorkflowPlan struct {
	Steps []WorkflowStepPlan
}

type WorkflowStepPlan struct {
	Name                 string
	ConditionType        string
	SuccessReason        string
	SuccessMessage       string
	ErrorReason          string
	SkipSuccessCondition bool
}

func Compile(infra *infrav1alpha1.Sandbox0Infra) *InfraPlan {
	compiled := &InfraPlan{infra: infra, Scope: common.NewObjectScope(infra)}
	compiled.Components = compileComponents(infra)
	compiled.Services = compileServices(infra)
	compiled.Scheduler = compileSchedulerPlan(infra, compiled)
	compiled.Manager = compileManagerPlan(infra, compiled)
	compiled.Netd = compileNetdPlan(infra, compiled)
	compiled.RegionalGateway = compileRegionalGatewayPlan(infra, compiled)
	compiled.Enterprise = compileEnterpriseLicensePlan(infra, compiled)
	compiled.Validation = compileValidationPlan(infra, compiled)
	compiled.Cleanup = compileCleanupPlan(infra, compiled)
	compiled.Status = compileStatusPlan(compiled)
	compiled.Workflow = compileWorkflowPlan(compiled)
	return compiled
}

func compileComponents(infra *infrav1alpha1.Sandbox0Infra) ComponentPlan {
	enableGlobalGateway := infrav1alpha1.IsGlobalGatewayEnabled(infra)
	enableRegionalGateway := infrav1alpha1.IsRegionalGatewayEnabled(infra)
	enableSSHGateway := infrav1alpha1.IsSSHGatewayEnabled(infra)
	enableScheduler := infrav1alpha1.IsSchedulerEnabled(infra)
	enableClusterGateway := infrav1alpha1.IsClusterGatewayEnabled(infra)
	enableManager := infrav1alpha1.IsManagerEnabled(infra)
	enableStorageProxy := infrav1alpha1.IsStorageProxyEnabled(infra)
	enableDatabase := infrav1alpha1.IsDatabaseEnabled(infra)

	hasControlPlane := enableRegionalGateway || enableSSHGateway || enableScheduler
	hasDataPlane := enableClusterGateway || enableManager || enableStorageProxy

	return ComponentPlan{
		EnableGlobalGateway:       enableGlobalGateway,
		HasControlPlane:           hasControlPlane,
		HasDataPlane:              hasDataPlane,
		EnableRegionalGateway:     enableRegionalGateway,
		EnableSSHGateway:          enableSSHGateway,
		EnableScheduler:           enableScheduler,
		EnableClusterGateway:      enableClusterGateway,
		EnableManager:             enableManager,
		EnableStorageProxy:        enableStorageProxy,
		EnableFusePlugin:          enableManager,
		EnableNetd:                infrav1alpha1.IsNetdEnabled(infra),
		EnableInternalAuth:        hasControlPlane || hasDataPlane,
		EnableDatabase:            enableDatabase,
		EnableStorage:             infrav1alpha1.IsStorageEnabled(infra),
		EnableRegistry:            infrav1alpha1.IsRegistryEnabled(infra),
		EnableInitUser:            enableDatabase && initUserConsumerEnabled(infra),
		EnableClusterRegistration: hasDataPlane && infra != nil && infra.Spec.Cluster != nil && infra.Spec.ControlPlane != nil,
	}
}

func compileServices(infra *infrav1alpha1.Sandbox0Infra) ServicePlan {
	return ServicePlan{
		Manager:        compileManagerServiceReference(infra),
		Scheduler:      compileSchedulerServiceReference(infra),
		ClusterGateway: compileClusterGatewayServiceReference(infra),
		StorageProxy:   compileStorageProxyServiceReference(infra),
	}
}

func compileSchedulerServiceReference(infra *infrav1alpha1.Sandbox0Infra) ServiceReference {
	if infra == nil || infra.Name == "" || infra.Namespace == "" || !infrav1alpha1.IsSchedulerEnabled(infra) {
		return ServiceReference{}
	}

	port := common.ResolveServicePort(schedulerServiceConfig(infra), int32(schedulerHTTPPort(infra)))
	name := fmt.Sprintf("%s-scheduler", infra.Name)

	return ServiceReference{
		Name: name,
		Port: port,
		URL:  fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", name, infra.Namespace, port),
	}
}

func compileManagerServiceReference(infra *infrav1alpha1.Sandbox0Infra) ServiceReference {
	if infra == nil || infra.Name == "" || infra.Namespace == "" {
		return ServiceReference{}
	}

	port := common.ResolveServicePort(managerServiceConfig(infra), int32(managerHTTPPort(infra)))
	name := fmt.Sprintf("%s-manager", infra.Name)

	return ServiceReference{
		Name: name,
		Port: port,
		URL:  fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", name, infra.Namespace, port),
	}
}

func compileClusterGatewayServiceReference(infra *infrav1alpha1.Sandbox0Infra) ServiceReference {
	if infra == nil || infra.Name == "" {
		return ServiceReference{}
	}

	port := common.ResolveServicePort(clusterGatewayServiceConfig(infra), int32(clusterGatewayHTTPPort(infra)))
	name := fmt.Sprintf("%s-cluster-gateway", infra.Name)

	return ServiceReference{
		Name: name,
		Port: port,
		URL:  fmt.Sprintf("http://%s:%d", name, port),
	}
}

func compileStorageProxyServiceReference(infra *infrav1alpha1.Sandbox0Infra) ServiceReference {
	if infra == nil || infra.Name == "" || !infrav1alpha1.IsStorageProxyEnabled(infra) {
		return ServiceReference{}
	}

	port := common.ResolveServicePort(storageProxyServiceConfig(infra), int32(storageProxyHTTPPort(infra)))
	name := fmt.Sprintf("%s-storage-proxy", infra.Name)

	return ServiceReference{
		Name: name,
		Port: port,
		URL:  fmt.Sprintf("http://%s:%d", name, port),
	}
}

func compileSchedulerPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) SchedulerPlan {
	schedulerPlan := SchedulerPlan{
		Enabled: infrav1alpha1.IsSchedulerEnabled(infra),
		Config:  &apiconfig.SchedulerConfig{},
	}
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Scheduler == nil {
		return schedulerPlan
	}

	svc := infra.Spec.Services.Scheduler
	schedulerPlan.Replicas = svc.Replicas
	if svc.Resources != nil {
		schedulerPlan.Resources = svc.Resources.DeepCopy()
	}
	if svc.Service != nil {
		schedulerPlan.ServiceConfig = svc.Service.DeepCopy()
	}
	if svc.Config != nil {
		schedulerPlan.Config = runtimeconfig.ToScheduler(svc.Config)
	}
	if resolvedRegistry := registry.ResolveRegistryConfig(infra); resolvedRegistry != nil {
		schedulerPlan.Config.RegistryPushRegistry = resolvedRegistry.PushRegistry
		schedulerPlan.Config.RegistryPullRegistry = resolvedRegistry.PullRegistry
	}
	schedulerPlan.HomeCluster = compileSchedulerHomeCluster(infra, compiled)
	return schedulerPlan
}

func compileSchedulerHomeCluster(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) *template.Cluster {
	if infra == nil || infra.Spec.Cluster == nil || compiled == nil {
		return nil
	}
	if !compiled.Components.EnableScheduler || !compiled.Components.EnableClusterGateway || compiled.Components.EnableClusterRegistration {
		return nil
	}
	if compiled.Services.ClusterGateway.URL == "" {
		return nil
	}

	name := infra.Spec.Cluster.Name
	if name == "" {
		name = infra.Spec.Cluster.ID
	}

	return &template.Cluster{
		ClusterID:         infra.Spec.Cluster.ID,
		ClusterName:       name,
		ClusterGatewayURL: compiled.Services.ClusterGateway.URL,
		Weight:            100,
		Enabled:           true,
	}
}

func compileManagerPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) ManagerPlan {
	nodeSelector, tolerations := common.ResolveSandboxNodePlacement(infra)
	nodeSelector = withDataPlaneReadyNodeSelector(nodeSelector, compiled)
	templateStoreEnabled := clusterGatewayAuthMode(infra) != defaultClusterGatewayAuthMode
	if infrav1alpha1.IsRegionalGatewayEnabled(infra) && !infrav1alpha1.IsSchedulerEnabled(infra) {
		templateStoreEnabled = true
	}

	managerPlan := ManagerPlan{
		Enabled:               infrav1alpha1.IsManagerEnabled(infra),
		Config:                &apiconfig.ManagerConfig{},
		TemplateStoreEnabled:  templateStoreEnabled,
		NetworkPolicyProvider: "noop",
		SandboxPodPlacement: apiconfig.SandboxPodPlacementConfig{
			NodeSelector: nodeSelector,
			Tolerations:  tolerations,
		},
	}

	if compiled != nil && compiled.Components.EnableNetd {
		managerPlan.NetworkPolicyProvider = "netd"
	}
	if infra != nil {
		managerPlan.RegionID = infra.Spec.Region
		if infra.Spec.Cluster != nil {
			managerPlan.DefaultClusterID = infra.Spec.Cluster.ID
		}
		if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil {
			svc := infra.Spec.Services.Manager
			managerPlan.Replicas = svc.Replicas
			if svc.Resources != nil {
				managerPlan.Resources = svc.Resources.DeepCopy()
			}
			if svc.Service != nil {
				managerPlan.ServiceConfig = svc.Service.DeepCopy()
			}
			if svc.Config != nil {
				managerPlan.Config = runtimeconfig.ToManager(svc.Config)
			}
		}
		compileManagerRuntimeConfig(&managerPlan, infra)
	}

	return managerPlan
}

func withDataPlaneReadyNodeSelector(selector map[string]string, compiled *InfraPlan) map[string]string {
	if compiled == nil || !compiled.Components.EnableManager {
		return selector
	}
	out := cloneStringMap(selector)
	if out == nil {
		out = make(map[string]string, 1)
	}
	out[dataplane.NodeDataPlaneReadyLabel] = dataplane.ReadyLabelValue
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func compileManagerRuntimeConfig(managerPlan *ManagerPlan, infra *infrav1alpha1.Sandbox0Infra) {
	if managerPlan == nil || infra == nil {
		return
	}
	cfg := managerPlan.Config
	if cfg == nil {
		cfg = &apiconfig.ManagerConfig{}
		managerPlan.Config = cfg
	}

	cfg.TemplateStoreEnabled = managerPlan.TemplateStoreEnabled
	cfg.NetworkPolicyProvider = managerPlan.NetworkPolicyProvider
	cfg.SandboxPodPlacement = managerPlan.SandboxPodPlacement
	cfg.DefaultClusterId = managerPlan.DefaultClusterID
	cfg.RegionID = managerPlan.RegionID
	cfg.CtldEnabled = managerPlan.Enabled
	if cfg.CtldEnabled && cfg.CtldPort == 0 {
		cfg.CtldPort = 8095
	}

	if resolvedRegistry := registry.ResolveRegistryConfig(infra); resolvedRegistry != nil {
		cfg.Registry.Provider = string(resolvedRegistry.Provider)
		cfg.Registry.PushRegistry = resolvedRegistry.PushRegistry
		cfg.Registry.PullRegistry = resolvedRegistry.PullRegistry
		cfg.Registry.PullSecretName = resolvedRegistry.TargetSecretName
		cfg.Registry.Namespace = infra.Namespace
		if resolvedRegistry.SourceSecretName != "" {
			cfg.Registry.PullCredentialsFile = registryCredentialsPath
		}
		if resolvedRegistry.Provider == infrav1alpha1.RegistryProviderBuiltin {
			cfg.Registry.Builtin = &apiconfig.RegistryBuiltinConfig{
				AuthSecretName: fmt.Sprintf("%s-registry-auth", infra.Name),
				UsernameKey:    "username",
				PasswordKey:    "password",
			}
		}
	}
	if infra.Spec.Registry != nil {
		switch infra.Spec.Registry.Provider {
		case infrav1alpha1.RegistryProviderAWS:
			if infra.Spec.Registry.AWS != nil {
				cfg.Registry.AWS = &apiconfig.RegistryAWSConfig{
					Region:           infra.Spec.Registry.AWS.Region,
					RegistryID:       infra.Spec.Registry.AWS.RegistryID,
					AssumeRoleARN:    infra.Spec.Registry.AWS.AssumeRoleARN,
					ExternalID:       infra.Spec.Registry.AWS.ExternalID,
					AccessKeySecret:  infra.Spec.Registry.AWS.CredentialsSecret.Name,
					AccessKeyKey:     infra.Spec.Registry.AWS.CredentialsSecret.AccessKeyKey,
					SecretKeyKey:     infra.Spec.Registry.AWS.CredentialsSecret.SecretKeyKey,
					SessionTokenKey:  infra.Spec.Registry.AWS.CredentialsSecret.SessionTokenKey,
					RegistryOverride: infra.Spec.Registry.AWS.Registry,
				}
			}
		case infrav1alpha1.RegistryProviderGCP:
			if infra.Spec.Registry.GCP != nil {
				cfg.Registry.GCP = &apiconfig.RegistryGCPConfig{
					Registry: infra.Spec.Registry.GCP.Registry,
				}
				if infra.Spec.Registry.GCP.ServiceAccountSecret != nil {
					cfg.Registry.GCP.ServiceAccountSecret = infra.Spec.Registry.GCP.ServiceAccountSecret.Name
					cfg.Registry.GCP.ServiceAccountKey = infra.Spec.Registry.GCP.ServiceAccountSecret.Key
				}
			}
		case infrav1alpha1.RegistryProviderAzure:
			if infra.Spec.Registry.Azure != nil {
				cfg.Registry.Azure = &apiconfig.RegistryAzureConfig{
					Registry:          infra.Spec.Registry.Azure.Registry,
					CredentialsSecret: infra.Spec.Registry.Azure.CredentialsSecret.Name,
					TenantIDKey:       infra.Spec.Registry.Azure.CredentialsSecret.TenantIDKey,
					ClientIDKey:       infra.Spec.Registry.Azure.CredentialsSecret.ClientIDKey,
					ClientSecretKey:   infra.Spec.Registry.Azure.CredentialsSecret.ClientSecretKey,
				}
			}
		case infrav1alpha1.RegistryProviderAliyun:
			if infra.Spec.Registry.Aliyun != nil {
				cfg.Registry.Aliyun = &apiconfig.RegistryAliyunConfig{
					Registry:          infra.Spec.Registry.Aliyun.Registry,
					Region:            infra.Spec.Registry.Aliyun.Region,
					InstanceID:        infra.Spec.Registry.Aliyun.InstanceID,
					CredentialsSecret: infra.Spec.Registry.Aliyun.CredentialsSecret.Name,
					AccessKeyKey:      infra.Spec.Registry.Aliyun.CredentialsSecret.AccessKeyKey,
					SecretKeyKey:      infra.Spec.Registry.Aliyun.CredentialsSecret.SecretKeyKey,
				}
			}
		case infrav1alpha1.RegistryProviderHarbor:
			if infra.Spec.Registry.Harbor != nil {
				cfg.Registry.Harbor = &apiconfig.RegistryHarborConfig{
					Registry:          infra.Spec.Registry.Harbor.Registry,
					CredentialsSecret: infra.Spec.Registry.Harbor.CredentialsSecret.Name,
					UsernameKey:       infra.Spec.Registry.Harbor.CredentialsSecret.UsernameKey,
					PasswordKey:       infra.Spec.Registry.Harbor.CredentialsSecret.PasswordKey,
				}
			}
		}
	}

	storageProxyConfig := &apiconfig.StorageProxyConfig{}
	storageProxyServiceConfig := (*infrav1alpha1.ServiceNetworkConfig)(nil)
	if infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		if infra.Spec.Services.StorageProxy.Config != nil {
			storageProxyConfig = runtimeconfig.ToStorageProxy(infra.Spec.Services.StorageProxy.Config)
		}
		storageProxyServiceConfig = infra.Spec.Services.StorageProxy.Service
	}
	if infrav1alpha1.IsStorageProxyEnabled(infra) {
		cfg.ProcdConfig.StorageProxyBaseURL = fmt.Sprintf("%s-storage-proxy.%s.svc.cluster.local", infra.Name, infra.Namespace)
		cfg.ProcdConfig.StorageProxyPort = int(common.ResolveServicePort(storageProxyServiceConfig, int32(storageProxyConfig.GRPCPort)))
	} else {
		cfg.ProcdConfig.StorageProxyBaseURL = ""
		cfg.ProcdConfig.StorageProxyPort = 0
	}
	if infra.Spec.PublicExposure != nil {
		cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
	}
}

func compileNetdPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) NetdPlan {
	nodeSelector, tolerations := common.ResolveSandboxNodePlacement(infra)
	netdPlan := NetdPlan{
		Enabled:      infrav1alpha1.IsNetdEnabled(infra),
		NodeSelector: nodeSelector,
		Tolerations:  tolerations,
		Config:       &apiconfig.NetdConfig{},
	}

	if infra != nil {
		netdPlan.RegionID = infra.Spec.Region
		if infra.Spec.Cluster != nil {
			netdPlan.ClusterID = infra.Spec.Cluster.ID
		}
		if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil {
			if infra.Spec.Services.Netd.Config != nil {
				netdPlan.Config = runtimeconfig.ToNetd(infra.Spec.Services.Netd.Config)
			}
			netdPlan.RuntimeClassName = infra.Spec.Services.Netd.RuntimeClassName
		}
	}

	if explicit := netdEgressAuthResolverURL(infra); explicit != "" {
		netdPlan.EgressAuthResolverURL = explicit
		return netdPlan
	}
	if compiled != nil && compiled.Components.EnableManager {
		netdPlan.EgressAuthResolverURL = compiled.Services.Manager.URL
	}

	return netdPlan
}

func compileRegionalGatewayPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) RegionalGatewayPlan {
	plan := RegionalGatewayPlan{
		Enabled: infrav1alpha1.IsRegionalGatewayEnabled(infra),
		Config:  &apiconfig.RegionalGatewayConfig{},
	}
	if compiled != nil {
		plan.DefaultClusterGatewayURL = compiled.Services.ClusterGateway.URL
	}
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.RegionalGateway == nil {
		return plan
	}

	svc := infra.Spec.Services.RegionalGateway
	plan.Replicas = svc.Replicas
	if svc.Resources != nil {
		plan.Resources = svc.Resources.DeepCopy()
	}
	if svc.Service != nil {
		plan.ServiceConfig = svc.Service.DeepCopy()
	}
	if svc.Ingress != nil {
		plan.IngressConfig = svc.Ingress.DeepCopy()
	}
	if svc.Config != nil {
		plan.Config = runtimeconfig.ToRegionalGateway(svc.Config)
	}
	compileRegionalGatewayRuntimeConfig(&plan, infra, compiled)
	return plan
}

func compileRegionalGatewayRuntimeConfig(plan *RegionalGatewayPlan, infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) {
	if plan == nil || infra == nil {
		return
	}
	cfg := plan.Config
	if cfg == nil {
		cfg = &apiconfig.RegionalGatewayConfig{}
		plan.Config = cfg
	}

	cfg.DefaultClusterGatewayURL = plan.DefaultClusterGatewayURL
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = int(regionalGatewayHTTPPort(infra))
	}
	if compiled != nil {
		cfg.SchedulerEnabled = compiled.Components.EnableScheduler
		cfg.SchedulerURL = compiled.Services.Scheduler.URL
	}

	sshPort := int32(defaultSSHGatewayPort)
	if infra.Spec.Services != nil && infra.Spec.Services.SSHGateway != nil && infra.Spec.Services.SSHGateway.Config != nil && infra.Spec.Services.SSHGateway.Config.SSHPort != 0 {
		sshPort = int32(infra.Spec.Services.SSHGateway.Config.SSHPort)
	}
	if sshHost, advertisedPort, ok := common.ResolveSSHEndpoint(infra, sshPort); ok {
		cfg.SSHEndpointHost = sshHost
		cfg.SSHEndpointPort = int(advertisedPort)
	}
	if strings.TrimSpace(infra.Spec.Region) != "" {
		cfg.RegionID = infra.Spec.Region
	}
	if infra.Spec.PublicExposure != nil {
		cfg.PublicExposureEnabled = infra.Spec.PublicExposure.Enabled
		cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
	}
}

func compileEnterpriseLicensePlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) EnterpriseLicensePlan {
	return EnterpriseLicensePlan{
		Scheduler:       infrav1alpha1.IsSchedulerEnabled(infra),
		RegionalGateway: regionalGatewayEnterpriseLicenseRequired(infra, compiled),
		GlobalGateway:   globalGatewayEnterpriseLicenseRequired(infra),
		ClusterGateway:  clusterGatewayEnterpriseLicenseRequired(infra),
	}
}

func compileValidationPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) ValidationPlan {
	plan := ValidationPlan{}
	if infra == nil {
		return plan
	}

	if compiled != nil && compiled.Components.HasDataPlane && infra.Spec.ControlPlane != nil {
		plan.RequireControlPlanePublicKey = true
		if strings.TrimSpace(infra.Spec.ControlPlane.InternalAuthPublicKeySecret.Name) == "" {
			plan.FatalErrors = append(plan.FatalErrors, "controlPlane.internalAuthPublicKeySecret.name is required when controlPlane are enabled")
		}
	}
	if compiled != nil && compiled.Components.EnableGlobalGateway && !compiled.Components.EnableDatabase {
		plan.FatalErrors = append(plan.FatalErrors, "globalGateway requires database to be enabled")
	}
	if compiled != nil && compiled.Components.EnableSSHGateway && !compiled.Components.EnableDatabase {
		plan.FatalErrors = append(plan.FatalErrors, "sshGateway requires database to be enabled")
	}
	if compiled != nil && compiled.Components.EnableSSHGateway && !compiled.Components.EnableRegionalGateway {
		plan.FatalErrors = append(plan.FatalErrors, "sshGateway requires regionalGateway to be enabled")
	}
	if compiled != nil && compiled.Components.EnableRegionalGateway && compiled.Components.EnableClusterGateway &&
		!clusterGatewayInternalAuthEnabled(clusterGatewayAuthMode(infra)) {
		plan.FatalErrors = append(plan.FatalErrors, "regionalGateway requires clusterGateway authMode internal/both when clusterGateway is enabled")
	}
	if infra.Spec.Cluster != nil {
		if err := naming.ValidateClusterID(infra.Spec.Cluster.ID); err != nil {
			plan.FatalErrors = append(plan.FatalErrors, fmt.Sprintf("spec.cluster.id is invalid: %v", err))
		}
		if compiled == nil || !compiled.Components.HasDataPlane {
			plan.FatalErrors = append(plan.FatalErrors, "cluster configuration requires at least one data-plane service")
		}
	}
	if compiled != nil && compiled.Components.EnableNetd && netdEgressAuthEnabled(infra) && !compiled.Components.EnableManager {
		plan.FatalErrors = append(plan.FatalErrors, "netd egress auth requires manager to be enabled")
	}
	if infra.Spec.InitUser != nil && (compiled == nil || compiled.Components.EnableDatabase) && !initUserConsumerEnabled(infra) {
		plan.FatalErrors = append(plan.FatalErrors, "initUser requires globalGateway, regionalGateway.authMode=self_hosted, or clusterGateway authMode public/both")
	}

	return plan
}

func compileCleanupPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) CleanupPlan {
	cleanup := CleanupPlan{}
	if infra == nil || compiled == nil {
		return cleanup
	}

	cleanup.CleanupBuiltinDatabase = !builtinDatabaseActive(infra)
	cleanup.CleanupBuiltinStorage = !builtinStorageActive(infra)
	cleanup.CleanupBuiltinRegistry = !builtinRegistryActive(infra)

	if !compiled.Components.EnableGlobalGateway {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-global-gateway", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-global-gateway", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-global-gateway", infra.Name)),
			namespacedRef("Ingress", infra.Namespace, fmt.Sprintf("%s-global-gateway", infra.Name)),
		)
	}
	if !compiled.Components.EnableRegionalGateway {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-regional-gateway", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-regional-gateway", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-regional-gateway", infra.Name)),
			namespacedRef("Ingress", infra.Namespace, fmt.Sprintf("%s-regional-gateway", infra.Name)),
		)
	}
	if !compiled.Components.EnableSSHGateway {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-ssh-gateway", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-ssh-gateway", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-ssh-gateway", infra.Name)),
			namespacedRef("Secret", infra.Namespace, fmt.Sprintf("%s-ssh-gateway-host-key", infra.Name)),
		)
	}
	if !compiled.Components.EnableScheduler {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-scheduler", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-scheduler", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-scheduler", infra.Name)),
			namespacedRef("ServiceAccount", infra.Namespace, fmt.Sprintf("%s-scheduler", infra.Name)),
		)
	}
	if !compiled.Components.EnableClusterGateway {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-cluster-gateway", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-cluster-gateway", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-cluster-gateway", infra.Name)),
		)
	}
	if !compiled.Components.EnableManager {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-manager", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-manager", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-manager", infra.Name)),
			namespacedRef("ServiceAccount", infra.Namespace, fmt.Sprintf("%s-manager", infra.Name)),
		)
		cleanup.DeleteClusterScoped = append(cleanup.DeleteClusterScoped,
			clusterScopedRef("ClusterRole", fmt.Sprintf("%s-manager", infra.Name)),
			clusterScopedRef("ClusterRoleBinding", fmt.Sprintf("%s-manager", infra.Name)),
		)
	}
	if !compiled.Components.EnableStorageProxy {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-storage-proxy", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-storage-proxy", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-storage-proxy", infra.Name)),
			namespacedRef("ServiceAccount", infra.Namespace, fmt.Sprintf("%s-storage-proxy", infra.Name)),
		)
		cleanup.DeleteClusterScoped = append(cleanup.DeleteClusterScoped,
			clusterScopedRef("ClusterRole", fmt.Sprintf("%s-storage-proxy", infra.Name)),
			clusterScopedRef("ClusterRoleBinding", fmt.Sprintf("%s-storage-proxy", infra.Name)),
		)
	}

	cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
		namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-egress-broker", infra.Name)),
		namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-egress-broker", infra.Name)),
		namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-egress-broker", infra.Name)),
	)

	if !compiled.Components.EnableNetd {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("DaemonSet", infra.Namespace, fmt.Sprintf("%s-netd", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-netd", infra.Name)),
			namespacedRef("ServiceAccount", infra.Namespace, fmt.Sprintf("%s-netd", infra.Name)),
		)
		cleanup.DeleteClusterScoped = append(cleanup.DeleteClusterScoped,
			clusterScopedRef("ClusterRole", fmt.Sprintf("%s-netd", infra.Name)),
			clusterScopedRef("ClusterRoleBinding", fmt.Sprintf("%s-netd", infra.Name)),
		)
	}
	if !compiled.Components.EnableFusePlugin {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("DaemonSet", infra.Namespace, fmt.Sprintf("%s-ctld", infra.Name)),
			namespacedRef("DaemonSet", infra.Namespace, fmt.Sprintf("%s-k8s-plugin", infra.Name)),
			namespacedRef("ServiceAccount", infra.Namespace, fmt.Sprintf("%s-ctld", infra.Name)),
		)
		cleanup.DeleteClusterScoped = append(cleanup.DeleteClusterScoped,
			clusterScopedRef("ClusterRole", fmt.Sprintf("%s-ctld", infra.Name)),
			clusterScopedRef("ClusterRoleBinding", fmt.Sprintf("%s-ctld", infra.Name)),
		)
	}
	if cleanup.CleanupBuiltinDatabase {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("StatefulSet", infra.Namespace, fmt.Sprintf("%s-postgres", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-postgres", infra.Name)),
		)
	}
	if cleanup.CleanupBuiltinStorage {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("StatefulSet", infra.Namespace, fmt.Sprintf("%s-rustfs", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-rustfs", infra.Name)),
		)
	}
	if cleanup.CleanupBuiltinRegistry {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-registry", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-registry", infra.Name)),
			namespacedRef("Ingress", infra.Namespace, fmt.Sprintf("%s-registry", infra.Name)),
		)
	}

	return cleanup
}

func namespacedRef(kind, namespace, name string) ResourceRef {
	return ResourceRef{Kind: kind, Namespace: namespace, Name: name}
}

func clusterScopedRef(kind, name string) ResourceRef {
	return ResourceRef{Kind: kind, Name: name}
}

func compileStatusPlan(compiled *InfraPlan) StatusPlan {
	if compiled == nil {
		return StatusPlan{}
	}

	components := compiled.Components
	expected := make([]string, 0, 13)
	if components.EnableInternalAuth {
		expected = append(expected, infrav1alpha1.ConditionTypeInternalAuthReady)
	}
	if components.EnableDatabase {
		expected = append(expected, infrav1alpha1.ConditionTypeDatabaseReady)
	}
	if components.EnableStorage {
		expected = append(expected, infrav1alpha1.ConditionTypeStorageReady)
	}
	if components.EnableRegistry {
		expected = append(expected, infrav1alpha1.ConditionTypeRegistryReady)
	}
	if components.EnableGlobalGateway {
		expected = append(expected, infrav1alpha1.ConditionTypeGlobalGatewayReady)
	}
	if components.EnableRegionalGateway {
		expected = append(expected, infrav1alpha1.ConditionTypeRegionalGatewayReady)
	}
	if components.EnableSSHGateway {
		expected = append(expected, infrav1alpha1.ConditionTypeSSHGatewayReady)
	}
	if components.EnableScheduler {
		expected = append(expected, infrav1alpha1.ConditionTypeSchedulerReady)
	}
	if components.EnableClusterGateway {
		expected = append(expected, infrav1alpha1.ConditionTypeClusterGatewayReady)
	}
	if components.EnableManager {
		expected = append(expected, infrav1alpha1.ConditionTypeManagerReady)
	}
	if components.EnableStorageProxy {
		expected = append(expected, infrav1alpha1.ConditionTypeStorageProxyReady)
	}
	if components.EnableNetd {
		expected = append(expected, infrav1alpha1.ConditionTypeNetdReady)
	}
	if components.EnableFusePlugin {
		expected = append(expected, infrav1alpha1.ConditionTypeFusePluginReady)
	}
	if components.EnableClusterRegistration {
		expected = append(expected, infrav1alpha1.ConditionTypeClusterRegistered)
	}

	return StatusPlan{
		ExpectedConditions: expected,
		Endpoints:          compileEndpointStatusPlan(compiled),
		Cluster:            compileClusterStatusPlan(compiled),
		RetainedResources:  compileRetainedResourceStatusPlan(compiled),
	}
}

func compileWorkflowPlan(compiled *InfraPlan) WorkflowPlan {
	if compiled == nil {
		return WorkflowPlan{}
	}

	steps := make([]WorkflowStepPlan, 0, 20)
	appendCheckStep := func(name, conditionType, errorReason string) {
		steps = append(steps, WorkflowStepPlan{
			Name:                 name,
			ConditionType:        conditionType,
			ErrorReason:          errorReason,
			SkipSuccessCondition: true,
		})
	}
	appendSuccessStep := func(name, conditionType, successReason, successMessage, errorReason string) {
		steps = append(steps, WorkflowStepPlan{
			Name:           name,
			ConditionType:  conditionType,
			SuccessReason:  successReason,
			SuccessMessage: successMessage,
			ErrorReason:    errorReason,
		})
	}

	if compiled.Validation.RequireControlPlanePublicKey {
		appendCheckStep("control-plane-public-key", infrav1alpha1.ConditionTypeInternalAuthReady, "PublicKeySecretNotFound")
	}
	if compiled.Components.EnableInternalAuth {
		appendSuccessStep("internal-auth", infrav1alpha1.ConditionTypeInternalAuthReady, "KeysReady", "Internal auth keys are ready", "KeyGenerationFailed")
	}
	if compiled.Components.EnableDatabase {
		appendSuccessStep("database", infrav1alpha1.ConditionTypeDatabaseReady, "DatabaseReady", "Database is ready", "DatabaseFailed")
	}
	if compiled.Components.EnableStorage {
		appendSuccessStep("storage", infrav1alpha1.ConditionTypeStorageReady, "StorageReady", "Storage is ready", "StorageFailed")
	}
	if compiled.Components.EnableRegistry {
		appendSuccessStep("registry", infrav1alpha1.ConditionTypeRegistryReady, "RegistryReady", "Registry is ready", "RegistryFailed")
	}
	if compiled.Components.EnableGlobalGateway && compiled.Enterprise.GlobalGateway {
		appendCheckStep("global-gateway-enterprise-license", infrav1alpha1.ConditionTypeGlobalGatewayReady, "EnterpriseLicenseMissing")
	}
	if compiled.Components.EnableGlobalGateway {
		appendSuccessStep("global-gateway", infrav1alpha1.ConditionTypeGlobalGatewayReady, "GlobalGatewayReady", "Global gateway is ready", "GlobalGatewayFailed")
	}
	if compiled.Components.EnableInitUser && InitUserPasswordSecretRequired(compiled.infra) {
		appendSuccessStep("init-user-secret", infrav1alpha1.ConditionTypeSecretsGenerated, "InitUserSecretReady", "Init user password secret is ready", "InitUserSecretFailed")
	}
	if compiled.Components.EnableRegionalGateway && compiled.Enterprise.RegionalGateway {
		appendCheckStep("regional-gateway-enterprise-license", infrav1alpha1.ConditionTypeRegionalGatewayReady, "EnterpriseLicenseMissing")
	}
	if compiled.Components.EnableRegionalGateway {
		appendSuccessStep("regional-gateway", infrav1alpha1.ConditionTypeRegionalGatewayReady, "RegionalGatewayReady", "Edge gateway is ready", "RegionalGatewayFailed")
	}
	if compiled.Components.EnableSSHGateway {
		appendSuccessStep("ssh-gateway", infrav1alpha1.ConditionTypeSSHGatewayReady, "SSHGatewayReady", "SSH gateway is ready", "SSHGatewayFailed")
	}
	if compiled.Components.EnableScheduler && compiled.Enterprise.Scheduler {
		appendCheckStep("scheduler-enterprise-license", infrav1alpha1.ConditionTypeSchedulerReady, "EnterpriseLicenseMissing")
	}
	if compiled.Components.EnableScheduler {
		appendCheckStep("scheduler-rbac", infrav1alpha1.ConditionTypeSchedulerReady, "SchedulerRBACFailed")
		appendSuccessStep("scheduler", infrav1alpha1.ConditionTypeSchedulerReady, "SchedulerReady", "Scheduler is ready", "SchedulerFailed")
	}
	if compiled.Components.EnableClusterGateway && compiled.Enterprise.ClusterGateway {
		appendCheckStep("cluster-gateway-enterprise-license", infrav1alpha1.ConditionTypeClusterGatewayReady, "EnterpriseLicenseMissing")
	}
	if compiled.Components.EnableClusterGateway {
		appendSuccessStep("cluster-gateway", infrav1alpha1.ConditionTypeClusterGatewayReady, "ClusterGatewayReady", "Internal gateway is ready", "ClusterGatewayFailed")
	}
	if compiled.Components.EnableFusePlugin {
		appendSuccessStep("fuse-device-plugin", infrav1alpha1.ConditionTypeFusePluginReady, "FusePluginReady", "FUSE device plugin is ready", "FusePluginFailed")
	}
	if compiled.Components.EnableManager {
		appendCheckStep("manager-rbac", infrav1alpha1.ConditionTypeManagerReady, "ManagerRBACFailed")
		appendSuccessStep("manager", infrav1alpha1.ConditionTypeManagerReady, "ManagerReady", "Manager is ready", "ManagerFailed")
	}
	if compiled.Components.EnableNetd {
		appendCheckStep("netd-rbac", infrav1alpha1.ConditionTypeNetdReady, "NetdRBACFailed")
		appendSuccessStep("netd", infrav1alpha1.ConditionTypeNetdReady, "NetdReady", "netd is ready", "NetdFailed")
	}
	if compiled.Components.EnableManager {
		appendCheckStep("data-plane-node-readiness", infrav1alpha1.ConditionTypeManagerReady, "DataPlaneNodesNotReady")
		appendCheckStep("builtin-template-pods", infrav1alpha1.ConditionTypeManagerReady, "BuiltinTemplatePodsNotReady")
	}
	if compiled.Components.EnableStorageProxy {
		appendCheckStep("storage-proxy-rbac", infrav1alpha1.ConditionTypeStorageProxyReady, "StorageProxyRBACFailed")
		appendSuccessStep("storage-proxy", infrav1alpha1.ConditionTypeStorageProxyReady, "StorageProxyReady", "Storage proxy is ready", "StorageProxyFailed")
	}
	if compiled.Components.EnableClusterRegistration {
		appendSuccessStep("register-cluster", infrav1alpha1.ConditionTypeClusterRegistered, "ClusterRegistered", "Cluster registration completed", "ClusterRegistrationFailed")
	}

	return WorkflowPlan{Steps: steps}
}

func compileEndpointStatusPlan(compiled *InfraPlan) EndpointStatusPlan {
	if compiled == nil {
		return EndpointStatusPlan{}
	}

	endpoints := EndpointStatusPlan{}
	if compiled.Components.EnableGlobalGateway {
		endpoints.GlobalGateway = globalGatewayStatusURL(compiled)
	}
	if compiled.Components.EnableRegionalGateway {
		endpoints.RegionalGatewayInternal = regionalGatewayInternalStatusURL(compiled)
		endpoints.RegionalGateway = regionalGatewayExternalStatusURL(compiled)
	}
	if compiled.Components.EnableClusterGateway {
		endpoints.ClusterGateway = compiled.Services.ClusterGateway.URL
	}
	return endpoints
}

func compileClusterStatusPlan(compiled *InfraPlan) ClusterStatusPlan {
	infra := compiledInfra(compiled)
	if compiled == nil || infra == nil || infra.Spec.Cluster == nil {
		return ClusterStatusPlan{}
	}
	return ClusterStatusPlan{
		Present: true,
		ID:      infra.Spec.Cluster.ID,
	}
}

func compileRetainedResourceStatusPlan(compiled *InfraPlan) []infrav1alpha1.RetainedResourceStatus {
	infra := compiledInfra(compiled)
	if compiled == nil || infra == nil || infra.Name == "" {
		return nil
	}

	var retained []infrav1alpha1.RetainedResourceStatus
	if infra.Spec.Database != nil && !builtinDatabaseActive(infra) && databaseStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		retained = append(retained,
			common.NewRetainedResourceStatus("database", "Secret", common.BuiltinDatabaseSecretName(infra.Name)),
			common.NewRetainedResourceStatus("database", "PersistentVolumeClaim", common.BuiltinDatabasePVCName(infra.Name)),
		)
	}
	if infra.Spec.Storage != nil && !builtinStorageActive(infra) && storageStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		retained = append(retained,
			common.NewRetainedResourceStatus("storage", "Secret", common.BuiltinStorageSecretName(infra.Name)),
			common.NewRetainedResourceStatus("storage", "PersistentVolumeClaim", common.BuiltinStoragePVCName(infra.Name)),
		)
	}
	if infra.Spec.Registry != nil && !builtinRegistryActive(infra) && registryStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		retained = append(retained,
			common.NewRetainedResourceStatus("registry", "PersistentVolumeClaim", common.BuiltinRegistryPVCName(infra.Name)),
		)
	}

	return retained
}

func managerHTTPPort(infra *infrav1alpha1.Sandbox0Infra) int {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.Manager != nil &&
		infra.Spec.Services.Manager.Config != nil && infra.Spec.Services.Manager.Config.HTTPPort > 0 {
		return infra.Spec.Services.Manager.Config.HTTPPort
	}
	return defaultManagerHTTPPort
}

func managerServiceConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ServiceNetworkConfig {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.Manager != nil {
		return infra.Spec.Services.Manager.Service
	}
	return nil
}

func clusterGatewayHTTPPort(infra *infrav1alpha1.Sandbox0Infra) int {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.ClusterGateway != nil &&
		infra.Spec.Services.ClusterGateway.Config != nil && infra.Spec.Services.ClusterGateway.Config.HTTPPort > 0 {
		return infra.Spec.Services.ClusterGateway.Config.HTTPPort
	}
	return defaultClusterGatewayHTTPPort
}

func clusterGatewayServiceConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ServiceNetworkConfig {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.ClusterGateway != nil {
		return infra.Spec.Services.ClusterGateway.Service
	}
	return nil
}

func schedulerHTTPPort(infra *infrav1alpha1.Sandbox0Infra) int {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.Scheduler != nil &&
		infra.Spec.Services.Scheduler.Config != nil && infra.Spec.Services.Scheduler.Config.HTTPPort > 0 {
		return infra.Spec.Services.Scheduler.Config.HTTPPort
	}
	return 8080
}

func schedulerServiceConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ServiceNetworkConfig {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.Scheduler != nil {
		return infra.Spec.Services.Scheduler.Service
	}
	return nil
}

func storageProxyHTTPPort(infra *infrav1alpha1.Sandbox0Infra) int {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil &&
		infra.Spec.Services.StorageProxy.Config != nil && infra.Spec.Services.StorageProxy.Config.HTTPPort > 0 {
		return infra.Spec.Services.StorageProxy.Config.HTTPPort
	}
	return defaultStorageProxyHTTPPort
}

func storageProxyServiceConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ServiceNetworkConfig {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		return infra.Spec.Services.StorageProxy.Service
	}
	return nil
}

func globalGatewayServiceConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ServiceNetworkConfig {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.GlobalGateway != nil {
		return infra.Spec.Services.GlobalGateway.Service
	}
	return nil
}

func globalGatewayHTTPPort(infra *infrav1alpha1.Sandbox0Infra) int32 {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.GlobalGateway != nil &&
		infra.Spec.Services.GlobalGateway.Config != nil && infra.Spec.Services.GlobalGateway.Config.HTTPPort > 0 {
		return int32(infra.Spec.Services.GlobalGateway.Config.HTTPPort)
	}
	return 8080
}

func regionalGatewayServiceConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ServiceNetworkConfig {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil {
		return infra.Spec.Services.RegionalGateway.Service
	}
	return nil
}

func regionalGatewayHTTPPort(infra *infrav1alpha1.Sandbox0Infra) int32 {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil &&
		infra.Spec.Services.RegionalGateway.Config != nil && infra.Spec.Services.RegionalGateway.Config.HTTPPort > 0 {
		return int32(infra.Spec.Services.RegionalGateway.Config.HTTPPort)
	}
	return 8080
}

func regionalGatewayIngressConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.IngressConfig {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil {
		return infra.Spec.Services.RegionalGateway.Ingress
	}
	return nil
}

func globalGatewayConfiguredBaseURL(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.GlobalGateway != nil &&
		infra.Spec.Services.GlobalGateway.Config != nil {
		return strings.TrimSpace(infra.Spec.Services.GlobalGateway.Config.BaseURL)
	}
	return ""
}

func regionalGatewayConfiguredBaseURL(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.RegionalGateway != nil &&
		infra.Spec.Services.RegionalGateway.Config != nil {
		return strings.TrimSpace(infra.Spec.Services.RegionalGateway.Config.BaseURL)
	}
	return ""
}

func globalGatewayStatusURL(compiled *InfraPlan) string {
	infra := compiledInfra(compiled)
	if infra == nil || infra.Name == "" {
		return ""
	}
	if baseURL := globalGatewayConfiguredBaseURL(infra); baseURL != "" {
		return baseURL
	}
	serviceName := fmt.Sprintf("%s-global-gateway", infra.Name)
	servicePort := common.ResolveServicePort(globalGatewayServiceConfig(infra), globalGatewayHTTPPort(infra))
	return fmt.Sprintf("http://%s:%d", serviceName, servicePort)
}

func regionalGatewayInternalStatusURL(compiled *InfraPlan) string {
	infra := compiledInfra(compiled)
	if infra == nil || infra.Name == "" {
		return ""
	}
	serviceName := fmt.Sprintf("%s-regional-gateway", infra.Name)
	servicePort := common.ResolveServicePort(regionalGatewayServiceConfig(infra), regionalGatewayHTTPPort(infra))
	return fmt.Sprintf("http://%s:%d", serviceName, servicePort)
}

func regionalGatewayExternalStatusURL(compiled *InfraPlan) string {
	infra := compiledInfra(compiled)
	if infra == nil {
		return ""
	}
	if baseURL := regionalGatewayConfiguredBaseURL(infra); baseURL != "" {
		return baseURL
	}
	ingress := regionalGatewayIngressConfig(infra)
	if ingress == nil || !ingress.Enabled || strings.TrimSpace(ingress.Host) == "" {
		return ""
	}
	scheme := "http"
	if ingress.TLSSecret != "" {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, ingress.Host)
}

func compiledInfra(compiled *InfraPlan) *infrav1alpha1.Sandbox0Infra {
	if compiled == nil {
		return nil
	}
	return compiled.infra
}

func clusterGatewayAuthMode(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.ClusterGateway == nil {
		return defaultClusterGatewayAuthMode
	}

	mode := ""
	if infra.Spec.Services.ClusterGateway.Config != nil {
		mode = infra.Spec.Services.ClusterGateway.Config.AuthMode
	}
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		return defaultClusterGatewayAuthMode
	}
	return mode
}

func regionalGatewayAuthMode(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.RegionalGateway == nil {
		return defaultRegionalGatewayAuthMode
	}

	mode := ""
	if infra.Spec.Services.RegionalGateway.Config != nil {
		mode = infra.Spec.Services.RegionalGateway.Config.AuthMode
	}
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		return defaultRegionalGatewayAuthMode
	}
	return mode
}

func initUserConsumerEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.InitUser == nil {
		return false
	}
	if infrav1alpha1.IsGlobalGatewayEnabled(infra) {
		return true
	}
	if infrav1alpha1.IsRegionalGatewayEnabled(infra) && regionalGatewayAuthMode(infra) != "federated_global" {
		return true
	}
	if infrav1alpha1.IsClusterGatewayEnabled(infra) && clusterGatewayPublicAuthEnabled(clusterGatewayAuthMode(infra)) {
		return true
	}
	return false
}

func InitUserPasswordSecretRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.InitUser == nil {
		return false
	}
	if infrav1alpha1.IsGlobalGatewayEnabled(infra) && globalGatewayInitUserPasswordRequired(infra) {
		return true
	}
	if infrav1alpha1.IsRegionalGatewayEnabled(infra) && regionalGatewayAuthMode(infra) != "federated_global" && regionalGatewayInitUserPasswordRequired(infra) {
		return true
	}
	if infrav1alpha1.IsClusterGatewayEnabled(infra) && clusterGatewayPublicAuthEnabled(clusterGatewayAuthMode(infra)) && clusterGatewayInitUserPasswordRequired(infra) {
		return true
	}
	return false
}

func globalGatewayInitUserPasswordRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.GlobalGateway == nil || infra.Spec.Services.GlobalGateway.Config == nil {
		return true
	}
	cfg := infra.Spec.Services.GlobalGateway.Config
	return cfg.BuiltInAuth.Enabled || !hasEnabledOIDCProviders(cfg.OIDCProviders)
}

func regionalGatewayInitUserPasswordRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.RegionalGateway == nil || infra.Spec.Services.RegionalGateway.Config == nil {
		return true
	}
	cfg := infra.Spec.Services.RegionalGateway.Config
	return cfg.BuiltInAuth.Enabled || !hasEnabledOIDCProviders(cfg.OIDCProviders)
}

func clusterGatewayInitUserPasswordRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.ClusterGateway == nil || infra.Spec.Services.ClusterGateway.Config == nil {
		return true
	}
	cfg := infra.Spec.Services.ClusterGateway.Config
	return cfg.BuiltInAuth.Enabled || !hasEnabledOIDCProviders(cfg.OIDCProviders)
}

func regionalGatewayEnterpriseLicenseRequired(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) bool {
	if !infrav1alpha1.IsRegionalGatewayEnabled(infra) || infra == nil || infra.Spec.Services == nil || infra.Spec.Services.RegionalGateway == nil {
		return false
	}

	cfg := infra.Spec.Services.RegionalGateway.Config
	if cfg == nil {
		cfg = &infrav1alpha1.RegionalGatewayConfig{}
	}

	schedulerConfigured := cfg.SchedulerEnabled && strings.TrimSpace(cfg.SchedulerURL) != ""
	if !schedulerConfigured && compiled != nil {
		schedulerConfigured = compiled.Components.EnableScheduler && strings.TrimSpace(compiled.Services.Scheduler.URL) != ""
	}

	return schedulerConfigured || hasEnabledOIDCProviders(cfg.OIDCProviders)
}

func globalGatewayEnterpriseLicenseRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	if !infrav1alpha1.IsGlobalGatewayEnabled(infra) || infra == nil || infra.Spec.Services == nil || infra.Spec.Services.GlobalGateway == nil {
		return false
	}

	cfg := infra.Spec.Services.GlobalGateway.Config
	if cfg == nil {
		return false
	}

	return hasEnabledOIDCProviders(cfg.OIDCProviders)
}

func clusterGatewayEnterpriseLicenseRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	if !infrav1alpha1.IsClusterGatewayEnabled(infra) || infra == nil || infra.Spec.Services == nil || infra.Spec.Services.ClusterGateway == nil {
		return false
	}

	cfg := infra.Spec.Services.ClusterGateway.Config
	if cfg == nil {
		return false
	}

	return clusterGatewayPublicAuthEnabled(cfg.AuthMode) && hasEnabledOIDCProviders(cfg.OIDCProviders)
}

func clusterGatewayPublicAuthEnabled(mode string) bool {
	mode = strings.TrimSpace(strings.ToLower(mode))
	return mode == "public" || mode == "both"
}

func clusterGatewayInternalAuthEnabled(mode string) bool {
	mode = strings.TrimSpace(strings.ToLower(mode))
	return mode == "internal" || mode == "both"
}

func hasEnabledOIDCProviders(providers []infrav1alpha1.OIDCProviderConfig) bool {
	for _, provider := range providers {
		if provider.Enabled {
			return true
		}
	}
	return false
}

func netdEgressAuthResolverURL(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Netd == nil || infra.Spec.Services.Netd.Config == nil {
		return ""
	}
	return infra.Spec.Services.Netd.Config.EgressAuthResolverURL
}

func netdEgressAuthEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Netd == nil || infra.Spec.Services.Netd.Config == nil {
		return false
	}
	return infra.Spec.Services.Netd.Config.EgressAuthEnabled
}

func builtinDatabaseActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Database == nil || infra.Spec.Database.Type != infrav1alpha1.DatabaseTypeBuiltin {
		return false
	}
	if infra.Spec.Database.Builtin == nil {
		return true
	}
	return infra.Spec.Database.Builtin.Enabled
}

func builtinStorageActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Storage == nil || infra.Spec.Storage.Type != infrav1alpha1.StorageTypeBuiltin {
		return false
	}
	if infra.Spec.Storage.Builtin == nil {
		return true
	}
	return infra.Spec.Storage.Builtin.Enabled
}

func builtinRegistryActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Registry == nil || infra.Spec.Registry.Provider != infrav1alpha1.RegistryProviderBuiltin {
		return false
	}
	if infra.Spec.Registry.Builtin == nil {
		return true
	}
	return infra.Spec.Registry.Builtin.Enabled
}

func databaseStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.Database == nil || infra.Spec.Database.Builtin == nil || infra.Spec.Database.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.Database.Builtin.StatefulResourcePolicy
}

func storageStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.Storage == nil || infra.Spec.Storage.Builtin == nil || infra.Spec.Storage.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.Storage.Builtin.StatefulResourcePolicy
}

func registryStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.Registry == nil || infra.Spec.Registry.Builtin == nil || infra.Spec.Registry.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.Registry.Builtin.StatefulResourcePolicy
}
