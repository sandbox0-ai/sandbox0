package plan

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

const (
	defaultManagerHTTPPort         = 8080
	defaultManagerMetricsPort      = 9090
	defaultManagerWebhookPort      = 9443
	defaultClusterGatewayHTTPPort  = 8443
	defaultClusterGatewayAuthMode  = "internal"
	defaultRegionalGatewayAuthMode = "self_hosted"
	defaultSSHGatewayPort          = 2222
	defaultStorageRuntimeHTTPPort  = 8081
	registryCredentialsPath        = "/etc/sandbox0/registry/.dockerconfigjson"
)

type InfraPlan struct {
	Scope           common.ObjectScope
	Components      ComponentPlan
	Services        ServicePlan
	Scheduler       SchedulerPlan
	Manager         ManagerPlan
	Network         NetworkPlan
	RegionalGateway RegionalGatewayPlan
	Enterprise      EnterpriseLicensePlan
	Validation      ValidationPlan
	Cleanup         CleanupPlan
	Status          StatusPlan
	Workflow        WorkflowPlan
	infra           *infrav1alpha1.Sandbox0Infra
}

type ComponentPlan struct {
	EnableGlobalGateway        bool
	HasControlPlane            bool
	HasDataPlane               bool
	EnableRegionalGateway      bool
	EnableSSHGateway           bool
	EnableScheduler            bool
	EnableClusterGateway       bool
	EnableManager              bool
	EnableStorageRuntime       bool
	EnableCtld                 bool
	EnableNetwork              bool
	EnableInternalAuth         bool
	EnableDatabase             bool
	EnableRedis                bool
	EnableCredentialVault      bool
	EnableStorage              bool
	EnableRegistry             bool
	EnableObservability        bool
	EnableClickHouse           bool
	EnableSandboxObservability bool
	EnableMetering             bool
	EnableInitUser             bool
	EnableClusterRegistration  bool
}

type ValidationPlan struct {
	FatalErrors                  []string
	RequireControlPlanePublicKey bool
}

type CleanupPlan struct {
	CleanupBuiltinDatabase        bool
	CleanupBuiltinRedis           bool
	CleanupBuiltinCredentialVault bool
	CleanupBuiltinStorage         bool
	CleanupBuiltinRegistry        bool
	CleanupBuiltinClickHouse      bool
	DeleteNamespaced              []ResourceRef
	DeleteClusterScoped           []ResourceRef
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
	ManagerStorage ServiceReference
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

type NetworkPlan struct {
	Enabled               bool
	Config                *apiconfig.NetdConfig
	EgressAuthResolverURL string
	RegionID              string
	ClusterID             string
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
	compiled.Network = compileNetworkPlan(infra, compiled)
	compiled.RegionalGateway = compileRegionalGatewayPlan(infra, compiled)
	compiled.Enterprise = compileEnterpriseLicensePlan(infra, compiled)
	compiled.Validation = compileValidationPlan(infra, compiled)
	compiled.Cleanup = compileCleanupPlan(infra, compiled)
	compiled.Status = compileStatusPlan(compiled)
	compiled.Workflow = compileWorkflowPlan(compiled)
	return compiled
}

// TeamQuotaPolicyOwner reports whether this resource owns the region Team
// Quota policy and state-plane identity.
func (compiled *InfraPlan) TeamQuotaPolicyOwner() bool {
	if compiled == nil {
		return false
	}
	return teamQuotaPolicyOwnerEnabled(compiled.infra, compiled)
}

// EffectiveTeamQuotaConfig returns an isolated runtime copy of the Team Quota
// configuration. A policy owner's create-once status identity is authoritative;
// a consumer-only resource takes the identity from spec.
func (compiled *InfraPlan) EffectiveTeamQuotaConfig() *infrav1alpha1.TeamQuotaConfig {
	if compiled == nil || compiled.infra == nil || compiled.infra.Spec.TeamQuota == nil {
		return nil
	}
	config := runtimeconfig.ResolveTeamQuotaSpec(compiled.infra)
	if compiled.TeamQuotaPolicyOwner() &&
		compiled.infra.Status.TeamQuota != nil &&
		compiled.infra.Status.TeamQuota.StateID != "" {
		config.StateID = compiled.infra.Status.TeamQuota.StateID
	}
	return config
}

func compileComponents(infra *infrav1alpha1.Sandbox0Infra) ComponentPlan {
	enableGlobalGateway := infrav1alpha1.IsGlobalGatewayEnabled(infra)
	enableRegionalGateway := infrav1alpha1.IsRegionalGatewayEnabled(infra)
	enableSSHGateway := infrav1alpha1.IsSSHGatewayEnabled(infra)
	enableScheduler := infrav1alpha1.IsSchedulerEnabled(infra)
	enableClusterGateway := infrav1alpha1.IsClusterGatewayEnabled(infra)
	enableManager := infrav1alpha1.IsManagerEnabled(infra)
	enableStorageRuntime := infrav1alpha1.IsStorageRuntimeEnabled(infra)
	enableDatabase := infrav1alpha1.IsDatabaseEnabled(infra)
	enableRedis := infrav1alpha1.IsRedisEnabled(infra)
	enableCredentialVault := infrav1alpha1.IsCredentialVaultEnabled(infra)

	hasControlPlane := enableRegionalGateway || enableSSHGateway || enableScheduler
	hasDataPlane := enableClusterGateway || enableManager

	return ComponentPlan{
		EnableGlobalGateway:        enableGlobalGateway,
		HasControlPlane:            hasControlPlane,
		HasDataPlane:               hasDataPlane,
		EnableRegionalGateway:      enableRegionalGateway,
		EnableSSHGateway:           enableSSHGateway,
		EnableScheduler:            enableScheduler,
		EnableClusterGateway:       enableClusterGateway,
		EnableManager:              enableManager,
		EnableStorageRuntime:       enableStorageRuntime,
		EnableCtld:                 enableManager,
		EnableNetwork:              infrav1alpha1.IsNetworkEnabled(infra),
		EnableInternalAuth:         hasControlPlane || hasDataPlane,
		EnableDatabase:             enableDatabase,
		EnableRedis:                enableRedis,
		EnableCredentialVault:      enableCredentialVault,
		EnableStorage:              infrav1alpha1.IsStorageEnabled(infra),
		EnableRegistry:             infrav1alpha1.IsRegistryEnabled(infra),
		EnableObservability:        common.ObservabilityBackendEnabled(infra),
		EnableClickHouse:           infrav1alpha1.IsClickHouseEnabled(infra),
		EnableSandboxObservability: infrav1alpha1.IsSandboxObservabilityEnabled(infra),
		EnableMetering:             infrav1alpha1.IsMeteringEnabled(infra),
		EnableInitUser:             enableDatabase && initUserConsumerEnabled(infra),
		EnableClusterRegistration:  hasDataPlane && infra != nil && infra.Spec.Cluster != nil && infra.Spec.ControlPlane != nil,
	}
}

func compileServices(infra *infrav1alpha1.Sandbox0Infra) ServicePlan {
	return ServicePlan{
		Manager:        compileManagerServiceReference(infra),
		Scheduler:      compileSchedulerServiceReference(infra),
		ClusterGateway: compileClusterGatewayServiceReference(infra),
		ManagerStorage: compileManagerStorageServiceReference(infra),
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

func compileManagerStorageServiceReference(infra *infrav1alpha1.Sandbox0Infra) ServiceReference {
	if infra == nil || infra.Name == "" || !infrav1alpha1.IsStorageRuntimeEnabled(infra) {
		return ServiceReference{}
	}

	port := int32(storageRuntimeHTTPPort(infra))
	name := fmt.Sprintf("%s-manager", infra.Name)

	return ServiceReference{
		Name: name,
		Port: port,
		URL:  fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", name, infra.Namespace, port),
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
	schedulerPlan.Config.RegionID = common.ResolveRegionID(infra)
	if teamQuota := compiled.EffectiveTeamQuotaConfig(); teamQuota != nil {
		schedulerPlan.Config.TeamQuotaStateID = teamQuota.StateID
	}
	if resolvedRegistry := registry.ResolveRegistryConfig(infra); resolvedRegistry != nil {
		schedulerPlan.Config.RegistryPushRegistry = resolvedRegistry.PushRegistry
		schedulerPlan.Config.RegistryPullRegistry = resolvedRegistry.PullRegistry
		schedulerPlan.Config.RegistryInternalRegistry = resolvedRegistry.InternalRegistry
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
		Enabled:               compiled != nil && compiled.Components.EnableManager,
		Config:                &apiconfig.ManagerConfig{},
		TemplateStoreEnabled:  templateStoreEnabled,
		NetworkPolicyProvider: "noop",
		SandboxPodPlacement: apiconfig.SandboxPodPlacementConfig{
			NodeSelector: nodeSelector,
			Tolerations:  tolerations,
		},
	}

	if compiled != nil && compiled.Components.EnableNetwork {
		managerPlan.NetworkPolicyProvider = "netd"
	}
	if infra != nil {
		managerPlan.RegionID = common.ResolveRegionID(infra)
		managerPlan.DefaultClusterID = common.ResolveClusterID(infra)
		if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil {
			svc := infra.Spec.Services.Manager
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
		managerPlan.Config.TeamQuotaDistributedEnforcement =
			runtimeconfig.ToTeamQuotaDistributedEnforcement(compiled.EffectiveTeamQuotaConfig())
		managerPlan.Replicas = managerReplicas(infra)
		compileManagerRuntimeConfig(&managerPlan, infra)
	}

	return managerPlan
}

func managerReplicas(infra *infrav1alpha1.Sandbox0Infra) int32 {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Manager == nil || !infrav1alpha1.IsManagerEnabled(infra) {
		return 0
	}
	return infra.Spec.Services.Manager.Replicas
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
	cfg.CtldEnabled = infrav1alpha1.IsManagerEnabled(infra)
	if cfg.CtldEnabled && cfg.CtldPort == 0 {
		cfg.CtldPort = 8095
	}

	if resolvedRegistry := registry.ResolveRegistryConfig(infra); resolvedRegistry != nil {
		cfg.Registry.Provider = string(resolvedRegistry.Provider)
		cfg.Registry.PushRegistry = resolvedRegistry.PushRegistry
		cfg.Registry.PullRegistry = resolvedRegistry.PullRegistry
		cfg.Registry.InternalRegistry = resolvedRegistry.InternalRegistry
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

	if infra.Spec.PublicExposure != nil {
		cfg.PublicRootDomain = infra.Spec.PublicExposure.RootDomain
		cfg.PublicRegionID = infra.Spec.PublicExposure.RegionID
	}
}

func compileNetworkPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) NetworkPlan {
	networkPlan := NetworkPlan{
		Enabled: infrav1alpha1.IsNetworkEnabled(infra),
		Config:  &apiconfig.NetdConfig{},
	}

	if infra != nil {
		networkPlan.RegionID = common.ResolveRegionID(infra)
		networkPlan.ClusterID = common.ResolveClusterID(infra)
		if runtimeConfig := infrav1alpha1.ResolveNetworkRuntimeConfig(infra); runtimeConfig != nil {
			networkPlan.Config = runtimeconfig.ToNetd(runtimeConfig)
		}
		networkPlan.Config.TeamQuotaDistributedEnforcement =
			runtimeconfig.ToTeamQuotaDistributedEnforcement(compiled.EffectiveTeamQuotaConfig())
	}

	if explicit := netdEgressAuthResolverURL(infra); explicit != "" {
		networkPlan.EgressAuthResolverURL = explicit
		return networkPlan
	}
	if compiled != nil && compiled.Components.EnableManager {
		networkPlan.EgressAuthResolverURL = compiled.Services.Manager.URL
	}

	return networkPlan
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
		if infra != nil {
			plan.Config.TeamQuota = runtimeconfig.ToTeamQuota(compiled.EffectiveTeamQuotaConfig())
			runtimeconfig.SetTeamQuotaOwnerVersion(&plan.Config.TeamQuota, infra)
		}
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
	plan.Config.TeamQuota = runtimeconfig.ToTeamQuota(compiled.EffectiveTeamQuotaConfig())
	runtimeconfig.SetTeamQuotaOwnerVersion(&plan.Config.TeamQuota, infra)
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
	cfg.RegionID = common.ResolveRegionID(infra)
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
	if compiled != nil && compiled.Components.EnableStorageRuntime && !compiled.Components.EnableManager {
		plan.FatalErrors = append(plan.FatalErrors, "storage.runtime requires services.manager to be enabled")
	}
	if compiled != nil && compiled.Components.EnableStorageRuntime && compiled.Manager.Replicas < 1 {
		plan.FatalErrors = append(plan.FatalErrors, "manager replicas must be at least 1 when the storage API is enabled")
	}
	if compiled != nil {
		plan.FatalErrors = append(plan.FatalErrors, managerServicePortValidationErrors(infra, compiled)...)
	}
	if compiled != nil && compiled.Components.EnableNetwork && !compiled.Components.EnableCtld {
		plan.FatalErrors = append(plan.FatalErrors, "network requires services.manager to be enabled")
	}
	if compiled != nil && compiled.Components.EnableNetwork && netdEgressAuthEnabled(infra) && !compiled.Components.EnableManager {
		plan.FatalErrors = append(plan.FatalErrors, "network egress auth requires manager to be enabled")
	}
	if compiled != nil && compiled.Components.EnableSandboxObservability && !compiled.Components.EnableClickHouse {
		plan.FatalErrors = append(plan.FatalErrors, "sandboxObservability backend clickhouse requires spec.clickHouse type builtin or external")
	}
	if infrav1alpha1.IsMeteringEnabled(infra) {
		if compiled != nil && !compiled.Components.EnableClickHouse {
			plan.FatalErrors = append(plan.FatalErrors, "metering requires spec.clickHouse type builtin or external")
		}
	}
	if infra.Spec.InitUser != nil && (compiled == nil || compiled.Components.EnableDatabase) && !initUserConsumerEnabled(infra) {
		plan.FatalErrors = append(plan.FatalErrors, "initUser requires globalGateway, regionalGateway.authMode=self_hosted, or clusterGateway authMode public/both")
	}
	if publicIdentityOverloadGuardEnabled(infra, compiled) &&
		(compiled == nil || !compiled.Components.EnableRedis) {
		plan.FatalErrors = append(
			plan.FatalErrors,
			"public identity overload guard requires spec.redis type builtin or external",
		)
	}
	teamQuotaOwner := teamQuotaPolicyOwnerEnabled(infra, compiled)
	teamQuotaPostgresConsumer := teamQuotaPostgresConsumerEnabled(infra, compiled)
	teamQuotaDistributedConsumer := teamQuotaDistributedConsumerEnabled(infra, compiled)
	if teamQuotaOwner {
		plan.FatalErrors = append(plan.FatalErrors, teamQuotaOwnerValidationErrors(infra.Spec.TeamQuota)...)
	}
	if teamQuotaPostgresConsumer {
		plan.FatalErrors = append(
			plan.FatalErrors,
			teamQuotaStateIdentityValidationErrors(infra, teamQuotaOwner)...,
		)
	}
	if teamQuotaDistributedConsumer {
		plan.FatalErrors = append(plan.FatalErrors, teamQuotaDistributedValidationErrors(infra.Spec.TeamQuota)...)
	}
	if teamQuotaPostgresConsumer && common.ResolveRegionID(infra) == "" {
		plan.FatalErrors = append(
			plan.FatalErrors,
			"Team Quota consumers require a non-empty region ID from spec.region or spec.publicExposure.regionId",
		)
	}
	if teamQuotaPostgresConsumer && !teamQuotaOwner && infra.Spec.ControlPlane == nil {
		plan.FatalErrors = append(
			plan.FatalErrors,
			"Team Quota consumers require a regional-gateway or fullmode cluster-gateway policy owner, or spec.controlPlane for an external region policy owner",
		)
	}
	if teamQuotaPostgresConsumer && (compiled == nil || !compiled.Components.EnableDatabase) {
		plan.FatalErrors = append(plan.FatalErrors, "Team Quota consumers require spec.database type builtin or external")
	}
	if teamQuotaPostgresConsumer &&
		!teamQuotaOwner &&
		infra.Spec.ControlPlane != nil &&
		(infra.Spec.Database == nil || infra.Spec.Database.Type != infrav1alpha1.DatabaseTypeExternal) {
		plan.FatalErrors = append(
			plan.FatalErrors,
			"consumer-only Team Quota services require spec.database.type external so they share the region PostgreSQL",
		)
	}
	if teamQuotaDistributedConsumer && (compiled == nil || !compiled.Components.EnableRedis) {
		plan.FatalErrors = append(plan.FatalErrors, "Team Quota distributed consumers require spec.redis type builtin or external")
	}
	if teamQuotaDistributedConsumer &&
		!teamQuotaOwner &&
		infra.Spec.ControlPlane != nil &&
		(infra.Spec.Redis == nil || infra.Spec.Redis.Type != infrav1alpha1.RedisTypeExternal) {
		plan.FatalErrors = append(
			plan.FatalErrors,
			"consumer-only distributed Team Quota services require spec.redis.type external so they share the region Redis",
		)
	}

	return plan
}

func teamQuotaPolicyOwnerEnabled(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) bool {
	if infra == nil || compiled == nil {
		return false
	}
	if compiled.Components.EnableRegionalGateway {
		return true
	}
	return compiled.Components.EnableClusterGateway &&
		infra.Spec.ControlPlane == nil &&
		clusterGatewayPublicAuthEnabled(clusterGatewayAuthMode(infra))
}

func teamQuotaPostgresConsumerEnabled(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) bool {
	if infra == nil || compiled == nil {
		return false
	}
	return teamQuotaDistributedConsumerEnabled(infra, compiled) ||
		compiled.Components.EnableScheduler
}

func teamQuotaDistributedConsumerEnabled(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) bool {
	if infra == nil || compiled == nil {
		return false
	}
	if compiled.Components.EnableRegionalGateway ||
		compiled.Components.EnableManager ||
		compiled.Components.EnableSSHGateway ||
		compiled.Components.EnableStorageRuntime ||
		compiled.Components.EnableCtld ||
		compiled.Components.EnableNetwork {
		return true
	}
	return compiled.Components.EnableClusterGateway
}

func teamQuotaOwnerValidationErrors(config *infrav1alpha1.TeamQuotaConfig) []string {
	requiredMessage := fmt.Sprintf(
		"region Team Quota owner requires spec.teamQuota with one complete policy for all %d keys",
		len(coreteamquota.Keys()),
	)
	if config == nil || len(config.Defaults) != len(coreteamquota.Keys()) {
		return []string{requiredMessage}
	}
	seen := make(map[coreteamquota.Key]bool, len(config.Defaults))
	for _, policy := range config.Defaults {
		key := coreteamquota.Key(policy.Key)
		if !coreteamquota.KnownKey(key) || seen[key] {
			return []string{requiredMessage}
		}
		seen[key] = true
		if err := validateTeamQuotaPolicy(policy); err != nil {
			return []string{fmt.Sprintf("spec.teamQuota default policy %q is invalid: %v", policy.Key, err)}
		}
	}
	for _, key := range coreteamquota.Keys() {
		if !seen[key] {
			return []string{requiredMessage}
		}
	}
	return nil
}

func teamQuotaStateIdentityValidationErrors(
	infra *infrav1alpha1.Sandbox0Infra,
	policyOwner bool,
) []string {
	var validationErrors []string
	config := infra.Spec.TeamQuota
	if !policyOwner {
		if config == nil || !validTeamQuotaStateID(config.StateID) {
			validationErrors = append(
				validationErrors,
				"Team Quota consumers require spec.teamQuota.stateId to be a canonical UUID v4 copied from the region owner's status.teamQuota.stateId",
			)
		}
		if config != nil && len(config.Defaults) != 0 {
			validationErrors = append(
				validationErrors,
				"consumer-only Team Quota configuration must omit spec.teamQuota.defaults",
			)
		}
		return validationErrors
	}

	statusStateID := ""
	if infra.Status.TeamQuota != nil {
		statusStateID = infra.Status.TeamQuota.StateID
	}
	if !validTeamQuotaStateID(statusStateID) {
		validationErrors = append(
			validationErrors,
			"region Team Quota owner requires a canonical UUID v4 in status.teamQuota.stateId initialized by infra-operator",
		)
	}
	if config != nil && config.StateID != "" {
		if !validTeamQuotaStateID(config.StateID) {
			validationErrors = append(
				validationErrors,
				"region Team Quota owner spec.teamQuota.stateId recovery input must be a canonical UUID v4",
			)
		} else if validTeamQuotaStateID(statusStateID) && config.StateID != statusStateID {
			validationErrors = append(
				validationErrors,
				"region Team Quota owner spec.teamQuota.stateId recovery input must match the immutable status.teamQuota.stateId",
			)
		}
	}
	return validationErrors
}

func validTeamQuotaStateID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil &&
		parsed.Version() == 4 &&
		parsed.String() == value
}

func teamQuotaDistributedValidationErrors(config *infrav1alpha1.TeamQuotaConfig) []string {
	if config == nil {
		return nil
	}
	if config.DistributedEnforcement.PolicyCacheTTL.Duration < 0 {
		return []string{"spec.teamQuota.distributedEnforcement.policyCacheTtl must be non-negative"}
	}
	leaseTTL := config.DistributedEnforcement.LeaseTTL.Duration
	if leaseTTL == 0 {
		leaseTTL = 15 * time.Second
	}
	renewInterval := config.DistributedEnforcement.RenewInterval.Duration
	if renewInterval == 0 {
		renewInterval = 5 * time.Second
	}
	if leaseTTL <= 0 {
		return []string{"spec.teamQuota.distributedEnforcement.leaseTtl must be positive"}
	}
	if renewInterval <= 0 {
		return []string{"spec.teamQuota.distributedEnforcement.renewInterval must be positive"}
	}
	if leaseTTL%time.Millisecond != 0 {
		return []string{"spec.teamQuota.distributedEnforcement.leaseTtl must use whole milliseconds"}
	}
	if renewInterval%time.Millisecond != 0 {
		return []string{"spec.teamQuota.distributedEnforcement.renewInterval must use whole milliseconds"}
	}
	if renewInterval > (leaseTTL-1)/2 {
		return []string{"spec.teamQuota.distributedEnforcement.renewInterval doubled must be less than leaseTtl"}
	}
	return nil
}

func validateTeamQuotaPolicy(policy infrav1alpha1.TeamQuotaPolicyConfig) error {
	corePolicy := coreteamquota.Policy{
		Key:  coreteamquota.Key(policy.Key),
		Kind: coreteamquota.Kind(policy.Kind),
	}
	switch corePolicy.Kind {
	case coreteamquota.KindCapacity, coreteamquota.KindConcurrency:
		if policy.Limit == nil {
			return fmt.Errorf("%s policy requires limit", corePolicy.Kind)
		}
		corePolicy.Limit = *policy.Limit
		if policy.Tokens != nil || policy.Interval != nil || policy.Burst != nil {
			return fmt.Errorf("%s policy must not set rate fields", corePolicy.Kind)
		}
	case coreteamquota.KindRate:
		if policy.Tokens == nil || policy.Interval == nil || policy.Burst == nil {
			return fmt.Errorf("rate policy requires tokens, interval, and burst")
		}
		if policy.Limit != nil {
			return fmt.Errorf("rate policy must not set limit")
		}
		intervalMillis, err := coreteamquota.RateIntervalMillis(policy.Interval.Duration)
		if err != nil {
			return err
		}
		corePolicy.Tokens = *policy.Tokens
		corePolicy.IntervalMillis = intervalMillis
		corePolicy.Burst = *policy.Burst
	default:
		return fmt.Errorf("unknown kind %q", policy.Kind)
	}
	return corePolicy.Validate()
}

func managerServicePortValidationErrors(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) []string {
	if infra == nil || compiled == nil || !compiled.Components.EnableManager {
		return nil
	}

	ports := []struct {
		field string
		port  int32
	}{
		{field: "services.manager.service.port", port: common.ResolveServicePort(managerServiceConfig(infra), int32(managerHTTPPort(infra)))},
		{field: "services.manager.config.metricsPort", port: int32(managerMetricsPort(infra))},
		{field: "services.manager.config.webhookPort", port: int32(managerWebhookPort(infra))},
	}
	if compiled.Components.EnableStorageRuntime {
		ports = append(ports, struct {
			field string
			port  int32
		}{field: "storage.runtime.httpPort", port: int32(storageRuntimeHTTPPort(infra))})
	}

	seen := make(map[int32]string, len(ports))
	errors := make([]string, 0)
	for _, entry := range ports {
		if previous, exists := seen[entry.port]; exists {
			errors = append(errors, fmt.Sprintf("manager Service port %d is configured by both %s and %s; use distinct ports", entry.port, previous, entry.field))
			continue
		}
		seen[entry.port] = entry.field
	}
	return errors
}

func compileCleanupPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) CleanupPlan {
	cleanup := CleanupPlan{}
	if infra == nil || compiled == nil {
		return cleanup
	}

	cleanup.CleanupBuiltinDatabase = !builtinDatabaseActive(infra)
	cleanup.CleanupBuiltinRedis = !builtinRedisActive(infra)
	cleanup.CleanupBuiltinCredentialVault = !builtinCredentialVaultActive(infra)
	cleanup.CleanupBuiltinStorage = !builtinStorageActive(infra)
	cleanup.CleanupBuiltinRegistry = !builtinRegistryActive(infra)
	cleanup.CleanupBuiltinClickHouse = !builtinClickHouseActive(infra)

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
	cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
		namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-function-gateway", infra.Name)),
		namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-function-gateway", infra.Name)),
		namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-function-gateway", infra.Name)),
		namespacedRef("Ingress", infra.Namespace, fmt.Sprintf("%s-function-gateway", infra.Name)),
	)
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
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-manager-storage", infra.Name)),
			namespacedRef("ServiceAccount", infra.Namespace, fmt.Sprintf("%s-manager", infra.Name)),
		)
		cleanup.DeleteClusterScoped = append(cleanup.DeleteClusterScoped,
			clusterScopedRef("ClusterRole", fmt.Sprintf("%s-manager", infra.Name)),
			clusterScopedRef("ClusterRoleBinding", fmt.Sprintf("%s-manager", infra.Name)),
		)
	}
	cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
		namespacedRef("Deployment", infra.Namespace, fmt.Sprintf("%s-egress-broker", infra.Name)),
		namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-egress-broker", infra.Name)),
		namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-egress-broker", infra.Name)),
	)

	if !compiled.Components.EnableCtld {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("DaemonSet", infra.Namespace, fmt.Sprintf("%s-ctld-a", infra.Name)),
			namespacedRef("DaemonSet", infra.Namespace, fmt.Sprintf("%s-ctld-b", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-ctld-network-metrics", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-ctld", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-netd", infra.Name)),
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
	if cleanup.CleanupBuiltinCredentialVault {
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("StatefulSet", infra.Namespace, fmt.Sprintf("%s-openbao", infra.Name)),
			namespacedRef("Service", infra.Namespace, fmt.Sprintf("%s-openbao", infra.Name)),
			namespacedRef("ConfigMap", infra.Namespace, fmt.Sprintf("%s-openbao", infra.Name)),
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
	if !compiled.Components.EnableObservability || !common.ManagedObservabilityCollectorEnabled(infra) {
		collectorName := common.ManagedObservabilityCollectorName(infra.Name)
		agentName := fmt.Sprintf("%s-otel-agent", infra.Name)
		cleanup.DeleteNamespaced = append(cleanup.DeleteNamespaced,
			namespacedRef("Deployment", infra.Namespace, collectorName),
			namespacedRef("DaemonSet", infra.Namespace, agentName),
			namespacedRef("Service", infra.Namespace, collectorName),
			namespacedRef("ServiceAccount", infra.Namespace, collectorName),
			namespacedRef("Secret", infra.Namespace, fmt.Sprintf("%s-config", collectorName)),
			namespacedRef("Secret", infra.Namespace, fmt.Sprintf("%s-config", agentName)),
		)
		cleanup.DeleteClusterScoped = append(cleanup.DeleteClusterScoped,
			clusterScopedRef("ClusterRole", collectorName),
			clusterScopedRef("ClusterRoleBinding", collectorName),
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
	if components.EnableRedis {
		expected = append(expected, infrav1alpha1.ConditionTypeRedisReady)
	}
	if components.EnableCredentialVault {
		expected = append(expected, infrav1alpha1.ConditionTypeCredentialVaultReady)
	}
	if components.EnableStorage {
		expected = append(expected, infrav1alpha1.ConditionTypeStorageReady)
	}
	if components.EnableRegistry {
		expected = append(expected, infrav1alpha1.ConditionTypeRegistryReady)
	}
	if components.EnableObservability {
		expected = append(expected, infrav1alpha1.ConditionTypeObservabilityReady)
	}
	if components.EnableClickHouse {
		expected = append(expected, infrav1alpha1.ConditionTypeClickHouseReady)
	}
	if components.EnableMetering {
		expected = append(expected, infrav1alpha1.ConditionTypeMeteringReady)
	}
	if components.EnableSandboxObservability {
		expected = append(expected, infrav1alpha1.ConditionTypeSandboxObservabilityReady)
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
	if components.EnableStorageRuntime {
		expected = append(expected, infrav1alpha1.ConditionTypeStorageRuntimeReady)
	}
	if components.EnableNetwork {
		expected = append(expected, infrav1alpha1.ConditionTypeNetworkReady)
	}
	if components.EnableCtld {
		expected = append(expected, infrav1alpha1.ConditionTypeCtldReady)
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
	appendClusterGatewaySteps := func() {
		if compiled.Components.EnableClusterGateway && compiled.Enterprise.ClusterGateway {
			appendCheckStep("cluster-gateway-enterprise-license", infrav1alpha1.ConditionTypeClusterGatewayReady, "EnterpriseLicenseMissing")
		}
		if compiled.Components.EnableClusterGateway {
			appendSuccessStep("cluster-gateway", infrav1alpha1.ConditionTypeClusterGatewayReady, "ClusterGatewayReady", "Internal gateway is ready", "ClusterGatewayFailed")
		}
	}
	clusterGatewayOwnsTeamQuota := compiled.Components.EnableClusterGateway &&
		!compiled.Components.EnableRegionalGateway &&
		teamQuotaPolicyOwnerEnabled(compiled.infra, compiled)

	if compiled.Validation.RequireControlPlanePublicKey {
		appendCheckStep("control-plane-public-key", infrav1alpha1.ConditionTypeInternalAuthReady, "PublicKeySecretNotFound")
	}
	if compiled.Components.EnableInternalAuth {
		appendSuccessStep("internal-auth", infrav1alpha1.ConditionTypeInternalAuthReady, "KeysReady", "Internal auth keys are ready", "KeyGenerationFailed")
	}
	if compiled.Components.EnableDatabase {
		appendSuccessStep("database", infrav1alpha1.ConditionTypeDatabaseReady, "DatabaseReady", "Database is ready", "DatabaseFailed")
	}
	if compiled.Components.EnableRedis {
		appendSuccessStep("redis", infrav1alpha1.ConditionTypeRedisReady, "RedisReady", "Redis is ready", "RedisFailed")
	}
	if compiled.Components.EnableCredentialVault {
		appendSuccessStep("credential-store", infrav1alpha1.ConditionTypeCredentialVaultReady, "CredentialVaultReady", "Credential vault is ready", "CredentialVaultFailed")
	}
	if compiled.Components.EnableStorage {
		appendSuccessStep("storage", infrav1alpha1.ConditionTypeStorageReady, "StorageReady", "Storage is ready", "StorageFailed")
	}
	if compiled.Components.EnableRegistry {
		appendSuccessStep("registry", infrav1alpha1.ConditionTypeRegistryReady, "RegistryReady", "Registry is ready", "RegistryFailed")
	}
	if compiled.Components.EnableObservability {
		appendSuccessStep("observability", infrav1alpha1.ConditionTypeObservabilityReady, "ObservabilityReady", "Observability backend integration is ready", "ObservabilityFailed")
	}
	if compiled.Components.EnableClickHouse {
		appendSuccessStep("clickhouse", infrav1alpha1.ConditionTypeClickHouseReady, "ClickHouseReady", "ClickHouse is ready", "ClickHouseFailed")
	}
	if compiled.Components.EnableMetering {
		appendSuccessStep("metering", infrav1alpha1.ConditionTypeMeteringReady, "MeteringReady", "Metering backend is ready", "MeteringFailed")
	}
	if compiled.Components.EnableSandboxObservability {
		appendSuccessStep("sandbox-observability", infrav1alpha1.ConditionTypeSandboxObservabilityReady, "SandboxObservabilityReady", "Sandbox observability backend is ready", "SandboxObservabilityFailed")
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
	if clusterGatewayOwnsTeamQuota {
		// Fullmode cluster-gateway establishes the region state identity before
		// validate-only data-plane and SSH consumers become ready.
		appendClusterGatewaySteps()
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
	if compiled.Components.EnableManager {
		appendCheckStep("manager-rbac", infrav1alpha1.ConditionTypeManagerReady, "ManagerRBACFailed")
		appendSuccessStep("manager", infrav1alpha1.ConditionTypeManagerReady, "ManagerReady", "Manager is ready", "ManagerFailed")
	}
	if !clusterGatewayOwnsTeamQuota {
		appendClusterGatewaySteps()
	}
	if compiled.Components.EnableStorageRuntime {
		appendSuccessStep("storage-runtime-ready", infrav1alpha1.ConditionTypeStorageRuntimeReady, "StorageRuntimeReady", "Manager storage API is ready", "StorageRuntimeFailed")
	}
	if compiled.Components.EnableCtld {
		appendCheckStep("ctld", infrav1alpha1.ConditionTypeCtldReady, "CtldFailed")
	}
	if compiled.Components.EnableCtld {
		appendSuccessStep("ctld-ready", infrav1alpha1.ConditionTypeCtldReady, "CtldReady", "ctld is ready", "CtldFailed")
	}
	if compiled.Components.EnableNetwork {
		appendSuccessStep("network-ready", infrav1alpha1.ConditionTypeNetworkReady, "NetworkReady", "ctld network runtime is ready", "NetworkFailed")
	}
	if compiled.Components.EnableManager {
		appendCheckStep("data-plane-node-readiness", infrav1alpha1.ConditionTypeManagerReady, "DataPlaneNodesNotReady")
		appendCheckStep("builtin-template-pods", infrav1alpha1.ConditionTypeManagerReady, "BuiltinTemplatePodsNotReady")
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
	if infra.Spec.CredentialVault != nil && !builtinCredentialVaultActive(infra) && credentialVaultStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		retained = append(retained,
			common.NewRetainedResourceStatus("credential-vault", "Secret", common.BuiltinCredentialVaultSecretName(infra.Name)),
			common.NewRetainedResourceStatus("credential-vault", "Secret", common.BuiltinCredentialVaultManagerTokenSecretName(infra.Name)),
			common.NewRetainedResourceStatus("credential-vault", "PersistentVolumeClaim", common.BuiltinCredentialVaultPVCName(infra.Name)),
		)
	}
	if infra.Spec.Registry != nil && !builtinRegistryActive(infra) && registryStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		retained = append(retained,
			common.NewRetainedResourceStatus("registry", "PersistentVolumeClaim", common.BuiltinRegistryPVCName(infra.Name)),
		)
	}
	if infra.Spec.ClickHouse != nil && !builtinClickHouseActive(infra) && clickHouseStatefulResourcePolicy(infra) == infrav1alpha1.BuiltinStatefulResourcePolicyRetain {
		retained = append(retained,
			common.NewRetainedResourceStatus("clickhouse", "Secret", fmt.Sprintf("%s-clickhouse-credentials", infra.Name)),
			common.NewRetainedResourceStatus("clickhouse", "PersistentVolumeClaim", fmt.Sprintf("%s-clickhouse-data", infra.Name)),
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

func managerMetricsPort(infra *infrav1alpha1.Sandbox0Infra) int {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.Manager != nil &&
		infra.Spec.Services.Manager.Config != nil && infra.Spec.Services.Manager.Config.MetricsPort > 0 {
		return infra.Spec.Services.Manager.Config.MetricsPort
	}
	return defaultManagerMetricsPort
}

func managerWebhookPort(infra *infrav1alpha1.Sandbox0Infra) int {
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.Manager != nil &&
		infra.Spec.Services.Manager.Config != nil && infra.Spec.Services.Manager.Config.WebhookPort > 0 {
		return infra.Spec.Services.Manager.Config.WebhookPort
	}
	return defaultManagerWebhookPort
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

func storageRuntimeHTTPPort(infra *infrav1alpha1.Sandbox0Infra) int {
	if infra != nil && infra.Spec.Storage != nil && infra.Spec.Storage.Runtime != nil {
		if infra.Spec.Storage.Runtime.HTTPPort > 0 {
			return infra.Spec.Storage.Runtime.HTTPPort
		}
		return defaultStorageRuntimeHTTPPort
	}
	return defaultStorageRuntimeHTTPPort
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

func publicIdentityOverloadGuardEnabled(
	infra *infrav1alpha1.Sandbox0Infra,
	compiled *InfraPlan,
) bool {
	if infra == nil || compiled == nil {
		return false
	}
	if compiled.Components.EnableGlobalGateway {
		return true
	}
	if compiled.Components.EnableRegionalGateway &&
		regionalGatewayAuthMode(infra) != "federated_global" {
		return true
	}
	return compiled.Components.EnableClusterGateway &&
		clusterGatewayPublicAuthEnabled(clusterGatewayAuthMode(infra))
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
	if infra == nil || !infrav1alpha1.IsClusterGatewayEnabled(infra) {
		return false
	}
	if infrav1alpha1.IsSandboxAuditEnabled(infra) {
		return true
	}
	if infra.Spec.Services == nil || infra.Spec.Services.ClusterGateway == nil {
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
	cfg := infrav1alpha1.ResolveNetworkRuntimeConfig(infra)
	if cfg == nil {
		return ""
	}
	return cfg.EgressAuthResolverURL
}

func netdEgressAuthEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	cfg := infrav1alpha1.ResolveNetworkRuntimeConfig(infra)
	if cfg == nil {
		return false
	}
	return cfg.EgressAuthEnabled
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

func builtinRedisActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Redis == nil {
		return false
	}
	if infra.Spec.Redis.Type != "" && infra.Spec.Redis.Type != infrav1alpha1.RedisTypeBuiltin {
		return false
	}
	if infra.Spec.Redis.Builtin == nil {
		return true
	}
	return infra.Spec.Redis.Builtin.Enabled
}

func builtinCredentialVaultActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.CredentialVault == nil {
		return false
	}
	if infra.Spec.CredentialVault.Type != "" && infra.Spec.CredentialVault.Type != infrav1alpha1.CredentialVaultTypeBuiltin {
		return false
	}
	if infra.Spec.CredentialVault.Builtin == nil {
		return true
	}
	return infra.Spec.CredentialVault.Builtin.Enabled
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

func builtinClickHouseActive(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil {
		return false
	}
	if infra.Spec.ClickHouse != nil {
		if infra.Spec.ClickHouse.Type != infrav1alpha1.ClickHouseTypeBuiltin {
			return false
		}
		if infra.Spec.ClickHouse.Builtin == nil {
			return true
		}
		return infra.Spec.ClickHouse.Builtin.Enabled
	}
	if infra.Spec.SandboxObservability == nil || infra.Spec.SandboxObservability.Type != infrav1alpha1.SandboxObservabilityTypeBuiltin {
		return false
	}
	if infra.Spec.SandboxObservability.Builtin == nil {
		return true
	}
	return infra.Spec.SandboxObservability.Builtin.Enabled
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

func credentialVaultStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.CredentialVault == nil || infra.Spec.CredentialVault.Builtin == nil || infra.Spec.CredentialVault.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.CredentialVault.Builtin.StatefulResourcePolicy
}

func registryStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil || infra.Spec.Registry == nil || infra.Spec.Registry.Builtin == nil || infra.Spec.Registry.Builtin.StatefulResourcePolicy == "" {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	return infra.Spec.Registry.Builtin.StatefulResourcePolicy
}

func clickHouseStatefulResourcePolicy(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinStatefulResourcePolicy {
	if infra == nil {
		return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
	}
	if infra.Spec.ClickHouse != nil && infra.Spec.ClickHouse.Builtin != nil && infra.Spec.ClickHouse.Builtin.StatefulResourcePolicy != "" {
		return infra.Spec.ClickHouse.Builtin.StatefulResourcePolicy
	}
	if infra.Spec.SandboxObservability != nil && infra.Spec.SandboxObservability.Builtin != nil &&
		infra.Spec.SandboxObservability.Builtin.ClickHouse.StatefulResourcePolicy != "" {
		return infra.Spec.SandboxObservability.Builtin.ClickHouse.StatefulResourcePolicy
	}
	return infrav1alpha1.BuiltinStatefulResourcePolicyRetain
}
