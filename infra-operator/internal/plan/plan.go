package plan

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

const (
	defaultManagerHTTPPort        = 8080
	defaultClusterGatewayHTTPPort = 8443
	defaultClusterGatewayAuthMode = "internal"
)

type InfraPlan struct {
	Components      ComponentPlan
	Services        ServicePlan
	Manager         ManagerPlan
	Netd            NetdPlan
	RegionalGateway RegionalGatewayPlan
	Enterprise      EnterpriseLicensePlan
	Status          StatusPlan
}

type ComponentPlan struct {
	EnableGlobalGateway       bool
	HasControlPlane           bool
	HasDataPlane              bool
	EnableRegionalGateway     bool
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
	RequireControlPlaneConfig bool
}

type ServicePlan struct {
	Manager        ServiceReference
	ClusterGateway ServiceReference
}

type ServiceReference struct {
	Name string
	Port int32
	URL  string
}

type ManagerPlan struct {
	TemplateStoreEnabled  bool
	NetworkPolicyProvider string
	SandboxPodPlacement   apiconfig.SandboxPodPlacementConfig
	DefaultClusterID      string
	RegionID              string
}

type NetdPlan struct {
	EgressAuthResolverURL string
	RegionID              string
	ClusterID             string
	NodeSelector          map[string]string
	Tolerations           []corev1.Toleration
}

type RegionalGatewayPlan struct {
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
}

type EndpointStatusPlan struct {
	IncludeGlobalGateway      bool
	IncludeRegionalGateway    bool
	IncludeRegionalGatewayInt bool
	IncludeClusterGateway     bool
}

func Compile(infra *infrav1alpha1.Sandbox0Infra) *InfraPlan {
	compiled := &InfraPlan{}
	compiled.Components = compileComponents(infra)
	compiled.Services = compileServices(infra)
	compiled.Manager = compileManagerPlan(infra, compiled)
	compiled.Netd = compileNetdPlan(infra, compiled)
	compiled.RegionalGateway = compileRegionalGatewayPlan(compiled)
	compiled.Enterprise = compileEnterpriseLicensePlan(infra)
	compiled.Status = compileStatusPlan(compiled)
	return compiled
}

func compileComponents(infra *infrav1alpha1.Sandbox0Infra) ComponentPlan {
	enableGlobalGateway := infrav1alpha1.IsGlobalGatewayEnabled(infra)
	enableRegionalGateway := infrav1alpha1.IsRegionalGatewayEnabled(infra)
	enableScheduler := infrav1alpha1.IsSchedulerEnabled(infra)
	enableClusterGateway := infrav1alpha1.IsClusterGatewayEnabled(infra)
	enableManager := infrav1alpha1.IsManagerEnabled(infra)
	enableStorageProxy := infrav1alpha1.IsStorageProxyEnabled(infra)
	enableDatabase := infrav1alpha1.IsDatabaseEnabled(infra)

	hasControlPlane := enableRegionalGateway || enableScheduler
	hasDataPlane := enableClusterGateway || enableManager || enableStorageProxy

	return ComponentPlan{
		EnableGlobalGateway:       enableGlobalGateway,
		HasControlPlane:           hasControlPlane,
		HasDataPlane:              hasDataPlane,
		EnableRegionalGateway:     enableRegionalGateway,
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
		EnableInitUser:            enableDatabase && infra != nil && infra.Spec.InitUser != nil,
		EnableClusterRegistration: hasDataPlane && infra != nil && infra.Spec.Cluster != nil,
		RequireControlPlaneConfig: hasDataPlane && infra != nil && infra.Spec.ControlPlane != nil,
	}
}

func compileServices(infra *infrav1alpha1.Sandbox0Infra) ServicePlan {
	return ServicePlan{
		Manager:        compileManagerServiceReference(infra),
		ClusterGateway: compileClusterGatewayServiceReference(infra),
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

func compileManagerPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) ManagerPlan {
	nodeSelector, tolerations := common.ResolveSandboxNodePlacement(infra)

	managerPlan := ManagerPlan{
		TemplateStoreEnabled:  clusterGatewayAuthMode(infra) != defaultClusterGatewayAuthMode,
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
	}

	return managerPlan
}

func compileNetdPlan(infra *infrav1alpha1.Sandbox0Infra, compiled *InfraPlan) NetdPlan {
	nodeSelector, tolerations := common.ResolveSandboxNodePlacement(infra)
	netdPlan := NetdPlan{
		NodeSelector: nodeSelector,
		Tolerations:  tolerations,
	}

	if infra != nil {
		netdPlan.RegionID = infra.Spec.Region
		if infra.Spec.Cluster != nil {
			netdPlan.ClusterID = infra.Spec.Cluster.ID
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

func compileRegionalGatewayPlan(compiled *InfraPlan) RegionalGatewayPlan {
	if compiled == nil {
		return RegionalGatewayPlan{}
	}
	return RegionalGatewayPlan{
		DefaultClusterGatewayURL: compiled.Services.ClusterGateway.URL,
	}
}

func compileEnterpriseLicensePlan(infra *infrav1alpha1.Sandbox0Infra) EnterpriseLicensePlan {
	return EnterpriseLicensePlan{
		Scheduler:       infrav1alpha1.IsSchedulerEnabled(infra),
		RegionalGateway: regionalGatewayEnterpriseLicenseRequired(infra),
		GlobalGateway:   globalGatewayEnterpriseLicenseRequired(infra),
		ClusterGateway:  clusterGatewayEnterpriseLicenseRequired(infra),
	}
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
	if components.EnableInitUser {
		expected = append(expected, infrav1alpha1.ConditionTypeInitUserReady)
	}
	if components.EnableClusterRegistration {
		expected = append(expected, infrav1alpha1.ConditionTypeClusterRegistered)
	}

	return StatusPlan{
		ExpectedConditions: expected,
		Endpoints: EndpointStatusPlan{
			IncludeGlobalGateway:      components.EnableGlobalGateway,
			IncludeRegionalGateway:    components.EnableRegionalGateway,
			IncludeRegionalGatewayInt: components.EnableRegionalGateway,
			IncludeClusterGateway:     components.EnableClusterGateway,
		},
	}
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

func regionalGatewayEnterpriseLicenseRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	if !infrav1alpha1.IsRegionalGatewayEnabled(infra) || infra == nil || infra.Spec.Services == nil || infra.Spec.Services.RegionalGateway == nil {
		return false
	}

	cfg := infra.Spec.Services.RegionalGateway.Config
	if cfg == nil {
		return false
	}

	return (cfg.SchedulerEnabled && strings.TrimSpace(cfg.SchedulerURL) != "") || hasEnabledOIDCProviders(cfg.OIDCProviders)
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
