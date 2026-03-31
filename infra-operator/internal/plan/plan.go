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
	defaultManagerHTTPPort         = 8080
	defaultClusterGatewayHTTPPort  = 8443
	defaultClusterGatewayAuthMode  = "internal"
	defaultRegionalGatewayAuthMode = "self_hosted"
)

type InfraPlan struct {
	Components      ComponentPlan
	Services        ServicePlan
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
	compiled := &InfraPlan{infra: infra}
	compiled.Components = compileComponents(infra)
	compiled.Services = compileServices(infra)
	compiled.Manager = compileManagerPlan(infra, compiled)
	compiled.Netd = compileNetdPlan(infra, compiled)
	compiled.RegionalGateway = compileRegionalGatewayPlan(compiled)
	compiled.Enterprise = compileEnterpriseLicensePlan(infra)
	compiled.Validation = compileValidationPlan(infra, compiled)
	compiled.Cleanup = compileCleanupPlan(infra, compiled)
	compiled.Status = compileStatusPlan(compiled)
	compiled.Workflow = compileWorkflowPlan(compiled)
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
		EnableInitUser:            enableDatabase && initUserConsumerEnabled(infra),
		EnableClusterRegistration: hasDataPlane && infra != nil && infra.Spec.Cluster != nil && infra.Spec.ControlPlane != nil,
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
	if infra.Spec.Cluster != nil && (compiled == nil || !compiled.Components.HasDataPlane) {
		plan.FatalErrors = append(plan.FatalErrors, "cluster configuration requires at least one data-plane service")
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
			namespacedRef("DaemonSet", infra.Namespace, fmt.Sprintf("%s-k8s-plugin", infra.Name)),
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
		appendCheckStep("builtin-template-pods", infrav1alpha1.ConditionTypeManagerReady, "BuiltinTemplatePodsNotReady")
	}
	if compiled.Components.EnableNetd {
		appendCheckStep("netd-rbac", infrav1alpha1.ConditionTypeNetdReady, "NetdRBACFailed")
		appendSuccessStep("netd", infrav1alpha1.ConditionTypeNetdReady, "NetdReady", "netd is ready", "NetdFailed")
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

func globalGatewayStatusURL(compiled *InfraPlan) string {
	infra := compiledInfra(compiled)
	if infra == nil || infra.Name == "" {
		return ""
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
