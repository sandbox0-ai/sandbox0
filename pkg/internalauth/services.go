package internalauth

const (
	ServiceClusterGateway  = "cluster-gateway"
	ServiceCtld            = "ctld"
	ServiceGlobalGateway   = "global-gateway"
	ServiceManager         = "manager"
	ServiceProcd           = "procd"
	ServiceRegionalGateway = "regional-gateway"
	ServiceScheduler       = "scheduler"
	ServiceSSHGateway      = "ssh-gateway"

	// ServiceLegacyNetworkRuntime is accepted only while ctld Pods from the
	// previous release drain. New tokens must use ServiceCtld.
	ServiceLegacyNetworkRuntime = "netd"
)

// IsCtldNetworkCaller reports whether caller is the current ctld identity or
// the rollout-only identity used before networking was embedded in ctld.
func IsCtldNetworkCaller(caller string) bool {
	return caller == ServiceCtld || caller == ServiceLegacyNetworkRuntime
}

// ManagerAllowedCallers returns the services allowed to call manager's
// internal HTTP surface.
func ManagerAllowedCallers() []string {
	return []string{ServiceClusterGateway, ServiceCtld, ServiceLegacyNetworkRuntime, ServiceSSHGateway}
}

// ProcdAllowedCallers returns the services allowed to call procd's internal
// HTTP surface.
func ProcdAllowedCallers() []string {
	return []string{ServiceClusterGateway, ServiceManager, ServiceSSHGateway}
}
