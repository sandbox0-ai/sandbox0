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
)

// IsCtldNetworkCaller reports whether caller is the ctld identity.
func IsCtldNetworkCaller(caller string) bool {
	return caller == ServiceCtld
}

// ManagerAllowedCallers returns the services allowed to call manager's
// internal HTTP surface.
func ManagerAllowedCallers() []string {
	return []string{ServiceClusterGateway, ServiceCtld, ServiceSSHGateway}
}

// ProcdAllowedCallers returns the services allowed to call procd's internal
// HTTP surface.
func ProcdAllowedCallers() []string {
	return []string{ServiceClusterGateway, ServiceManager, ServiceSSHGateway}
}
