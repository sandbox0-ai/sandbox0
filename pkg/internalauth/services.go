package internalauth

const (
	ServiceClusterGateway  = "cluster-gateway"
	ServiceCtld            = "ctld"
	ServiceGlobalGateway   = "global-gateway"
	ServiceManager         = "manager"
	ServiceNetd            = "netd"
	ServiceProcd           = "procd"
	ServiceRegionalGateway = "regional-gateway"
	ServiceScheduler       = "scheduler"
	ServiceSSHGateway      = "ssh-gateway"
	ServiceStorageProxy    = "storage-proxy"
)

// ManagerAllowedCallers returns the services allowed to call manager's
// internal HTTP surface.
func ManagerAllowedCallers() []string {
	return []string{ServiceClusterGateway, ServiceNetd, ServiceSSHGateway}
}

// ProcdAllowedCallers returns the services allowed to call procd's internal
// HTTP surface.
func ProcdAllowedCallers() []string {
	return []string{ServiceClusterGateway, ServiceManager, ServiceSSHGateway}
}
