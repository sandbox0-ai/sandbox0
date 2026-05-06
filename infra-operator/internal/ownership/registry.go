package ownership

import "strings"

type UpdateSemantics string

const (
	UpdateSemanticsDeclarative     UpdateSemantics = "declarative"
	UpdateSemanticsCreateOnce      UpdateSemantics = "create_once"
	UpdateSemanticsMixed           UpdateSemantics = "mixed"
	UpdateSemanticsDeprecatedAlias UpdateSemantics = "deprecated_alias"
)

type Entry struct {
	Path              string
	CoversDescendants bool
	Owner             string
	Consumers         []string
	CompiledInto      []string
	UpdateSemantics   UpdateSemantics
	Notes             string
}

func Registry() []Entry {
	return []Entry{
		exact("spec.database.type", "plan", []string{"database"}, []string{"InfraPlan.Components.EnableDatabase"}, UpdateSemanticsDeclarative, "Selects builtin versus external database reconciliation."),
		prefix("spec.database.builtin", "database", []string{"database"}, nil, UpdateSemanticsMixed, "Builtin database fields are reconciled by the database service; immutable subpaths are declared separately."),
		exact("spec.database.builtin.enabled", "database", []string{"database", "status"}, []string{"InfraPlan.Components.EnableDatabase"}, UpdateSemanticsDeclarative, "Enables builtin database reconciliation and disabled cleanup."),
		exact("spec.database.builtin.port", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Copied into generated credentials and init settings for the builtin PostgreSQL."),
		exact("spec.database.builtin.username", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Copied into generated credentials and bootstrap user settings."),
		exact("spec.database.builtin.database", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Copied into generated credentials and bootstrap database settings."),
		exact("spec.database.builtin.persistence", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Defines the builtin PostgreSQL PVC and cannot be changed after creation."),
		exact("spec.database.builtin.statefulResourcePolicy", "database", []string{"database", "status"}, []string{"cleanup policy"}, UpdateSemanticsDeclarative, "Controls retain versus delete semantics when builtin database is disabled."),
		prefix("spec.database.external", "database", []string{"database", "global-gateway", "regional-gateway", "scheduler", "cluster-gateway", "manager", "storage-proxy"}, nil, UpdateSemanticsDeclarative, "External database DSN is consumed by database-backed services."),

		prefix("spec.metadataDatabase", "storage-proxy", []string{"storage-proxy"}, nil, UpdateSemanticsDeclarative, "Controls S0FS metadata database selection."),

		exact("spec.storage.type", "plan", []string{"storage", "storage-proxy"}, []string{"InfraPlan.Components.EnableStorage"}, UpdateSemanticsDeclarative, "Selects builtin versus external object storage reconciliation."),
		prefix("spec.storage.builtin", "storage", []string{"storage", "storage-proxy"}, nil, UpdateSemanticsMixed, "Builtin storage fields are reconciled by the storage service; immutable subpaths are declared separately."),
		exact("spec.storage.builtin.enabled", "storage", []string{"storage", "status"}, []string{"InfraPlan.Components.EnableStorage"}, UpdateSemanticsDeclarative, "Enables builtin storage reconciliation and disabled cleanup."),
		exact("spec.storage.builtin.persistence", "storage", []string{"storage"}, nil, UpdateSemanticsCreateOnce, "Defines the builtin RustFS PVC and cannot be changed after creation."),
		exact("spec.storage.builtin.credentials", "storage", []string{"storage", "storage-proxy"}, nil, UpdateSemanticsCreateOnce, "Seeds the generated RustFS credentials secret."),
		exact("spec.storage.builtin.statefulResourcePolicy", "storage", []string{"storage", "status"}, []string{"cleanup policy"}, UpdateSemanticsDeclarative, "Controls retain versus delete semantics when builtin storage is disabled."),
		prefix("spec.storage.s3", "storage-proxy", []string{"storage", "storage-proxy"}, nil, UpdateSemanticsDeclarative, "External S3 settings are converted into storage and storage-proxy runtime config."),
		prefix("spec.storage.gcs", "storage-proxy", []string{"storage", "storage-proxy", "ctld"}, nil, UpdateSemanticsDeclarative, "External GCS settings are converted into storage runtime config and storage client service accounts."),
		prefix("spec.storage.oss", "storage-proxy", []string{"storage", "storage-proxy"}, nil, UpdateSemanticsDeclarative, "External OSS settings are converted into storage and storage-proxy runtime config."),

		exact("spec.registry.provider", "plan", []string{"registry"}, []string{"InfraPlan.Components.EnableRegistry"}, UpdateSemanticsDeclarative, "Selects builtin versus external registry reconciliation."),
		exact("spec.registry.imagePullSecretName", "registry", []string{"registry", "manager"}, nil, UpdateSemanticsDeclarative, "Controls the pull secret propagated into template namespaces."),
		prefix("spec.registry.builtin", "registry", []string{"registry"}, nil, UpdateSemanticsMixed, "Builtin registry fields are reconciled by the registry service; immutable subpaths are declared separately."),
		exact("spec.registry.builtin.enabled", "registry", []string{"registry", "status"}, []string{"InfraPlan.Components.EnableRegistry"}, UpdateSemanticsDeclarative, "Enables builtin registry reconciliation and disabled cleanup."),
		exact("spec.registry.builtin.persistence", "registry", []string{"registry"}, nil, UpdateSemanticsCreateOnce, "Defines the builtin registry PVC and cannot be changed after creation."),
		exact("spec.registry.builtin.statefulResourcePolicy", "registry", []string{"registry", "status"}, []string{"cleanup policy"}, UpdateSemanticsDeclarative, "Controls retain versus delete semantics when builtin registry is disabled."),
		prefix("spec.registry.aws", "registry", []string{"registry"}, nil, UpdateSemanticsDeclarative, "AWS registry credentials and pull-secret integration."),
		prefix("spec.registry.gcp", "registry", []string{"registry"}, nil, UpdateSemanticsDeclarative, "GCP registry credentials and pull-secret integration."),
		prefix("spec.registry.azure", "registry", []string{"registry"}, nil, UpdateSemanticsDeclarative, "Azure registry credentials and pull-secret integration."),
		prefix("spec.registry.aliyun", "registry", []string{"registry"}, nil, UpdateSemanticsDeclarative, "Aliyun registry credentials and pull-secret integration."),
		prefix("spec.registry.harbor", "registry", []string{"registry"}, nil, UpdateSemanticsDeclarative, "Harbor registry credentials and pull-secret integration."),

		prefix("spec.controlPlane", "plan", []string{"cluster-gateway", "manager", "storage-proxy", "netd", "status"}, []string{"InfraPlan.Validation.RequireControlPlanePublicKey"}, UpdateSemanticsDeclarative, "External control-plane connection required by data-plane services that integrate with a control plane."),
		prefix("spec.internalAuth", "internal-auth", []string{"internal-auth", "regional-gateway", "scheduler", "cluster-gateway", "manager", "storage-proxy", "netd", "ssh-gateway"}, nil, UpdateSemanticsDeclarative, "Controls generated or imported internal JWT keys consumed by control-plane and data-plane services."),
		prefix("spec.enterpriseLicense", "plan", []string{"regional-gateway", "scheduler", "cluster-gateway", "global-gateway", "status"}, []string{"InfraPlan.Enterprise"}, UpdateSemanticsDeclarative, "Shared enterprise license secret reference for licensed services."),

		exact("spec.observability.enabled", "observability", []string{"observability", "global-gateway", "regional-gateway", "ssh-gateway", "scheduler", "cluster-gateway", "manager", "storage-proxy", "netd", "status"}, []string{"InfraPlan.Components.EnableObservability", "InfraPlan.Observability"}, UpdateSemanticsDeclarative, "Enables the regional observability collection and storage stack."),
		prefix("spec.observability.collector", "observability", []string{"observability", "global-gateway", "regional-gateway", "ssh-gateway", "scheduler", "cluster-gateway", "manager", "storage-proxy", "netd"}, []string{"InfraPlan.Observability.CollectorEnabled", "InfraPlan.Observability.CollectorServiceURL"}, UpdateSemanticsDeclarative, "Configures the OpenTelemetry Collector deployment and service endpoint consumed by instrumented services."),
		exact("spec.observability.clickHouse.type", "observability", []string{"observability"}, []string{"InfraPlan.Observability.ClickHouseBuiltin"}, UpdateSemanticsDeclarative, "Selects builtin versus external ClickHouse storage for observability data."),
		prefix("spec.observability.clickHouse.builtin", "observability", []string{"observability"}, nil, UpdateSemanticsMixed, "Builtin ClickHouse fields are reconciled by the observability service; immutable subpaths are declared separately."),
		exact("spec.observability.clickHouse.builtin.enabled", "observability", []string{"observability", "status"}, []string{"InfraPlan.Observability.ClickHouseEnabled"}, UpdateSemanticsDeclarative, "Enables builtin ClickHouse reconciliation."),
		exact("spec.observability.clickHouse.builtin.persistence", "observability", []string{"observability"}, nil, UpdateSemanticsCreateOnce, "Defines the builtin ClickHouse PVC and cannot be changed after creation."),
		exact("spec.observability.clickHouse.builtin.statefulResourcePolicy", "observability", []string{"observability", "status"}, []string{"cleanup policy"}, UpdateSemanticsDeclarative, "Controls retain versus delete semantics when builtin ClickHouse is disabled."),
		prefix("spec.observability.clickHouse.external", "observability", []string{"observability"}, []string{"InfraPlan.Observability.ClickHouseEndpoint", "InfraPlan.Observability.ClickHouseHTTPURL", "InfraPlan.Observability.ClickHouseDatabase"}, UpdateSemanticsDeclarative, "External ClickHouse settings are consumed by the collector and future query APIs."),

		prefix("spec.services.globalGateway", "global-gateway", []string{"global-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for global-gateway."),
		prefix("spec.services.regionalGateway", "regional-gateway", []string{"regional-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for regional-gateway."),
		prefix("spec.services.sshGateway", "ssh-gateway", []string{"ssh-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for ssh-gateway."),
		prefix("spec.services.scheduler", "scheduler", []string{"scheduler", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for scheduler."),
		prefix("spec.services.clusterGateway", "cluster-gateway", []string{"cluster-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for cluster-gateway."),
		prefix("spec.services.manager", "manager", []string{"manager", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for manager."),
		prefix("spec.services.storageProxy", "storage-proxy", []string{"storage-proxy", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for storage-proxy."),
		prefix("spec.services.netd", "netd", []string{"netd", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for netd."),

		exact("spec.services.clusterGateway.config.authMode", "plan", []string{"cluster-gateway", "manager"}, []string{"InfraPlan.Manager.TemplateStoreEnabled", "InfraPlan.Enterprise.ClusterGateway"}, UpdateSemanticsDeclarative, "Auth mode affects manager template-store behavior and enterprise-license requirements."),
		exact("spec.services.clusterGateway.config.oidcProviders", "plan", []string{"cluster-gateway"}, []string{"InfraPlan.Enterprise.ClusterGateway"}, UpdateSemanticsDeclarative, "Enabled OIDC providers drive cluster-gateway enterprise-license requirements."),
		exact("spec.services.clusterGateway.service.type", "plan", []string{"cluster-gateway", "regional-gateway"}, []string{"InfraPlan.Services.ClusterGateway.Port", "validation"}, UpdateSemanticsDeclarative, "Service type determines how the cluster-gateway address is projected downstream."),
		exact("spec.services.clusterGateway.service.port", "plan", []string{"cluster-gateway", "regional-gateway"}, []string{"InfraPlan.Services.ClusterGateway.Port", "InfraPlan.RegionalGateway.DefaultClusterGatewayURL", "validation"}, UpdateSemanticsDeclarative, "Cluster-gateway service port is projected into regional-gateway upstream config."),

		exact("spec.services.manager.config.httpPort", "plan", []string{"manager", "netd"}, []string{"InfraPlan.Services.Manager.URL", "InfraPlan.Netd.EgressAuthResolverURL"}, UpdateSemanticsDeclarative, "Manager HTTP port feeds the manager runtime config and netd derived resolver URL."),
		exact("spec.services.manager.service.port", "plan", []string{"manager", "cluster-gateway", "netd"}, []string{"InfraPlan.Services.Manager.Port", "InfraPlan.Services.Manager.URL", "InfraPlan.Netd.EgressAuthResolverURL"}, UpdateSemanticsDeclarative, "Manager service exposure port is consumed by cluster-gateway and netd."),

		exact("spec.services.storageProxy.config.httpPort", "plan", []string{"storage-proxy", "cluster-gateway"}, nil, UpdateSemanticsDeclarative, "Storage-proxy HTTP port is consumed by cluster-gateway runtime config."),
		exact("spec.services.storageProxy.service.port", "plan", []string{"storage-proxy", "cluster-gateway"}, nil, UpdateSemanticsDeclarative, "Storage-proxy service exposure port is consumed by cluster-gateway runtime config."),

		exact("spec.services.netd.config.egressAuthResolverUrl", "plan", []string{"netd"}, []string{"InfraPlan.Netd.EgressAuthResolverURL"}, UpdateSemanticsDeclarative, "Explicit resolver URL overrides the manager-derived default."),
		exact("spec.services.netd.nodeSelector", "netd", []string{"netd"}, nil, UpdateSemanticsDeprecatedAlias, "Deprecated alias for spec.sandboxNodePlacement.nodeSelector."),
		exact("spec.services.netd.tolerations", "netd", []string{"netd"}, nil, UpdateSemanticsDeprecatedAlias, "Deprecated alias for spec.sandboxNodePlacement.tolerations."),

		prefix("spec.sandboxNodePlacement", "plan", []string{"manager", "netd"}, []string{"InfraPlan.Manager.SandboxPodPlacement", "InfraPlan.Netd.NodeSelector", "InfraPlan.Netd.Tolerations"}, UpdateSemanticsDeclarative, "Shared node placement for sandbox workloads and node-local services."),
		exact("spec.region", "plan", []string{"global-gateway", "regional-gateway", "manager", "netd"}, []string{"InfraPlan.Manager.RegionID", "InfraPlan.Netd.RegionID"}, UpdateSemanticsDeclarative, "Region identifier propagated into control-plane and data-plane services."),

		prefix("spec.publicExposure", "plan", []string{"global-gateway", "cluster-gateway", "manager"}, nil, UpdateSemanticsDeclarative, "Shared public-exposure settings consumed by gateway and manager runtime config."),
		exact("spec.publicExposure.rootDomain", "plan", []string{"global-gateway", "cluster-gateway", "manager"}, nil, UpdateSemanticsDeclarative, "Public root domain feeds generated external URLs and callbacks."),
		exact("spec.publicExposure.regionId", "plan", []string{"global-gateway", "cluster-gateway", "manager"}, nil, UpdateSemanticsDeclarative, "Public DNS-safe region label feeds generated external URLs and callbacks."),

		prefix("spec.cluster", "plan", []string{"manager", "netd", "status"}, []string{"InfraPlan.Manager.DefaultClusterID", "InfraPlan.Netd.ClusterID", "cluster registration"}, UpdateSemanticsDeclarative, "Cluster identity and capacity metadata propagated into manager, netd, and registration status."),
		prefix("spec.initUser", "plan", []string{"global-gateway", "regional-gateway", "cluster-gateway", "database"}, []string{"InfraPlan.Components.EnableInitUser"}, UpdateSemanticsDeclarative, "Initial admin bootstrap for self-hosted installs. Ignored for federated regional gateways and internal-only cluster gateways."),
		prefix("spec.builtinTemplates", "manager", []string{"manager", "scheduler"}, nil, UpdateSemanticsDeclarative, "Builtin template seeds consumed by manager and scheduler template stores."),
	}
}

func Lookup(path string) (Entry, bool) {
	path = normalize(path)
	var (
		best     Entry
		bestRank = -1
	)
	for _, entry := range Registry() {
		if !matches(entry, path) {
			continue
		}
		rank := len(entry.Path)
		if rank > bestRank {
			best = entry
			bestRank = rank
		}
	}
	if bestRank == -1 {
		return Entry{}, false
	}
	return best, true
}

func exact(path, owner string, consumers, compiledInto []string, semantics UpdateSemantics, notes string) Entry {
	return Entry{
		Path:            path,
		Owner:           owner,
		Consumers:       consumers,
		CompiledInto:    compiledInto,
		UpdateSemantics: semantics,
		Notes:           notes,
	}
}

func prefix(path, owner string, consumers, compiledInto []string, semantics UpdateSemantics, notes string) Entry {
	entry := exact(path, owner, consumers, compiledInto, semantics, notes)
	entry.CoversDescendants = true
	return entry
}

func matches(entry Entry, path string) bool {
	if path == entry.Path {
		return true
	}
	return entry.CoversDescendants && strings.HasPrefix(path, entry.Path+".")
}

func normalize(path string) string {
	return strings.Trim(strings.TrimSpace(path), ".")
}
