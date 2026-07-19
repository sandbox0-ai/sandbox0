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
		exact("spec.database.type", "plan", []string{"database", "global-gateway", "regional-gateway", "scheduler", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, []string{"InfraPlan.Components.EnableDatabase"}, UpdateSemanticsDeclarative, "Selects builtin versus external database reconciliation and the shared PostgreSQL injected into database-backed services."),
		prefix("spec.database.builtin", "database", []string{"database", "global-gateway", "regional-gateway", "scheduler", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, nil, UpdateSemanticsMixed, "Builtin database fields are reconciled by the database service; generated connection settings are injected into database-backed services and immutable subpaths are declared separately."),
		exact("spec.database.builtin.enabled", "database", []string{"database", "status"}, []string{"InfraPlan.Components.EnableDatabase"}, UpdateSemanticsDeclarative, "Enables builtin database reconciliation and disabled cleanup."),
		exact("spec.database.builtin.port", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Copied into generated credentials and init settings for the builtin PostgreSQL."),
		exact("spec.database.builtin.username", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Copied into generated credentials and bootstrap user settings."),
		exact("spec.database.builtin.database", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Copied into generated credentials and bootstrap database settings."),
		exact("spec.database.builtin.persistence", "database", []string{"database"}, nil, UpdateSemanticsCreateOnce, "Defines the builtin PostgreSQL PVC and cannot be changed after creation."),
		exact("spec.database.builtin.statefulResourcePolicy", "database", []string{"database", "status"}, []string{"cleanup policy"}, UpdateSemanticsDeclarative, "Controls retain versus delete semantics when builtin database is disabled."),
		prefix("spec.database.external", "database", []string{"database", "global-gateway", "regional-gateway", "scheduler", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, nil, UpdateSemanticsDeclarative, "External database DSN is consumed by identity, template, and Team Quota services, including consumers that validate the region state identity."),

		prefix("spec.metadataDatabase", "manager", []string{"manager"}, nil, UpdateSemanticsDeclarative, "Controls S0FS metadata database selection."),

		exact("spec.storage.type", "plan", []string{"storage", "manager"}, []string{"InfraPlan.Components.EnableStorage"}, UpdateSemanticsDeclarative, "Selects builtin versus external object storage reconciliation."),
		prefix("spec.storage.builtin", "storage", []string{"storage", "manager"}, nil, UpdateSemanticsMixed, "Builtin storage fields are reconciled by the storage service; immutable subpaths are declared separately."),
		exact("spec.storage.builtin.enabled", "storage", []string{"storage", "status"}, []string{"InfraPlan.Components.EnableStorage"}, UpdateSemanticsDeclarative, "Enables builtin storage reconciliation and disabled cleanup."),
		exact("spec.storage.builtin.persistence", "storage", []string{"storage"}, nil, UpdateSemanticsCreateOnce, "Defines the builtin RustFS PVC and cannot be changed after creation."),
		exact("spec.storage.builtin.credentials", "storage", []string{"storage", "manager"}, nil, UpdateSemanticsCreateOnce, "Seeds the generated RustFS credentials secret."),
		exact("spec.storage.builtin.statefulResourcePolicy", "storage", []string{"storage", "status"}, []string{"cleanup policy"}, UpdateSemanticsDeclarative, "Controls retain versus delete semantics when builtin storage is disabled."),
		prefix("spec.storage.s3", "manager", []string{"storage", "manager"}, nil, UpdateSemanticsDeclarative, "External S3 settings are converted into manager storage config."),
		prefix("spec.storage.gcs", "manager", []string{"storage", "manager", "ctld"}, nil, UpdateSemanticsDeclarative, "External GCS settings are converted into manager and ctld storage config."),
		prefix("spec.storage.oss", "manager", []string{"storage", "manager"}, nil, UpdateSemanticsDeclarative, "External OSS settings are converted into manager storage config."),
		prefix("spec.storage.runtime", "manager", []string{"manager", "ctld", "cluster-gateway", "status"}, []string{"InfraPlan.Components.EnableStorageRuntime", "InfraPlan.Services.ManagerStorage"}, UpdateSemanticsDeclarative, "Configures volume APIs in manager and node-local mounts in ctld."),

		prefix("spec.network", "ctld", []string{"ctld", "manager", "status"}, []string{"InfraPlan.Components.EnableNetwork", "InfraPlan.Network"}, UpdateSemanticsDeclarative, "Configures sandbox network enforcement in ctld."),

		exact("spec.redis.type", "plan", []string{"redis", "global-gateway", "regional-gateway", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, []string{"InfraPlan.Components.EnableRedis"}, UpdateSemanticsDeclarative, "Selects builtin versus external Redis reconciliation and runtime connection injection."),
		prefix("spec.redis.builtin", "redis", []string{"redis", "global-gateway", "regional-gateway", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, nil, UpdateSemanticsDeclarative, "Builtin Redis fields are reconciled by the Redis service and injected into Redis-capable runtime config."),
		exact("spec.redis.builtin.enabled", "redis", []string{"redis", "status"}, []string{"InfraPlan.Components.EnableRedis"}, UpdateSemanticsDeclarative, "Enables builtin Redis reconciliation and disabled cleanup."),
		prefix("spec.redis.external", "redis", []string{"redis", "global-gateway", "regional-gateway", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, nil, UpdateSemanticsDeclarative, "External Redis URL secret is injected into Redis-capable runtime config."),
		exact("spec.redis.keyPrefix", "redis", []string{"global-gateway", "regional-gateway", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, nil, UpdateSemanticsDeclarative, "Provides the base namespace from which each Redis-backed feature derives its own key prefix."),
		exact("spec.redis.operationTimeout", "redis", []string{"global-gateway", "regional-gateway", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, nil, UpdateSemanticsDeclarative, "Controls Redis operation timeout for Redis-backed service features."),

		exact("spec.teamQuota.stateId", "region-control-plane", []string{"infra-operator", "regional-gateway", "scheduler", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, []string{"status.teamQuota.stateId", "TeamQuotaDistributedEnforcementConfig.StateID", "SchedulerConfig.TeamQuotaStateID"}, UpdateSemanticsMixed, "Required consumer copy of the region owner status identity; optional on an owner only as first-reconcile recovery input for the same retained state plane."),
		exact("status.teamQuota.stateId", "infra-operator", []string{"regional-gateway", "scheduler", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, []string{"TeamQuotaDistributedEnforcementConfig.StateID", "SchedulerConfig.TeamQuotaStateID"}, UpdateSemanticsCreateOnce, "Fresh UUID v4 generated once for a new region owner, or initialized once from its explicit recovery input; immutable for that owner lifecycle."),
		prefix("spec.teamQuota.defaults", "region-entrypoint", []string{"regional-gateway", "cluster-gateway"}, []string{"RegionalGatewayConfig.TeamQuota.Defaults", "ClusterGatewayConfig.TeamQuota.Defaults"}, UpdateSemanticsDeclarative, "Region defaults are reconciled only by regional-gateway, or by fullmode cluster-gateway when no regional-gateway is enabled."),
		prefix("spec.teamQuota.distributedEnforcement", "plan", []string{"regional-gateway", "cluster-gateway", "manager", "ctld", "storage-proxy", "netd", "ssh-gateway"}, []string{"TeamQuotaDistributedEnforcementConfig"}, UpdateSemanticsDeclarative, "Redis-backed policy-cache, rate, and concurrency lease settings are projected only to distributed Team Quota consumers; PostgreSQL-only capacity consumers do not receive them."),

		exact("spec.credentialVault.type", "plan", []string{"credential-store", "manager"}, []string{"InfraPlan.Components.EnableCredentialVault"}, UpdateSemanticsDeclarative, "Selects builtin OpenBao versus external HashiCorp Vault reconciliation and manager Vault connection injection."),
		prefix("spec.credentialVault.builtin", "credential-store", []string{"credential-store", "manager", "status"}, nil, UpdateSemanticsMixed, "Builtin OpenBao fields are reconciled by the credential-store service and injected into manager runtime config."),
		exact("spec.credentialVault.builtin.enabled", "credential-store", []string{"credential-store", "status"}, []string{"InfraPlan.Components.EnableCredentialVault"}, UpdateSemanticsDeclarative, "Enables builtin OpenBao reconciliation and disabled cleanup."),
		exact("spec.credentialVault.builtin.persistence", "credential-store", []string{"credential-store"}, nil, UpdateSemanticsCreateOnce, "Defines the builtin OpenBao PVC and cannot be changed after creation."),
		exact("spec.credentialVault.builtin.statefulResourcePolicy", "credential-store", []string{"credential-store", "status"}, []string{"cleanup policy"}, UpdateSemanticsDeclarative, "Controls retain versus delete semantics when builtin OpenBao is disabled."),
		prefix("spec.credentialVault.external", "credential-store", []string{"credential-store", "manager"}, nil, UpdateSemanticsDeclarative, "External HashiCorp Vault endpoint and token secret are validated and mounted into manager."),

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

		prefix("spec.controlPlane", "plan", []string{"cluster-gateway", "manager", "ctld", "status"}, []string{"InfraPlan.Validation.RequireControlPlanePublicKey"}, UpdateSemanticsDeclarative, "External control-plane connection required by data-plane services that integrate with a control plane."),
		prefix("spec.internalAuth", "internal-auth", []string{"internal-auth", "regional-gateway", "scheduler", "cluster-gateway", "manager", "ctld", "ssh-gateway"}, nil, UpdateSemanticsDeclarative, "Controls generated or imported internal JWT keys consumed by control-plane and data-plane services."),
		prefix("spec.enterpriseLicense", "plan", []string{"regional-gateway", "scheduler", "cluster-gateway", "global-gateway", "status"}, []string{"InfraPlan.Enterprise"}, UpdateSemanticsDeclarative, "Shared enterprise license secret reference for licensed services."),
		prefix("spec.observability", "observability", []string{"global-gateway", "regional-gateway", "ssh-gateway", "scheduler", "cluster-gateway", "manager", "ctld"}, []string{"platform service environment", "InfraPlan.Components.EnableObservability"}, UpdateSemanticsDeclarative, "Standard OpenTelemetry collection env injection and optional external export integration. User-facing query APIs and adapters are out of scope."),
		prefix("spec.observability.backend", "observability", []string{"observability", "global-gateway", "regional-gateway", "ssh-gateway", "scheduler", "cluster-gateway", "manager", "ctld", "status"}, []string{"InfraPlan.Components.EnableObservability"}, UpdateSemanticsDeclarative, "Controls disabled or external observability export integration."),
		prefix("spec.observability.backend.external", "observability", []string{"observability", "platform service environment"}, nil, UpdateSemanticsDeclarative, "External OTLP endpoint settings are consumed by managed collectors or injected into platform services for existing collectors."),
		prefix("spec.observability.collection", "observability", []string{"observability", "platform service environment"}, nil, UpdateSemanticsDeclarative, "Controls logs, metrics, and traces collection toggles for enabled backends."),
		prefix("spec.observability.traces", "observability", []string{"global-gateway", "regional-gateway", "ssh-gateway", "scheduler", "cluster-gateway", "manager", "ctld"}, []string{"platform service environment"}, UpdateSemanticsDeclarative, "Legacy direct trace exporter env injection. Explicit fields override export-derived trace defaults."),
		prefix("spec.clickHouse", "clickhouse", []string{"clickhouse", "cluster-gateway", "manager", "ctld", "status"}, []string{"InfraPlan.Components.EnableClickHouse"}, UpdateSemanticsDeclarative, "Region-level ClickHouse component consumed by ClickHouse-backed features such as sandbox observability and metering."),
		prefix("spec.clickHouse.builtin", "clickhouse", []string{"clickhouse", "status"}, nil, UpdateSemanticsMixed, "Builtin ClickHouse fields are reconciled by the clickhouse service; immutable subpaths are declared in the CRD schema."),
		prefix("spec.clickHouse.external", "clickhouse", []string{"cluster-gateway", "manager", "ctld", "status"}, nil, UpdateSemanticsDeclarative, "External ClickHouse DSN secret is resolved by the clickhouse service and injected into ClickHouse-backed feature configs."),
		prefix("spec.clickHouse.databases", "clickhouse", []string{"cluster-gateway", "manager", "ctld"}, nil, UpdateSemanticsDeclarative, "Logical ClickHouse databases for observability and metering."),
		prefix("spec.sandboxObservability", "sandbox-observability", []string{"sandbox-observability", "cluster-gateway", "ctld", "manager", "status"}, []string{"InfraPlan.Components.EnableSandboxObservability"}, UpdateSemanticsDeclarative, "Region-local per-sandbox historical observability feature, retention, ctld runtime samples and network events, manager logs, and user-facing query APIs."),
		prefix("spec.sandboxObservability.builtin", "sandbox-observability", []string{"clickhouse", "cluster-gateway", "ctld", "status"}, nil, UpdateSemanticsDeprecatedAlias, "Legacy compatibility for builtin ClickHouse settings; new configurations should use spec.clickHouse."),
		prefix("spec.sandboxObservability.external", "sandbox-observability", []string{"clickhouse", "cluster-gateway", "ctld", "status"}, nil, UpdateSemanticsDeprecatedAlias, "Legacy compatibility for external ClickHouse settings; new configurations should use spec.clickHouse."),
		prefix("spec.metering", "metering", []string{"manager", "ctld", "cluster-gateway", "regional-gateway", "status"}, []string{"InfraPlan.Validation"}, UpdateSemanticsDeclarative, "Region usage ledger backend configuration. Billing/pricing remains outside sandbox0."),

		prefix("spec.services.globalGateway", "global-gateway", []string{"global-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for global-gateway."),
		prefix("spec.services.regionalGateway", "regional-gateway", []string{"regional-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for regional-gateway."),
		prefix("spec.services.sshGateway", "ssh-gateway", []string{"ssh-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for ssh-gateway."),
		prefix("spec.services.scheduler", "scheduler", []string{"scheduler", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for scheduler."),
		prefix("spec.services.clusterGateway", "cluster-gateway", []string{"cluster-gateway", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for cluster-gateway."),
		prefix("spec.services.manager", "manager", []string{"manager", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for manager."),
		prefix("spec.services.ctld", "ctld", []string{"ctld", "status"}, nil, UpdateSemanticsDeclarative, "Direct runtime configuration for ctld."),

		exact("spec.services.clusterGateway.config.authMode", "plan", []string{"cluster-gateway", "manager"}, []string{"InfraPlan.Manager.TemplateStoreEnabled", "InfraPlan.Enterprise.ClusterGateway"}, UpdateSemanticsDeclarative, "Auth mode affects manager template-store behavior and enterprise-license requirements."),
		exact("spec.services.clusterGateway.config.oidcProviders", "plan", []string{"cluster-gateway"}, []string{"InfraPlan.Enterprise.ClusterGateway"}, UpdateSemanticsDeclarative, "Enabled OIDC providers drive cluster-gateway enterprise-license requirements."),
		exact("spec.services.sshGateway.endpointPort", "plan", []string{"regional-gateway", "cluster-gateway"}, []string{"ssh endpoint advertisement"}, UpdateSemanticsDeclarative, "Optional public SSH endpoint port advertised by regional-gateway and cluster-gateway."),
		exact("spec.services.clusterGateway.service.port", "plan", []string{"cluster-gateway", "regional-gateway"}, []string{"InfraPlan.Services.ClusterGateway.Port", "InfraPlan.RegionalGateway.DefaultClusterGatewayURL"}, UpdateSemanticsDeclarative, "Cluster-gateway service port is projected into regional-gateway routing."),
		exact("spec.services.clusterGateway.service.type", "plan", []string{"cluster-gateway", "regional-gateway"}, []string{"InfraPlan.Services.ClusterGateway.Port", "validation"}, UpdateSemanticsDeclarative, "Service type determines how the cluster-gateway address is projected downstream."),

		exact("spec.services.manager.config.httpPort", "plan", []string{"manager", "ctld"}, []string{"InfraPlan.Services.Manager.URL", "InfraPlan.Network.EgressAuthResolverURL"}, UpdateSemanticsDeclarative, "Manager HTTP port feeds the manager runtime config and the ctld network resolver URL."),
		exact("spec.services.manager.service.port", "plan", []string{"manager", "cluster-gateway", "ctld"}, []string{"InfraPlan.Services.Manager.Port", "InfraPlan.Services.Manager.URL", "InfraPlan.Network.EgressAuthResolverURL"}, UpdateSemanticsDeclarative, "Manager service exposure port is consumed by cluster-gateway and ctld."),

		exact("spec.storage.runtime.httpPort", "plan", []string{"manager", "cluster-gateway"}, []string{"InfraPlan.Services.ManagerStorage"}, UpdateSemanticsDeclarative, "Manager exposes this storage port to cluster-gateway."),
		exact("spec.network.config.egressAuthResolverUrl", "plan", []string{"ctld"}, []string{"InfraPlan.Network.EgressAuthResolverURL"}, UpdateSemanticsDeclarative, "Explicit resolver URL overrides the manager-derived default."),

		prefix("spec.sandboxNodePlacement", "plan", []string{"manager", "ctld"}, []string{"InfraPlan.Manager.SandboxPodPlacement", "ctld workload placement"}, UpdateSemanticsDeclarative, "Shared node placement for sandbox workloads and node-local services."),
		exact("spec.region", "plan", []string{"global-gateway", "regional-gateway", "manager", "ctld"}, []string{"InfraPlan.Manager.RegionID", "InfraPlan.Network.RegionID"}, UpdateSemanticsDeclarative, "Region identifier propagated into control-plane and data-plane services."),

		prefix("spec.publicExposure", "plan", []string{"global-gateway", "cluster-gateway", "manager"}, nil, UpdateSemanticsDeclarative, "Shared public-exposure settings consumed by gateway and manager runtime config."),
		exact("spec.publicExposure.rootDomain", "plan", []string{"global-gateway", "cluster-gateway", "manager"}, nil, UpdateSemanticsDeclarative, "Public root domain feeds generated external URLs and callbacks."),
		exact("spec.publicExposure.regionId", "plan", []string{"global-gateway", "cluster-gateway", "manager"}, nil, UpdateSemanticsDeclarative, "Public DNS-safe region label feeds generated external URLs and callbacks."),

		prefix("spec.cluster", "plan", []string{"manager", "ctld", "status"}, []string{"InfraPlan.Manager.DefaultClusterID", "InfraPlan.Network.ClusterID", "cluster registration"}, UpdateSemanticsDeclarative, "Cluster identity and capacity metadata propagated into manager, ctld, and registration status."),
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
