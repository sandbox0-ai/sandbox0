package v1alpha1

import (
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GatewayConfig defines user-facing gateway settings shared by global,
// regional, and cluster gateway services.
type GatewayConfig struct {
	// JWTIssuer sets the JWT issuer for gateway-issued tokens.
	// +optional
	JWTIssuer string `json:"jwtIssuer,omitempty"`
	// JWTPrivateKeyPEM sets the PEM-encoded Ed25519 private key used to sign
	// user-facing JWTs.
	// +optional
	JWTPrivateKeyPEM string `json:"jwtPrivateKeyPEM,omitempty"`
	// JWTPublicKeyPEM sets the PEM-encoded Ed25519 public key used to verify
	// user-facing JWTs.
	// +optional
	JWTPublicKeyPEM string `json:"jwtPublicKeyPEM,omitempty"`
	// JWTPrivateKeyFile points at a PEM-encoded Ed25519 private key file used to
	// sign user-facing JWTs.
	// +optional
	JWTPrivateKeyFile string `json:"jwtPrivateKeyFile,omitempty"`
	// JWTPublicKeyFile points at a PEM-encoded Ed25519 public key file used to
	// verify user-facing JWTs.
	// +optional
	JWTPublicKeyFile string `json:"jwtPublicKeyFile,omitempty"`
	// +optional
	// +kubebuilder:default="15m"
	JWTAccessTokenTTL metav1.Duration `json:"jwtAccessTokenTTL,omitempty"`
	// +optional
	// +kubebuilder:default="168h"
	JWTRefreshTokenTTL metav1.Duration `json:"jwtRefreshTokenTTL,omitempty"`

	RedisURL       string          `json:"-"`
	RedisKeyPrefix string          `json:"-"`
	RedisTimeout   metav1.Duration `json:"-"`

	// Rate limiting
	// +optional
	// +kubebuilder:default=100
	RateLimitRPS int `json:"rateLimitRps,omitempty"`
	// +optional
	// +kubebuilder:default=200
	RateLimitBurst int `json:"rateLimitBurst,omitempty"`
	// +optional
	// +kubebuilder:default="10m"
	RateLimitCleanupInterval metav1.Duration `json:"rateLimitCleanupInterval,omitempty"`
	RateLimitBackend         string          `json:"-"`
	RateLimitRedisURL        string          `json:"-"`
	RateLimitRedisKeyPrefix  string          `json:"-"`
	RateLimitRedisTimeout    metav1.Duration `json:"-"`
	RateLimitFailOpen        bool            `json:"-"`

	// Identity and Teams
	// +optional
	// +kubebuilder:default="Personal Team"
	DefaultTeamName string `json:"defaultTeamName,omitempty"`

	// BuiltInAuth configures local email/password authentication.
	// +optional
	// +kubebuilder:default={}
	BuiltInAuth BuiltInAuthConfig `json:"builtInAuth,omitempty"`

	// OIDCProviders configures external identity providers.
	// +optional
	OIDCProviders []OIDCProviderConfig `json:"oidcProviders,omitempty"`
	// +optional
	// +kubebuilder:default="10m"
	OIDCStateTTL metav1.Duration `json:"oidcStateTtl,omitempty"`
	// +optional
	// +kubebuilder:default="5m"
	OIDCStateCleanupInterval metav1.Duration `json:"oidcStateCleanupInterval,omitempty"`

	// BaseURL sets the external base URL used by browser-facing auth flows.
	// +optional
	// +kubebuilder:default="http://localhost:8080"
	BaseURL string `json:"baseUrl,omitempty"`
}

// BuiltInAuthConfig configures built-in authentication.
type BuiltInAuthConfig struct {
	// Enabled enables built-in email/password authentication.
	// +optional
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// AllowRegistration allows new users to register.
	// +optional
	AllowRegistration bool `json:"allowRegistration,omitempty"`

	// EmailVerificationRequired requires email verification.
	// +optional
	EmailVerificationRequired bool `json:"emailVerificationRequired,omitempty"`

	// AdminOnly restricts built-in auth to admin accounts only.
	// +optional
	AdminOnly bool `json:"adminOnly,omitempty"`
}

// OIDCProviderConfig configures an OIDC identity provider.
type OIDCProviderConfig struct {
	// +optional
	ID string `json:"id,omitempty"`
	// +optional
	Name string `json:"name,omitempty"`
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	ClientID string `json:"clientId,omitempty"`
	// +optional
	ClientSecret string `json:"clientSecret,omitempty"`
	// +optional
	DiscoveryURL string `json:"discoveryUrl,omitempty"`
	// +optional
	TokenEndpointAuthMethod string `json:"tokenEndpointAuthMethod,omitempty"`
	// +optional
	// +kubebuilder:default={"openid","email","profile"}
	Scopes []string `json:"scopes,omitempty"`
	// +optional
	AutoProvision bool `json:"autoProvision,omitempty"`
	// +optional
	TeamMapping *TeamMappingConfig `json:"teamMapping,omitempty"`
	// +optional
	ExternalAuthPortalURL string `json:"externalAuthPortalUrl,omitempty"`
}

// TeamMappingConfig configures automatic team mapping for OIDC users.
type TeamMappingConfig struct {
	// +optional
	Domain string `json:"domain,omitempty"`
	// +optional
	DefaultRole string `json:"defaultRole,omitempty"`
	// +optional
	DefaultTeamID string `json:"defaultTeamId,omitempty"`
}

// GlobalGatewayConfig defines user-facing configuration for global-gateway.
type GlobalGatewayConfig struct {
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `json:"httpPort,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `json:"databaseMaxConns,omitempty"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `json:"databaseMinConns,omitempty"`
	// +optional
	// +kubebuilder:default="global_gateway"
	DatabaseSchema string `json:"databaseSchema,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `json:"shutdownTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ServerReadTimeout metav1.Duration `json:"serverReadTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="60s"
	ServerWriteTimeout metav1.Duration `json:"serverWriteTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="120s"
	ServerIdleTimeout metav1.Duration `json:"serverIdleTimeout,omitempty"`
	// +optional
	GatewayConfig `json:",inline"`
}

// RegionalGatewayConfig defines user-facing configuration for regional-gateway.
type RegionalGatewayConfig struct {
	// +optional
	// +kubebuilder:default="self-hosted"
	Edition string `json:"edition,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=self_hosted;federated_global
	// +kubebuilder:default="self_hosted"
	AuthMode string `json:"authMode,omitempty"`
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `json:"httpPort,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `json:"databaseMaxConns,omitempty"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `json:"databaseMinConns,omitempty"`
	// +optional
	SchedulerEnabled bool `json:"schedulerEnabled,omitempty"`
	// +optional
	SchedulerURL string `json:"schedulerUrl,omitempty"`
	// +kubebuilder:default="30s"
	InternalAuthTTL metav1.Duration `json:"internalAuthTtl,omitempty"`
	// +optional
	// +kubebuilder:default="regional-gateway"
	InternalAuthCaller string `json:"internalAuthCaller,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ClusterCacheTTL metav1.Duration `json:"clusterCacheTtl,omitempty"`
	// +optional
	// +kubebuilder:default="10s"
	ProxyTimeout metav1.Duration `json:"proxyTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `json:"shutdownTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ServerReadTimeout metav1.Duration `json:"serverReadTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="60s"
	ServerWriteTimeout metav1.Duration `json:"serverWriteTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="120s"
	ServerIdleTimeout metav1.Duration `json:"serverIdleTimeout,omitempty"`
	// +optional
	GatewayConfig `json:",inline"`
}

// SSHGatewayConfig defines user-facing configuration for ssh-gateway.
type SSHGatewayConfig struct {
	// +optional
	// +kubebuilder:default=2222
	SSHPort int `json:"sshPort,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `json:"databaseMaxConns,omitempty"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `json:"databaseMinConns,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	InternalAuthTTL metav1.Duration `json:"internalAuthTtl,omitempty"`
	// +optional
	// +kubebuilder:default="ssh-gateway"
	InternalAuthCaller string `json:"internalAuthCaller,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ResumeTimeout metav1.Duration `json:"resumeTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="500ms"
	ResumePollInterval metav1.Duration `json:"resumePollInterval,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `json:"shutdownTimeout,omitempty"`
}

// DatabasePoolConfig defines scheduler database pool settings.
type DatabasePoolConfig struct {
	// +optional
	// +kubebuilder:default=10
	MaxConns int32 `json:"maxConns,omitempty"`
	// +optional
	// +kubebuilder:default=2
	MinConns int32 `json:"minConns,omitempty"`
	// +optional
	// +kubebuilder:default="30m"
	MaxConnLifetime metav1.Duration `json:"maxConnLifetime,omitempty"`
	// +optional
	// +kubebuilder:default="5m"
	MaxConnIdleTime metav1.Duration `json:"maxConnIdleTime,omitempty"`
}

// SchedulerConfig defines user-facing configuration for scheduler.
type SchedulerConfig struct {
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `json:"httpPort,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ReconcileInterval metav1.Duration `json:"reconcileInterval,omitempty"`
	// +optional
	// +kubebuilder:default=50
	PodsPerNode int `json:"podsPerNode,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `json:"shutdownTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ReadTimeout metav1.Duration `json:"readTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="60s"
	WriteTimeout metav1.Duration `json:"writeTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="120s"
	IdleTimeout metav1.Duration `json:"idleTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="10s"
	ProxyTimeout metav1.Duration `json:"proxyTimeout,omitempty"`
	// +optional
	DatabasePool DatabasePoolConfig `json:"databasePool,omitempty"`
}

// ClusterGatewayConfig defines user-facing configuration for cluster-gateway.
type ClusterGatewayConfig struct {
	// +optional
	// +kubebuilder:default=8443
	HTTPPort int `json:"httpPort,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=internal;public;both
	// +kubebuilder:default="internal"
	AuthMode string `json:"authMode,omitempty"`
	// +optional
	// +kubebuilder:default={"regional-gateway","scheduler","cluster-gateway"}
	AllowedCallers []string `json:"allowedCallers,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `json:"shutdownTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="10s"
	HealthCheckPeriod metav1.Duration `json:"healthCheckPeriod,omitempty"`
	// +optional
	// +kubebuilder:default="10s"
	ProxyTimeout metav1.Duration `json:"proxyTimeout,omitempty"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `json:"databaseMaxConns,omitempty"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `json:"databaseMinConns,omitempty"`
	GatewayConfig    `json:",inline"`
	// +optional
	// +kubebuilder:default={"*:*"}
	SchedulerPermissions []string `json:"schedulerPermissions,omitempty"`
}

// SandboxObservabilityConfig defines region-level per-sandbox historical
// observability settings. Service runtime configs are generated by the
// infra-operator from this top-level CR field.
type SandboxObservabilityConfig struct {
	// Enabled enables the per-sandbox historical observability feature. When
	// omitted, the legacy type field controls enablement.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Backend selects the historical query backend. New configurations should
	// use clickhouse with spec.clickHouse. The legacy type field is still
	// accepted for compatibility.
	// +optional
	// +kubebuilder:validation:Enum=disabled;clickhouse
	// +kubebuilder:default=disabled
	Backend SandboxObservabilityBackend `json:"backend,omitempty"`

	// Type selects the historical query backend mode. Defaults to disabled so
	// ClickHouse is installed only when explicitly requested.
	// +optional
	// +kubebuilder:validation:Enum=disabled;builtin;external
	// +kubebuilder:default=disabled
	Type SandboxObservabilityType `json:"type,omitempty"`

	// Builtin configures the operator-managed ClickHouse backend.
	// +optional
	Builtin *BuiltinSandboxObservabilityConfig `json:"builtin,omitempty"`

	// External configures a user-owned ClickHouse backend.
	// +optional
	External *ExternalSandboxObservabilityConfig `json:"external,omitempty"`

	// Audit configures the licensed centralized per-sandbox audit signal. Audit
	// collection and query remain disabled unless explicitly enabled.
	// +optional
	Audit *SandboxObservabilityAuditConfig `json:"audit,omitempty"`

	// Retention configures TTLs for the per-sandbox read model.
	// +optional
	// +kubebuilder:default={}
	Retention SandboxObservabilityRetentionConfig `json:"retention,omitempty"`

	// Ingest configures bounded async producer-to-gateway ingestion.
	// +optional
	// +kubebuilder:default={}
	Ingest SandboxObservabilityIngestConfig `json:"ingest,omitempty"`
}

// SandboxObservabilityAuditConfig configures centralized per-sandbox audit
// collection and query.
// +kubebuilder:validation:XValidation:rule="has(self.deliveryPersistence) == has(oldSelf.deliveryPersistence)",message="deliveryPersistence presence is immutable after creation"
type SandboxObservabilityAuditConfig struct {
	// Enabled enables the enterprise sandbox_audit feature.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// DeliveryMode controls admission for non-mutating API requests, public
	// exposure requests, and flows observed by the ctld network runtime.
	// ClickHouse remains the only canonical audit store in both modes. Mutating
	// APIs always use canonical_sync.
	// +optional
	// +kubebuilder:validation:Enum=durable_async;canonical_sync
	// +kubebuilder:default=durable_async
	DeliveryMode sandboxobservability.AuditDeliveryMode `json:"deliveryMode,omitempty"`

	// StoragePolicy optionally selects a ClickHouse storage policy for the
	// canonical audit events table.
	// +optional
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_]*$`
	StoragePolicy string `json:"storagePolicy,omitempty"`

	// DeliveryPersistence configures the durable, non-canonical audit event
	// delivery buffer used while ClickHouse is unavailable. The current
	// implementation requires one cluster-gateway replica and a storage class
	// whose volume can be reattached after cross-node rescheduling.
	// +optional
	DeliveryPersistence *SandboxAuditDeliveryPersistenceConfig `json:"deliveryPersistence,omitempty"`
}

// SandboxAuditDeliveryPersistenceConfig configures cluster-gateway's audit
// event delivery PVC. ClickHouse remains the only audit system of record.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="audit delivery persistence is immutable after creation"
type SandboxAuditDeliveryPersistenceConfig struct {
	// Size specifies the delivery buffer capacity.
	// +optional
	// +kubebuilder:default="1Gi"
	Size resource.Quantity `json:"size,omitempty"`

	// StorageClass selects a durable Kubernetes storage class. Empty uses the
	// cluster default.
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
}

// SandboxObservabilityType selects the backend provisioning mode.
// +kubebuilder:validation:Enum=disabled;builtin;external
type SandboxObservabilityType string

const (
	SandboxObservabilityTypeDisabled SandboxObservabilityType = "disabled"
	SandboxObservabilityTypeBuiltin  SandboxObservabilityType = "builtin"
	SandboxObservabilityTypeExternal SandboxObservabilityType = "external"
)

// SandboxObservabilityBackend selects the per-sandbox observability query backend.
// +kubebuilder:validation:Enum=disabled;clickhouse
type SandboxObservabilityBackend string

const (
	SandboxObservabilityBackendDisabled   SandboxObservabilityBackend = "disabled"
	SandboxObservabilityBackendClickHouse SandboxObservabilityBackend = "clickhouse"
)

// ClickHouseConfig defines the region-level ClickHouse data component.
type ClickHouseConfig struct {
	// Type selects whether ClickHouse is disabled, operator-managed, or external.
	// +optional
	// +kubebuilder:validation:Enum=disabled;builtin;external
	// +kubebuilder:default=disabled
	Type ClickHouseType `json:"type,omitempty"`

	// Builtin configures the operator-managed ClickHouse instance.
	// +optional
	Builtin *BuiltinClickHouseConfig `json:"builtin,omitempty"`

	// External configures a user-owned ClickHouse instance.
	// +optional
	External *ExternalClickHouseConfig `json:"external,omitempty"`

	// Databases configures logical ClickHouse databases used by Sandbox0 features.
	// +optional
	// +kubebuilder:default={}
	Databases ClickHouseDatabaseConfig `json:"databases,omitempty"`

	// SchemaMigration configures whether services run ClickHouse schema migrations.
	// +optional
	// +kubebuilder:default={}
	SchemaMigration ClickHouseSchemaMigrationConfig `json:"schemaMigration,omitempty"`
}

// ClickHouseType selects the ClickHouse provisioning mode.
// +kubebuilder:validation:Enum=disabled;builtin;external
type ClickHouseType string

const (
	ClickHouseTypeDisabled ClickHouseType = "disabled"
	ClickHouseTypeBuiltin  ClickHouseType = "builtin"
	ClickHouseTypeExternal ClickHouseType = "external"
)

// BuiltinClickHouseConfig defines operator-managed ClickHouse settings.
// +kubebuilder:validation:XValidation:rule="has(self.persistence) == has(oldSelf.persistence)",message="persistence presence is immutable after creation"
type BuiltinClickHouseConfig struct {
	// Enabled enables the built-in ClickHouse instance when type is builtin.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Image is the ClickHouse server image.
	// +kubebuilder:default="clickhouse/clickhouse-server:24.8"
	// +optional
	Image string `json:"image,omitempty"`

	// NativePort is the ClickHouse native TCP port.
	// +kubebuilder:default=9000
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nativePort is immutable after creation"
	// +optional
	NativePort int32 `json:"nativePort,omitempty"`

	// HTTPPort is the ClickHouse HTTP port.
	// +kubebuilder:default=8123
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="httpPort is immutable after creation"
	// +optional
	HTTPPort int32 `json:"httpPort,omitempty"`

	// Persistence configures ClickHouse storage.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="persistence is immutable after creation"
	// +optional
	Persistence *PersistenceConfig `json:"persistence,omitempty"`

	// StatefulResourcePolicy controls what happens to the builtin PVC and
	// generated credentials secret when the builtin backend is disabled or
	// replaced by an external backend.
	// +kubebuilder:default=Retain
	// +optional
	StatefulResourcePolicy BuiltinStatefulResourcePolicy `json:"statefulResourcePolicy,omitempty"`
}

// ExternalClickHouseConfig defines a user-owned ClickHouse instance.
type ExternalClickHouseConfig struct {
	// DSNSecret references a secret key containing the ClickHouse database/sql DSN.
	DSNSecret ClickHouseDSNSecretRef `json:"dsnSecret"`

	// ConnectTimeout bounds startup connection and schema checks.
	// +optional
	// +kubebuilder:default="10s"
	ConnectTimeout metav1.Duration `json:"connectTimeout,omitempty"`
}

// ClickHouseDSNSecretRef references a key containing a ClickHouse DSN.
type ClickHouseDSNSecretRef struct {
	// Name is the name of the secret.
	Name string `json:"name"`
	// Key is the key in the secret.
	// +kubebuilder:default="dsn"
	// +optional
	Key string `json:"key,omitempty"`
}

// ClickHouseDatabaseConfig defines Sandbox0 ClickHouse database names.
type ClickHouseDatabaseConfig struct {
	// Observability is the database for per-sandbox historical observability.
	// +optional
	// +kubebuilder:default="sandbox0_observability"
	Observability string `json:"observability,omitempty"`

	// Metering is the database for the regional usage ledger.
	// +optional
	// +kubebuilder:default="sandbox0_metering"
	Metering string `json:"metering,omitempty"`
}

// ClickHouseSchemaMigrationConfig controls ClickHouse schema migrations.
type ClickHouseSchemaMigrationConfig struct {
	// Enabled allows Sandbox0 services to run ClickHouse CREATE/ALTER statements.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`
}

// MeteringConfig defines the region usage ledger backend.
type MeteringConfig struct {
	// Enabled enables PostgreSQL-buffered, ClickHouse-backed metering. Metering is disabled by default.
	// +optional
	// +kubebuilder:default=false
	Enabled *bool `json:"enabled,omitempty"`

	// ClickHouse configures table names and write policy for ClickHouse metering.
	// +optional
	// +kubebuilder:default={}
	ClickHouse MeteringClickHouseConfig `json:"clickHouse,omitempty"`
}

// MeteringClickHouseConfig defines ClickHouse metering table and durability settings.
type MeteringClickHouseConfig struct {
	// +optional
	// +kubebuilder:default="usage_events"
	EventsTable string `json:"eventsTable,omitempty"`
	// +optional
	// +kubebuilder:default="usage_windows"
	WindowsTable string `json:"windowsTable,omitempty"`
	// +optional
	// +kubebuilder:default="producer_watermarks"
	WatermarksTable string `json:"watermarksTable,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox_projection_state"
	SandboxStateTable string `json:"sandboxStateTable,omitempty"`
	// +optional
	// +kubebuilder:default="storage_projection_state"
	StorageStateTable string `json:"storageStateTable,omitempty"`
	// +optional
	// +kubebuilder:default={}
	Retention MeteringClickHouseRetentionConfig `json:"retention,omitempty"`
	// +optional
	// +kubebuilder:default={}
	Write MeteringClickHouseWriteConfig `json:"write,omitempty"`
}

// MeteringClickHouseRetentionConfig configures usage ledger retention.
type MeteringClickHouseRetentionConfig struct {
	// +optional
	// +kubebuilder:default=400
	EventsDays int `json:"eventsDays,omitempty"`
	// +optional
	// +kubebuilder:default=400
	WindowsDays int `json:"windowsDays,omitempty"`
}

// MeteringClickHouseWriteConfig configures ClickHouse metering write behavior.
type MeteringClickHouseWriteConfig struct {
	// Mode selects durable write semantics. Only durable is accepted.
	// +optional
	// +kubebuilder:validation:Enum=durable
	// +kubebuilder:default=durable
	Mode string `json:"mode,omitempty"`
	// FailPolicy selects behavior when durable writes cannot complete.
	// +optional
	// +kubebuilder:validation:Enum=buffer;fail-closed
	// +kubebuilder:default=buffer
	FailPolicy string `json:"failPolicy,omitempty"`
}

// BuiltinSandboxObservabilityConfig defines the built-in ClickHouse backend.
// +kubebuilder:validation:XValidation:rule="has(self.clickHouse.persistence) == has(oldSelf.clickHouse.persistence)",message="clickHouse persistence presence is immutable after creation"
type BuiltinSandboxObservabilityConfig struct {
	// Enabled enables the built-in backend when type is builtin.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ClickHouse configures the operator-managed ClickHouse instance.
	// +optional
	// +kubebuilder:default={}
	ClickHouse BuiltinSandboxObservabilityClickHouseConfig `json:"clickHouse,omitempty"`
}

// BuiltinSandboxObservabilityClickHouseConfig defines built-in ClickHouse settings.
type BuiltinSandboxObservabilityClickHouseConfig struct {
	// Image is the ClickHouse server image.
	// +kubebuilder:default="clickhouse/clickhouse-server:24.8"
	// +optional
	Image string `json:"image,omitempty"`

	// NativePort is the ClickHouse native TCP port.
	// +kubebuilder:default=9000
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nativePort is immutable after creation"
	// +optional
	NativePort int32 `json:"nativePort,omitempty"`

	// HTTPPort is the ClickHouse HTTP port.
	// +kubebuilder:default=8123
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="httpPort is immutable after creation"
	// +optional
	HTTPPort int32 `json:"httpPort,omitempty"`

	// +optional
	// +kubebuilder:default="sandbox0_observability"
	Database string `json:"database,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox_audit_events"
	EventsTable string `json:"eventsTable,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox_logs"
	LogsTable string `json:"logsTable,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox_runtime_samples"
	RuntimeSamplesTable string `json:"runtimeSamplesTable,omitempty"`

	// Persistence configures ClickHouse storage.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="persistence is immutable after creation"
	// +optional
	Persistence *PersistenceConfig `json:"persistence,omitempty"`

	// StatefulResourcePolicy controls what happens to the builtin PVC and
	// generated credentials secret when the builtin backend is disabled or
	// replaced by an external backend.
	// +kubebuilder:default=Retain
	// +optional
	StatefulResourcePolicy BuiltinStatefulResourcePolicy `json:"statefulResourcePolicy,omitempty"`
}

// ExternalSandboxObservabilityConfig defines a user-owned ClickHouse backend.
type ExternalSandboxObservabilityConfig struct {
	// ClickHouse configures the external query backend.
	ClickHouse ExternalSandboxObservabilityClickHouseConfig `json:"clickHouse"`
}

// ExternalSandboxObservabilityClickHouseConfig defines external ClickHouse settings.
type ExternalSandboxObservabilityClickHouseConfig struct {
	// DSNSecret references a secret key containing the ClickHouse database/sql DSN.
	DSNSecret SandboxObservabilityClickHouseDSNSecretRef `json:"dsnSecret"`

	// +optional
	// +kubebuilder:default="sandbox0_observability"
	Database string `json:"database,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox_audit_events"
	EventsTable string `json:"eventsTable,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox_logs"
	LogsTable string `json:"logsTable,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox_runtime_samples"
	RuntimeSamplesTable string `json:"runtimeSamplesTable,omitempty"`

	// +optional
	// +kubebuilder:default="10s"
	ConnectTimeout metav1.Duration `json:"connectTimeout,omitempty"`

	// SkipSchemaMigration disables cluster-gateway ClickHouse CREATE/ALTER TABLE statements.
	// +optional
	SkipSchemaMigration bool `json:"skipSchemaMigration,omitempty"`
}

// SandboxObservabilityClickHouseDSNSecretRef references a key containing a ClickHouse DSN.
type SandboxObservabilityClickHouseDSNSecretRef struct {
	// Name is the name of the secret.
	// +optional
	Name string `json:"name"`
	// Key is the key in the secret.
	// +kubebuilder:default="dsn"
	Key string `json:"key,omitempty"`
}

// SandboxObservabilityRetentionConfig configures ClickHouse TTL per signal.
type SandboxObservabilityRetentionConfig struct {
	// AuditDays controls retention for audit and lifecycle events.
	// +kubebuilder:default=90
	// +optional
	AuditDays int `json:"auditDays,omitempty"`

	// LogDays controls retention for sandbox process logs.
	// +kubebuilder:default=7
	// +optional
	LogDays int `json:"logDays,omitempty"`

	// RuntimeSampleDays controls retention for sandbox runtime samples.
	// +kubebuilder:default=30
	// +optional
	RuntimeSampleDays int `json:"runtimeSampleDays,omitempty"`
}

// SandboxObservabilityIngestConfig configures bounded producer ingestion.
type SandboxObservabilityIngestConfig struct {
	// QueueSize bounds each producer queue.
	// +kubebuilder:default=1024
	// +optional
	QueueSize int `json:"queueSize,omitempty"`

	// BatchSize controls producer POST batch size.
	// +kubebuilder:default=100
	// +optional
	BatchSize int `json:"batchSize,omitempty"`

	// FlushInterval controls producer batch flush cadence.
	// +kubebuilder:default="1s"
	// +optional
	FlushInterval metav1.Duration `json:"flushInterval,omitempty"`

	// RequestTimeout bounds producer POST requests.
	// +kubebuilder:default="2s"
	// +optional
	RequestTimeout metav1.Duration `json:"requestTimeout,omitempty"`

	// MaxRetries controls producer retry attempts before dropping a batch.
	// +kubebuilder:default=3
	// +optional
	MaxRetries int `json:"maxRetries,omitempty"`

	// RetryBackoff controls producer retry delay.
	// +kubebuilder:default="100ms"
	// +optional
	RetryBackoff metav1.Duration `json:"retryBackoff,omitempty"`
}

// ProcdConfig defines user-facing procd settings managed by manager.
type ProcdConfig struct {
	// +optional
	// +kubebuilder:default=49983
	HTTPPort int `json:"httpPort,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:default="/workspace"
	RootPath string `json:"rootPath,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ContextCleanupInterval metav1.Duration `json:"contextCleanupInterval,omitempty"`
	// +optional
	// +kubebuilder:default="0s"
	ContextIdleTimeout metav1.Duration `json:"contextIdleTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="0s"
	ContextMaxLifetime metav1.Duration `json:"contextMaxLifetime,omitempty"`
	// +optional
	// +kubebuilder:default="0s"
	ContextFinishedTTL metav1.Duration `json:"contextFinishedTtl,omitempty"`
	// +optional
	// +kubebuilder:default=256
	WebhookQueueSize int `json:"webhookQueueSize,omitempty"`
	// +optional
	// +kubebuilder:default="5s"
	WebhookRequestTimeout metav1.Duration `json:"webhookRequestTimeout,omitempty"`
	// +optional
	// +kubebuilder:default=3
	WebhookMaxRetries int `json:"webhookMaxRetries,omitempty"`
	// +optional
	// +kubebuilder:default="500ms"
	WebhookBaseBackoff metav1.Duration `json:"webhookBaseBackoff,omitempty"`
	// +optional
	WebhookOutboxDir *string `json:"webhookOutboxDir,omitempty"`
}

// AutoscalerConfig defines manager autoscaler settings.
type AutoscalerConfig struct {
	// +optional
	// +kubebuilder:default="100ms"
	MinScaleInterval metav1.Duration `json:"minScaleInterval,omitempty"`
	// +optional
	// +kubebuilder:default="1.5"
	ScaleUpFactor string `json:"scaleUpFactor,omitempty"`
	// +optional
	// +kubebuilder:default=10
	MaxScaleStep int32 `json:"maxScaleStep,omitempty"`
	// +optional
	// +kubebuilder:default=2
	MinIdleBuffer int32 `json:"minIdleBuffer,omitempty"`
	// +optional
	// +kubebuilder:default="0.2"
	TargetIdleRatio string `json:"targetIdleRatio,omitempty"`
	// +optional
	// +kubebuilder:default="10m"
	NoTrafficScaleDownAfter metav1.Duration `json:"noTrafficScaleDownAfter,omitempty"`
	// +optional
	// +kubebuilder:default="0.1"
	ScaleDownPercent string `json:"scaleDownPercent,omitempty"`
}

// ManagerConfig defines user-facing configuration for manager.
type ManagerConfig struct {
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `json:"httpPort,omitempty"`
	// +optional
	KubeConfig string `json:"kubeConfig,omitempty"`
	// +optional
	// +kubebuilder:default=50
	K8sClientQPS int `json:"k8sClientQps,omitempty"`
	// +optional
	// +kubebuilder:default=100
	K8sClientBurst int `json:"k8sClientBurst,omitempty"`
	// +optional
	// +kubebuilder:default=true
	LeaderElection bool `json:"leaderElection,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ResyncPeriod metav1.Duration `json:"resyncPeriod,omitempty"`
	// +optional
	// +kubebuilder:default=10
	DatabaseMaxConns int32 `json:"databaseMaxConns,omitempty"`
	// +optional
	// +kubebuilder:default=2
	DatabaseMinConns int32 `json:"databaseMinConns,omitempty"`
	// +optional
	// +kubebuilder:default="60s"
	CleanupInterval metav1.Duration `json:"cleanupInterval,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:default=9090
	MetricsPort int `json:"metricsPort,omitempty"`
	// +optional
	// +kubebuilder:default=9443
	WebhookPort int `json:"webhookPort,omitempty"`
	// +optional
	// +kubebuilder:default="/tmp/k8s-webhook-server/serving-certs/tls.crt"
	WebhookCertPath string `json:"webhookCertPath,omitempty"`
	// +optional
	// +kubebuilder:default="/tmp/k8s-webhook-server/serving-certs/tls.key"
	WebhookKeyPath string `json:"webhookKeyPath,omitempty"`
	// +optional
	// +kubebuilder:default="0s"
	DefaultSandboxTTL metav1.Duration `json:"defaultSandboxTtl,omitempty"`
	// +optional
	// +kubebuilder:default="4Gi"
	TeamTemplateMemoryPerCPU string `json:"teamTemplateMemoryPerCpu,omitempty"`
	// SandboxMaxMemory is the maximum memory limit accepted for a single sandbox.
	// +optional
	// +kubebuilder:default="32Gi"
	SandboxMaxMemory string `json:"sandboxMaxMemory,omitempty"`
	// +optional
	SandboxRuntimeClassName string `json:"sandboxRuntimeClassName,omitempty"`
	// ProcdBinImageRef overrides the OCI image used for the procd binary image volume.
	// +optional
	ProcdBinImageRef string `json:"procdBinImageRef,omitempty"`
	// DefaultTeamQuotas declaratively reconciles region-wide quota defaults.
	// Team-specific database policies override these defaults.
	// +optional
	// +kubebuilder:validation:MaxItems=7
	// +listType=map
	// +listMapKey=dimension
	DefaultTeamQuotas []TeamQuotaLimitConfig `json:"defaultTeamQuotas,omitempty"`
	// AllowColdStartWithoutReadyDataPlane lets cold claims create Pending pods
	// when no sandbox data-plane-ready nodes exist yet. This is required for
	// node autoscaler scale-from-zero deployments.
	// +optional
	AllowColdStartWithoutReadyDataPlane bool `json:"allowColdStartWithoutReadyDataPlane,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	NetdPolicyApplyTimeout metav1.Duration `json:"netdPolicyApplyTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="500ms"
	NetdPolicyApplyPollInterval metav1.Duration `json:"netdPolicyApplyPollInterval,omitempty"`
	// EgressAuthDefaultResolveTTL controls the default lifetime of resolved
	// egress auth material cached by the ctld network runtime when a binding does
	// not set cachePolicy.ttl.
	// +optional
	// +kubebuilder:default="5m"
	EgressAuthDefaultResolveTTL metav1.Duration `json:"egressAuthDefaultResolveTtl,omitempty"`
	// +optional
	// +kubebuilder:default="10Mi"
	PauseMinMemoryRequest string `json:"pauseMinMemoryRequest,omitempty"`
	// +optional
	// +kubebuilder:default="32Mi"
	PauseMinMemoryLimit string `json:"pauseMinMemoryLimit,omitempty"`
	// +optional
	// +kubebuilder:default="1.1"
	PauseMemoryBufferRatio string `json:"pauseMemoryBufferRatio,omitempty"`
	// +optional
	// +kubebuilder:default="10m"
	PauseMinCPU string `json:"pauseMinCpu,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ProcdClientTimeout metav1.Duration `json:"procdClientTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="6s"
	ProcdInitTimeout metav1.Duration `json:"procdInitTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `json:"shutdownTimeout,omitempty"`
	// +optional
	// +kubebuilder:default={}
	ProcdConfig ProcdConfig `json:"procdConfig,omitempty"`
	// +optional
	// +kubebuilder:default={}
	Autoscaler AutoscalerConfig `json:"autoscaler,omitempty"`
}

// TeamQuotaLimitConfig configures a region default for teams without an
// override for the same dimension.
type TeamQuotaLimitConfig struct {
	// +kubebuilder:validation:Enum=active_sandboxes;sandbox_claims;volume_storage_gb;snapshot_storage_gb;api_requests;network_egress_bytes;network_ingress_bytes
	Dimension string `json:"dimension"`
	// +kubebuilder:validation:Minimum=0
	LimitValue int64 `json:"limitValue"`
	// IntervalMS is the token refill interval for rate quotas and is omitted for capacity quotas.
	// +optional
	// +kubebuilder:validation:Minimum=0
	IntervalMS int64 `json:"intervalMs,omitempty"`
	// BurstValue is the maximum immediately available tokens for a rate quota.
	// +optional
	// +kubebuilder:validation:Minimum=0
	BurstValue int64 `json:"burstValue,omitempty"`
}

// StorageProxyConfig defines user-facing configuration for the manager storage runtime.
type StorageProxyConfig struct {
	// +optional
	// +kubebuilder:default="0.0.0.0"
	HTTPAddr string `json:"httpAddr,omitempty"`
	// +optional
	// +kubebuilder:default=8081
	HTTPPort int `json:"httpPort,omitempty"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `json:"databaseMaxConns,omitempty"`
	// +optional
	// +kubebuilder:default=5
	DatabaseMinConns int `json:"databaseMinConns,omitempty"`
	// +optional
	// +kubebuilder:default="storage_proxy"
	DatabaseSchema string `json:"databaseSchema,omitempty"`
	// +optional
	// +kubebuilder:default="sandbox0"
	FilesystemName string `json:"filesystemName,omitempty"`
	// +optional
	// +kubebuilder:default=4096
	FilesystemBlockSize int `json:"filesystemBlockSize,omitempty"`
	// +optional
	// +kubebuilder:default="lz4"
	FilesystemCompression string `json:"filesystemCompression,omitempty"`
	// +optional
	// +kubebuilder:default=1
	FilesystemTrashDays int `json:"filesystemTrashDays,omitempty"`
	// +optional
	// +kubebuilder:default=10
	FilesystemMetaRetries int `json:"filesystemMetaRetries,omitempty"`
	// +optional
	// +kubebuilder:default=20
	FilesystemMaxUpload int `json:"filesystemMaxUpload,omitempty"`
	// +optional
	// +kubebuilder:default="4Mi"
	S0FSSegmentTargetSize string `json:"s0fsSegmentTargetSize,omitempty"`
	// +optional
	// +kubebuilder:default="1m"
	S0FSCompactionInterval string `json:"s0fsCompactionInterval,omitempty"`
	// +optional
	// +kubebuilder:default="0.5"
	S0FSCompactionMinDeadRatio string `json:"s0fsCompactionMinDeadRatio,omitempty"`
	// +optional
	// +kubebuilder:default="1Mi"
	S0FSCompactionMinReclaimSize string `json:"s0fsCompactionMinReclaimSize,omitempty"`
	// +optional
	// +kubebuilder:default=true
	ObjectEncryptionEnabled bool `json:"objectEncryptionEnabled,omitempty"`
	// +optional
	ObjectEncryptionPassphrase string `json:"objectEncryptionPassphrase,omitempty"`
	// +optional
	// +kubebuilder:default="aes256gcm-rsa"
	ObjectEncryptionAlgo string `json:"objectEncryptionAlgo,omitempty"`
	// +optional
	// +kubebuilder:default="1s"
	FilesystemAttrTimeout string `json:"filesystemAttrTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="1s"
	FilesystemEntryTimeout string `json:"filesystemEntryTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="1s"
	FilesystemDirEntryTimeout string `json:"filesystemDirEntryTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="5s"
	HeartbeatInterval string `json:"heartbeatInterval,omitempty"`
	// +optional
	// +kubebuilder:default=15
	HeartbeatTimeout int `json:"heartbeatTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	FlushTimeout string `json:"flushTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="60s"
	CleanupInterval string `json:"cleanupInterval,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	DirectVolumeFileIdleTTL string `json:"directVolumeFileIdleTTL,omitempty"`
	// +optional
	// +kubebuilder:default="/var/lib/storage-proxy/cache"
	CacheDir string `json:"cacheDir,omitempty"`
	// +optional
	// +kubebuilder:default="20Gi"
	CacheSizeLimit string `json:"cacheSizeLimit,omitempty"`
	// +optional
	// +kubebuilder:default="1Gi"
	LogSizeLimit string `json:"logSizeLimit,omitempty"`
	// +optional
	// +kubebuilder:default="20Gi"
	VolumePortalCacheSizeLimit string `json:"volumePortalCacheSizeLimit,omitempty"`
	// +optional
	// +kubebuilder:default="5Gi"
	VolumePortalRootMinFree string `json:"volumePortalRootMinFree,omitempty"`
	// +optional
	// +kubebuilder:default=true
	MetricsEnabled bool `json:"metricsEnabled,omitempty"`
	// +optional
	// +kubebuilder:default=9090
	MetricsPort int `json:"metricsPort,omitempty"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	// +kubebuilder:default=true
	AuditLog bool `json:"auditLog,omitempty"`
	// +optional
	// +kubebuilder:default="/var/log/storage-proxy/audit.log"
	AuditFile string `json:"auditFile,omitempty"`
	// +optional
	// +kubebuilder:default="15s"
	HTTPReadTimeout string `json:"httpReadTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="15s"
	HTTPWriteTimeout string `json:"httpWriteTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="60s"
	HTTPIdleTimeout string `json:"httpIdleTimeout,omitempty"`
	// +optional
	// +kubebuilder:default=true
	WatchEventsEnabled bool `json:"watchEventsEnabled,omitempty"`
	// +optional
	// +kubebuilder:default=256
	WatchEventQueueSize int `json:"watchEventQueueSize,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	RestoreRemountTimeout string `json:"restoreRemountTimeout,omitempty"`
	// +optional
	KubeconfigPath string `json:"kubeconfigPath,omitempty"`
}

// NetdConfig defines user-facing configuration for the ctld network runtime.
type NetdConfig struct {
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	NodeName string `json:"nodeName,omitempty"`
	// +optional
	EgressAuthResolverURL string `json:"egressAuthResolverUrl,omitempty"`
	// +optional
	// +kubebuilder:default=false
	EgressAuthEnabled bool `json:"egressAuthEnabled,omitempty"`
	// +optional
	// +kubebuilder:default="2s"
	EgressAuthResolverTimeout metav1.Duration `json:"egressAuthResolverTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="fail-closed"
	EgressAuthFailurePolicy string `json:"egressAuthFailurePolicy,omitempty"`
	// +optional
	// +kubebuilder:default="1h"
	MITMLeafTTL metav1.Duration `json:"mitmLeafTtl,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ResyncPeriod metav1.Duration `json:"resyncPeriod,omitempty"`
	// +optional
	// +kubebuilder:default=9091
	MetricsPort int `json:"metricsPort,omitempty"`
	// +optional
	// +kubebuilder:default=8081
	HealthPort int `json:"healthPort,omitempty"`
	// +optional
	// +kubebuilder:default=true
	FailClosed bool `json:"failClosed,omitempty"`
	// +optional
	// +kubebuilder:default=true
	PreferNFT *bool `json:"preferNft,omitempty"`
	// +optional
	// +kubebuilder:default="0.125"
	BurstRatio string `json:"burstRatio,omitempty"`
	// +optional
	// +kubebuilder:default="0.0.0.0"
	ProxyListenAddr string `json:"proxyListenAddr,omitempty"`
	// +optional
	// +kubebuilder:default=18080
	ProxyHTTPPort int `json:"proxyHttpPort,omitempty"`
	// +optional
	// +kubebuilder:default=18443
	ProxyHTTPSPort int `json:"proxyHttpsPort,omitempty"`
	// +optional
	ProxyHeaderLimit int64 `json:"proxyHeaderLimit,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	ProxyUpstreamTimeout metav1.Duration `json:"proxyUpstreamTimeout,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	// Per-sandbox egress bandwidth limit in bytes per second. Zero disables throttling.
	EgressBandwidthBytesPerSecond int64 `json:"egressBandwidthBytesPerSecond,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	// Per-sandbox ingress bandwidth limit in bytes per second. Zero disables throttling.
	IngressBandwidthBytesPerSecond int64 `json:"ingressBandwidthBytesPerSecond,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	// Token bucket burst in bytes for bandwidth limiting. Zero uses one second of the configured rate.
	BandwidthBurstBytes int64 `json:"bandwidthBurstBytes,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	// Deprecated: use services.manager.config.defaultTeamQuotas with network_egress_bytes.
	TeamEgressBandwidthBytesPerSecond int64 `json:"teamEgressBandwidthBytesPerSecond,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	// Deprecated: use services.manager.config.defaultTeamQuotas with network_ingress_bytes.
	TeamIngressBandwidthBytesPerSecond int64 `json:"teamIngressBandwidthBytesPerSecond,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	// Deprecated: use each network quota policy's burstValue.
	TeamBandwidthBurstBytes int64 `json:"teamBandwidthBurstBytes,omitempty"`
	// +optional
	// +kubebuilder:default=53
	DNSPort int `json:"dnsPort,omitempty"`
	// +optional
	PlatformAllowedCIDRs []string `json:"platformAllowedCidrs,omitempty"`
	// +optional
	PlatformDeniedCIDRs []string `json:"platformDeniedCidrs,omitempty"`
	// +optional
	PlatformAllowedDomains []string `json:"platformAllowedDomains,omitempty"`
	// +optional
	PlatformDeniedDomains []string `json:"platformDeniedDomains,omitempty"`
	// +optional
	UseEBPF bool `json:"useEbpf,omitempty"`
	// +optional
	BPFFSPath string `json:"bpfFsPath,omitempty"`
	// +optional
	BPFPinPath string `json:"bpfPinPath,omitempty"`
	// +optional
	UseEDT bool `json:"useEdt,omitempty"`
	// +optional
	// +kubebuilder:default="200ms"
	EDTHorizon metav1.Duration `json:"edtHorizon,omitempty"`
	// +optional
	VethPrefix string `json:"vethPrefix,omitempty"`
	// +optional
	// +kubebuilder:default="10s"
	MetricsReportInterval metav1.Duration `json:"metricsReportInterval,omitempty"`
	// +optional
	// +kubebuilder:default="10s"
	MeteringReportInterval metav1.Duration `json:"meteringReportInterval,omitempty"`
	// +optional
	AuditLogPath string `json:"auditLogPath,omitempty"`
	// +optional
	// +kubebuilder:default=104857600
	AuditLogMaxBytes int64 `json:"auditLogMaxBytes,omitempty"`
	// +optional
	// +kubebuilder:default=5
	AuditLogMaxBackups int `json:"auditLogMaxBackups,omitempty"`
	// +optional
	// +kubebuilder:default="2s"
	ShutdownDelay metav1.Duration `json:"shutdownDelay,omitempty"`
}
