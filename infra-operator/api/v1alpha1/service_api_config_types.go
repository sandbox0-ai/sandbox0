package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	// +optional
	// +kubebuilder:default="sandbox0.site"
	FunctionRootDomain string `json:"functionRootDomain,omitempty"`
	// +optional
	FunctionRegionID string `json:"functionRegionId,omitempty"`
	// +optional
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

// FunctionGatewayConfig defines user-facing configuration for function-gateway.
type FunctionGatewayConfig struct {
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
	// +kubebuilder:default="sandbox0.site"
	FunctionRootDomain string `json:"functionRootDomain,omitempty"`
	// +optional
	FunctionRegionID string `json:"functionRegionId,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	InternalAuthTTL metav1.Duration `json:"internalAuthTtl,omitempty"`
	// +optional
	// +kubebuilder:default="function-gateway"
	InternalAuthCaller string `json:"internalAuthCaller,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
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
	// +kubebuilder:default={"regional-gateway","scheduler","function-gateway","cluster-gateway"}
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
	// +optional
	// +kubebuilder:default="sandbox0.site"
	FunctionRootDomain string `json:"functionRootDomain,omitempty"`
	// +optional
	FunctionRegionID string `json:"functionRegionId,omitempty"`
	// +optional
	GatewayConfig `json:",inline"`
	// +optional
	// +kubebuilder:default={"*:*"}
	SchedulerPermissions []string `json:"schedulerPermissions,omitempty"`
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
	// +optional
	SandboxRuntimeClassName string `json:"sandboxRuntimeClassName,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	NetdPolicyApplyTimeout metav1.Duration `json:"netdPolicyApplyTimeout,omitempty"`
	// +optional
	// +kubebuilder:default="500ms"
	NetdPolicyApplyPollInterval metav1.Duration `json:"netdPolicyApplyPollInterval,omitempty"`
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

// StorageProxyConfig defines user-facing configuration for storage-proxy.
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
	// +kubebuilder:default=10000
	MaxOpsPerSecond int `json:"maxOpsPerSecond,omitempty"`
	// +optional
	// +kubebuilder:default=1073741824
	MaxBytesPerSecond int64 `json:"maxBytesPerSecond,omitempty"`
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

// NetdConfig defines user-facing configuration for netd.
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
