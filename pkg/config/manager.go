package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultNodeAuthorityPort        = 8421
	NodeAuthorityTLSMountDir        = "/etc/sandbox0/node-authority/tls"
	NodeAuthorityControlMountDir    = "/etc/sandbox0/node-authority/control"
	NodeAuthorityClaimMountDir      = "/etc/sandbox0/node-authority/claim"
	NodeAuthorityServerCertPath     = NodeAuthorityTLSMountDir + "/tls.crt"
	NodeAuthorityServerKeyPath      = NodeAuthorityTLSMountDir + "/tls.key"
	NodeAuthorityClientCAPath       = NodeAuthorityTLSMountDir + "/client-ca.crt"
	NodeAuthorityNomadEndpointsPath = NodeAuthorityControlMountDir + "/nomad-endpoints.json"
	NodeAuthorityRuntimeClassesPath = NodeAuthorityClaimMountDir + "/runtime-classes.json"
	NodeAuthorityWriterTokenKeyPath = NodeAuthorityClaimMountDir + "/writer-token.key"
)

// ManagerConfig holds the configuration for the manager.
type ManagerConfig struct {
	// HTTP Server
	HTTPPort int `yaml:"http_port" json:"httpPort"`

	DefaultClusterId     string `yaml:"default_cluster_id" json:"-"`
	RegionID             string `yaml:"region_id" json:"-"`
	TemplateStoreEnabled bool   `yaml:"template_store_enabled" json:"-"`

	// Database
	DatabaseURL      string `yaml:"database_url" json:"-"`
	DatabaseMaxConns int32  `yaml:"database_max_conns" json:"databaseMaxConns"`
	DatabaseMinConns int32  `yaml:"database_min_conns" json:"databaseMinConns"`

	// Cleanup Controller
	CleanupInterval Duration `yaml:"cleanup_interval" json:"cleanupInterval"`

	// Logging
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Metrics
	MetricsPort int `yaml:"metrics_port" json:"metricsPort"`

	// Sandbox
	DefaultSandboxTTL        Duration `yaml:"default_sandbox_ttl" json:"defaultSandboxTTL"`
	TeamTemplateMemoryPerCPU string   `yaml:"team_template_memory_per_cpu" json:"teamTemplateMemoryPerCpu"`
	// SandboxMaxMemory is the maximum memory limit accepted for a single sandbox.
	SandboxMaxMemory string `yaml:"sandbox_max_memory" json:"sandboxMaxMemory"`
	// DefaultTeamQuotas declaratively reconciles region-wide quota defaults.
	// Team-specific database policies override these defaults.
	DefaultTeamQuotas []TeamQuotaLimitConfig `yaml:"default_team_quotas" json:"defaultTeamQuotas"`
	// Timeouts
	ProcdClientTimeout Duration `yaml:"procd_client_timeout" json:"procdClientTimeout"`
	// Procd defaults used by runtime launch and process APIs.
	ProcdConfig         ProcdConfig               `yaml:"procd_config" json:"procdConfig"`
	RootFSMaintenance   RootFSMaintenanceConfig   `yaml:"rootfs_maintenance" json:"-"`
	RootFSImporter      RootFSImporterConfig      `yaml:"rootfs_importer" json:"-"`
	RootFSObjectStorage RootFSObjectStorageConfig `yaml:"rootfs_object_storage" json:"-"`
	NodeAuthority       NodeAuthorityConfig       `yaml:"node_authority" json:"-"`

	// Metering configures the optional region usage ledger.
	Metering MeteringConfig `yaml:"metering" json:"metering"`

	// Registry config used by the durable OCI importer.
	Registry RegistryConfig `yaml:"registry" json:"-"`

	// Public exposure config for generating public URLs.
	PublicRootDomain string `yaml:"public_root_domain" json:"-"`
	PublicRegionID   string `yaml:"public_region_id" json:"-"`

	// Runtime egress authentication resolver settings.
	// Manager resolves matching egress authentication rules at runtime.
	EgressAuthDefaultResolveTTL Duration                 `yaml:"egress_auth_default_resolve_ttl" json:"-"`
	EgressAuthStaticAuth        []StaticEgressAuthConfig `yaml:"egress_auth_static_auth" json:"-"`
	CredentialStore             CredentialStoreConfig    `yaml:"credential_store" json:"-"`
}

// TeamQuotaLimitConfig configures a region default for teams without an
// override for the same dimension.
type TeamQuotaLimitConfig struct {
	Dimension  string `yaml:"dimension" json:"dimension"`
	LimitValue int64  `yaml:"limit_value" json:"limitValue"`
	IntervalMS int64  `yaml:"interval_ms,omitempty" json:"intervalMs,omitempty"`
	BurstValue int64  `yaml:"burst_value,omitempty" json:"burstValue,omitempty"`
}

// StaticEgressAuthConfig defines a static auth directive for runtime egress auth injection.
type StaticEgressAuthConfig struct {
	AuthRef string            `yaml:"auth_ref" json:"authRef"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	TTL     Duration          `yaml:"ttl" json:"ttl"`
}

// CredentialStoreConfig configures credential source secret storage.
type CredentialStoreConfig struct {
	DefaultStorageKind string                       `yaml:"default_storage_kind" json:"-"`
	EncryptedPG        CredentialEncryptedPGConfig  `yaml:"encrypted_pg" json:"-"`
	Vault              CredentialVaultRuntimeConfig `yaml:"vault" json:"-"`
}

type RootFSMaintenanceConfig struct {
	Disabled                        bool     `yaml:"disabled" json:"-"`
	Interval                        Duration `yaml:"interval" json:"-"`
	BatchSize                       int      `yaml:"batch_size" json:"-"`
	MaxBatchesPerRun                int      `yaml:"max_batches_per_run" json:"-"`
	Workers                         int      `yaml:"workers" json:"-"`
	ObjectDeleteClaimTTL            Duration `yaml:"object_delete_claim_ttl" json:"-"`
	ObjectDeleteBackoffBase         Duration `yaml:"object_delete_backoff_base" json:"-"`
	ObjectDeleteBackoffMax          Duration `yaml:"object_delete_backoff_max" json:"-"`
	ObjectDeleteMaxAttempts         int      `yaml:"object_delete_max_attempts" json:"-"`
	SquashDisabled                  bool     `yaml:"squash_disabled" json:"-"`
	SquashMaxChainDepth             int      `yaml:"squash_max_chain_depth" json:"-"`
	SquashMaxChainBytes             int64    `yaml:"squash_max_chain_bytes" json:"-"`
	MaterializerDisabled            bool     `yaml:"materializer_disabled" json:"-"`
	MaterializerInterval            Duration `yaml:"materializer_interval" json:"-"`
	MaterializerScanLimit           int      `yaml:"materializer_scan_limit" json:"-"`
	MaterializerMinPackBytes        int64    `yaml:"materializer_min_pack_bytes" json:"-"`
	MaterializerMaxDelay            Duration `yaml:"materializer_max_delay" json:"-"`
	MaterializerForcedFlushesPerRun int      `yaml:"materializer_forced_flushes_per_run" json:"-"`
	MaterializerGarbageInterval     Duration `yaml:"materializer_garbage_interval" json:"-"`
	MaterializerUploadingStale      Duration `yaml:"materializer_uploading_stale" json:"-"`
	MaterializerTerminalRetention   Duration `yaml:"materializer_terminal_retention" json:"-"`
}

// RootFSImporterConfig configures the active-active digest-pinned OCI to
// immutable block artifact worker.
type RootFSImporterConfig struct {
	Disabled          bool     `yaml:"disabled" json:"-"`
	WorkerID          string   `yaml:"worker_id" json:"-"`
	Interval          Duration `yaml:"interval" json:"-"`
	DiscoveryInterval Duration `yaml:"discovery_interval" json:"-"`
	DiscoveryPageSize int      `yaml:"discovery_page_size" json:"-"`
	BuildTimeout      Duration `yaml:"build_timeout" json:"-"`
	LeaseTTL          Duration `yaml:"lease_ttl" json:"-"`
	LeaseRenewal      Duration `yaml:"lease_renewal" json:"-"`
	MaxAttempts       int      `yaml:"max_attempts" json:"-"`
	GarbageInterval   Duration `yaml:"garbage_interval" json:"-"`
	TerminalRetention Duration `yaml:"terminal_retention" json:"-"`
	GarbageLimit      int      `yaml:"garbage_limit" json:"-"`
	WorkRoot          string   `yaml:"work_root" json:"-"`
	ProcdPath         string   `yaml:"procd_path" json:"-"`
	ProcdProtocol     string   `yaml:"procd_protocol" json:"-"`
	ProcdDigest       string   `yaml:"procd_digest" json:"-"`
	PlainHTTPHosts    []string `yaml:"plain_http_hosts" json:"-"`
}

type RootFSObjectStorageConfig struct {
	Type                       string `yaml:"type" json:"-"`
	Bucket                     string `yaml:"bucket" json:"-"`
	Region                     string `yaml:"region" json:"-"`
	Endpoint                   string `yaml:"endpoint" json:"-"`
	AccessKey                  string `yaml:"access_key" json:"-"`
	SecretKey                  string `yaml:"secret_key" json:"-"`
	SessionToken               string `yaml:"session_token" json:"-"`
	ObjectEncryptionEnabled    bool   `yaml:"object_encryption_enabled" json:"-"`
	ObjectEncryptionKeyPath    string `yaml:"object_encryption_key_path" json:"-"`
	ObjectEncryptionPassphrase string `yaml:"object_encryption_passphrase" json:"-"`
	ObjectEncryptionAlgo       string `yaml:"object_encryption_algo" json:"-"`
}

// NodeAuthorityConfig configures manager's dedicated mTLS listener for
// trusted node agents. The normal manager HTTP listener never serves these
// endpoints.
type NodeAuthorityConfig struct {
	Enabled      bool                          `yaml:"enabled" json:"-"`
	ListenHost   string                        `yaml:"listen_host" json:"-"`
	Port         int                           `yaml:"port" json:"-"`
	CertFile     string                        `yaml:"cert_file" json:"-"`
	KeyFile      string                        `yaml:"key_file" json:"-"`
	ClientCAFile string                        `yaml:"client_ca_file" json:"-"`
	Identities   []NodeAuthorityIdentityConfig `yaml:"identities" json:"-"`

	WriterLeaseTTL          Duration                  `yaml:"writer_lease_ttl" json:"-"`
	WriterRenewalGrace      Duration                  `yaml:"writer_renewal_grace" json:"-"`
	RuntimeSlotHeartbeatTTL Duration                  `yaml:"runtime_slot_heartbeat_ttl" json:"-"`
	Claim                   RuntimeSlotClaimConfig    `yaml:"claim" json:"-"`
	Terminal                RuntimeSlotTerminalConfig `yaml:"terminal" json:"-"`
}

// RuntimeSlotClaimConfig controls the manager request path that binds logical
// sandboxes to exact Nomad warm-slot compatibility classes.
type RuntimeSlotClaimConfig struct {
	ClassCatalogFile   string   `yaml:"class_catalog_file" json:"-"`
	WriterTokenKeyFile string   `yaml:"writer_token_key_file" json:"-"`
	ClaimTTL           Duration `yaml:"claim_ttl" json:"-"`
	SLO                Duration `yaml:"slo" json:"-"`
}

// NodeAuthorityIdentityConfig binds one verified certificate common name to
// an exact node route.
type NodeAuthorityIdentityConfig struct {
	CommonName string `yaml:"common_name" json:"-"`
	ClusterID  string `yaml:"cluster_id" json:"-"`
	NodeID     string `yaml:"node_id" json:"-"`
	NodeUID    string `yaml:"node_uid" json:"-"`
	AgentUID   string `yaml:"agent_uid" json:"-"`
}

// RuntimeSlotTerminalConfig controls active-active terminal reconciliation.
type RuntimeSlotTerminalConfig struct {
	Enabled            bool     `yaml:"enabled" json:"-"`
	NomadEndpointsFile string   `yaml:"nomad_endpoints_file" json:"-"`
	Interval           Duration `yaml:"interval" json:"-"`
	PassTimeout        Duration `yaml:"pass_timeout" json:"-"`
	ScanLimit          int      `yaml:"scan_limit" json:"-"`
}

type CredentialEncryptedPGConfig struct {
	KeyID   string `yaml:"key_id" json:"-"`
	KeyFile string `yaml:"key_file" json:"-"`
	Key     string `yaml:"key" json:"-"`
}

type CredentialVaultRuntimeConfig struct {
	Connections []CredentialVaultConnectionConfig `yaml:"connections" json:"-"`
}

type CredentialVaultConnectionConfig struct {
	Name                string   `yaml:"name" json:"-"`
	Provider            string   `yaml:"provider" json:"-"`
	Address             string   `yaml:"address" json:"-"`
	TokenFile           string   `yaml:"token_file" json:"-"`
	CACertFile          string   `yaml:"ca_cert_file" json:"-"`
	Namespace           string   `yaml:"namespace" json:"-"`
	DefaultMount        string   `yaml:"default_mount" json:"-"`
	KVVersion           int      `yaml:"kv_version" json:"-"`
	SkipTLSVerify       bool     `yaml:"skip_tls_verify" json:"-"`
	AllowedPathPrefixes []string `yaml:"allowed_path_prefixes" json:"-"`
}

// RegistryConfig holds registry settings for manager.
type RegistryConfig struct {
	// Provider is the registry provider identifier.
	Provider string `yaml:"provider" json:"-"`

	// PushRegistry is the registry hostname (host:port) for external image push.
	PushRegistry string `yaml:"push_registry" json:"-"`

	// PullRegistry is the registry hostname (host:port) reachable by sandbox nodes.
	PullRegistry string `yaml:"pull_registry" json:"-"`

	// InternalRegistry is the registry endpoint used by manager for server-side image publication.
	InternalRegistry string `yaml:"internal_registry" json:"-"`

	// PullCredentialsFile is the path to a dockerconfigjson file used for image pulls.
	PullCredentialsFile string `yaml:"pull_credentials_file" json:"-"`

	// AWS configures AWS registry integration.
	AWS *RegistryAWSConfig `yaml:"aws" json:"-"`

	// GCP configures GCP registry integration.
	GCP *RegistryGCPConfig `yaml:"gcp" json:"-"`

	// Azure configures Azure registry integration.
	Azure *RegistryAzureConfig `yaml:"azure" json:"-"`

	// Aliyun configures Aliyun registry integration.
	Aliyun *RegistryAliyunConfig `yaml:"aliyun" json:"-"`

	// Harbor configures Harbor registry integration.
	Harbor *RegistryHarborConfig `yaml:"harbor" json:"-"`

	// Builtin configures builtin registry integration.
	Builtin *RegistryBuiltinConfig `yaml:"builtin" json:"-"`
}

// RegistryAWSConfig defines AWS registry config.
type RegistryAWSConfig struct {
	Region              string `yaml:"region" json:"-"`
	RegistryID          string `yaml:"registry_id" json:"-"`
	AssumeRoleARN       string `yaml:"assume_role_arn" json:"-"`
	ExternalID          string `yaml:"external_id" json:"-"`
	RegistryOverride    string `yaml:"registry_override" json:"-"`
	AccessKeyID         string `yaml:"access_key_id" json:"-"`
	AccessKeyIDFile     string `yaml:"access_key_id_file" json:"-"`
	SecretAccessKey     string `yaml:"secret_access_key" json:"-"`
	SecretAccessKeyFile string `yaml:"secret_access_key_file" json:"-"`
	SessionToken        string `yaml:"session_token" json:"-"`
	SessionTokenFile    string `yaml:"session_token_file" json:"-"`
}

// RegistryGCPConfig defines GCP registry config.
type RegistryGCPConfig struct {
	Registry               string `yaml:"registry" json:"-"`
	ServiceAccountJSON     string `yaml:"service_account_json" json:"-"`
	ServiceAccountJSONFile string `yaml:"service_account_json_file" json:"-"`
}

// RegistryAzureConfig defines Azure registry config.
type RegistryAzureConfig struct {
	Registry         string `yaml:"registry" json:"-"`
	TenantID         string `yaml:"tenant_id" json:"-"`
	TenantIDFile     string `yaml:"tenant_id_file" json:"-"`
	ClientID         string `yaml:"client_id" json:"-"`
	ClientIDFile     string `yaml:"client_id_file" json:"-"`
	ClientSecret     string `yaml:"client_secret" json:"-"`
	ClientSecretFile string `yaml:"client_secret_file" json:"-"`
}

// RegistryAliyunConfig defines Aliyun registry config.
type RegistryAliyunConfig struct {
	Registry            string `yaml:"registry" json:"-"`
	Namespace           string `yaml:"namespace" json:"-"`
	Region              string `yaml:"region" json:"-"`
	InstanceID          string `yaml:"instance_id" json:"-"`
	AssumeRoleARN       string `yaml:"assume_role_arn" json:"-"`
	ExternalID          string `yaml:"external_id" json:"-"`
	SessionDuration     int64  `yaml:"session_duration_seconds" json:"-"`
	AccessKeyID         string `yaml:"access_key_id" json:"-"`
	AccessKeyIDFile     string `yaml:"access_key_id_file" json:"-"`
	AccessKeySecret     string `yaml:"access_key_secret" json:"-"`
	AccessKeySecretFile string `yaml:"access_key_secret_file" json:"-"`
}

// RegistryHarborConfig defines Harbor registry config.
type RegistryHarborConfig struct {
	Registry     string `yaml:"registry" json:"-"`
	Username     string `yaml:"username" json:"-"`
	UsernameFile string `yaml:"username_file" json:"-"`
	Password     string `yaml:"password" json:"-"`
	PasswordFile string `yaml:"password_file" json:"-"`
}

// RegistryBuiltinConfig defines builtin registry config.
type RegistryBuiltinConfig struct {
	Username     string `yaml:"username" json:"-"`
	UsernameFile string `yaml:"username_file" json:"-"`
	Password     string `yaml:"password" json:"-"`
	PasswordFile string `yaml:"password_file" json:"-"`
}

// LoadManagerConfig returns the manager configuration.
func LoadManagerConfig() *ManagerConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using default config\n", path, err)
		cfg = defaultManagerConfig()
	}
	if cfg.EgressAuthDefaultResolveTTL.Duration == 0 {
		cfg.EgressAuthDefaultResolveTTL = Duration{Duration: 5 * time.Minute}
	}
	for idx := range cfg.EgressAuthStaticAuth {
		if cfg.EgressAuthStaticAuth[idx].TTL.Duration == 0 {
			cfg.EgressAuthStaticAuth[idx].TTL = cfg.EgressAuthDefaultResolveTTL
		}
	}
	if cfg.CredentialStore.DefaultStorageKind == "" {
		cfg.CredentialStore.DefaultStorageKind = "encrypted_pg"
	}
	applyRootFSMaintenanceDefaults(cfg)
	applyRootFSImporterDefaults(cfg)
	applyRuntimeDefaults(cfg)
	applyNodeAuthorityDefaults(cfg)
	return cfg
}

func applyRootFSMaintenanceDefaults(cfg *ManagerConfig) {
	if cfg == nil {
		return
	}
	if cfg.RootFSMaintenance.Interval.Duration == 0 {
		cfg.RootFSMaintenance.Interval = Duration{Duration: time.Minute}
	}
	if cfg.RootFSMaintenance.BatchSize <= 0 {
		cfg.RootFSMaintenance.BatchSize = 100
	}
	if cfg.RootFSMaintenance.MaxBatchesPerRun <= 0 {
		cfg.RootFSMaintenance.MaxBatchesPerRun = 10
	}
	if cfg.RootFSMaintenance.Workers <= 0 {
		cfg.RootFSMaintenance.Workers = 1
	}
	if cfg.RootFSMaintenance.ObjectDeleteClaimTTL.Duration == 0 {
		cfg.RootFSMaintenance.ObjectDeleteClaimTTL = Duration{Duration: 2 * time.Minute}
	}
	if cfg.RootFSMaintenance.ObjectDeleteBackoffBase.Duration == 0 {
		cfg.RootFSMaintenance.ObjectDeleteBackoffBase = Duration{Duration: 5 * time.Second}
	}
	if cfg.RootFSMaintenance.ObjectDeleteBackoffMax.Duration == 0 {
		cfg.RootFSMaintenance.ObjectDeleteBackoffMax = Duration{Duration: 10 * time.Minute}
	}
	if cfg.RootFSMaintenance.SquashMaxChainDepth <= 0 {
		cfg.RootFSMaintenance.SquashMaxChainDepth = 8
	}
	if cfg.RootFSMaintenance.SquashMaxChainBytes <= 0 {
		cfg.RootFSMaintenance.SquashMaxChainBytes = 512 * 1024 * 1024
	}
	if cfg.RootFSMaintenance.MaterializerInterval.Duration == 0 {
		cfg.RootFSMaintenance.MaterializerInterval = Duration{Duration: time.Second}
	}
	if cfg.RootFSMaintenance.MaterializerScanLimit <= 0 {
		cfg.RootFSMaintenance.MaterializerScanLimit = 1000
	}
	if cfg.RootFSMaintenance.MaterializerMinPackBytes <= 0 {
		cfg.RootFSMaintenance.MaterializerMinPackBytes = 32 << 20
	}
	if cfg.RootFSMaintenance.MaterializerMaxDelay.Duration == 0 {
		cfg.RootFSMaintenance.MaterializerMaxDelay = Duration{Duration: 5 * time.Minute}
	}
	if cfg.RootFSMaintenance.MaterializerForcedFlushesPerRun <= 0 {
		cfg.RootFSMaintenance.MaterializerForcedFlushesPerRun = 1
	}
	if cfg.RootFSMaintenance.MaterializerGarbageInterval.Duration == 0 {
		cfg.RootFSMaintenance.MaterializerGarbageInterval = Duration{Duration: time.Minute}
	}
	if cfg.RootFSMaintenance.MaterializerUploadingStale.Duration == 0 {
		cfg.RootFSMaintenance.MaterializerUploadingStale = Duration{Duration: time.Hour}
	}
	if cfg.RootFSMaintenance.MaterializerTerminalRetention.Duration == 0 {
		cfg.RootFSMaintenance.MaterializerTerminalRetention = Duration{Duration: 24 * time.Hour}
	}
}

func applyRootFSImporterDefaults(cfg *ManagerConfig) {
	if cfg == nil {
		return
	}
	if cfg.RootFSImporter.Interval.Duration == 0 {
		cfg.RootFSImporter.Interval = Duration{Duration: time.Second}
	}
	if cfg.RootFSImporter.DiscoveryInterval.Duration == 0 {
		cfg.RootFSImporter.DiscoveryInterval = Duration{Duration: 5 * time.Second}
	}
	if cfg.RootFSImporter.DiscoveryPageSize == 0 {
		cfg.RootFSImporter.DiscoveryPageSize = 100
	}
	if cfg.RootFSImporter.BuildTimeout.Duration == 0 {
		cfg.RootFSImporter.BuildTimeout = Duration{Duration: 2 * time.Hour}
	}
	if cfg.RootFSImporter.LeaseTTL.Duration == 0 {
		cfg.RootFSImporter.LeaseTTL = Duration{Duration: 2 * time.Minute}
	}
	if cfg.RootFSImporter.LeaseRenewal.Duration == 0 {
		cfg.RootFSImporter.LeaseRenewal = Duration{Duration: 30 * time.Second}
	}
	if cfg.RootFSImporter.MaxAttempts == 0 {
		cfg.RootFSImporter.MaxAttempts = 5
	}
	if cfg.RootFSImporter.GarbageInterval.Duration == 0 {
		cfg.RootFSImporter.GarbageInterval = Duration{Duration: time.Minute}
	}
	if cfg.RootFSImporter.TerminalRetention.Duration == 0 {
		cfg.RootFSImporter.TerminalRetention = Duration{Duration: 24 * time.Hour}
	}
	if cfg.RootFSImporter.GarbageLimit == 0 {
		cfg.RootFSImporter.GarbageLimit = 100
	}
}

func applyNodeAuthorityDefaults(cfg *ManagerConfig) {
	if cfg == nil || !cfg.NodeAuthority.Enabled {
		return
	}
	if cfg.NodeAuthority.Port == 0 {
		cfg.NodeAuthority.Port = DefaultNodeAuthorityPort
	}
	if cfg.NodeAuthority.WriterLeaseTTL.Duration == 0 {
		cfg.NodeAuthority.WriterLeaseTTL = Duration{Duration: 30 * time.Second}
	}
	if cfg.NodeAuthority.WriterRenewalGrace.Duration == 0 {
		cfg.NodeAuthority.WriterRenewalGrace = Duration{Duration: 5 * time.Second}
	}
	if cfg.NodeAuthority.RuntimeSlotHeartbeatTTL.Duration == 0 {
		cfg.NodeAuthority.RuntimeSlotHeartbeatTTL = Duration{Duration: 30 * time.Second}
	}
}

func applyRuntimeDefaults(cfg *ManagerConfig) {
	if cfg == nil {
		return
	}
	if cfg.NodeAuthority.Claim.ClassCatalogFile == "" {
		cfg.NodeAuthority.Claim.ClassCatalogFile = NodeAuthorityRuntimeClassesPath
	}
	if cfg.NodeAuthority.Claim.WriterTokenKeyFile == "" {
		cfg.NodeAuthority.Claim.WriterTokenKeyFile = NodeAuthorityWriterTokenKeyPath
	}
	if cfg.NodeAuthority.Claim.ClaimTTL.Duration == 0 {
		cfg.NodeAuthority.Claim.ClaimTTL = Duration{Duration: 15 * time.Second}
	}
	if cfg.NodeAuthority.Claim.SLO.Duration == 0 {
		cfg.NodeAuthority.Claim.SLO = Duration{Duration: time.Second}
	}
}

func loadManagerConfig(path string) (*ManagerConfig, error) {
	cfg := defaultManagerConfig()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	data = []byte(os.ExpandEnv(string(data)))

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}

func defaultManagerConfig() *ManagerConfig { return &ManagerConfig{} }
