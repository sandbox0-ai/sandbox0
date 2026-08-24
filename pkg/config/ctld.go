package config

import (
	"fmt"
	"os"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"gopkg.in/yaml.v3"
)

// CtldConfig holds rootfs persistence, metering, and node-local runtime
// metric producer settings.
type CtldConfig struct {
	DatabaseURL      string `yaml:"database_url" json:"-"`
	DatabaseMaxConns int    `yaml:"database_max_conns" json:"-"`
	DatabaseMinConns int    `yaml:"database_min_conns" json:"-"`
	RegionID         string `yaml:"region_id" json:"-"`
	DefaultClusterId string `yaml:"default_cluster_id" json:"-"`

	RootFSObjectStorage RootFSObjectStorageConfig `yaml:"rootfs_object_storage" json:"-"`
	NomadRuntime        CtldNomadRuntimeConfig    `yaml:"nomad_runtime" json:"-"`
	Metering            MeteringConfig            `yaml:"metering" json:"-"`

	SandboxObservabilityRuntimeSamplesIngestURL string `yaml:"sandbox_observability_runtime_samples_ingest_url" json:"-"`
	SandboxObservabilityIngestQueueSize         int    `yaml:"sandbox_observability_ingest_queue_size" json:"-"`
	SandboxObservabilityIngestBatchSize         int    `yaml:"sandbox_observability_ingest_batch_size" json:"-"`

	SandboxObservabilityIngestFlushInterval   Duration `yaml:"sandbox_observability_ingest_flush_interval" json:"-"`
	SandboxObservabilityIngestRequestTimeout  Duration `yaml:"sandbox_observability_ingest_request_timeout" json:"-"`
	SandboxObservabilityIngestMaxRetries      int      `yaml:"sandbox_observability_ingest_max_retries" json:"-"`
	SandboxObservabilityIngestRetryBackoff    Duration `yaml:"sandbox_observability_ingest_retry_backoff" json:"-"`
	SandboxObservabilityRuntimeSampleInterval Duration `yaml:"sandbox_observability_runtime_sample_interval" json:"-"`
	SandboxObservabilityRuntimeSampleJitter   Duration `yaml:"sandbox_observability_runtime_sample_jitter" json:"-"`
}

// CtldNomadRuntimeConfig configures the HA-primary-scoped privileged Nomad
// node runtime. Every path is resolved in ctld's host namespaces.
type CtldNomadRuntimeConfig struct {
	Enabled bool `yaml:"enabled" json:"-"`

	SocketPath            string `yaml:"socket_path" json:"-"`
	RunscPath             string `yaml:"runsc_path" json:"-"`
	RunscRoot             string `yaml:"runsc_root" json:"-"`
	Platform              string `yaml:"platform" json:"-"`
	Overlay2              string `yaml:"overlay2" json:"-"`
	FileAccess            string `yaml:"file_access" json:"-"`
	DirectFS              *bool  `yaml:"directfs" json:"-"`
	ResourceCgroupRoot    string `yaml:"resource_cgroup_root" json:"-"`
	ResourceCPUMillicores int64  `yaml:"resource_cpu_millicores" json:"-"`
	ResourceMemoryBytes   int64  `yaml:"resource_memory_bytes" json:"-"`
	ResourceCPUSetCPUs    string `yaml:"resource_cpuset_cpus" json:"-"`
	ResourceCPUSetMems    string `yaml:"resource_cpuset_mems" json:"-"`

	StatePath                       string   `yaml:"state_path" json:"-"`
	BranchRoot                      string   `yaml:"branch_root" json:"-"`
	MountRoot                       string   `yaml:"mount_root" json:"-"`
	ConsumerMountRoot               string   `yaml:"consumer_mount_root" json:"-"`
	ConsumerNetNSRoot               string   `yaml:"consumer_netns_root" json:"-"`
	MaxDirtyTailBytes               int64    `yaml:"max_dirty_tail_bytes" json:"-"`
	MaxNodeDirtyTailBytes           int64    `yaml:"max_node_dirty_tail_bytes" json:"-"`
	DirtyTailRetirementReserveBytes int64    `yaml:"dirty_tail_retirement_reserve_bytes" json:"-"`
	NBDDevices                      []string `yaml:"nbd_devices" json:"-"`
	RuntimeSlotJournalPath          string   `yaml:"runtime_slot_journal_path" json:"-"`
	NodeBootIDFile                  string   `yaml:"node_boot_id_file" json:"-"`

	AuthorityURL            string `yaml:"authority_url" json:"-"`
	AuthorityCAFile         string `yaml:"authority_ca_file" json:"-"`
	AuthorityClientCertFile string `yaml:"authority_client_cert_file" json:"-"`
	AuthorityClientKeyFile  string `yaml:"authority_client_key_file" json:"-"`
	AuthorityTokenFile      string `yaml:"authority_token_file" json:"-"`
	AuthorityPeerURISAN     string `yaml:"authority_peer_uri_san" json:"-"`

	NomadAddress   string `yaml:"nomad_address" json:"-"`
	NomadNodeID    string `yaml:"nomad_node_id" json:"-"`
	NomadTokenFile string `yaml:"nomad_token_file" json:"-"`
	NomadCAFile    string `yaml:"nomad_ca_file" json:"-"`
	NomadCertFile  string `yaml:"nomad_cert_file" json:"-"`
	NomadKeyFile   string `yaml:"nomad_key_file" json:"-"`

	NodeUID            string   `yaml:"node_uid" json:"-"`
	ControlRoot        string   `yaml:"control_root" json:"-"`
	NodeControlTimeout Duration `yaml:"node_control_timeout" json:"-"`
}

// LoadCtldConfig loads the shared ctld configuration file.
func LoadCtldConfig() *CtldConfig {
	path := ctldConfigPath()
	cfg, err := loadCtldConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load ctld config from %s: %v, using defaults\n", path, err)
		cfg = &CtldConfig{}
	}
	applyCtldDefaults(cfg)
	return cfg
}

// LoadCtldConfigStrict loads the configured ctld file without falling back to
// defaults. Production ctld startup uses this path so malformed or missing
// node authority configuration cannot select another runtime mode.
func LoadCtldConfigStrict() (*CtldConfig, error) {
	path := ctldConfigPath()
	cfg, err := loadCtldConfig(path)
	if err != nil {
		return nil, fmt.Errorf("load ctld config from %s: %w", path, err)
	}
	return cfg, nil
}

func ctldConfigPath() string {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}
	return path
}

func loadCtldConfig(path string) (*CtldConfig, error) {
	if path == "" {
		cfg := &CtldConfig{}
		applyCtldDefaults(cfg)
		return cfg, nil
	}
	cfg := &CtldConfig{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	data = []byte(os.ExpandEnv(string(data)))
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	applyCtldDefaults(cfg)
	return cfg, nil
}

func applyCtldDefaults(cfg *CtldConfig) {
	if cfg == nil {
		return
	}
	if cfg.SandboxObservabilityIngestQueueSize <= 0 {
		cfg.SandboxObservabilityIngestQueueSize = 1024
	}
	if cfg.SandboxObservabilityIngestBatchSize <= 0 {
		cfg.SandboxObservabilityIngestBatchSize = 100
	}
	if cfg.SandboxObservabilityIngestFlushInterval.Duration <= 0 {
		cfg.SandboxObservabilityIngestFlushInterval.Duration = time.Second
	}
	if cfg.SandboxObservabilityIngestRequestTimeout.Duration <= 0 {
		cfg.SandboxObservabilityIngestRequestTimeout.Duration = 2 * time.Second
	}
	if cfg.SandboxObservabilityIngestMaxRetries <= 0 {
		cfg.SandboxObservabilityIngestMaxRetries = 3
	}
	if cfg.SandboxObservabilityIngestRetryBackoff.Duration <= 0 {
		cfg.SandboxObservabilityIngestRetryBackoff.Duration = 100 * time.Millisecond
	}
	if cfg.SandboxObservabilityRuntimeSampleInterval.Duration <= 0 {
		cfg.SandboxObservabilityRuntimeSampleInterval.Duration = sandboxobservability.DefaultRuntimeSampleInterval
	}
	if cfg.SandboxObservabilityRuntimeSampleJitter.Duration <= 0 {
		cfg.SandboxObservabilityRuntimeSampleJitter.Duration = sandboxobservability.DefaultRuntimeSampleJitter
	}
}
