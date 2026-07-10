package config

import (
	"fmt"
	"os"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CtldConfig combines the storage settings used by volume portals with
// node-local ctld runtime metric producer settings.
type CtldConfig struct {
	StorageProxyConfig `yaml:",inline"`

	SandboxObservabilityRuntimeSamplesIngestURL string `yaml:"sandbox_observability_runtime_samples_ingest_url" json:"-"`
	SandboxObservabilityIngestQueueSize         int    `yaml:"sandbox_observability_ingest_queue_size" json:"-"`
	SandboxObservabilityIngestBatchSize         int    `yaml:"sandbox_observability_ingest_batch_size" json:"-"`

	SandboxObservabilityIngestFlushInterval   metav1.Duration `yaml:"sandbox_observability_ingest_flush_interval" json:"-"`
	SandboxObservabilityIngestRequestTimeout  metav1.Duration `yaml:"sandbox_observability_ingest_request_timeout" json:"-"`
	SandboxObservabilityIngestMaxRetries      int             `yaml:"sandbox_observability_ingest_max_retries" json:"-"`
	SandboxObservabilityIngestRetryBackoff    metav1.Duration `yaml:"sandbox_observability_ingest_retry_backoff" json:"-"`
	SandboxObservabilityRuntimeSampleInterval metav1.Duration `yaml:"sandbox_observability_runtime_sample_interval" json:"-"`
	SandboxObservabilityRuntimeSampleJitter   metav1.Duration `yaml:"sandbox_observability_runtime_sample_jitter" json:"-"`
}

// LoadCtldConfig loads the shared ctld configuration file.
func LoadCtldConfig() *CtldConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}
	cfg, err := loadCtldConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load ctld config from %s: %v, using defaults\n", path, err)
		cfg = &CtldConfig{}
	}
	applyCtldDefaults(cfg)
	return cfg
}

func loadCtldConfig(path string) (*CtldConfig, error) {
	if path == "" {
		cfg := &CtldConfig{}
		applyCtldDefaults(cfg)
		return cfg, nil
	}
	storageCfg, err := loadStorageProxyConfig(path)
	if err != nil {
		return nil, err
	}
	cfg := &CtldConfig{StorageProxyConfig: *storageCfg}
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
