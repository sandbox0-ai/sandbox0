package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadCtldConfigLoadsInlineStorageAndRuntimeMetricSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctld.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
region_id: aws/us-east-1
default_cluster_id: cluster-a
database_url: postgres://storage
sandbox_observability_runtime_samples_ingest_url: http://cluster-gateway/internal/v1/sandbox-observability/runtime-samples
sandbox_observability_ingest_queue_size: 77
sandbox_observability_ingest_batch_size: 11
sandbox_observability_ingest_flush_interval:
  duration: 2s
sandbox_observability_ingest_request_timeout:
  duration: 3s
sandbox_observability_ingest_max_retries: 4
sandbox_observability_ingest_retry_backoff:
  duration: 250ms
sandbox_observability_runtime_sample_interval:
  duration: 20s
sandbox_observability_runtime_sample_jitter:
  duration: 2s
`), 0o600))

	cfg, err := loadCtldConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "aws/us-east-1", cfg.RegionID)
	assert.Equal(t, "cluster-a", cfg.DefaultClusterId)
	assert.Equal(t, "postgres://storage", cfg.DatabaseURL)
	assert.Equal(t, 77, cfg.SandboxObservabilityIngestQueueSize)
	assert.Equal(t, 11, cfg.SandboxObservabilityIngestBatchSize)
	assert.Equal(t, 2*time.Second, cfg.SandboxObservabilityIngestFlushInterval.Duration)
	assert.Equal(t, 3*time.Second, cfg.SandboxObservabilityIngestRequestTimeout.Duration)
	assert.Equal(t, 4, cfg.SandboxObservabilityIngestMaxRetries)
	assert.Equal(t, 250*time.Millisecond, cfg.SandboxObservabilityIngestRetryBackoff.Duration)
	assert.Equal(t, 20*time.Second, cfg.SandboxObservabilityRuntimeSampleInterval.Duration)
	assert.Equal(t, 2*time.Second, cfg.SandboxObservabilityRuntimeSampleJitter.Duration)
}

func TestLoadCtldConfigAppliesProducerDefaults(t *testing.T) {
	cfg, err := loadCtldConfig("")
	require.NoError(t, err)
	assert.Equal(t, 1024, cfg.SandboxObservabilityIngestQueueSize)
	assert.Equal(t, 100, cfg.SandboxObservabilityIngestBatchSize)
	assert.Equal(t, time.Second, cfg.SandboxObservabilityIngestFlushInterval.Duration)
	assert.Equal(t, 2*time.Second, cfg.SandboxObservabilityIngestRequestTimeout.Duration)
	assert.Equal(t, 3, cfg.SandboxObservabilityIngestMaxRetries)
	assert.Equal(t, 100*time.Millisecond, cfg.SandboxObservabilityIngestRetryBackoff.Duration)
	assert.Equal(t, sandboxobservability.DefaultRuntimeSampleInterval, cfg.SandboxObservabilityRuntimeSampleInterval.Duration)
	assert.Equal(t, sandboxobservability.DefaultRuntimeSampleJitter, cfg.SandboxObservabilityRuntimeSampleJitter.Duration)
}

func TestLoadCtldConfigPreservesStorageProxyLoaderValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctld.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
filesystem_name: sandbox-root
filesystem_block_size: 8192
cache_dir: /var/lib/custom-cache
metrics_enabled: true
metrics_port: 9191
`), 0o600))

	storageCfg, err := loadStorageProxyConfig(path)
	require.NoError(t, err)
	ctldCfg, err := loadCtldConfig(path)
	require.NoError(t, err)

	assert.Equal(t, storageCfg.FilesystemName, ctldCfg.FilesystemName)
	assert.Equal(t, storageCfg.FilesystemBlockSize, ctldCfg.FilesystemBlockSize)
	assert.Equal(t, storageCfg.CacheDir, ctldCfg.CacheDir)
	assert.Equal(t, storageCfg.MetricsEnabled, ctldCfg.MetricsEnabled)
	assert.Equal(t, storageCfg.MetricsPort, ctldCfg.MetricsPort)
}
