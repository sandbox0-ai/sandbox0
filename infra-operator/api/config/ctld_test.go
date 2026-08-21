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

func TestLoadCtldConfigLoadsRootFSObjectStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctld.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
rootfs_object_storage:
  type: s3
  bucket: rootfs-bucket
  region: us-east-1
  endpoint: https://s3.example.com
`), 0o600))

	ctldCfg, err := loadCtldConfig(path)
	require.NoError(t, err)

	assert.Equal(t, "s3", ctldCfg.RootFSObjectStorage.Type)
	assert.Equal(t, "rootfs-bucket", ctldCfg.RootFSObjectStorage.Bucket)
	assert.Equal(t, "us-east-1", ctldCfg.RootFSObjectStorage.Region)
	assert.Equal(t, "https://s3.example.com", ctldCfg.RootFSObjectStorage.Endpoint)
}

func TestLoadCtldConfigLoadsNomadRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctld.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
nomad_runtime:
  enabled: true
  socket_path: /run/sandbox0/ctld-nomad-runtime.sock
  directfs: false
  nbd_devices: [/dev/nbd0, /dev/nbd1]
  max_node_dirty_tail_bytes: 42949672960
  authority_url: https://manager.internal:9444
  nomad_address: https://nomad.internal:4646
  nomad_node_id: node-1
  node_uid: node-uid-1
`), 0o600))

	cfg, err := loadCtldConfig(path)
	require.NoError(t, err)
	require.True(t, cfg.NomadRuntime.Enabled)
	require.NotNil(t, cfg.NomadRuntime.DirectFS)
	assert.False(t, *cfg.NomadRuntime.DirectFS)
	assert.Equal(t, []string{"/dev/nbd0", "/dev/nbd1"}, cfg.NomadRuntime.NBDDevices)
	assert.Equal(t, int64(40<<30), cfg.NomadRuntime.MaxNodeDirtyTailBytes)
	assert.Equal(t, "node-1", cfg.NomadRuntime.NomadNodeID)
	assert.Equal(t, "node-uid-1", cfg.NomadRuntime.NodeUID)
}

func TestLoadCtldConfigStrictDoesNotFallBackToAnotherRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctld.yaml")
	require.NoError(t, os.WriteFile(path, []byte("nomad_runtime: [invalid\n"), 0o600))
	t.Setenv("CONFIG_PATH", path)
	config, err := LoadCtldConfigStrict()
	require.Error(t, err)
	require.Nil(t, config)
}
