package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestStorageProxyStageMetricsAreRegistered(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics := NewStorageProxy(registry)

	metrics.ObserveGRPCStage("Write", "resolve_inode_path", time.Millisecond)
	metrics.ObserveGRPCSetAttr("time", "16", "absent", "write")
	metrics.ObserveVolumeMutationBarrierStage("shared", "acquire_lock", time.Millisecond)
	metrics.ObserveVolumeSyncStage("record_remote_change", "transaction", time.Millisecond)

	if got := testutil.CollectAndCount(metrics.GRPCStageDuration); got == 0 {
		t.Fatal("gRPC stage duration histogram was not collected")
	}
	if got := testutil.CollectAndCount(metrics.GRPCSetAttrTotal); got == 0 {
		t.Fatal("gRPC setattr counter was not collected")
	}
	if got := testutil.CollectAndCount(metrics.VolumeMutationBarrierStageDuration); got == 0 {
		t.Fatal("volume mutation barrier stage duration histogram was not collected")
	}
	if got := testutil.CollectAndCount(metrics.VolumeSyncStageDuration); got == 0 {
		t.Fatal("volume sync stage duration histogram was not collected")
	}
}
