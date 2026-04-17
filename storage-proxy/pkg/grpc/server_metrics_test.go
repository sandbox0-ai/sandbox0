package grpc

import (
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestObserveJuiceFSOperationRecordsWritebackAndStatus(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewStorageProxy(registry)
	server := NewFileSystemServer(nil, nil, nil, nil, nil, nil, nil)
	server.SetMetrics(metrics)

	volCtx := &volume.VolumeContext{
		Config: &volume.VolumeConfig{Writeback: true},
	}
	server.observeJuiceFSOperation(volCtx, "Flush", 0, time.Now().Add(-time.Millisecond))
	server.observeJuiceFSOperation(volCtx, "Flush", syscall.EIO, time.Now().Add(-time.Millisecond))

	if got := testutil.ToFloat64(metrics.JuiceFSOperationsTotal.WithLabelValues("Flush", "true", "success")); got != 1 {
		t.Fatalf("success operations = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.JuiceFSOperationsTotal.WithLabelValues("Flush", "true", "error")); got != 1 {
		t.Fatalf("error operations = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.JuiceFSOperationDuration); got == 0 {
		t.Fatal("duration histogram was not collected")
	}
}
