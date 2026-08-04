package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMeteringOutboxObserversUseExistingSeries(t *testing.T) {
	metrics := NewManager(prometheus.NewRegistry())
	metrics.ObserveMeteringOutboxStats(7, 12.5)
	metrics.ObserveMeteringOutboxBatch("success")
	metrics.ObserveMeteringOutboxOperation("event", "success")

	if got := testutil.ToFloat64(metrics.MeteringOutboxPendingOperations); got != 7 {
		t.Fatalf("pending operations = %v, want 7", got)
	}
	if got := testutil.ToFloat64(metrics.MeteringOutboxOldestPendingAge); got != 12.5 {
		t.Fatalf("oldest pending age = %v, want 12.5", got)
	}
	if got := testutil.ToFloat64(metrics.MeteringOutboxBatchesTotal.WithLabelValues("success")); got != 1 {
		t.Fatalf("batch successes = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.MeteringOutboxOperationsTotal.WithLabelValues("event", "success")); got != 1 {
		t.Fatalf("event successes = %v, want 1", got)
	}
}
