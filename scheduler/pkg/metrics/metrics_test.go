package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestTemplateReconcilerObserversUseExistingSeries(t *testing.T) {
	metrics := NewScheduler(prometheus.NewRegistry())
	metrics.ObserveCapacityClamp("cluster-a", "template-a")
	metrics.ObserveTemplateAllocation("cluster-a", "template-a", "team-a", "min_idle", 3)
	metrics.ObserveReconcileDuration(time.Second)
	metrics.ObserveReconcileResult("success")
	metrics.ObserveLastReconcileTimestamp()
	metrics.ObserveClusterCapacity("cluster-a", "available_headroom", 12)
	metrics.ObserveClusterSummaryAge("cluster-a", 0)
	metrics.ObserveTemplateSyncStatus("cluster-a", "template-a", "team-a", 1)
	metrics.ObserveOrphanRemoved("cluster-a")

	if got := testutil.ToFloat64(metrics.CapacityClamps.WithLabelValues("cluster-a", "template-a")); got != 1 {
		t.Fatalf("capacity clamps = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.TemplateAllocations.WithLabelValues("cluster-a", "template-a", "team-a", "min_idle")); got != 3 {
		t.Fatalf("template allocation = %v, want 3", got)
	}
	if got := testutil.ToFloat64(metrics.ReconcileTotal.WithLabelValues("success")); got != 1 {
		t.Fatalf("reconcile successes = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.LastReconcileTimestamp); got <= 0 {
		t.Fatalf("last reconcile timestamp = %v, want positive value", got)
	}
	if got := testutil.ToFloat64(metrics.ClusterCapacity.WithLabelValues("cluster-a", "available_headroom")); got != 12 {
		t.Fatalf("available headroom = %v, want 12", got)
	}
	if got := testutil.ToFloat64(metrics.ClusterSummaryAge.WithLabelValues("cluster-a")); got != 0 {
		t.Fatalf("cluster summary age = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.TemplateSyncStatus.WithLabelValues("cluster-a", "template-a", "team-a")); got != 1 {
		t.Fatalf("template sync status = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.OrphansRemoved.WithLabelValues("cluster-a")); got != 1 {
		t.Fatalf("orphans removed = %v, want 1", got)
	}
}
