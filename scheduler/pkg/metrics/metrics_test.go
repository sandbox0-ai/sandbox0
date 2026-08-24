package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestResourceCapacitySeries(t *testing.T) {
	metrics := NewScheduler(prometheus.NewRegistry())
	metrics.ObserveClusterCapacity("cluster-a", "claim_capacity", 12)
	metrics.RoutingDecisions.WithLabelValues("cluster-a", "resource_capacity").Inc()

	if got := testutil.ToFloat64(metrics.ClusterCapacity.WithLabelValues("cluster-a", "claim_capacity")); got != 12 {
		t.Fatalf("claim capacity = %v, want 12", got)
	}
	if got := testutil.ToFloat64(metrics.RoutingDecisions.WithLabelValues("cluster-a", "resource_capacity")); got != 1 {
		t.Fatalf("routing decisions = %v, want 1", got)
	}
}
