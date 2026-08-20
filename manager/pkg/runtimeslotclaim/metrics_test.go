package runtimeslotclaim

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPrometheusObserverUsesBoundedOutcomeLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer := NewPrometheusObserver(registry)
	observer.ObserveRuntimeSlotClaim(Observation{
		OperationID: "operation-high-cardinality", SandboxID: "sandbox-high-cardinality", SlotID: "slot-high-cardinality",
		Duration: 750 * time.Millisecond, Succeeded: true, WithinSLO: true,
	})
	observer.ObserveRuntimeSlotClaim(Observation{
		OperationID: "another-operation", Duration: 1250 * time.Millisecond,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "manager_runtime_slot_claim_end_to_end_duration_seconds" {
			continue
		}
		if len(family.Metric) != 2 {
			t.Fatalf("metric series = %d, want 2", len(family.Metric))
		}
		seen := map[string]float64{}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if len(labels) != 2 {
				t.Fatalf("labels = %+v, want only result and slo", labels)
			}
			seen[labels["result"]+"/"+labels["slo"]] = metric.GetHistogram().GetSampleSum()
		}
		if seen["success/met"] != .75 || seen["error/missed"] != 1.25 {
			t.Fatalf("series = %+v", seen)
		}
		return
	}
	t.Fatal("runtime slot claim metric was not gathered")
}

func TestNewPrometheusObserverAllowsDisabledMetrics(t *testing.T) {
	if observer := NewPrometheusObserver(nil); observer != nil {
		t.Fatalf("observer = %+v, want nil", observer)
	}
}
