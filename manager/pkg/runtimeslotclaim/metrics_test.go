package runtimeslotclaim

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPrometheusObserverUsesBoundedOutcomeLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer := NewPrometheusObserver(registry)
	observer.ObserveRuntimeSlotClaim(Observation{
		OperationID: "operation-high-cardinality", SandboxID: "sandbox-high-cardinality", SlotID: "slot-high-cardinality",
		Duration: 750 * time.Millisecond, Succeeded: true, WithinSLO: true,
		Phases: []PhaseObservation{
			{Phase: PhaseNodeClaim, Duration: 250 * time.Millisecond, Succeeded: true},
			{Phase: "dynamic-high-cardinality", Duration: 10 * time.Millisecond},
		},
	})
	observer.ObserveRuntimeSlotClaim(Observation{
		OperationID: "another-operation", Duration: 1250 * time.Millisecond,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	endToEndFound, phaseFound := false, false
	for _, family := range families {
		switch family.GetName() {
		case "manager_runtime_slot_claim_end_to_end_duration_seconds":
			endToEndFound = true
			if len(family.Metric) != 2 {
				t.Fatalf("metric series = %d, want 2", len(family.Metric))
			}
			seen := map[string]float64{}
			for _, metric := range family.Metric {
				labels := metricLabels(metric)
				if len(labels) != 2 {
					t.Fatalf("labels = %+v, want only result and slo", labels)
				}
				seen[labels["result"]+"/"+labels["slo"]] = metric.GetHistogram().GetSampleSum()
			}
			if seen["success/met"] != .75 || seen["error/missed"] != 1.25 {
				t.Fatalf("series = %+v", seen)
			}
		case "manager_runtime_slot_claim_phase_duration_seconds":
			phaseFound = true
			if len(family.Metric) != 2 {
				t.Fatalf("phase metric series = %d, want 2", len(family.Metric))
			}
			seen := map[string]float64{}
			for _, metric := range family.Metric {
				labels := metricLabels(metric)
				if len(labels) != 2 {
					t.Fatalf("phase labels = %+v, want only phase and result", labels)
				}
				seen[labels["phase"]+"/"+labels["result"]] = metric.GetHistogram().GetSampleSum()
			}
			if seen[PhaseNodeClaim+"/success"] != .25 || seen["unknown/error"] != .01 {
				t.Fatalf("phase series = %+v", seen)
			}
		}
	}
	if !endToEndFound || !phaseFound {
		t.Fatalf("claim metrics gathered: end-to-end=%t phase=%t", endToEndFound, phaseFound)
	}
}

func metricLabels(metric *dto.Metric) map[string]string {
	labels := map[string]string{}
	for _, label := range metric.GetLabel() {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}

func TestNewPrometheusObserverAllowsDisabledMetrics(t *testing.T) {
	if observer := NewPrometheusObserver(nil); observer != nil {
		t.Fatalf("observer = %+v, want nil", observer)
	}
}
