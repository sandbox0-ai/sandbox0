package runtimeslotclaim

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PrometheusObserver exports complete regional-ingress-to-procd claim samples
// without operation, sandbox, or slot labels.
type PrometheusObserver struct {
	duration *prometheus.HistogramVec
	phase    *prometheus.HistogramVec
}

// NewPrometheusObserver registers the runtime slot claim SLO histogram. It
// returns nil when registerer is nil.
func NewPrometheusObserver(registerer prometheus.Registerer) *PrometheusObserver {
	if registerer == nil {
		return nil
	}
	return &PrometheusObserver{
		duration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_runtime_slot_claim_end_to_end_duration_seconds",
			Help:    "End-to-end duration from trusted regional ingress to authenticated procd command readiness",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, .75, 1, 1.5, 2.5, 5, 10, 30, 60},
		}, []string{"result", "slo"}),
		phase: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_runtime_slot_claim_phase_duration_seconds",
			Help:    "Bounded phase durations within the regional-ingress-to-procd claim sample",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, .75, 1, 2.5, 5, 10, 30},
		}, []string{"phase", "result"}),
	}
}

// ObserveRuntimeSlotClaim records one complete sample with bounded outcome
// labels.
func (o *PrometheusObserver) ObserveRuntimeSlotClaim(observation Observation) {
	if o == nil || o.duration == nil {
		return
	}
	result := "error"
	if observation.Succeeded {
		result = "success"
	}
	slo := "missed"
	if observation.WithinSLO {
		slo = "met"
	}
	duration := observation.Duration.Seconds()
	if duration < 0 {
		duration = 0
	}
	o.duration.WithLabelValues(result, slo).Observe(duration)
	for _, phase := range observation.Phases {
		phaseResult := "error"
		if phase.Succeeded {
			phaseResult = "success"
		}
		phaseDuration := phase.Duration.Seconds()
		if phaseDuration < 0 {
			phaseDuration = 0
		}
		o.phase.WithLabelValues(metricPhase(phase.Phase), phaseResult).Observe(phaseDuration)
	}
}

func metricPhase(phase string) string {
	switch phase {
	case PhaseRequestValidation, PhaseIngressToPlanner, PhaseRootFSMetadata, PhaseSlotAcquire,
		PhaseNetworkPrepare, PhaseWriterIssueBind, PhaseNodeClaim, PhaseProcdProbe,
		PhaseCommandReadyCommit:
		return phase
	default:
		return "unknown"
	}
}

var _ Observer = (*PrometheusObserver)(nil)
