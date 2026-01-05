package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Template metrics
	TemplatesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "manager_templates_total",
		Help: "Total number of sandbox templates",
	})

	// Pool metrics
	IdlePodsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "manager_idle_pods_total",
		Help: "Total number of idle pods per template",
	}, []string{"template"})

	ActivePodsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "manager_active_pods_total",
		Help: "Total number of active pods per template",
	}, []string{"template"})

	// Claim metrics
	SandboxClaimsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "manager_sandbox_claims_total",
		Help: "Total number of sandbox claims",
	}, []string{"template", "status"})

	SandboxClaimDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "manager_sandbox_claim_duration_seconds",
		Help:    "Duration of sandbox claim operations",
		Buckets: prometheus.DefBuckets,
	}, []string{"template", "type"}) // type: "hot" (from pool) or "cold" (new pod)

	// Cleanup metrics
	PodsCleanedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "manager_pods_cleaned_total",
		Help: "Total number of pods cleaned up",
	}, []string{"template", "reason"}) // reason: "excess" or "expired"

	// Reconcile metrics
	ReconcileTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "manager_reconcile_total",
		Help: "Total number of reconciliation operations",
	}, []string{"template", "result"}) // result: "success" or "error"

	ReconcileDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "manager_reconcile_duration_seconds",
		Help:    "Duration of reconciliation operations",
		Buckets: prometheus.DefBuckets,
	}, []string{"template"})
)
