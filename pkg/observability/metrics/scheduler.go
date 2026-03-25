package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SchedulerMetrics holds Prometheus metrics for the scheduler service.
type SchedulerMetrics struct {
	ReconcileTotal         *prometheus.CounterVec
	ReconcileDuration      prometheus.Histogram
	TemplateAllocations    *prometheus.GaugeVec
	ClusterCapacity        *prometheus.GaugeVec
	ClusterSummaryAge      *prometheus.GaugeVec
	TemplateSyncStatus     *prometheus.GaugeVec
	OrphansRemoved         *prometheus.CounterVec
	LastReconcileTimestamp prometheus.Gauge
	CapacityClamps         *prometheus.CounterVec
	RoutingDecisions       *prometheus.CounterVec
}

// NewScheduler registers and returns scheduler metrics.
// Returns nil when registry is nil.
func NewScheduler(registry prometheus.Registerer) *SchedulerMetrics {
	if registry == nil {
		return nil
	}

	factory := promauto.With(registry)

	return &SchedulerMetrics{
		ReconcileTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scheduler_reconcile_total",
				Help: "Total number of reconciliation cycles",
			},
			[]string{"status"}, // success, error
		),
		ReconcileDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "scheduler_reconcile_duration_seconds",
				Help:    "Duration of reconciliation cycles in seconds",
				Buckets: prometheus.DefBuckets,
			},
		),
		TemplateAllocations: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "scheduler_template_allocations",
				Help: "Number of template allocations per cluster",
			},
			[]string{"cluster_id", "template_id", "tenant", "type"}, // type: min_idle, max_idle
		),
		ClusterCapacity: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "scheduler_cluster_capacity",
				Help: "Cluster capacity metrics",
			},
			[]string{"cluster_id", "metric"}, // metric: nodes, total_nodes, sandbox_nodes, idle_pods, active_pods, pending_active_pods, total_pods, available_headroom
		),
		ClusterSummaryAge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "scheduler_cluster_summary_age_seconds",
				Help: "Age of the cached cluster summary in seconds",
			},
			[]string{"cluster_id"},
		),
		TemplateSyncStatus: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "scheduler_template_sync_status",
				Help: "Template sync status (1=synced, 0=error, 0.5=pending)",
			},
			[]string{"cluster_id", "template_id", "tenant"},
		),
		OrphansRemoved: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scheduler_orphans_removed_total",
				Help: "Total number of orphaned templates removed",
			},
			[]string{"cluster_id"},
		),
		LastReconcileTimestamp: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "scheduler_last_reconcile_timestamp_seconds",
				Help: "Timestamp of the last successful reconciliation",
			},
		),
		CapacityClamps: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scheduler_capacity_clamps_total",
				Help: "Total number of times allocations were clamped by cluster capacity",
			},
			[]string{"cluster_id", "template_id"},
		),
		RoutingDecisions: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scheduler_routing_decisions_total",
				Help: "Total number of scheduler shard routing decisions by reason",
			},
			[]string{"cluster_id", "reason"},
		),
	}
}
