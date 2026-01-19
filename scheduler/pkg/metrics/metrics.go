package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ReconcileTotal tracks total reconciliation attempts
	ReconcileTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_reconcile_total",
			Help: "Total number of reconciliation cycles",
		},
		[]string{"status"}, // success, error
	)

	// ReconcileDuration tracks reconciliation duration
	ReconcileDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "scheduler_reconcile_duration_seconds",
			Help:    "Duration of reconciliation cycles in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	// TemplateAllocations tracks template allocations per cluster
	TemplateAllocations = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_template_allocations",
			Help: "Number of template allocations per cluster",
		},
		[]string{"cluster_id", "template_id", "tenant", "type"}, // type: min_idle, max_idle
	)

	// ClusterCapacity tracks cluster capacity metrics
	ClusterCapacity = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_cluster_capacity",
			Help: "Cluster capacity metrics",
		},
		[]string{"cluster_id", "metric"}, // metric: nodes, idle_pods, active_pods, total_pods
	)

	// TemplateSyncStatus tracks template sync status per cluster
	TemplateSyncStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_template_sync_status",
			Help: "Template sync status (1=synced, 0=error, 0.5=pending)",
		},
		[]string{"cluster_id", "template_id", "tenant"},
	)

	// OrphansRemoved tracks orphaned templates removed
	OrphansRemoved = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_orphans_removed_total",
			Help: "Total number of orphaned templates removed",
		},
		[]string{"cluster_id"},
	)

	// LastReconcileTimestamp tracks the last successful reconcile time
	LastReconcileTimestamp = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "scheduler_last_reconcile_timestamp_seconds",
			Help: "Timestamp of the last successful reconciliation",
		},
	)

	// CapacityClamps tracks when allocations were clamped by capacity
	CapacityClamps = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_capacity_clamps_total",
			Help: "Total number of times allocations were clamped by cluster capacity",
		},
		[]string{"cluster_id", "template_id"},
	)
)
