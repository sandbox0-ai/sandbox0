package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ManagerMetrics holds Prometheus metrics for the manager service.
type ManagerMetrics struct {
	TemplatesTotal                 prometheus.Gauge
	IdlePodsTotal                  *prometheus.GaugeVec
	ActivePodsTotal                *prometheus.GaugeVec
	SandboxClaimsTotal             *prometheus.CounterVec
	SandboxClaimDuration           *prometheus.HistogramVec
	SandboxClaimPhaseDuration      *prometheus.HistogramVec
	PodsCleanedTotal               *prometheus.CounterVec
	ReconcileTotal                 *prometheus.CounterVec
	ReconcileDuration              *prometheus.HistogramVec
	MeteringEventsTotal            *prometheus.CounterVec
	MeteringWindowsTotal           *prometheus.CounterVec
	MeteringErrorsTotal            *prometheus.CounterVec
	RootFSMaintenanceRunsTotal     *prometheus.CounterVec
	RootFSMaintenanceDuration      *prometheus.HistogramVec
	RootFSGCLayersTotal            prometheus.Counter
	RootFSObjectDeletesTotal       *prometheus.CounterVec
	RootFSObjectDeletionQueueDepth *prometheus.GaugeVec
	RootFSStorageBytes             prometheus.Gauge
	RootFSStorageObjects           prometheus.Gauge
}

// NewManager registers and returns manager metrics.
// Returns nil when registry is nil.
func NewManager(registry prometheus.Registerer) *ManagerMetrics {
	if registry == nil {
		return nil
	}

	factory := promauto.With(registry)

	return &ManagerMetrics{
		TemplatesTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_templates_total",
			Help: "Total number of sandbox templates",
		}),
		IdlePodsTotal: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_idle_pods_total",
			Help: "Total number of idle pods per template",
		}, []string{"template"}),
		ActivePodsTotal: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_active_pods_total",
			Help: "Total number of active pods per template",
		}, []string{"template"}),
		SandboxClaimsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_sandbox_claims_total",
			Help: "Total number of sandbox claims",
		}, []string{"template", "status"}),
		SandboxClaimDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_sandbox_claim_duration_seconds",
			Help:    "Duration of sandbox claim operations",
			Buckets: prometheus.DefBuckets,
		}, []string{"template", "type"}), // type: "hot" (from pool) or "cold" (new pod)
		SandboxClaimPhaseDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_sandbox_claim_phase_duration_seconds",
			Help:    "Duration of sandbox claim phases",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"template", "type", "phase", "status"}), // type: "hot", "cold", or "unknown"
		PodsCleanedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_pods_cleaned_total",
			Help: "Total number of pods cleaned up",
		}, []string{"template", "reason"}), // reason: "excess" or "expired"
		ReconcileTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_reconcile_total",
			Help: "Total number of reconciliation operations",
		}, []string{"template", "result"}), // result: "success" or "error"
		ReconcileDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_reconcile_duration_seconds",
			Help:    "Duration of reconciliation operations",
			Buckets: prometheus.DefBuckets,
		}, []string{"template"}),
		MeteringEventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_events_total",
			Help: "Total number of manager metering lifecycle events attempted",
		}, []string{"event_type", "result"}),
		MeteringWindowsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_windows_total",
			Help: "Total number of manager metering usage windows attempted",
		}, []string{"window_type", "result"}),
		MeteringErrorsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_errors_total",
			Help: "Total number of manager metering projector errors",
		}, []string{"operation"}),
		RootFSMaintenanceRunsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_rootfs_maintenance_runs_total",
			Help: "Total number of rootfs maintenance cycles",
		}, []string{"status"}),
		RootFSMaintenanceDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_rootfs_maintenance_duration_seconds",
			Help:    "Duration of rootfs maintenance cycles",
			Buckets: prometheus.DefBuckets,
		}, []string{"status"}),
		RootFSGCLayersTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "manager_rootfs_gc_layers_total",
			Help: "Total number of rootfs layer metadata records garbage-collected",
		}),
		RootFSObjectDeletesTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_rootfs_object_deletes_total",
			Help: "Total number of rootfs object deletion attempts by status",
		}, []string{"status"}),
		RootFSObjectDeletionQueueDepth: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_rootfs_object_deletion_queue_depth",
			Help: "Rootfs object deletion queue depth by state",
		}, []string{"state"}),
		RootFSStorageBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_rootfs_storage_bytes",
			Help: "Current reachable persistent rootfs COW object bytes",
		}),
		RootFSStorageObjects: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_rootfs_storage_objects",
			Help: "Current reachable persistent rootfs COW object count",
		}),
	}
}
