package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ManagerMetrics holds Prometheus metrics for durable manager workers.
type ManagerMetrics struct {
	SandboxDeletionWebhookAttemptsTotal    *prometheus.CounterVec
	SandboxDeletionWebhookDeliveryDuration *prometheus.HistogramVec
	SandboxDeletionWebhookQueueDepth       *prometheus.GaugeVec
	SandboxDeletionWebhookOldestPendingAge prometheus.Gauge
	SandboxDeletionWebhookExpiredTotal     prometheus.Counter
	MeteringEventsTotal                    *prometheus.CounterVec
	MeteringWindowsTotal                   *prometheus.CounterVec
	MeteringErrorsTotal                    *prometheus.CounterVec
	MeteringOutboxBatchesTotal             *prometheus.CounterVec
	MeteringOutboxOperationsTotal          *prometheus.CounterVec
	MeteringOutboxPendingOperations        prometheus.Gauge
	MeteringOutboxOldestPendingAge         prometheus.Gauge
	RootFSMaintenanceRunsTotal             *prometheus.CounterVec
	RootFSMaintenanceDuration              *prometheus.HistogramVec
	RootFSObjectDeletesTotal               *prometheus.CounterVec
	RootFSObjectDeletionQueueDepth         *prometheus.GaugeVec
	RootFSStorageBytes                     prometheus.Gauge
	RootFSStorageObjects                   prometheus.Gauge
}

// NewManager registers and returns manager metrics. It returns nil when the
// registry is nil.
func NewManager(registry prometheus.Registerer) *ManagerMetrics {
	if registry == nil {
		return nil
	}
	factory := promauto.With(registry)
	return &ManagerMetrics{
		SandboxDeletionWebhookAttemptsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_sandbox_deletion_webhook_attempts_total",
			Help: "Total number of sandbox.deleted webhook delivery attempts by result and HTTP status class",
		}, []string{"result", "status_class"}),
		SandboxDeletionWebhookDeliveryDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_sandbox_deletion_webhook_delivery_duration_seconds",
			Help:    "Duration of sandbox.deleted webhook delivery attempts by result",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"result"}),
		SandboxDeletionWebhookQueueDepth: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "manager_sandbox_deletion_webhook_queue_depth",
			Help: "Sandbox deletion webhook outbox records by state",
		}, []string{"state"}),
		SandboxDeletionWebhookOldestPendingAge: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_sandbox_deletion_webhook_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest pending sandbox deletion webhook",
		}),
		SandboxDeletionWebhookExpiredTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "manager_sandbox_deletion_webhook_expired_total",
			Help: "Total number of sandbox deletion webhooks whose bounded delivery window expired",
		}),
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
		MeteringOutboxBatchesTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_outbox_batches_total",
			Help: "Total number of PostgreSQL metering outbox batches by delivery result",
		}, []string{"result"}),
		MeteringOutboxOperationsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_metering_outbox_operations_total",
			Help: "Total number of metering outbox operation delivery attempts by type and result",
		}, []string{"operation_type", "result"}),
		MeteringOutboxPendingOperations: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_metering_outbox_pending_operations",
			Help: "Number of metering outbox operations awaiting ClickHouse delivery",
		}),
		MeteringOutboxOldestPendingAge: factory.NewGauge(prometheus.GaugeOpts{
			Name: "manager_metering_outbox_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest metering outbox operation awaiting ClickHouse delivery",
		}),
		RootFSMaintenanceRunsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "manager_rootfs_maintenance_runs_total",
			Help: "Total number of rootfs maintenance cycles",
		}, []string{"status"}),
		RootFSMaintenanceDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "manager_rootfs_maintenance_duration_seconds",
			Help:    "Duration of rootfs maintenance cycles",
			Buckets: prometheus.DefBuckets,
		}, []string{"status"}),
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

// ObserveMeteringOutboxStats records the current delivery backlog state.
func (m *ManagerMetrics) ObserveMeteringOutboxStats(pending int64, oldestPendingAgeSeconds float64) {
	if m == nil {
		return
	}
	if m.MeteringOutboxPendingOperations != nil {
		m.MeteringOutboxPendingOperations.Set(float64(pending))
	}
	if m.MeteringOutboxOldestPendingAge != nil {
		m.MeteringOutboxOldestPendingAge.Set(oldestPendingAgeSeconds)
	}
}

// ObserveMeteringOutboxBatch records a metering outbox batch delivery result.
func (m *ManagerMetrics) ObserveMeteringOutboxBatch(result string) {
	if m == nil || m.MeteringOutboxBatchesTotal == nil {
		return
	}
	m.MeteringOutboxBatchesTotal.WithLabelValues(result).Inc()
}

// ObserveMeteringOutboxOperation records an individual operation delivery result.
func (m *ManagerMetrics) ObserveMeteringOutboxOperation(operationType, result string) {
	if m == nil || m.MeteringOutboxOperationsTotal == nil {
		return
	}
	m.MeteringOutboxOperationsTotal.WithLabelValues(operationType, result).Inc()
}
