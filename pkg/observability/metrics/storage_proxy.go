package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// StorageProxyMetrics holds Prometheus metrics for the storage-proxy service.
type StorageProxyMetrics struct {
	VolumesTotal       prometheus.Gauge
	VolumesMounted     prometheus.Gauge
	VolumesMountErrors prometheus.Counter

	OperationsTotal   *prometheus.CounterVec
	OperationDuration *prometheus.HistogramVec
	OperationErrors   *prometheus.CounterVec

	CacheHitRate    *prometheus.GaugeVec
	CacheUsedBytes  *prometheus.GaugeVec
	CacheTotalBytes *prometheus.GaugeVec

	S3OperationsTotal *prometheus.CounterVec
	S3BytesTotal      *prometheus.CounterVec
	S3Duration        *prometheus.HistogramVec

	GRPCRequestsTotal   *prometheus.CounterVec
	GRPCRequestDuration *prometheus.HistogramVec

	AuthenticationTotal  *prometheus.CounterVec
	AuthenticationErrors *prometheus.CounterVec

	SnapshotsTotal            prometheus.Gauge
	SnapshotOperationsTotal   *prometheus.CounterVec
	SnapshotOperationDuration *prometheus.HistogramVec
	SnapshotSizeBytes         *prometheus.HistogramVec
	SnapshotErrors            *prometheus.CounterVec

	CoordinatorMountsActive             prometheus.Gauge
	CoordinatorMountRegistrations       *prometheus.CounterVec
	CoordinatorMountUnregistrations     *prometheus.CounterVec
	CoordinatorHeartbeatsTotal          prometheus.Counter
	CoordinatorHeartbeatErrors          prometheus.Counter
	CoordinatorStaleMountsCleaned       prometheus.Counter
	CoordinatorFlushCoordinationsTotal  *prometheus.CounterVec
	CoordinatorFlushCoordinationDuration prometheus.Histogram
	CoordinatorFlushRequestsSent        prometheus.Counter
	CoordinatorFlushRequestsReceived    prometheus.Counter
	CoordinatorFlushResponsesTotal      *prometheus.CounterVec
	CoordinatorFlushLatency             prometheus.Histogram
	CoordinatorActiveCoordinations      prometheus.Gauge
	CoordinatorNotificationsReceived    *prometheus.CounterVec
	CoordinatorNotificationErrors       prometheus.Counter

	HTTPRequestsTotal    *prometheus.CounterVec
	HTTPRequestDuration  *prometheus.HistogramVec
	HTTPRequestSize      *prometheus.HistogramVec
	HTTPResponseSize     *prometheus.HistogramVec
}

// NewStorageProxy registers and returns storage-proxy metrics.
// Returns nil when registry is nil.
func NewStorageProxy(registry prometheus.Registerer) *StorageProxyMetrics {
	if registry == nil {
		return nil
	}

	factory := promauto.With(registry)

	return &StorageProxyMetrics{
		VolumesTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "storage_proxy_volumes_total",
			Help: "Total number of volumes",
		}),
		VolumesMounted: factory.NewGauge(prometheus.GaugeOpts{
			Name: "storage_proxy_volumes_mounted",
			Help: "Number of mounted volumes",
		}),
		VolumesMountErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "storage_proxy_volumes_mount_errors_total",
			Help: "Total number of volume mount errors",
		}),
		OperationsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_operations_total",
			Help: "Total number of operations",
		}, []string{"operation", "volume_id"}),
		OperationDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storage_proxy_operations_duration_seconds",
			Help:    "Duration of operations in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation", "volume_id"}),
		OperationErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_operations_errors_total",
			Help: "Total number of operation errors",
		}, []string{"operation", "volume_id", "error_type"}),
		CacheHitRate: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "storage_proxy_cache_hit_rate",
			Help: "Cache hit rate",
		}, []string{"volume_id"}),
		CacheUsedBytes: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "storage_proxy_cache_used_bytes",
			Help: "Cache used bytes",
		}, []string{"volume_id"}),
		CacheTotalBytes: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "storage_proxy_cache_total_bytes",
			Help: "Cache total bytes",
		}, []string{"volume_id"}),
		S3OperationsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_s3_operations_total",
			Help: "Total number of S3 operations",
		}, []string{"operation", "volume_id"}),
		S3BytesTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_s3_bytes_total",
			Help: "Total bytes transferred to/from S3",
		}, []string{"operation", "volume_id"}),
		S3Duration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storage_proxy_s3_duration_seconds",
			Help:    "Duration of S3 operations in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation", "volume_id"}),
		GRPCRequestsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_grpc_requests_total",
			Help: "Total number of gRPC requests",
		}, []string{"method", "status"}),
		GRPCRequestDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storage_proxy_grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
		AuthenticationTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_authentication_total",
			Help: "Total number of authentication attempts",
		}, []string{"status"}),
		AuthenticationErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_authentication_errors_total",
			Help: "Total number of authentication errors",
		}, []string{"error_type"}),
		SnapshotsTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "storage_proxy_snapshots_total",
			Help: "Total number of snapshots",
		}),
		SnapshotOperationsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_snapshot_operations_total",
			Help: "Total number of snapshot operations",
		}, []string{"operation", "status"}), // operation: create/delete/restore; status: success/failure
		SnapshotOperationDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storage_proxy_snapshot_operation_duration_seconds",
			Help:    "Duration of snapshot operations in seconds",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300}, // Snapshots can take longer
		}, []string{"operation"}), // operation: create/delete/restore
		SnapshotSizeBytes: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "storage_proxy_snapshot_size_bytes",
			Help: "Size of snapshots in bytes",
			Buckets: []float64{
				1024 * 1024,              // 1MB
				10 * 1024 * 1024,         // 10MB
				100 * 1024 * 1024,        // 100MB
				1024 * 1024 * 1024,       // 1GB
				10 * 1024 * 1024 * 1024,  // 10GB
				100 * 1024 * 1024 * 1024, // 100GB
			},
		}, []string{"volume_id"}),
		SnapshotErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_snapshot_errors_total",
			Help: "Total number of snapshot errors",
		}, []string{"operation", "error_type"}),
		CoordinatorMountsActive: factory.NewGauge(prometheus.GaugeOpts{
			Name: "storage_proxy_coordinator_mounts_active",
			Help: "Number of active volume mounts tracked by coordinator",
		}),
		CoordinatorMountRegistrations: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_mount_registrations_total",
			Help: "Total number of mount registrations",
		}, []string{"status"}), // status: success/failure
		CoordinatorMountUnregistrations: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_mount_unregistrations_total",
			Help: "Total number of mount unregistrations",
		}, []string{"status"}), // status: success/failure
		CoordinatorHeartbeatsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_heartbeats_total",
			Help: "Total number of heartbeat updates sent",
		}),
		CoordinatorHeartbeatErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_heartbeat_errors_total",
			Help: "Total number of heartbeat update errors",
		}),
		CoordinatorStaleMountsCleaned: factory.NewCounter(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_stale_mounts_cleaned_total",
			Help: "Total number of stale mounts cleaned up",
		}),
		CoordinatorFlushCoordinationsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_flush_coordinations_total",
			Help: "Total number of flush coordination requests",
		}, []string{"status"}), // status: success/failure/timeout
		CoordinatorFlushCoordinationDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "storage_proxy_coordinator_flush_coordination_duration_seconds",
			Help:    "Duration of flush coordination in seconds",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30}, // Most should complete within seconds
		}),
		CoordinatorFlushRequestsSent: factory.NewCounter(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_flush_requests_sent_total",
			Help: "Total number of flush requests sent to other instances",
		}),
		CoordinatorFlushRequestsReceived: factory.NewCounter(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_flush_requests_received_total",
			Help: "Total number of flush requests received from other instances",
		}),
		CoordinatorFlushResponsesTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_flush_responses_total",
			Help: "Total number of flush responses",
		}, []string{"success"}), // success: true/false
		CoordinatorFlushLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "storage_proxy_coordinator_flush_latency_seconds",
			Help:    "Latency of local flush operations in seconds",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10},
		}),
		CoordinatorActiveCoordinations: factory.NewGauge(prometheus.GaugeOpts{
			Name: "storage_proxy_coordinator_active_coordinations",
			Help: "Number of currently active flush coordinations",
		}),
		CoordinatorNotificationsReceived: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_notifications_received_total",
			Help: "Total number of PostgreSQL notifications received",
		}, []string{"channel"}), // channel: snapshot_flush_request/snapshot_flush_response
		CoordinatorNotificationErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "storage_proxy_coordinator_notification_errors_total",
			Help: "Total number of notification handling errors",
		}),
		HTTPRequestsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "storage_proxy_http_requests_total",
			Help: "Total number of HTTP requests",
		}, []string{"method", "path", "status"}),
		HTTPRequestDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storage_proxy_http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		HTTPRequestSize: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storage_proxy_http_request_size_bytes",
			Help:    "Size of HTTP requests in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 8), // 100B to ~10MB
		}, []string{"method", "path"}),
		HTTPResponseSize: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "storage_proxy_http_response_size_bytes",
			Help:    "Size of HTTP responses in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 8),
		}, []string{"method", "path"}),
	}
}
