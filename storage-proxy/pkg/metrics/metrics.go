package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Volume metrics
	VolumesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "storage_proxy_volumes_total",
		Help: "Total number of volumes",
	})

	VolumesMounted = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "storage_proxy_volumes_mounted",
		Help: "Number of mounted volumes",
	})

	VolumesMountErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "storage_proxy_volumes_mount_errors_total",
		Help: "Total number of volume mount errors",
	})

	// Operation metrics
	OperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_proxy_operations_total",
		Help: "Total number of operations",
	}, []string{"operation", "volume_id"})

	OperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "storage_proxy_operations_duration_seconds",
		Help:    "Duration of operations in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "volume_id"})

	OperationErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_proxy_operations_errors_total",
		Help: "Total number of operation errors",
	}, []string{"operation", "volume_id", "error_type"})

	// Cache metrics
	CacheHitRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_proxy_cache_hit_rate",
		Help: "Cache hit rate",
	}, []string{"volume_id"})

	CacheUsedBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_proxy_cache_used_bytes",
		Help: "Cache used bytes",
	}, []string{"volume_id"})

	CacheTotalBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_proxy_cache_total_bytes",
		Help: "Cache total bytes",
	}, []string{"volume_id"})

	// S3 metrics
	S3OperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_proxy_s3_operations_total",
		Help: "Total number of S3 operations",
	}, []string{"operation", "volume_id"})

	S3BytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_proxy_s3_bytes_total",
		Help: "Total bytes transferred to/from S3",
	}, []string{"operation", "volume_id"})

	S3Duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "storage_proxy_s3_duration_seconds",
		Help:    "Duration of S3 operations in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "volume_id"})

	// gRPC metrics
	GRPCRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_proxy_grpc_requests_total",
		Help: "Total number of gRPC requests",
	}, []string{"method", "status"})

	GRPCRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "storage_proxy_grpc_request_duration_seconds",
		Help:    "Duration of gRPC requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	// Authentication metrics
	AuthenticationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_proxy_authentication_total",
		Help: "Total number of authentication attempts",
	}, []string{"status"})

	AuthenticationErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_proxy_authentication_errors_total",
		Help: "Total number of authentication errors",
	}, []string{"error_type"})
)
