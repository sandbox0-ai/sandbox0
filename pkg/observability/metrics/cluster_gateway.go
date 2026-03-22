package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ClusterGatewayMetrics holds Prometheus metrics for the cluster-gateway service.
type ClusterGatewayMetrics struct {
	RequestsTotal           *prometheus.CounterVec
	RequestDuration         *prometheus.HistogramVec
	ActiveConnections       prometheus.Gauge
	ProxyRequestsTotal      *prometheus.CounterVec
	ProxyRequestDuration    *prometheus.HistogramVec
	AuthFailures            *prometheus.CounterVec
	RateLimitHits           *prometheus.CounterVec
	SandboxVolumeOperations *prometheus.CounterVec
}

// NewClusterGateway registers and returns cluster-gateway metrics.
// Returns nil when registry is nil.
func NewClusterGateway(registry prometheus.Registerer) *ClusterGatewayMetrics {
	if registry == nil {
		return nil
	}

	factory := promauto.With(registry)

	return &ClusterGatewayMetrics{
		RequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "path", "status"},
		),
		RequestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gateway_http_request_duration_seconds",
				Help:    "HTTP request latency in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "path"},
		),
		ActiveConnections: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "gateway_active_connections",
				Help: "Number of active connections",
			},
		),
		ProxyRequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_proxy_requests_total",
				Help: "Total number of proxied requests",
			},
			[]string{"target", "status"},
		),
		ProxyRequestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gateway_proxy_request_duration_seconds",
				Help:    "Proxied request latency in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"target"},
		),
		AuthFailures: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_auth_failures_total",
				Help: "Total number of authentication failures",
			},
			[]string{"reason"},
		),
		RateLimitHits: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_rate_limit_hits_total",
				Help: "Total number of rate limit hits",
			},
			[]string{"team_id"},
		),
		SandboxVolumeOperations: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_sandboxvolume_operations_total",
				Help: "Total number of sandboxvolume operations",
			},
			[]string{"operation", "status"},
		),
	}
}
