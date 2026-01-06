package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestsTotal is the total number of HTTP requests
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// RequestDuration is the HTTP request latency
	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// ActiveConnections is the number of active connections
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_active_connections",
			Help: "Number of active connections",
		},
	)

	// ProxyRequestsTotal is the total number of proxied requests
	ProxyRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_proxy_requests_total",
			Help: "Total number of proxied requests",
		},
		[]string{"target", "status"},
	)

	// ProxyRequestDuration is the proxied request latency
	ProxyRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_proxy_request_duration_seconds",
			Help:    "Proxied request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"target"},
	)

	// AuthFailures is the total number of authentication failures
	AuthFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_auth_failures_total",
			Help: "Total number of authentication failures",
		},
		[]string{"reason"},
	)

	// RateLimitHits is the total number of rate limit hits
	RateLimitHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_rate_limit_hits_total",
			Help: "Total number of rate limit hits",
		},
		[]string{"team_id"},
	)

	// SandboxVolumeOperations is the total number of sandboxvolume operations
	SandboxVolumeOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_sandboxvolume_operations_total",
			Help: "Total number of sandboxvolume operations",
		},
		[]string{"operation", "status"},
	)
)
