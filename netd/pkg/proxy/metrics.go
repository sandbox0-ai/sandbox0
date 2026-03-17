package proxy

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type proxyMetricsRegistry struct {
	egressAuthResolveTotal    *prometheus.CounterVec
	egressAuthResolveDuration *prometheus.HistogramVec
	egressAuthDecisionTotal   *prometheus.CounterVec
	egressAuthCacheEntries    prometheus.Gauge
}

var proxyMetrics = newProxyMetricsRegistry()

func newProxyMetricsRegistry() *proxyMetricsRegistry {
	return &proxyMetricsRegistry{
		egressAuthResolveTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "egress_auth_resolve_total",
			Help:      "Total number of egress auth resolution attempts by protocol and result.",
		}, []string{"protocol", "result"}),
		egressAuthResolveDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "egress_auth_resolve_duration_seconds",
			Help:      "Duration of egress auth resolution attempts.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"protocol", "result"}),
		egressAuthDecisionTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "egress_auth_decisions_total",
			Help:      "Total number of egress auth enforcement decisions.",
		}, []string{"protocol", "result", "reason"}),
		egressAuthCacheEntries: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "egress_auth_cache_entries",
			Help:      "Current number of cached egress auth entries in netd.",
		}),
	}
}

func (m *proxyMetricsRegistry) RecordEgressAuthResolve(protocol, result string, duration time.Duration) {
	if m == nil {
		return
	}
	m.egressAuthResolveTotal.WithLabelValues(metricValue(protocol, "unknown"), metricValue(result, "unknown")).Inc()
	m.egressAuthResolveDuration.WithLabelValues(metricValue(protocol, "unknown"), metricValue(result, "unknown")).Observe(duration.Seconds())
}

func (m *proxyMetricsRegistry) RecordEgressAuthDecision(protocol, result, reason string) {
	if m == nil {
		return
	}
	m.egressAuthDecisionTotal.WithLabelValues(
		metricValue(protocol, "unknown"),
		metricValue(result, "unknown"),
		metricValue(reason, "none"),
	).Inc()
}

func (m *proxyMetricsRegistry) SetEgressAuthCacheEntries(entries int) {
	if m == nil {
		return
	}
	m.egressAuthCacheEntries.Set(float64(entries))
}

func metricValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
