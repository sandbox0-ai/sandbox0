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
	auditIngestEventsTotal    *prometheus.CounterVec
	auditIngestBatchesTotal   *prometheus.CounterVec
	proxyConnectionsActive    *prometheus.GaugeVec
	proxyAdmissionDropped     *prometheus.CounterVec
	networkOperationAdmission *prometheus.CounterVec
}

var proxyMetrics = newProxyMetricsRegistry()

func newProxyMetricsRegistry() *proxyMetricsRegistry {
	registry := &proxyMetricsRegistry{
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
			Help:      "Current number of cached egress auth entries in the ctld network runtime.",
		}),
		auditIngestEventsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "audit_ingest_events_total",
			Help:      "Total number of network audit events handled by the sandbox observability ingest sink.",
		}, []string{"result"}),
		auditIngestBatchesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "audit_ingest_batches_total",
			Help:      "Total number of network audit ingest batches by result.",
		}, []string{"result"}),
		proxyConnectionsActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "netd_proxy_connections_active",
			Help: "Current number of active downstream TCP proxy connections by listener.",
		}, []string{"listener"}),
		proxyAdmissionDropped: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "proxy_admission_dropped_total",
			Help:      "Traffic dropped by node-local proxy work bounds before tenant admission.",
		}, []string{"transport", "reason"}),
		networkOperationAdmission: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sandbox0",
			Subsystem: "netd",
			Name:      "team_quota_network_operation_admission_total",
			Help:      "Network operations admitted or rejected by region-shared Team Quota.",
		}, []string{"transport", "outcome"}),
	}
	for _, listener := range []string{"http", "https"} {
		registry.proxyConnectionsActive.WithLabelValues(listener)
	}
	return registry
}

func (m *proxyMetricsRegistry) RecordNetworkOperationAdmission(transport, outcome string) {
	if m == nil || m.networkOperationAdmission == nil {
		return
	}
	m.networkOperationAdmission.WithLabelValues(
		metricValue(transport, "unknown"),
		metricValue(outcome, "unknown"),
	).Inc()
}

func (m *proxyMetricsRegistry) RecordProxyAdmissionDrop(transport, reason string) {
	if m == nil || m.proxyAdmissionDropped == nil {
		return
	}
	m.proxyAdmissionDropped.WithLabelValues(
		metricValue(transport, "unknown"),
		metricValue(reason, "unknown"),
	).Inc()
}

func (m *proxyMetricsRegistry) IncProxyConnectionsActive(listener string) {
	if m == nil || m.proxyConnectionsActive == nil {
		return
	}
	m.proxyConnectionsActive.WithLabelValues(metricValue(listener, "unknown")).Inc()
}

func (m *proxyMetricsRegistry) DecProxyConnectionsActive(listener string) {
	if m == nil || m.proxyConnectionsActive == nil {
		return
	}
	m.proxyConnectionsActive.WithLabelValues(metricValue(listener, "unknown")).Dec()
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

func (m *proxyMetricsRegistry) RecordAuditIngestEvents(result string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.auditIngestEventsTotal.WithLabelValues(metricValue(result, "unknown")).Add(float64(count))
}

func (m *proxyMetricsRegistry) RecordAuditIngestBatch(result string) {
	if m == nil {
		return
	}
	m.auditIngestBatchesTotal.WithLabelValues(metricValue(result, "unknown")).Inc()
}

func metricValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
