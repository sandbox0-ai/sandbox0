package metrics

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/internal/promutil"
)

// FunctionGatewayMetrics holds product-level Function serving metrics.
type FunctionGatewayMetrics struct {
	FunctionRequestsTotal         *prometheus.CounterVec
	FunctionIngressFailuresTotal  *prometheus.CounterVec
	FunctionRequestDuration       *prometheus.HistogramVec
	FunctionResponseSize          *prometheus.HistogramVec
	RuntimeAcquireTotal           *prometheus.CounterVec
	RuntimeStartupDuration        *prometheus.HistogramVec
	RuntimeStartupFailuresTotal   *prometheus.CounterVec
	RuntimeLifecycleEventsTotal   *prometheus.CounterVec
	RuntimeLifecycleEventDuration *prometheus.HistogramVec
	RuntimeScaleDownTotal         *prometheus.CounterVec
	RuntimeScaleDownDuration      *prometheus.HistogramVec
}

func NewFunctionGateway(registry prometheus.Registerer) *FunctionGatewayMetrics {
	if registry == nil {
		return nil
	}
	prefix := promutil.MetricPrefix("function-gateway")
	return &FunctionGatewayMetrics{
		FunctionRequestsTotal: promutil.RegisterCounterVec(registry, prometheus.CounterOpts{
			Name: prefix + "_function_requests_total",
			Help: "Total number of function requests served by function-gateway",
		}, []string{"team_id", "function_id", "revision_id", "route_id", "method", "status"}),
		FunctionIngressFailuresTotal: promutil.RegisterCounterVec(registry, prometheus.CounterOpts{
			Name: prefix + "_function_ingress_failures_total",
			Help: "Total number of function ingress failures rejected before runtime proxying",
		}, []string{"team_id", "function_id", "revision_id", "route_id", "reason", "status"}),
		FunctionRequestDuration: promutil.RegisterHistogramVec(registry, prometheus.HistogramOpts{
			Name:    prefix + "_function_request_duration_seconds",
			Help:    "Function request duration in seconds",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"team_id", "function_id", "revision_id", "route_id", "method"}),
		FunctionResponseSize: promutil.RegisterHistogramVec(registry, prometheus.HistogramOpts{
			Name:    prefix + "_function_response_size_bytes",
			Help:    "Function response size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 8),
		}, []string{"team_id", "function_id", "revision_id", "route_id"}),
		RuntimeAcquireTotal: promutil.RegisterCounterVec(registry, prometheus.CounterOpts{
			Name: prefix + "_runtime_acquire_total",
			Help: "Total number of function runtime acquire attempts",
		}, []string{"team_id", "function_id", "revision_id", "path", "result", "reason"}),
		RuntimeStartupDuration: promutil.RegisterHistogramVec(registry, prometheus.HistogramOpts{
			Name:    prefix + "_runtime_startup_duration_seconds",
			Help:    "Function runtime startup duration in seconds",
			Buckets: []float64{.01, .025, .05, .1, .2, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"team_id", "function_id", "revision_id", "result", "readiness"}),
		RuntimeStartupFailuresTotal: promutil.RegisterCounterVec(registry, prometheus.CounterOpts{
			Name: prefix + "_runtime_startup_failures_total",
			Help: "Total number of function runtime startup failures",
		}, []string{"team_id", "function_id", "revision_id", "reason"}),
		RuntimeLifecycleEventsTotal: promutil.RegisterCounterVec(registry, prometheus.CounterOpts{
			Name: prefix + "_runtime_lifecycle_events_total",
			Help: "Total number of function runtime lifecycle events",
		}, []string{"team_id", "function_id", "revision_id", "phase", "reason"}),
		RuntimeLifecycleEventDuration: promutil.RegisterHistogramVec(registry, prometheus.HistogramOpts{
			Name:    prefix + "_runtime_lifecycle_event_duration_seconds",
			Help:    "Duration attached to function runtime lifecycle events",
			Buckets: []float64{.01, .025, .05, .1, .2, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"team_id", "function_id", "revision_id", "phase"}),
		RuntimeScaleDownTotal: promutil.RegisterCounterVec(registry, prometheus.CounterOpts{
			Name: prefix + "_runtime_scale_down_total",
			Help: "Total number of function runtime scale-down attempts",
		}, []string{"team_id", "function_id", "revision_id", "result", "reason"}),
		RuntimeScaleDownDuration: promutil.RegisterHistogramVec(registry, prometheus.HistogramOpts{
			Name:    prefix + "_runtime_scale_down_duration_seconds",
			Help:    "Function runtime scale-down duration in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"team_id", "function_id", "revision_id", "result"}),
	}
}

func (m *FunctionGatewayMetrics) ObserveFunctionRequest(teamID, functionID, revisionID, routeID, method string, status int, duration time.Duration, responseSize int) {
	if m == nil {
		return
	}
	statusLabel := strconv.Itoa(status)
	m.FunctionRequestsTotal.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(routeID), label(method), statusLabel).Inc()
	m.FunctionRequestDuration.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(routeID), label(method)).Observe(duration.Seconds())
	if responseSize > 0 {
		m.FunctionResponseSize.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(routeID)).Observe(float64(responseSize))
	}
}

func (m *FunctionGatewayMetrics) ObserveFunctionIngressFailure(teamID, functionID, revisionID, routeID, reason string, status int) {
	if m == nil {
		return
	}
	m.FunctionIngressFailuresTotal.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(routeID), reasonLabel(reason), strconv.Itoa(status)).Inc()
}

func (m *FunctionGatewayMetrics) ObserveRuntimeAcquire(teamID, functionID, revisionID, path, result, reason string) {
	if m == nil {
		return
	}
	m.RuntimeAcquireTotal.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(path), label(result), reasonLabel(reason)).Inc()
}

func (m *FunctionGatewayMetrics) ObserveRuntimeStartup(teamID, functionID, revisionID, result, readiness string, duration time.Duration) {
	if m == nil {
		return
	}
	m.RuntimeStartupDuration.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(result), label(readiness)).Observe(duration.Seconds())
}

func (m *FunctionGatewayMetrics) ObserveRuntimeStartupFailure(teamID, functionID, revisionID, reason string) {
	if m == nil {
		return
	}
	m.RuntimeStartupFailuresTotal.WithLabelValues(label(teamID), label(functionID), label(revisionID), reasonLabel(reason)).Inc()
}

func (m *FunctionGatewayMetrics) ObserveRuntimeLifecycleEvent(teamID, functionID, revisionID, phase, reason string, duration time.Duration) {
	if m == nil {
		return
	}
	m.RuntimeLifecycleEventsTotal.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(phase), reasonLabel(reason)).Inc()
	if duration > 0 {
		m.RuntimeLifecycleEventDuration.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(phase)).Observe(duration.Seconds())
	}
}

func (m *FunctionGatewayMetrics) ObserveRuntimeScaleDown(teamID, functionID, revisionID, result, reason string, duration time.Duration) {
	if m == nil {
		return
	}
	m.RuntimeScaleDownTotal.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(result), reasonLabel(reason)).Inc()
	if duration > 0 {
		m.RuntimeScaleDownDuration.WithLabelValues(label(teamID), label(functionID), label(revisionID), label(result)).Observe(duration.Seconds())
	}
}

func label(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func reasonLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "none"
	}
	if len(value) > 80 {
		return "other"
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			continue
		}
		return "other"
	}
	return value
}
