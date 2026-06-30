package daemon

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type daemonMetricsRegistry struct {
	redirectSyncTotal         *prometheus.CounterVec
	redirectSyncDuration      *prometheus.HistogramVec
	redirectSyncStageDuration *prometheus.HistogramVec
	redirectSyncObjects       *prometheus.GaugeVec
}

var daemonMetrics = newDaemonMetricsRegistry()

func newDaemonMetricsRegistry() *daemonMetricsRegistry {
	return &daemonMetricsRegistry{
		redirectSyncTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "netd_redirect_sync_total",
			Help: "Total number of netd redirect sync attempts by result",
		}, []string{"result"}),
		redirectSyncDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netd_redirect_sync_duration_seconds",
			Help:    "Duration of netd redirect sync attempts by result",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"result"}),
		redirectSyncStageDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netd_redirect_sync_stage_duration_seconds",
			Help:    "Duration of netd redirect sync stages by stage and result",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"stage", "result"}),
		redirectSyncObjects: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "netd_redirect_sync_objects",
			Help: "Object counts from the most recent netd redirect sync by kind",
		}, []string{"kind"}),
	}
}

func (m *daemonMetricsRegistry) RecordRedirectSync(result string, duration time.Duration) {
	if m == nil {
		return
	}
	if result == "" {
		result = "unknown"
	}
	m.redirectSyncTotal.WithLabelValues(result).Inc()
	m.redirectSyncDuration.WithLabelValues(result).Observe(duration.Seconds())
}

func (m *daemonMetricsRegistry) RecordRedirectSyncStage(stage, result string, duration time.Duration) {
	if m == nil {
		return
	}
	if stage == "" {
		stage = "unknown"
	}
	if result == "" {
		result = "unknown"
	}
	m.redirectSyncStageDuration.WithLabelValues(stage, result).Observe(duration.Seconds())
}

func (m *daemonMetricsRegistry) SetRedirectSyncObjectCount(kind string, count int) {
	if m == nil || kind == "" {
		return
	}
	m.redirectSyncObjects.WithLabelValues(kind).Set(float64(count))
}
