package runtimemetrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Observer records low-cardinality health signals for runtime metric collection.
type Observer struct {
	collectionDuration *prometheus.HistogramVec
	collections        *prometheus.CounterVec
	targets            prometheus.Gauge
	samples            *prometheus.CounterVec
}

// NewObserver registers runtime metric collection health signals when a registry is available.
func NewObserver(registry prometheus.Registerer) *Observer {
	observer := &Observer{}
	if registry == nil {
		return observer
	}
	factory := promauto.With(registry)
	observer.collectionDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_runtime_metric_collection_duration_seconds",
		Help:    "Duration of bounded ctld runtime metric collection attempts",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 15},
	}, []string{"status"})
	observer.collections = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "ctld_runtime_metric_collections_total",
		Help: "Ctld runtime metric collection attempts by result",
	}, []string{"status"})
	observer.targets = factory.NewGauge(prometheus.GaugeOpts{
		Name: "ctld_runtime_metric_collection_targets",
		Help: "Claimed sandboxes selected by the latest ctld runtime metric collection",
	})
	observer.samples = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "ctld_runtime_metric_samples_total",
		Help: "Ctld runtime metric samples by collection result",
	}, []string{"result"})
	return observer
}

// ObserveCollection records one complete bounded collection attempt.
func (o *Observer) ObserveCollection(duration time.Duration, result CollectResult, err error) {
	if o == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
		if result.Enqueued > 0 {
			status = "partial"
		}
	}
	if o.collectionDuration != nil {
		o.collectionDuration.WithLabelValues(status).Observe(duration.Seconds())
	}
	if o.collections != nil {
		o.collections.WithLabelValues(status).Inc()
	}
	if o.targets != nil {
		o.targets.Set(float64(result.Matched))
	}
	if o.samples != nil {
		if result.Enqueued > 0 {
			o.samples.WithLabelValues("enqueued").Add(float64(result.Enqueued))
		}
		if result.Dropped > 0 {
			o.samples.WithLabelValues("dropped").Add(float64(result.Dropped))
		}
		if result.Failed > 0 {
			o.samples.WithLabelValues("failed").Add(float64(result.Failed))
		}
	}
}
