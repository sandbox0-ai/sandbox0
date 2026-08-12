package rootfs

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Observer records node-local rootfs checkpoint phases without adding
// sandbox identifiers to metric labels.
type Observer struct {
	logger        *zap.Logger
	phaseDuration *prometheus.HistogramVec
	cacheRequests *prometheus.CounterVec
}

func NewObserver(registry prometheus.Registerer, logger *zap.Logger) *Observer {
	observer := &Observer{logger: logger}
	if registry == nil {
		return observer
	}
	factory := promauto.With(registry)
	durationBuckets := []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}
	observer.phaseDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_rootfs_phase_duration_seconds",
		Help:    "Duration of ctld rootfs checkpoint phases",
		Buckets: durationBuckets,
	}, []string{"operation", "phase", "status"})
	observer.cacheRequests = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "ctld_rootfs_object_cache_requests_total",
		Help: "Rootfs object cache requests by result",
	}, []string{"result"})
	return observer
}

func (o *Observer) ObservePhase(operation, phase string, started time.Time, err error) {
	if o == nil {
		return
	}
	o.ObservePhaseDuration(operation, phase, time.Since(started), err)
}

func (o *Observer) ObservePhaseDuration(operation, phase string, duration time.Duration, err error) {
	if o == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	if o.phaseDuration != nil {
		o.phaseDuration.WithLabelValues(operation, phase, status).Observe(duration.Seconds())
	}
	if o.logger != nil {
		fields := []zap.Field{
			zap.String("operation", operation),
			zap.String("phase", phase),
			zap.String("status", status),
			zap.Duration("duration", duration),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		o.logger.Debug("Rootfs checkpoint phase completed", fields...)
	}
}

func (o *Observer) ObserveCache(result string) {
	if o != nil && o.cacheRequests != nil {
		o.cacheRequests.WithLabelValues(result).Inc()
	}
}
