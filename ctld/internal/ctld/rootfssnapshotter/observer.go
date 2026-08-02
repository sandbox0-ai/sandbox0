package rootfssnapshotter

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Observer separates bounded metadata-head work from Kubernetes Pod readiness.
type Observer struct {
	logger   *zap.Logger
	duration *prometheus.HistogramVec
}

func NewObserver(registry prometheus.Registerer, logger *zap.Logger) *Observer {
	observer := &Observer{logger: logger}
	if registry == nil {
		return observer
	}
	observer.duration = promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rootfs_snapshotter_operation_duration_seconds",
		Help:    "Duration of bounded metadata-head operations in the external rootfs snapshotter",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"operation", "status"})
	return observer
}

func (o *Observer) Observe(operation, reference string, started time.Time, err error) {
	if o == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	duration := time.Since(started)
	if o.duration != nil {
		o.duration.WithLabelValues(operation, status).Observe(duration.Seconds())
	}
	if o.logger == nil {
		return
	}
	fields := []zap.Field{
		zap.String("operation", operation),
		zap.String("status", status),
		zap.Duration("duration", duration),
		zap.String("reference", reference),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	o.logger.Info("Rootfs snapshotter operation completed", fields...)
}
