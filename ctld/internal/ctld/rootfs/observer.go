package rootfs

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
)

const (
	rootFSLargeCheckpointBytes  = 512 << 20
	rootFSSlowOperationDuration = 10 * time.Second
)

// Observer records node-local rootfs checkpoint phases without adding
// sandbox identifiers to metric labels.
type Observer struct {
	logger            *zap.Logger
	operationDuration *prometheus.HistogramVec
	phaseDuration     *prometheus.HistogramVec
	checkpointBytes   *prometheus.HistogramVec
	chainDepth        *prometheus.HistogramVec
	cacheRequests     *prometheus.CounterVec
}

func NewObserver(registry prometheus.Registerer, logger *zap.Logger) *Observer {
	observer := &Observer{logger: logger}
	if registry == nil {
		return observer
	}
	factory := promauto.With(registry)
	durationBuckets := []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}
	byteBuckets := []float64{1 << 10, 1 << 20, 16 << 20, 64 << 20, 256 << 20, 512 << 20, 1 << 30, 2 << 30, 4 << 30}
	observer.operationDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_rootfs_operation_duration_seconds",
		Help:    "Duration of ctld rootfs checkpoint operations",
		Buckets: durationBuckets,
	}, []string{"operation", "status"})
	observer.phaseDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_rootfs_phase_duration_seconds",
		Help:    "Duration of ctld rootfs checkpoint phases",
		Buckets: durationBuckets,
	}, []string{"operation", "phase", "status"})
	observer.checkpointBytes = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_rootfs_checkpoint_bytes",
		Help:    "Rootfs checkpoint bytes by operation and byte class",
		Buckets: byteBuckets,
	}, []string{"operation", "kind"})
	observer.chainDepth = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_rootfs_checkpoint_chain_depth",
		Help:    "Rootfs checkpoint layer chain depth by operation",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
	}, []string{"operation"})
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

func (o *Observer) ObserveBytes(operation, kind string, bytes int64) {
	if o == nil || o.checkpointBytes == nil || bytes < 0 {
		return
	}
	o.checkpointBytes.WithLabelValues(operation, kind).Observe(float64(bytes))
}

func (o *Observer) ObserveCache(result string) {
	if o != nil && o.cacheRequests != nil {
		o.cacheRequests.WithLabelValues(result).Inc()
	}
}

func (o *Observer) ObserveOperation(operation string, target ctldapi.RootFSContainerRef, chainDepth int, inputBytes, outputBytes, excludedBytes int64, started time.Time, statusCode int, message string) {
	if o == nil {
		return
	}
	duration := time.Since(started)
	status := "success"
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		status = "error"
	}
	if status == "success" && excludedBytes < 0 && inputBytes >= outputBytes && outputBytes >= 0 {
		excludedBytes = inputBytes - outputBytes
	}
	if o.operationDuration != nil {
		o.operationDuration.WithLabelValues(operation, status).Observe(duration.Seconds())
	}
	if o.chainDepth != nil && chainDepth > 0 {
		o.chainDepth.WithLabelValues(operation).Observe(float64(chainDepth))
	}
	o.ObserveBytes(operation, "input", inputBytes)
	o.ObserveBytes(operation, "output", outputBytes)
	o.ObserveBytes(operation, "excluded", excludedBytes)
	if o.logger == nil {
		return
	}
	fields := []zap.Field{
		zap.String("operation", operation),
		zap.String("status", status),
		zap.Duration("duration", duration),
		zap.String("namespace", target.Namespace),
		zap.String("pod", target.PodName),
		zap.String("container", target.ContainerName),
	}
	if chainDepth >= 0 {
		fields = append(fields, zap.Int("chain_depth", chainDepth))
	}
	if inputBytes >= 0 {
		fields = append(fields, zap.Int64("input_bytes", inputBytes))
	}
	if outputBytes >= 0 {
		fields = append(fields, zap.Int64("output_bytes", outputBytes))
	}
	if excludedBytes >= 0 {
		fields = append(fields, zap.Int64("excluded_bytes", excludedBytes))
	}
	if message != "" {
		fields = append(fields, zap.String("error", message))
	}
	if duration >= rootFSSlowOperationDuration || inputBytes >= rootFSLargeCheckpointBytes || outputBytes >= rootFSLargeCheckpointBytes {
		o.logger.Warn("Slow or large rootfs checkpoint operation", fields...)
		return
	}
	o.logger.Info("Rootfs checkpoint operation completed", fields...)
}
