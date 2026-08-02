package portal

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"go.uber.org/zap"
)

// Observer records low-cardinality volume portal latency and cache metrics.
// Sandbox and volume identifiers are only emitted in structured logs.
type Observer struct {
	logger                 *zap.Logger
	bindDuration           *prometheus.HistogramVec
	phaseDuration          *prometheus.HistogramVec
	stateBytes             *prometheus.HistogramVec
	stateItems             *prometheus.HistogramVec
	hotCacheRequests       *prometheus.CounterVec
	hotCacheAdmissions     *prometheus.CounterVec
	hotCacheEvictions      *prometheus.CounterVec
	hotCacheEntries        prometheus.Gauge
	hotCacheEstimatedBytes prometheus.Gauge
	hotCacheSegmentEntries *prometheus.GaugeVec
	hotCacheSegmentBytes   *prometheus.GaugeVec
	hotCacheBudgetBytes    prometheus.Gauge
	hotCacheEntryBytes     *prometheus.HistogramVec
	hotCacheActiveDuration *prometheus.HistogramVec
	hotCacheOpenDuration   *prometheus.HistogramVec
	hotCacheResidence      *prometheus.HistogramVec
}

func NewObserver(registry prometheus.Registerer, logger *zap.Logger) *Observer {
	observer := &Observer{logger: logger}
	if registry == nil {
		return observer
	}
	factory := promauto.With(registry)
	durationBuckets := []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
	byteBuckets := []float64{1 << 10, 64 << 10, 1 << 20, 4 << 20, 16 << 20, 64 << 20, 256 << 20, 1 << 30}
	itemBuckets := []float64{1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000}
	observer.bindDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_volume_portal_bind_duration_seconds",
		Help:    "Duration of ctld volume portal bind operations",
		Buckets: durationBuckets,
	}, []string{"backend", "path", "status"})
	observer.phaseDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_volume_portal_phase_duration_seconds",
		Help:    "Duration of ctld volume portal bind, S0FS open, and recovery phases",
		Buckets: durationBuckets,
	}, []string{"operation", "phase", "source", "format", "status"})
	observer.stateBytes = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_s0fs_open_read_bytes",
		Help:    "Encoded state or WAL bytes read during S0FS engine open",
		Buckets: byteBuckets,
	}, []string{"phase", "source", "format"})
	observer.stateItems = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_s0fs_state_items",
		Help:    "S0FS state item counts observed after engine open",
		Buckets: itemBuckets,
	}, []string{"kind", "source", "format"})
	observer.hotCacheRequests = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "ctld_s0fs_hot_cache_requests_total",
		Help: "S0FS hot engine cache requests by result",
	}, []string{"result", "segment"})
	observer.hotCacheAdmissions = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "ctld_s0fs_hot_cache_admissions_total",
		Help: "S0FS hot engine cache admission decisions",
	}, []string{"decision", "reason"})
	observer.hotCacheEvictions = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "ctld_s0fs_hot_cache_evictions_total",
		Help: "S0FS hot engine cache evictions by reason",
	}, []string{"reason", "segment"})
	observer.hotCacheEntries = factory.NewGauge(prometheus.GaugeOpts{
		Name: "ctld_s0fs_hot_cache_entries",
		Help: "Current S0FS hot engine cache entry count",
	})
	observer.hotCacheEstimatedBytes = factory.NewGauge(prometheus.GaugeOpts{
		Name: "ctld_s0fs_hot_cache_estimated_bytes",
		Help: "Conservatively estimated memory retained by the S0FS hot engine cache",
	})
	observer.hotCacheSegmentEntries = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ctld_s0fs_hot_cache_segment_entries",
		Help: "Current S0FS hot engine cache entry count by segment",
	}, []string{"segment"})
	observer.hotCacheSegmentBytes = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ctld_s0fs_hot_cache_segment_estimated_bytes",
		Help: "Conservatively estimated S0FS hot engine cache memory by segment",
	}, []string{"segment"})
	observer.hotCacheBudgetBytes = factory.NewGauge(prometheus.GaugeOpts{
		Name: "ctld_s0fs_hot_cache_budget_bytes",
		Help: "Configured S0FS hot engine cache memory budget",
	})
	observer.hotCacheEntryBytes = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_s0fs_hot_cache_entry_estimated_bytes",
		Help:    "Conservatively estimated memory charged for S0FS hot cache candidates",
		Buckets: byteBuckets,
	}, []string{"segment"})
	observer.hotCacheActiveDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_s0fs_hot_cache_candidate_active_duration_seconds",
		Help:    "Mounted duration of S0FS hot cache candidates",
		Buckets: []float64{1, 5, 10, 30, 60, 300, 900, 3600, 21600, 86400},
	}, []string{"segment"})
	observer.hotCacheOpenDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_s0fs_hot_cache_candidate_cold_open_duration_seconds",
		Help:    "Last cold engine-open duration of S0FS hot cache candidates",
		Buckets: durationBuckets,
	}, []string{"segment"})
	observer.hotCacheResidence = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ctld_s0fs_hot_cache_residence_duration_seconds",
		Help:    "Time S0FS engines remain in the hot cache before a hit or eviction",
		Buckets: []float64{1, 5, 10, 30, 60, 300, 900, 3600, 21600, 86400},
	}, []string{"segment", "outcome"})
	return observer
}

func (o *Observer) ObserveBind(backend, path, volumeID string, started time.Time, err error) {
	if o == nil {
		return
	}
	status := observationStatus(err)
	duration := time.Since(started)
	if o.bindDuration != nil {
		o.bindDuration.WithLabelValues(backend, path, status).Observe(duration.Seconds())
	}
	if o.logger != nil {
		fields := []zap.Field{
			zap.String("backend", backend),
			zap.String("path", path),
			zap.String("volume_id", volumeID),
			zap.String("status", status),
			zap.Duration("duration", duration),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		o.logger.Info("Volume portal bind completed", fields...)
	}
}

func (o *Observer) ObservePhase(operation, phase, source string, format int, volumeID string, started time.Time, err error) {
	if o == nil {
		return
	}
	duration := time.Since(started)
	o.observePhaseDuration(operation, phase, source, format, volumeID, duration, err)
}

func (o *Observer) ObserveS0FSOpen(observation s0fs.OpenObservation) {
	if o == nil {
		return
	}
	o.observePhaseDuration("s0fs_open", observation.Phase, observation.Source, observation.Format, observation.VolumeID, observation.Duration, observation.Err)
	format := observationFormat(observation.Format)
	if observation.Bytes >= 0 && o.stateBytes != nil {
		o.stateBytes.WithLabelValues(observation.Phase, observation.Source, format).Observe(float64(observation.Bytes))
	}
	if observation.Phase == "complete" && o.stateItems != nil {
		o.stateItems.WithLabelValues("nodes", observation.Source, format).Observe(float64(observation.Nodes))
		o.stateItems.WithLabelValues("directory_entries", observation.Source, format).Observe(float64(observation.DirectoryEntries))
		o.stateItems.WithLabelValues("segments", observation.Source, format).Observe(float64(observation.Segments))
		o.stateItems.WithLabelValues("wal_records", observation.Source, format).Observe(float64(observation.WALRecords))
		o.stateItems.WithLabelValues("wal_records_scanned", observation.Source, format).Observe(float64(observation.WALRecordsScanned))
		o.stateItems.WithLabelValues("wal_records_skipped", observation.Source, format).Observe(float64(observation.WALRecordsSkipped))
	}
	if observation.Phase == "complete" && observation.WALMaxRecordBytes > 0 && o.stateBytes != nil {
		o.stateBytes.WithLabelValues("wal_max_record", observation.Source, format).Observe(float64(observation.WALMaxRecordBytes))
	}
	if observation.Phase == "complete" && observation.WALMaxDecodedBytes > 0 && o.stateBytes != nil {
		o.stateBytes.WithLabelValues("wal_max_decoded_record", observation.Source, format).Observe(float64(observation.WALMaxDecodedBytes))
	}
	if observation.Phase == "wal_replay" && o.logger != nil {
		fields := []zap.Field{
			zap.String("volume_id", observation.VolumeID),
			zap.String("source", observation.Source),
			zap.String("format", format),
			zap.Int64("wal_bytes_scanned", observation.Bytes),
			zap.Int("wal_records_scanned", observation.WALRecordsScanned),
			zap.Int("wal_records_skipped", observation.WALRecordsSkipped),
			zap.Int("wal_records_replayed", observation.WALRecords),
			zap.Int64("wal_max_record_bytes", observation.WALMaxRecordBytes),
			zap.Int64("wal_max_decoded_record_bytes", observation.WALMaxDecodedBytes),
			zap.Duration("duration", observation.Duration),
			zap.String("status", observationStatus(observation.Err)),
		}
		if observation.Err != nil {
			fields = append(fields, zap.Error(observation.Err))
		}
		o.logger.Debug("S0FS WAL recovery completed", fields...)
	}
}

func (o *Observer) ObserveHotCacheRequest(result, segment string) {
	if o != nil && o.hotCacheRequests != nil {
		o.hotCacheRequests.WithLabelValues(result, segment).Inc()
	}
}

func (o *Observer) ObserveHotCacheAdmission(decision, reason string) {
	if o != nil && o.hotCacheAdmissions != nil {
		o.hotCacheAdmissions.WithLabelValues(decision, reason).Inc()
	}
}

func (o *Observer) ObserveHotCacheEviction(reason, segment string) {
	if o != nil && o.hotCacheEvictions != nil {
		o.hotCacheEvictions.WithLabelValues(reason, segment).Inc()
	}
}

func (o *Observer) ObserveHotCacheCandidate(segment string, estimatedBytes int64, activeDuration, coldOpenDuration time.Duration) {
	if o == nil {
		return
	}
	if o.hotCacheEntryBytes != nil {
		o.hotCacheEntryBytes.WithLabelValues(segment).Observe(float64(estimatedBytes))
	}
	if o.hotCacheActiveDuration != nil {
		o.hotCacheActiveDuration.WithLabelValues(segment).Observe(activeDuration.Seconds())
	}
	if o.hotCacheOpenDuration != nil {
		o.hotCacheOpenDuration.WithLabelValues(segment).Observe(coldOpenDuration.Seconds())
	}
}

func (o *Observer) ObserveHotCacheResidence(segment, outcome string, duration time.Duration) {
	if o != nil && o.hotCacheResidence != nil {
		o.hotCacheResidence.WithLabelValues(segment, outcome).Observe(duration.Seconds())
	}
}

func (o *Observer) SetHotCacheBudget(bytes int64) {
	if o != nil && o.hotCacheBudgetBytes != nil {
		o.hotCacheBudgetBytes.Set(float64(bytes))
	}
}

func (o *Observer) SetHotCacheSize(entries int, estimatedBytes int64, probationEntries int, probationBytes int64, protectedEntries int, protectedBytes int64) {
	if o == nil {
		return
	}
	if o.hotCacheEntries != nil {
		o.hotCacheEntries.Set(float64(entries))
	}
	if o.hotCacheEstimatedBytes != nil {
		o.hotCacheEstimatedBytes.Set(float64(estimatedBytes))
	}
	if o.hotCacheSegmentEntries != nil {
		o.hotCacheSegmentEntries.WithLabelValues(string(hotCacheSegmentProbation)).Set(float64(probationEntries))
		o.hotCacheSegmentEntries.WithLabelValues(string(hotCacheSegmentProtected)).Set(float64(protectedEntries))
	}
	if o.hotCacheSegmentBytes != nil {
		o.hotCacheSegmentBytes.WithLabelValues(string(hotCacheSegmentProbation)).Set(float64(probationBytes))
		o.hotCacheSegmentBytes.WithLabelValues(string(hotCacheSegmentProtected)).Set(float64(protectedBytes))
	}
}

func (o *Observer) observePhaseDuration(operation, phase, source string, format int, volumeID string, duration time.Duration, err error) {
	status := observationStatus(err)
	formatLabel := observationFormat(format)
	if o.phaseDuration != nil {
		o.phaseDuration.WithLabelValues(operation, phase, source, formatLabel, status).Observe(duration.Seconds())
	}
	if o.logger == nil {
		return
	}
	fields := []zap.Field{
		zap.String("operation", operation),
		zap.String("phase", phase),
		zap.String("source", source),
		zap.String("format", formatLabel),
		zap.String("volume_id", volumeID),
		zap.String("status", status),
		zap.Duration("duration", duration),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	o.logger.Debug("Volume portal phase completed", fields...)
}

func observationStatus(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, s0fs.ErrCommittedHeadNotFound) || errors.Is(err, s0fs.ErrMaterializedManifestNotFound) || errors.Is(err, s0fs.ErrSnapshotNotFound) {
		return "not_found"
	}
	return "error"
}

func observationFormat(format int) string {
	switch format {
	case s0fs.StateFormatV1:
		return "v1"
	case s0fs.StateFormatV2:
		return "v2"
	default:
		return "unknown"
	}
}
