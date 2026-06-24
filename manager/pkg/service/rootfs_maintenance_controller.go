package service

import (
	"context"
	"time"

	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultRootFSMaintenanceInterval         = time.Minute
	defaultRootFSMaintenanceBatchSize        = 100
	defaultRootFSMaintenanceMaxBatchesPerRun = 10
	defaultRootFSMaintenanceWorkers          = 1
)

type RootFSMaintenanceControllerConfig struct {
	Interval         time.Duration
	BatchSize        int
	MaxBatchesPerRun int
	Workers          int
}

// RootFSMaintenanceController runs internal rootfs metadata and object-store
// maintenance. It is not user-facing API surface.
type RootFSMaintenanceController struct {
	store            *PGSandboxStore
	objectStore      objectstore.Store
	cfg              RootFSMaintenanceControllerConfig
	logger           *zap.Logger
	metrics          *obsmetrics.ManagerMetrics
	meteringRecorder RootFSStorageMeteringRecorder
}

func NewRootFSMaintenanceController(store *PGSandboxStore, objectStore objectstore.Store, cfg RootFSMaintenanceControllerConfig, logger *zap.Logger, metrics *obsmetrics.ManagerMetrics) *RootFSMaintenanceController {
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg = normalizeRootFSMaintenanceControllerConfig(cfg)
	return &RootFSMaintenanceController{
		store:       store,
		objectStore: objectStore,
		cfg:         cfg,
		logger:      logger,
		metrics:     metrics,
	}
}

func (c *RootFSMaintenanceController) SetStorageMeteringRecorder(recorder RootFSStorageMeteringRecorder) {
	if c == nil {
		return
	}
	c.meteringRecorder = recorder
}

func (c *RootFSMaintenanceController) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.store == nil {
		c.logger.Warn("Rootfs maintenance controller disabled; store is missing")
		return nil
	}
	workers := c.cfg.Workers
	if workers <= 0 {
		workers = defaultRootFSMaintenanceWorkers
	}

	defer runtime.HandleCrash()
	c.logger.Info("Starting rootfs maintenance controller",
		zap.Int("workers", workers),
		zap.Duration("interval", c.cfg.Interval),
		zap.Int("batchSize", c.cfg.BatchSize),
	)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, c.cfg.Interval)
	}
	<-ctx.Done()
	c.logger.Info("Rootfs maintenance controller stopped")
	return ctx.Err()
}

func (c *RootFSMaintenanceController) runWorker(ctx context.Context) {
	if err := c.RunOnce(ctx); err != nil && ctx.Err() == nil {
		c.logger.Warn("Rootfs maintenance cycle failed", zap.Error(err))
	}
}

func (c *RootFSMaintenanceController) RunOnce(ctx context.Context) error {
	if c == nil || c.store == nil {
		return nil
	}
	started := time.Now()
	status := "success"
	var totalLayers int
	var totalObjects int
	var runErr error
	defer func() {
		c.observeRun(status, time.Since(started), totalLayers, totalObjects)
	}()

	for batch := 0; batch < c.cfg.MaxBatchesPerRun; batch++ {
		if err := ctx.Err(); err != nil {
			status = "error"
			return err
		}
		result, err := c.store.GarbageCollectRootFSFilesystem(ctx, c.objectStore, "", c.cfg.BatchSize)
		if result != nil {
			totalLayers += len(result.Layers)
			totalObjects += len(result.DeletedS0FSSegments) + len(result.DeletedS0FSManifests)
		}
		if err != nil {
			status = "error"
			runErr = err
			break
		}
		if result == nil || (len(result.Layers) == 0 && len(result.DeletedS0FSSegments) == 0 && len(result.DeletedS0FSManifests) == 0 && result.ExpiredSnapshots == 0 && result.DeletedFilesystems == 0) {
			break
		}
	}
	if err := c.observeStorageUsage(ctx); err != nil {
		status = "error"
		if runErr == nil {
			runErr = err
		}
	}
	return runErr
}

func (c *RootFSMaintenanceController) observeRun(status string, duration time.Duration, layers, objects int) {
	if c == nil || c.metrics == nil {
		return
	}
	if c.metrics.RootFSMaintenanceRunsTotal != nil {
		c.metrics.RootFSMaintenanceRunsTotal.WithLabelValues(status).Inc()
	}
	if c.metrics.RootFSMaintenanceDuration != nil {
		c.metrics.RootFSMaintenanceDuration.WithLabelValues(status).Observe(duration.Seconds())
	}
	if layers > 0 && c.metrics.RootFSGCLayersTotal != nil {
		c.metrics.RootFSGCLayersTotal.Add(float64(layers))
	}
	if objects > 0 && c.metrics.RootFSS0FSObjectDeletesTotal != nil {
		c.metrics.RootFSS0FSObjectDeletesTotal.WithLabelValues("success").Add(float64(objects))
	}
	if status == "error" && c.metrics.RootFSS0FSObjectDeletesTotal != nil {
		c.metrics.RootFSS0FSObjectDeletesTotal.WithLabelValues("error").Inc()
	}
}

func (c *RootFSMaintenanceController) observeStorageUsage(ctx context.Context) error {
	if c == nil || c.store == nil {
		return nil
	}
	var usages []RootFSStorageUsage
	var err error
	if c.meteringRecorder != nil {
		usages, err = c.store.RecordRootFSStorageObservations(ctx, c.objectStore, c.meteringRecorder, "", time.Now().UTC())
	} else {
		usages, err = c.store.ListRootFSStorageUsage(ctx, c.objectStore, "")
	}
	if err != nil {
		c.logger.Warn("Failed to collect rootfs storage usage", zap.Error(err))
		return err
	}
	if c.metrics == nil {
		return nil
	}
	var totalBytes int64
	var totalObjects int64
	for _, usage := range usages {
		totalBytes += usage.StorageBytes
		totalObjects += usage.ObjectCount
	}
	if c.metrics.RootFSStorageBytes != nil {
		c.metrics.RootFSStorageBytes.Set(float64(totalBytes))
	}
	if c.metrics.RootFSStorageObjects != nil {
		c.metrics.RootFSStorageObjects.Set(float64(totalObjects))
	}
	return nil
}

func normalizeRootFSMaintenanceControllerConfig(cfg RootFSMaintenanceControllerConfig) RootFSMaintenanceControllerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRootFSMaintenanceInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultRootFSMaintenanceBatchSize
	}
	if cfg.BatchSize > maxRootFSGCLimit {
		cfg.BatchSize = maxRootFSGCLimit
	}
	if cfg.MaxBatchesPerRun <= 0 {
		cfg.MaxBatchesPerRun = defaultRootFSMaintenanceMaxBatchesPerRun
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultRootFSMaintenanceWorkers
	}
	return cfg
}
