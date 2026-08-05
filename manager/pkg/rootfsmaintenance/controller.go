// Package rootfsmaintenance owns background rootfs metadata and object-store
// reconciliation.
package rootfsmaintenance

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultRootFSMaintenanceInterval         = time.Minute
	defaultRootFSMaintenanceBatchSize        = 100
	defaultRootFSMaintenanceMaxBatchesPerRun = 10
	defaultRootFSMaintenanceWorkers          = 1
	defaultRootFSUnknownObjectGrace          = 24 * time.Hour
)

type Config struct {
	Interval           time.Duration
	BatchSize          int
	MaxBatchesPerRun   int
	Workers            int
	DeleteOptions      sandboxstore.DeletePendingRootFSObjectsOptions
	UnknownObjectGrace time.Duration
}

// Controller runs internal rootfs metadata and object-store maintenance. It is
// not user-facing API surface.
type Controller struct {
	store            *sandboxstore.PGSandboxStore
	deleter          sandboxstore.RootFSObjectDeleter
	cfg              Config
	logger           *zap.Logger
	metrics          *obsmetrics.ManagerMetrics
	objectInspector  sandboxstore.RootFSObjectInspector
	objectLister     sandboxstore.RootFSObjectLister
	objectReader     RootFSObjectReader
	meteringRecorder sandboxstore.RootFSStorageMeteringRecorder
	workerID         string
	orphanScanMu     sync.Mutex
	orphanScanCursor string
}

func New(store *sandboxstore.PGSandboxStore, deleter sandboxstore.RootFSObjectDeleter, cfg Config, logger *zap.Logger, metrics *obsmetrics.ManagerMetrics) *Controller {
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg = normalizeConfig(cfg)
	controller := &Controller{
		store:    store,
		deleter:  deleter,
		cfg:      cfg,
		logger:   logger,
		metrics:  metrics,
		workerID: "manager-rootfs-v3-" + uuid.NewString(),
	}
	controller.objectReader, _ = deleter.(RootFSObjectReader)
	return controller
}

func (c *Controller) SetStorageMeteringRecorder(recorder sandboxstore.RootFSStorageMeteringRecorder) {
	if c == nil {
		return
	}
	if recorder, ok := sandboxstore.ConfiguredRootFSStorageMeteringRecorder(recorder); ok {
		c.meteringRecorder = recorder
		return
	}
	c.meteringRecorder = nil
}

func (c *Controller) SetObjectInspector(inspector sandboxstore.RootFSObjectInspector) {
	if c == nil {
		return
	}
	c.objectInspector = inspector
}

func (c *Controller) SetObjectLister(lister sandboxstore.RootFSObjectLister) {
	if c != nil {
		c.objectLister = lister
	}
}

func (c *Controller) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.store == nil || c.deleter == nil {
		c.logger.Warn("Rootfs maintenance controller disabled; store or object deleter is missing")
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

func (c *Controller) runWorker(ctx context.Context) {
	if err := c.RunOnce(ctx); err != nil && ctx.Err() == nil {
		c.logger.Warn("Rootfs maintenance cycle failed", zap.Error(err))
	}
}

func (c *Controller) RunOnce(ctx context.Context) error {
	if c == nil || c.store == nil || c.deleter == nil {
		return nil
	}
	started := time.Now()
	status := "success"
	var totalObjects int
	var runErr error
	defer func() {
		c.observeRun(status, time.Since(started), totalObjects)
		c.observeQueueStats(ctx)
	}()
	if err := c.processV3Inventory(ctx); err != nil {
		status = "error"
		runErr = errors.Join(runErr, err)
	}
	if _, err := c.scanUnknownRootFSObjects(ctx); err != nil {
		status = "error"
		runErr = errors.Join(runErr, err)
	}
	for batch := 0; batch < c.cfg.MaxBatchesPerRun; batch++ {
		if err := ctx.Err(); err != nil {
			status = "error"
			return err
		}
		expiredSnapshots, err := c.store.DeleteExpiredRootFSSnapshots(ctx, "", c.cfg.BatchSize)
		if err != nil {
			status = "error"
			runErr = errors.Join(runErr, err)
			break
		}
		deletedFilesystems, err := c.store.DeleteUnreferencedRootFSFilesystems(ctx, "", c.cfg.BatchSize)
		if err != nil {
			status = "error"
			runErr = errors.Join(runErr, err)
			break
		}
		gcResult, err := c.store.GarbageCollectRootFSV3(ctx, "", 30*time.Minute, c.cfg.BatchSize)
		if err != nil {
			status = "error"
			runErr = errors.Join(runErr, err)
			break
		}
		queuedObjects := 0
		if gcResult != nil {
			queuedObjects = gcResult.QueuedObjects
		}
		opts := c.cfg.DeleteOptions
		opts.Limit = c.cfg.BatchSize
		opts.ContinueOnError = true
		deletedObjectKeys, err := c.store.DeletePendingRootFSObjectsWithOptions(ctx, c.deleter, opts)
		totalObjects += len(deletedObjectKeys)
		if err != nil {
			status = "error"
			runErr = errors.Join(runErr, err)
			break
		}
		deletedHeads := 0
		if gcResult != nil {
			deletedHeads = gcResult.DeletedHeads
		}
		if expiredSnapshots == 0 && deletedFilesystems == 0 && deletedHeads == 0 && queuedObjects == 0 && len(deletedObjectKeys) == 0 {
			break
		}
	}
	if err := c.auditRootFSObjects(ctx); err != nil {
		status = "error"
		runErr = errors.Join(runErr, err)
	}
	if err := c.observeStorageUsage(ctx); err != nil {
		status = "error"
		runErr = errors.Join(runErr, err)
	}
	return runErr
}

func (c *Controller) observeRun(status string, duration time.Duration, objects int) {
	if c == nil || c.metrics == nil {
		return
	}
	if c.metrics.RootFSMaintenanceRunsTotal != nil {
		c.metrics.RootFSMaintenanceRunsTotal.WithLabelValues(status).Inc()
	}
	if c.metrics.RootFSMaintenanceDuration != nil {
		c.metrics.RootFSMaintenanceDuration.WithLabelValues(status).Observe(duration.Seconds())
	}
	if objects > 0 && c.metrics.RootFSObjectDeletesTotal != nil {
		c.metrics.RootFSObjectDeletesTotal.WithLabelValues("success").Add(float64(objects))
	}
	if status == "error" && c.metrics.RootFSObjectDeletesTotal != nil {
		c.metrics.RootFSObjectDeletesTotal.WithLabelValues("error").Inc()
	}
}

func (c *Controller) observeQueueStats(ctx context.Context) {
	if c == nil || c.metrics == nil || c.store == nil {
		return
	}
	if c.metrics.RootFSObjectDeletionQueueDepth != nil {
		stats, err := c.store.RootFSObjectDeletionQueueStats(ctx)
		if err != nil {
			c.logger.Warn("Failed to collect rootfs deletion queue stats", zap.Error(err))
		} else if stats != nil {
			c.metrics.RootFSObjectDeletionQueueDepth.WithLabelValues("pending").Set(float64(stats.Pending))
			c.metrics.RootFSObjectDeletionQueueDepth.WithLabelValues("due").Set(float64(stats.Due))
			c.metrics.RootFSObjectDeletionQueueDepth.WithLabelValues("claimed").Set(float64(stats.Claimed))
			c.metrics.RootFSObjectDeletionQueueDepth.WithLabelValues("dead_lettered").Set(float64(stats.DeadLettered))
		}
	}
	if c.metrics.RootFSInventoryJobs == nil && c.metrics.RootFSHeadPrefixGuards == nil {
		return
	}
	stats, err := c.store.RootFSInventoryStats(ctx)
	if err != nil {
		c.logger.Warn("Failed to collect rootfs inventory stats", zap.Error(err))
		return
	}
	if stats == nil {
		return
	}
	if c.metrics.RootFSInventoryJobs != nil {
		c.metrics.RootFSInventoryJobs.WithLabelValues("pending").Set(float64(stats.Pending))
		c.metrics.RootFSInventoryJobs.WithLabelValues("running").Set(float64(stats.Running))
		c.metrics.RootFSInventoryJobs.WithLabelValues("complete").Set(float64(stats.Complete))
		c.metrics.RootFSInventoryJobs.WithLabelValues("dead").Set(float64(stats.Dead))
	}
	if c.metrics.RootFSHeadPrefixGuards != nil {
		c.metrics.RootFSHeadPrefixGuards.Set(float64(stats.PrefixGuards))
	}
}

func (c *Controller) auditRootFSObjects(ctx context.Context) error {
	if c == nil || c.store == nil || c.objectInspector == nil {
		return nil
	}
	result, err := c.store.AuditRootFSObjects(ctx, c.objectInspector, "", c.cfg.BatchSize)
	if err != nil {
		c.logger.Warn("Failed to audit rootfs object store consistency", zap.Error(err))
		return err
	}
	if result != nil && (result.Missing > 0 || result.SizeMismatched > 0) {
		c.logger.Warn("Rootfs object store audit found inconsistent objects",
			zap.Int("checked", result.Checked),
			zap.Int("missing", result.Missing),
			zap.Int("sizeMismatched", result.SizeMismatched),
		)
	}
	return nil
}

func (c *Controller) observeStorageUsage(ctx context.Context) error {
	if c == nil || c.store == nil {
		return nil
	}
	var usages []sandboxstore.RootFSStorageUsage
	var err error
	if c.meteringRecorder != nil {
		usages, err = c.store.RecordRootFSStorageObservations(ctx, c.meteringRecorder, "", time.Now().UTC())
	} else {
		usages, err = c.store.ListRootFSStorageUsage(ctx, "")
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

func normalizeConfig(cfg Config) Config {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRootFSMaintenanceInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultRootFSMaintenanceBatchSize
	}
	if cfg.BatchSize > sandboxstore.MaxRootFSObjectDeleteLimit {
		cfg.BatchSize = sandboxstore.MaxRootFSObjectDeleteLimit
	}
	if cfg.MaxBatchesPerRun <= 0 {
		cfg.MaxBatchesPerRun = defaultRootFSMaintenanceMaxBatchesPerRun
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultRootFSMaintenanceWorkers
	}
	if cfg.UnknownObjectGrace <= 0 {
		cfg.UnknownObjectGrace = defaultRootFSUnknownObjectGrace
	}
	if cfg.DeleteOptions.ClaimTTL <= 0 {
		cfg.DeleteOptions.ClaimTTL = sandboxstore.DefaultRootFSObjectDeleteClaimTTL
	}
	if cfg.DeleteOptions.BackoffBase <= 0 {
		cfg.DeleteOptions.BackoffBase = sandboxstore.DefaultRootFSObjectDeleteBackoffBase
	}
	if cfg.DeleteOptions.BackoffMax <= 0 {
		cfg.DeleteOptions.BackoffMax = sandboxstore.DefaultRootFSObjectDeleteBackoffMax
	}
	return cfg
}
