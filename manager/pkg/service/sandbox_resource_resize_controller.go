package service

import (
	"context"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"go.uber.org/zap"
)

const (
	defaultSandboxResourceResizeResyncPeriod = 30 * time.Second
	defaultSandboxResourceResizeScanLimit    = 500
)

type sandboxResourceResizeStore interface {
	ListPendingSandboxResourceResizes(context.Context, int) ([]*sandboxstore.SandboxResourceResize, error)
}

type sandboxResourceResizeReconciler interface {
	CompleteSandboxResourceResize(context.Context, string) error
}

// SandboxResourceResizeController retries durable compute replacements
// independently of an API request, manager replica, or Nomad task plugin.
type SandboxResourceResizeController struct {
	store          sandboxResourceResizeStore
	reconciler     sandboxResourceResizeReconciler
	logger         *zap.Logger
	queue          *retryQueue[string]
	resyncInterval time.Duration
	scanLimit      int
}

func NewSandboxResourceResizeController(
	store sandboxResourceResizeStore,
	reconciler sandboxResourceResizeReconciler,
	logger *zap.Logger,
) *SandboxResourceResizeController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxResourceResizeController{
		store: store, reconciler: reconciler, logger: logger,
		queue:          newRetryQueue[string](),
		resyncInterval: defaultSandboxResourceResizeResyncPeriod,
		scanLimit:      defaultSandboxResourceResizeScanLimit,
	}
}

// EnqueueSandboxResourceResize schedules an immediate retry after a
// synchronous API attempt. PostgreSQL scanning remains the recovery source.
func (c *SandboxResourceResizeController) EnqueueSandboxResourceResize(sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID != "" {
		c.queue.Add(sandboxID)
	}
}

func (c *SandboxResourceResizeController) Run(ctx context.Context, workers int) error {
	if c == nil || c.store == nil || c.reconciler == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.scanLimit <= 0 {
		c.scanLimit = defaultSandboxResourceResizeScanLimit
	}
	if c.resyncInterval <= 0 {
		c.resyncInterval = defaultSandboxResourceResizeResyncPeriod
	}
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox resource resize controller", zap.Int("workers", workers))
	c.enqueuePending(ctx)
	for range workers {
		go c.runWorker(ctx)
	}
	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox resource resize controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueuePending(ctx)
		}
	}
}

func (c *SandboxResourceResizeController) enqueuePending(ctx context.Context) {
	mutations, err := c.store.ListPendingSandboxResourceResizes(ctx, c.scanLimit)
	if err != nil {
		c.logger.Warn("Failed to list pending sandbox resource resizes", zap.Error(err))
		return
	}
	for _, mutation := range mutations {
		if mutation != nil {
			c.EnqueueSandboxResourceResize(mutation.SandboxID)
		}
	}
}

func (c *SandboxResourceResizeController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxResourceResizeController) processNextWorkItem(ctx context.Context) bool {
	sandboxID, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(sandboxID)
	err := c.reconciler.CompleteSandboxResourceResize(ctx, sandboxID)
	if err == nil || apierror.IsConflict(err) {
		if apierror.IsConflict(err) {
			c.logger.Info("Sandbox resource resize was preempted",
				zap.String("sandboxID", sandboxID), zap.Error(err))
		}
		c.queue.Forget(sandboxID)
		return true
	}
	c.logger.Warn("Sandbox resource resize completion failed, requeueing",
		zap.String("sandboxID", sandboxID), zap.Error(err))
	c.queue.AddRateLimited(sandboxID)
	return true
}
