package service

import (
	"context"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	defaultSandboxForkResyncPeriod = 30 * time.Second
	defaultSandboxForkScanLimit    = 500
)

type sandboxForkLifecycleStore interface {
	ListActiveLifecycleTxns(ctx context.Context, kind string, limit int) ([]*sandboxstore.SandboxLifecycleTxn, error)
}

// SandboxForkController retries durable fork transactions independently of
// the API request and of Nomad allocation plugin availability.
type SandboxForkController struct {
	store          sandboxForkLifecycleStore
	reconciler     SandboxForkReconciler
	logger         *zap.Logger
	queue          workqueue.TypedRateLimitingInterface[string]
	resyncInterval time.Duration
	scanLimit      int
}

func NewSandboxForkController(
	store sandboxForkLifecycleStore,
	reconciler SandboxForkReconciler,
	logger *zap.Logger,
) *SandboxForkController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxForkController{
		store: store, reconciler: reconciler, logger: logger,
		queue:          workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		resyncInterval: defaultSandboxForkResyncPeriod, scanLimit: defaultSandboxForkScanLimit,
	}
}

func (c *SandboxForkController) EnqueueSandboxFork(sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	if sandboxID = strings.TrimSpace(sandboxID); sandboxID != "" {
		c.queue.Add(sandboxID)
	}
}

func (c *SandboxForkController) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.scanLimit <= 0 {
		c.scanLimit = defaultSandboxForkScanLimit
	}
	if c.resyncInterval <= 0 {
		c.resyncInterval = defaultSandboxForkResyncPeriod
	}
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox fork controller", zap.Int("workers", workers))
	c.enqueueActiveForks(ctx)
	for range workers {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox fork controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueueActiveForks(ctx)
		}
	}
}

func (c *SandboxForkController) enqueueActiveForks(ctx context.Context) {
	if c == nil || c.store == nil {
		return
	}
	txns, err := c.store.ListActiveLifecycleTxns(
		ctx, sandboxstore.SandboxLifecycleKindFork, c.scanLimit,
	)
	if err != nil {
		c.logger.Warn("Failed to list active fork lifecycle transactions", zap.Error(err))
		return
	}
	for _, txn := range txns {
		if txn != nil {
			c.EnqueueSandboxFork(txn.SandboxID)
		}
	}
}

func (c *SandboxForkController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxForkController) processNextWorkItem(ctx context.Context) bool {
	sandboxID, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(sandboxID)
	if c.reconciler == nil {
		c.queue.Forget(sandboxID)
		return true
	}
	if err := c.reconciler.CompleteSandboxFork(ctx, sandboxID); err != nil {
		c.logger.Warn("Sandbox fork completion failed, requeueing",
			zap.String("sandboxID", sandboxID), zap.Error(err))
		c.queue.AddRateLimited(sandboxID)
		return true
	}
	c.queue.Forget(sandboxID)
	return true
}
