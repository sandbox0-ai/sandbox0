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
	defaultSandboxRootFSResyncPeriod = 30 * time.Second
	defaultSandboxRootFSScanLimit    = 500
)

type sandboxRootFSLifecycleStore interface {
	ListActiveLifecycleTxns(ctx context.Context, kind string, limit int) ([]*sandboxstore.SandboxLifecycleTxn, error)
	ListPendingNomadPausedRebases(ctx context.Context, limit int) ([]*sandboxstore.SandboxLifecycleTxn, error)
}

type sandboxRootFSWorkItem struct {
	kind      string
	sandboxID string
}

// SandboxRootFSController retries durable fork, rebase publication, and
// rebase acknowledgement independently of API requests and Nomad plugins.
type SandboxRootFSController struct {
	store          sandboxRootFSLifecycleStore
	fork           SandboxForkReconciler
	rebase         SandboxRootFSRebaseReconciler
	logger         *zap.Logger
	queue          workqueue.TypedRateLimitingInterface[sandboxRootFSWorkItem]
	resyncInterval time.Duration
	scanLimit      int
}

func NewSandboxRootFSController(
	store sandboxRootFSLifecycleStore,
	fork SandboxForkReconciler,
	rebase SandboxRootFSRebaseReconciler,
	logger *zap.Logger,
) *SandboxRootFSController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxRootFSController{
		store: store, fork: fork, rebase: rebase, logger: logger,
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[sandboxRootFSWorkItem](),
		),
		resyncInterval: defaultSandboxRootFSResyncPeriod, scanLimit: defaultSandboxRootFSScanLimit,
	}
}

func (c *SandboxRootFSController) EnqueueSandboxFork(sandboxID string) {
	c.enqueue(sandboxstore.SandboxLifecycleKindFork, sandboxID)
}

func (c *SandboxRootFSController) EnqueueSandboxRebase(sandboxID string) {
	c.enqueue(sandboxstore.SandboxLifecycleKindRebase, sandboxID)
}

func (c *SandboxRootFSController) enqueue(kind, sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID != "" {
		c.queue.Add(sandboxRootFSWorkItem{kind: kind, sandboxID: sandboxID})
	}
}

func (c *SandboxRootFSController) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.scanLimit <= 0 {
		c.scanLimit = defaultSandboxRootFSScanLimit
	}
	if c.resyncInterval <= 0 {
		c.resyncInterval = defaultSandboxRootFSResyncPeriod
	}
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox RootFS operation controller", zap.Int("workers", workers))
	c.enqueuePending(ctx)
	for range workers {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox RootFS operation controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueuePending(ctx)
		}
	}
}

func (c *SandboxRootFSController) enqueuePending(ctx context.Context) {
	if c == nil || c.store == nil {
		return
	}
	if c.fork != nil {
		txns, err := c.store.ListActiveLifecycleTxns(
			ctx, sandboxstore.SandboxLifecycleKindFork, c.scanLimit,
		)
		if err != nil {
			c.logger.Warn("Failed to list active fork lifecycle transactions", zap.Error(err))
		} else {
			for _, txn := range txns {
				if txn != nil {
					c.EnqueueSandboxFork(txn.SandboxID)
				}
			}
		}
	}
	if c.rebase != nil {
		txns, err := c.store.ListPendingNomadPausedRebases(ctx, c.scanLimit)
		if err != nil {
			c.logger.Warn("Failed to list pending rebase lifecycle transactions", zap.Error(err))
		} else {
			for _, txn := range txns {
				if txn != nil {
					c.EnqueueSandboxRebase(txn.SandboxID)
				}
			}
		}
	}
}

func (c *SandboxRootFSController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxRootFSController) processNextWorkItem(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)
	var err error
	switch item.kind {
	case sandboxstore.SandboxLifecycleKindFork:
		if c.fork != nil {
			err = c.fork.CompleteSandboxFork(ctx, item.sandboxID)
		}
	case sandboxstore.SandboxLifecycleKindRebase:
		if c.rebase != nil {
			err = c.rebase.CompleteSandboxRootFSRebase(ctx, item.sandboxID)
		}
	default:
		c.logger.Error("Dropped unsupported sandbox RootFS work item", zap.String("kind", item.kind))
	}
	if err != nil {
		c.logger.Warn("Sandbox RootFS operation completion failed, requeueing",
			zap.String("kind", item.kind), zap.String("sandboxID", item.sandboxID), zap.Error(err))
		c.queue.AddRateLimited(item)
		return true
	}
	c.queue.Forget(item)
	return true
}
