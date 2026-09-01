package service

import (
	"context"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
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

// SandboxRootFSController retries durable snapshot, fork, rebase publication,
// and rebase acknowledgement independently of API requests and Nomad plugins.
type SandboxRootFSController struct {
	store          sandboxRootFSLifecycleStore
	snapshot       SandboxRootFSSnapshotReconciler
	fork           SandboxForkReconciler
	rebase         SandboxRootFSRebaseReconciler
	logger         *zap.Logger
	queue          *retryQueue[sandboxRootFSWorkItem]
	resyncInterval time.Duration
	scanLimit      int
}

func NewSandboxRootFSController(
	store sandboxRootFSLifecycleStore,
	snapshot SandboxRootFSSnapshotReconciler,
	fork SandboxForkReconciler,
	rebase SandboxRootFSRebaseReconciler,
	logger *zap.Logger,
) *SandboxRootFSController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxRootFSController{
		store: store, snapshot: snapshot, fork: fork, rebase: rebase, logger: logger,
		queue:          newRetryQueue[sandboxRootFSWorkItem](),
		resyncInterval: defaultSandboxRootFSResyncPeriod, scanLimit: defaultSandboxRootFSScanLimit,
	}
}

func (c *SandboxRootFSController) EnqueueSandboxSnapshot(sandboxID string) {
	c.enqueue(sandboxstore.SandboxLifecycleKindSnapshot, sandboxID)
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
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox RootFS operation controller", zap.Int("workers", workers))
	c.enqueuePending(ctx)
	for range workers {
		go c.runWorker(ctx)
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
	if c.snapshot != nil {
		txns, err := c.store.ListActiveLifecycleTxns(
			ctx, sandboxstore.SandboxLifecycleKindSnapshot, c.scanLimit,
		)
		if err != nil {
			c.logger.Warn("Failed to list active snapshot lifecycle transactions", zap.Error(err))
		} else {
			for _, txn := range txns {
				if txn != nil {
					c.EnqueueSandboxSnapshot(txn.SandboxID)
				}
			}
		}
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
	case sandboxstore.SandboxLifecycleKindSnapshot:
		if c.snapshot != nil {
			err = c.snapshot.CompleteSandboxRootFSSnapshot(ctx, item.sandboxID)
		}
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
