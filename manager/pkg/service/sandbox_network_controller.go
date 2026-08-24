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
	defaultSandboxNetworkMutationResyncPeriod = 30 * time.Second
	defaultSandboxNetworkMutationScanLimit    = 500
)

type sandboxNetworkMutationStore interface {
	ListPendingNomadSandboxNetworkMutations(context.Context, int) ([]*sandboxstore.NomadSandboxNetworkMutation, error)
}

type sandboxNetworkMutationReconciler interface {
	CompleteNomadSandboxNetworkMutation(context.Context, string) error
}

// SandboxNetworkMutationController retries durable active-policy operations
// independently of an API request, manager replica, or Nomad task plugin.
type SandboxNetworkMutationController struct {
	store          sandboxNetworkMutationStore
	reconciler     sandboxNetworkMutationReconciler
	logger         *zap.Logger
	queue          *retryQueue[string]
	resyncInterval time.Duration
	scanLimit      int
}

func NewSandboxNetworkMutationController(
	store sandboxNetworkMutationStore,
	reconciler sandboxNetworkMutationReconciler,
	logger *zap.Logger,
) *SandboxNetworkMutationController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxNetworkMutationController{
		store: store, reconciler: reconciler, logger: logger,
		queue:          newRetryQueue[string](),
		resyncInterval: defaultSandboxNetworkMutationResyncPeriod,
		scanLimit:      defaultSandboxNetworkMutationScanLimit,
	}
}

// EnqueueSandboxNetworkMutation schedules an immediate retry after a
// synchronous API attempt. PostgreSQL scanning remains the recovery source.
func (c *SandboxNetworkMutationController) EnqueueSandboxNetworkMutation(sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID != "" {
		c.queue.Add(sandboxID)
	}
}

func (c *SandboxNetworkMutationController) Run(ctx context.Context, workers int) error {
	if c == nil || c.store == nil || c.reconciler == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.scanLimit <= 0 {
		c.scanLimit = defaultSandboxNetworkMutationScanLimit
	}
	if c.resyncInterval <= 0 {
		c.resyncInterval = defaultSandboxNetworkMutationResyncPeriod
	}
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox network mutation controller", zap.Int("workers", workers))
	c.enqueuePending(ctx)
	for range workers {
		go c.runWorker(ctx)
	}
	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox network mutation controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueuePending(ctx)
		}
	}
}

func (c *SandboxNetworkMutationController) enqueuePending(ctx context.Context) {
	mutations, err := c.store.ListPendingNomadSandboxNetworkMutations(ctx, c.scanLimit)
	if err != nil {
		c.logger.Warn("Failed to list pending sandbox network mutations", zap.Error(err))
		return
	}
	for _, mutation := range mutations {
		if mutation != nil {
			c.EnqueueSandboxNetworkMutation(mutation.SandboxID)
		}
	}
}

func (c *SandboxNetworkMutationController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxNetworkMutationController) processNextWorkItem(ctx context.Context) bool {
	sandboxID, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(sandboxID)
	err := c.reconciler.CompleteNomadSandboxNetworkMutation(ctx, sandboxID)
	if err == nil || apierror.IsConflict(err) {
		if apierror.IsConflict(err) {
			c.logger.Info("Sandbox network mutation was preempted",
				zap.String("sandboxID", sandboxID), zap.Error(err))
		}
		c.queue.Forget(sandboxID)
		return true
	}
	c.logger.Warn("Sandbox network mutation completion failed, requeueing",
		zap.String("sandboxID", sandboxID), zap.Error(err))
	c.queue.AddRateLimited(sandboxID)
	return true
}
