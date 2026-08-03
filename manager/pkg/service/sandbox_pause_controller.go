package service

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	defaultSandboxPauseResyncPeriod = 30 * time.Second
	defaultSandboxPauseScanLimit    = 500
)

type sandboxPauseItem struct {
	SandboxID string
	Resume    bool
}

type pendingRuntimeRecoveryStore interface {
	ListPendingRuntimeRecoverySandboxIDs(ctx context.Context, limit int) ([]string, error)
}

// SandboxPauseController completes durable pause transactions outside the API request path.
type SandboxPauseController struct {
	service        *SandboxService
	logger         *zap.Logger
	queue          workqueue.TypedRateLimitingInterface[sandboxPauseItem]
	resyncInterval time.Duration
	scanLimit      int
	complete       func(context.Context, string) error
	resume         func(context.Context, string) error
}

func NewSandboxPauseController(service *SandboxService, logger *zap.Logger) *SandboxPauseController {
	if logger == nil {
		logger = zap.NewNop()
	}
	controller := &SandboxPauseController{
		service:        service,
		logger:         logger,
		queue:          workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[sandboxPauseItem]()),
		resyncInterval: defaultSandboxPauseResyncPeriod,
		scanLimit:      defaultSandboxPauseScanLimit,
	}
	if service != nil {
		controller.complete = service.CompletePausingSandboxRuntime
		controller.resume = func(ctx context.Context, sandboxID string) error {
			_, err := service.ResumePausedSandboxRuntime(ctx, sandboxID)
			return err
		}
	}
	return controller
}

func (c *SandboxPauseController) EnqueueSandboxPause(sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return
	}
	c.queue.Add(sandboxPauseItem{SandboxID: sandboxID})
}

// EnqueueSandboxRecovery completes the pause transaction and reconstructs the runtime.
func (c *SandboxPauseController) EnqueueSandboxRecovery(sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return
	}
	c.queue.Add(sandboxPauseItem{SandboxID: sandboxID, Resume: true})
}

func (c *SandboxPauseController) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.queue == nil {
		c.queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[sandboxPauseItem]())
	}
	if c.scanLimit <= 0 {
		c.scanLimit = defaultSandboxPauseScanLimit
	}
	if c.resyncInterval <= 0 {
		c.resyncInterval = defaultSandboxPauseResyncPeriod
	}

	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox pause controller", zap.Int("workers", workers))
	c.enqueuePausingSandboxes(ctx)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox pause controller stopped")
			return ctx.Err()
		case <-ticker.C:
			c.enqueuePausingSandboxes(ctx)
		}
	}
}

func (c *SandboxPauseController) enqueuePausingSandboxes(ctx context.Context) {
	if c == nil || c.service == nil || c.service.sandboxStore == nil {
		return
	}
	txns, err := c.service.sandboxStore.ListActiveLifecycleTxns(ctx, SandboxLifecycleKindPause, c.scanLimit)
	if err != nil {
		c.logger.Warn("Failed to list active pause lifecycle transactions", zap.Error(err))
		return
	}
	for _, txn := range txns {
		if txn != nil {
			if sandboxLifecycleSourceReconstructsRuntime(txn.Source) {
				c.EnqueueSandboxRecovery(txn.SandboxID)
			} else {
				c.EnqueueSandboxPause(txn.SandboxID)
			}
		}
	}
	recoveryStore, ok := c.service.sandboxStore.(pendingRuntimeRecoveryStore)
	if !ok {
		return
	}
	sandboxIDs, err := recoveryStore.ListPendingRuntimeRecoverySandboxIDs(ctx, c.scanLimit)
	if err != nil {
		c.logger.Warn("Failed to list pending sandbox runtime recoveries", zap.Error(err))
		return
	}
	for _, sandboxID := range sandboxIDs {
		c.EnqueueSandboxRecovery(sandboxID)
	}
}

func sandboxLifecycleSourceReconstructsRuntime(source string) bool {
	return source == SandboxLifecycleSourceCrash || source == SandboxLifecycleSourceHealth || source == SandboxLifecycleSourceLost
}

func (c *SandboxPauseController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxPauseController) processNextWorkItem(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)

	if c.complete == nil {
		c.queue.Forget(item)
		return true
	}
	if err := c.complete(ctx, item.SandboxID); err != nil {
		c.logger.Warn("Sandbox pause completion failed, requeueing",
			zap.String("sandboxID", item.SandboxID),
			zap.Error(err),
		)
		c.queue.AddRateLimited(item)
		return true
	}
	if item.Resume {
		if c.resume == nil {
			c.queue.Forget(item)
			return true
		}
		if err := c.resume(ctx, item.SandboxID); err != nil {
			c.logger.Warn("Sandbox runtime reconstruction failed, requeueing",
				zap.String("sandboxID", item.SandboxID),
				zap.Error(err),
			)
			c.queue.AddRateLimited(item)
			return true
		}
	}
	c.queue.Forget(item)
	return true
}
