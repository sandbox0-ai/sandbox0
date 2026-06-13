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

// SandboxPauseController completes durable pausing transitions outside the API request path.
type SandboxPauseController struct {
	service        *SandboxService
	logger         *zap.Logger
	queue          workqueue.TypedRateLimitingInterface[string]
	resyncInterval time.Duration
	scanLimit      int
}

func NewSandboxPauseController(service *SandboxService, logger *zap.Logger) *SandboxPauseController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxPauseController{
		service:        service,
		logger:         logger,
		queue:          workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		resyncInterval: defaultSandboxPauseResyncPeriod,
		scanLimit:      defaultSandboxPauseScanLimit,
	}
}

func (c *SandboxPauseController) EnqueueSandboxPause(sandboxID string) {
	if c == nil || c.queue == nil {
		return
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return
	}
	c.queue.Add(sandboxID)
}

func (c *SandboxPauseController) Run(ctx context.Context, workers int) error {
	if c == nil {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if c.queue == nil {
		c.queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
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
	records, err := c.service.sandboxStore.ListPausingSandboxes(ctx, c.scanLimit)
	if err != nil {
		c.logger.Warn("Failed to list pausing sandboxes", zap.Error(err))
		return
	}
	for _, record := range records {
		if record != nil {
			c.EnqueueSandboxPause(record.ID)
		}
	}
}

func (c *SandboxPauseController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *SandboxPauseController) processNextWorkItem(ctx context.Context) bool {
	sandboxID, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(sandboxID)

	if c.service == nil {
		c.queue.Forget(sandboxID)
		return true
	}
	if err := c.service.CompletePausingSandboxRuntime(ctx, sandboxID); err != nil {
		c.logger.Warn("Sandbox pause completion failed, requeueing",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.queue.AddRateLimited(sandboxID)
		return true
	}
	c.queue.Forget(sandboxID)
	return true
}
