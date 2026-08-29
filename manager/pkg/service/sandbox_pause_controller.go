package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
)

const (
	defaultSandboxPauseResyncPeriod = 30 * time.Second
	defaultSandboxPauseScanLimit    = 500
	defaultSandboxRecoveryLease     = 2 * time.Minute
	defaultSandboxRecoveryRetryBase = time.Second
	defaultSandboxRecoveryRetryMax  = 30 * time.Second
	defaultSandboxRecoverySettle    = 10 * time.Second
)

type sandboxPauseItem struct {
	SandboxID string
	Resume    bool
}

type sandboxPauseLifecycleStore interface {
	ListActiveLifecycleTxns(ctx context.Context, kind string, limit int) ([]*sandboxstore.SandboxLifecycleTxn, error)
	ListPendingRuntimeRecoverySandboxIDs(ctx context.Context, limit int) ([]string, error)
	IsRuntimeRecoveryPending(ctx context.Context, sandboxID string) (bool, error)
	ClaimSandboxRuntimeRecovery(ctx context.Context, sandboxID, workerID string, leaseDuration time.Duration) (*sandboxstore.SandboxRuntimeRecoveryClaim, error)
	RenewSandboxRuntimeRecoveryClaim(ctx context.Context, claim *sandboxstore.SandboxRuntimeRecoveryClaim, leaseDuration time.Duration) error
	FailSandboxRuntimeRecoveryClaim(ctx context.Context, claim *sandboxstore.SandboxRuntimeRecoveryClaim, retryDelay time.Duration, reason string) error
	CompleteSandboxRuntimeRecoveryClaim(ctx context.Context, claim *sandboxstore.SandboxRuntimeRecoveryClaim) error
}

// SandboxPauseController completes durable pause transactions outside the API request path.
type SandboxPauseController struct {
	store          sandboxPauseLifecycleStore
	logger         *zap.Logger
	queue          *retryQueue[sandboxPauseItem]
	resyncInterval time.Duration
	scanLimit      int
	complete       func(context.Context, string) error
	resume         func(context.Context, string) error
	workerID       string
	recoveryLease  time.Duration
	retryBase      time.Duration
	retryMax       time.Duration
}

func NewSandboxPauseController(
	store sandboxPauseLifecycleStore,
	backend SandboxPauseReconciler,
	logger *zap.Logger,
) *SandboxPauseController {
	if logger == nil {
		logger = zap.NewNop()
	}
	controller := &SandboxPauseController{
		store:          store,
		logger:         logger,
		queue:          newRetryQueue[sandboxPauseItem](),
		resyncInterval: defaultSandboxPauseResyncPeriod,
		scanLimit:      defaultSandboxPauseScanLimit,
		workerID:       "sandbox-runtime-recovery-" + uuid.NewString(),
		recoveryLease:  defaultSandboxRecoveryLease,
		retryBase:      defaultSandboxRecoveryRetryBase,
		retryMax:       defaultSandboxRecoveryRetryMax,
	}
	if backend != nil {
		controller.complete = backend.CompletePausingSandboxRuntime
		controller.resume = func(ctx context.Context, sandboxID string) error {
			_, err := backend.ResumePausedSandboxRuntime(ctx, sandboxID)
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
		c.queue = newRetryQueue[sandboxPauseItem]()
	}
	if c.scanLimit <= 0 {
		c.scanLimit = defaultSandboxPauseScanLimit
	}
	if c.resyncInterval <= 0 {
		c.resyncInterval = defaultSandboxPauseResyncPeriod
	}
	if strings.TrimSpace(c.workerID) == "" {
		c.workerID = "sandbox-runtime-recovery-" + uuid.NewString()
	}
	if c.recoveryLease <= 0 {
		c.recoveryLease = defaultSandboxRecoveryLease
	}
	if c.retryBase <= 0 {
		c.retryBase = defaultSandboxRecoveryRetryBase
	}
	if c.retryMax < c.retryBase {
		c.retryMax = defaultSandboxRecoveryRetryMax
	}

	defer c.queue.ShutDown()

	c.logger.Info("Starting sandbox pause controller", zap.Int("workers", workers))
	c.enqueuePausingSandboxes(ctx)
	for i := 0; i < workers; i++ {
		go c.runWorker(ctx)
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
	if c == nil || c.store == nil {
		return
	}
	txns, err := c.store.ListActiveLifecycleTxns(ctx, sandboxstore.SandboxLifecycleKindPause, c.scanLimit)
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
	sandboxIDs, err := c.store.ListPendingRuntimeRecoverySandboxIDs(ctx, c.scanLimit)
	if err != nil {
		c.logger.Warn("Failed to list pending sandbox runtime recoveries", zap.Error(err))
		return
	}
	for _, sandboxID := range sandboxIDs {
		c.EnqueueSandboxRecovery(sandboxID)
	}
}

func sandboxLifecycleSourceReconstructsRuntime(source string) bool {
	return source == sandboxstore.SandboxLifecycleSourceCrash || source == sandboxstore.SandboxLifecycleSourceHealth || source == sandboxstore.SandboxLifecycleSourceLost
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
	if item.Resume {
		return c.processRuntimeRecoveryWorkItem(ctx, item)
	}
	if err := c.complete(ctx, item.SandboxID); err != nil {
		c.logger.Warn("Sandbox pause completion failed, requeueing",
			zap.String("sandboxID", item.SandboxID),
			zap.Error(err),
		)
		c.queue.AddRateLimited(item)
		return true
	}
	c.queue.Forget(item)
	return true
}

func (c *SandboxPauseController) processRuntimeRecoveryWorkItem(
	ctx context.Context,
	item sandboxPauseItem,
) bool {
	claim, err := c.store.ClaimSandboxRuntimeRecovery(
		ctx, item.SandboxID, c.workerID, c.recoveryLease,
	)
	if err != nil {
		c.logger.Warn("Sandbox runtime recovery claim failed, requeueing",
			zap.String("sandboxID", item.SandboxID), zap.Error(err),
		)
		c.queue.Forget(item)
		c.queue.AddAfter(item, c.retryBase)
		return true
	}
	if claim == nil {
		c.queue.Forget(item)
		return true
	}

	workCtx, cancelWork := context.WithCancel(ctx)
	renewalDone := make(chan error, 1)
	go c.monitorRuntimeRecoveryClaim(workCtx, cancelWork, claim, renewalDone)
	workErr := c.reconstructSandboxRuntime(workCtx, item.SandboxID)
	cancelWork()
	if renewalErr := <-renewalDone; workErr == nil && renewalErr != nil {
		workErr = renewalErr
	}

	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(ctx), defaultSandboxRecoverySettle)
	defer cancelSettle()
	c.queue.Forget(item)
	if workErr == nil {
		if err := c.store.CompleteSandboxRuntimeRecoveryClaim(settleCtx, claim); err != nil {
			c.logger.Warn("Sandbox runtime recovery completion lease failed",
				zap.String("sandboxID", item.SandboxID), zap.Error(err),
			)
			c.queue.AddAfter(item, c.retryBase)
		}
		return true
	}

	retryDelay := sandboxRuntimeRecoveryBackoff(claim.AttemptCount, c.retryBase, c.retryMax)
	if err := c.store.FailSandboxRuntimeRecoveryClaim(
		settleCtx, claim, retryDelay, workErr.Error(),
	); err != nil {
		c.logger.Warn("Sandbox runtime recovery failure lease could not be released",
			zap.String("sandboxID", item.SandboxID),
			zap.Int("attempt", claim.AttemptCount),
			zap.Error(err),
		)
		c.queue.AddAfter(item, c.retryBase)
		return true
	}
	c.logger.Warn("Sandbox runtime reconstruction failed, durably deferred",
		zap.String("sandboxID", item.SandboxID),
		zap.Int("attempt", claim.AttemptCount),
		zap.Duration("retryAfter", retryDelay),
		zap.Error(workErr),
	)
	c.queue.AddAfter(item, retryDelay)
	return true
}

func (c *SandboxPauseController) reconstructSandboxRuntime(ctx context.Context, sandboxID string) error {
	pending, err := c.runtimeRecoveryPending(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("runtime recovery preflight: %w", err)
	}
	if !pending {
		return nil
	}
	if err := c.complete(ctx, sandboxID); err != nil {
		return fmt.Errorf("complete failed runtime pause: %w", err)
	}
	pending, err = c.runtimeRecoveryPending(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("runtime recovery revalidation: %w", err)
	}
	if !pending || c.resume == nil {
		return nil
	}
	if err := c.resume(ctx, sandboxID); err != nil {
		return fmt.Errorf("reconstruct runtime: %w", err)
	}
	return nil
}

func (c *SandboxPauseController) monitorRuntimeRecoveryClaim(
	ctx context.Context,
	cancelWork context.CancelFunc,
	claim *sandboxstore.SandboxRuntimeRecoveryClaim,
	done chan<- error,
) {
	interval := c.recoveryLease / 3
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := c.store.RenewSandboxRuntimeRecoveryClaim(ctx, claim, c.recoveryLease); err != nil {
				if ctx.Err() != nil && errors.Is(err, context.Canceled) {
					done <- nil
					return
				}
				cancelWork()
				done <- fmt.Errorf("renew runtime recovery claim: %w", err)
				return
			}
		}
	}
}

func sandboxRuntimeRecoveryBackoff(attempt int, base, maximum time.Duration) time.Duration {
	if base <= 0 {
		base = defaultSandboxRecoveryRetryBase
	}
	if maximum < base {
		maximum = base
	}
	backoff := base
	for current := 1; current < attempt && backoff < maximum; current++ {
		if backoff > maximum/2 {
			return maximum
		}
		backoff *= 2
	}
	if backoff > maximum {
		return maximum
	}
	return backoff
}

func (c *SandboxPauseController) runtimeRecoveryPending(ctx context.Context, sandboxID string) (bool, error) {
	if c.store == nil {
		return false, nil
	}
	return c.store.IsRuntimeRecoveryPending(ctx, sandboxID)
}
