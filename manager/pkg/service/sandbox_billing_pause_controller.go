package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
)

type BillingPauseLister interface {
	ListBillingPauseCandidates(context.Context, string, string, int) ([]sandboxstore.BillingPauseCandidate, error)
}

type BillingPauser interface {
	PauseSandboxForBilling(context.Context, string, int64) error
}

// SandboxBillingPauseController turns a durable regional admission projection
// into checkpoint pauses. A cursor prevents one failing sandbox from starving
// later candidates, while the store rechecks every request under a row lock.
type SandboxBillingPauseController struct {
	lister         BillingPauseLister
	pauser         BillingPauser
	clusterID      string
	interval       time.Duration
	batchSize      int
	afterSandboxID string
	logger         *zap.Logger
}

func NewSandboxBillingPauseController(
	lister BillingPauseLister, pauser BillingPauser, clusterID string, interval time.Duration, logger *zap.Logger,
) (*SandboxBillingPauseController, error) {
	if lister == nil || pauser == nil || clusterID == "" {
		return nil, fmt.Errorf("billing pause lister, pauser, and cluster id are required")
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxBillingPauseController{
		lister: lister, pauser: pauser, clusterID: clusterID, interval: interval, batchSize: 500, logger: logger,
	}, nil
}

func (c *SandboxBillingPauseController) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Error("Initial billing pause scan failed", zap.Error(err))
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Error("Billing pause scan failed", zap.Error(err))
			}
		}
	}
}

func (c *SandboxBillingPauseController) runOnce(ctx context.Context) error {
	candidates, err := c.lister.ListBillingPauseCandidates(ctx, c.clusterID, c.afterSandboxID, c.batchSize)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		c.afterSandboxID = ""
		return nil
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.pauser.PauseSandboxForBilling(ctx, candidate.SandboxID, candidate.AdmissionVersion); err != nil {
			c.logger.Warn("Billing pause request failed", zap.String("sandbox_id", candidate.SandboxID), zap.Error(err))
		}
		c.afterSandboxID = candidate.SandboxID
	}
	if len(candidates) < c.batchSize {
		c.afterSandboxID = ""
	}
	return nil
}
