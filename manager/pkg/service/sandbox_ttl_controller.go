package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"go.uber.org/zap"
)

const (
	defaultSandboxTTLInterval         = 30 * time.Second
	defaultSandboxTTLBatchSize        = 500
	defaultSandboxTTLMaxBatchesPerRun = 4
)

// SandboxExpirationLister returns durable expiration candidates.
type SandboxExpirationLister interface {
	ListSandboxExpirationCandidates(
		context.Context,
		time.Time,
		int,
	) ([]sandboxstore.SandboxExpirationCandidate, error)
}

// SandboxHardExpiryTerminator revalidates a hard deadline under the durable
// sandbox lock before committing deletion intent.
type SandboxHardExpiryTerminator interface {
	TerminateHardExpiredSandbox(context.Context, string) error
}

// SandboxTTLControllerConfig bounds each scan independently from the number
// of expired sandboxes in a region.
type SandboxTTLControllerConfig struct {
	Interval         time.Duration
	BatchSize        int
	MaxBatchesPerRun int
}

// SandboxTTLController turns durable soft and hard deadlines into exact
// lifecycle requests from the durable runtime-slot projection.
type SandboxTTLController struct {
	lister           SandboxExpirationLister
	pauser           SandboxAutoPauser
	terminator       SandboxHardExpiryTerminator
	interval         time.Duration
	batchSize        int
	maxBatchesPerRun int
	now              func() time.Time
	logger           *zap.Logger
}

func NewSandboxTTLController(
	lister SandboxExpirationLister,
	pauser SandboxAutoPauser,
	terminator SandboxHardExpiryTerminator,
	config SandboxTTLControllerConfig,
	now func() time.Time,
	logger *zap.Logger,
) (*SandboxTTLController, error) {
	if lister == nil || pauser == nil || terminator == nil {
		return nil, fmt.Errorf("sandbox expiration lister, pauser, and hard-expiry terminator are required")
	}
	if config.Interval <= 0 {
		config.Interval = defaultSandboxTTLInterval
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaultSandboxTTLBatchSize
	}
	if config.MaxBatchesPerRun <= 0 {
		config.MaxBatchesPerRun = defaultSandboxTTLMaxBatchesPerRun
	}
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxTTLController{
		lister: lister, pauser: pauser, terminator: terminator,
		interval: config.Interval, batchSize: config.BatchSize,
		maxBatchesPerRun: config.MaxBatchesPerRun,
		now:              now, logger: logger,
	}, nil
}

// Run performs an immediate scan so a manager restart does not add a full
// interval to deadline handling, then continues at a bounded cadence.
func (c *SandboxTTLController) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.logger.Info("Starting sandbox TTL controller",
		zap.Duration("interval", c.interval),
		zap.Int("batchSize", c.batchSize),
		zap.Int("maxBatchesPerRun", c.maxBatchesPerRun),
	)
	if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Error("Initial sandbox TTL scan failed", zap.Error(err))
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Sandbox TTL controller stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Error("Sandbox TTL scan failed", zap.Error(err))
			}
		}
	}
}

func (c *SandboxTTLController) runOnce(ctx context.Context) error {
	now := c.now().UTC()
	seen := make(map[string]struct{})
	for batch := 0; batch < c.maxBatchesPerRun; batch++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidates, err := c.lister.ListSandboxExpirationCandidates(
			ctx, now, c.batchSize,
		)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}
		unseen := 0
		for _, candidate := range candidates {
			sandboxID := strings.TrimSpace(candidate.SandboxID)
			if sandboxID == "" {
				continue
			}
			if _, duplicate := seen[sandboxID]; duplicate {
				continue
			}
			seen[sandboxID] = struct{}{}
			unseen++
			candidate.SandboxID = sandboxID
			if err := c.processCandidate(ctx, now, candidate); err != nil {
				c.logger.Warn("Sandbox expiration request failed",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
			}
		}
		if len(candidates) < c.batchSize || unseen == 0 {
			return nil
		}
	}
	return nil
}

func (c *SandboxTTLController) processCandidate(
	ctx context.Context,
	now time.Time,
	candidate sandboxstore.SandboxExpirationCandidate,
) error {
	if !candidate.HardExpiresAt.IsZero() && !candidate.HardExpiresAt.After(now) {
		return c.terminator.TerminateHardExpiredSandbox(ctx, candidate.SandboxID)
	}
	if candidate.DesiredState != sandboxstore.SandboxDesiredStateActive ||
		candidate.ExpiresAt.IsZero() || candidate.ExpiresAt.After(now) {
		return nil
	}
	if err := c.pauser.PauseSandboxByID(ctx, candidate.SandboxID); err != nil {
		if errors.Is(err, sandboxstore.ErrNomadSandboxHardTTLExpired) {
			return c.terminator.TerminateHardExpiredSandbox(ctx, candidate.SandboxID)
		}
		return err
	}
	return nil
}
