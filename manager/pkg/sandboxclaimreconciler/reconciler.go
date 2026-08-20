// Package sandboxclaimreconciler cleans logical Nomad claims that never
// reached a committed command-ready runtime binding.
package sandboxclaimreconciler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

const (
	DefaultLimit    = 1_000
	DefaultInterval = time.Second
)

// Store is the region-authoritative logical and physical cleanup boundary.
// Physical teardown remains owned by the independent runtime-slot terminal
// reconciler; this worker deletes the sandbox only after that slot is terminal.
type Store interface {
	ListSandboxRuntimeClaimsForCleanup(context.Context, int) ([]sandboxstore.SandboxRuntimeClaim, error)
	FenceSandboxRuntimeClaimForCleanup(context.Context, string, string, string) (*sandboxstore.SandboxClaimCleanupCandidate, error)
	MarkSandboxDeleted(context.Context, string, time.Time) error
	MarkSandboxRuntimeClaimCleaned(context.Context, string, string) error
}

type Config struct {
	Store    Store
	Limit    int
	Interval time.Duration
	Now      func() time.Time
}

// Result summarizes one bounded active-active cleanup pass.
type Result struct {
	Scanned int
	Fenced  int
	Pending int
	Cleaned int
	Skipped int
	Failed  int
}

// Worker uses PostgreSQL row locks and idempotent transitions, so every
// manager replica may run it without leader election.
type Worker struct {
	store    Store
	limit    int
	interval time.Duration
	now      func() time.Time
}

func New(config Config) (*Worker, error) {
	if config.Store == nil {
		return nil, fmt.Errorf("sandbox claim cleanup store is required")
	}
	if config.Limit == 0 {
		config.Limit = DefaultLimit
	}
	if config.Limit < 1 || config.Limit > sandboxstore.MaxSandboxRuntimeClaimCleanupLimit {
		return nil, fmt.Errorf("sandbox claim cleanup limit must be between 1 and %d",
			sandboxstore.MaxSandboxRuntimeClaimCleanupLimit)
	}
	if config.Interval == 0 {
		config.Interval = DefaultInterval
	}
	if config.Interval < 100*time.Millisecond || config.Interval > time.Hour {
		return nil, fmt.Errorf("sandbox claim cleanup interval must be between 100ms and 1h")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Worker{store: config.Store, limit: config.Limit, interval: config.Interval, now: config.Now}, nil
}

func (w *Worker) RunOnce(ctx context.Context) (Result, error) {
	claims, err := w.store.ListSandboxRuntimeClaimsForCleanup(ctx, w.limit)
	if err != nil {
		return Result{}, fmt.Errorf("list abandoned sandbox claims: %w", err)
	}
	result := Result{Scanned: len(claims)}
	var resultErr error
	for index := range claims {
		claim := claims[index]
		candidate, err := w.store.FenceSandboxRuntimeClaimForCleanup(
			ctx, claim.SandboxID, claim.OperationID, "claim lease expired before commit",
		)
		if err != nil {
			result.Failed++
			resultErr = errors.Join(resultErr, fmt.Errorf("fence sandbox claim %s: %w", claim.SandboxID, err))
			continue
		}
		if candidate == nil {
			result.Skipped++
			continue
		}
		result.Fenced++
		if candidate.SlotID != "" && candidate.SlotState != sandboxstore.RuntimeSlotStateTerminal {
			result.Pending++
			continue
		}
		if err := w.store.MarkSandboxDeleted(ctx, claim.SandboxID, w.now().UTC()); err != nil {
			result.Failed++
			resultErr = errors.Join(resultErr, fmt.Errorf("delete abandoned sandbox %s: %w", claim.SandboxID, err))
			continue
		}
		if err := w.store.MarkSandboxRuntimeClaimCleaned(ctx, claim.SandboxID, claim.OperationID); err != nil {
			result.Failed++
			resultErr = errors.Join(resultErr, fmt.Errorf("complete sandbox claim cleanup %s: %w", claim.SandboxID, err))
			continue
		}
		result.Cleaned++
	}
	return result, resultErr
}

func (w *Worker) Run(ctx context.Context, report func(Result, error)) {
	for {
		result, err := w.RunOnce(ctx)
		if report != nil {
			report(result, err)
		}
		timer := time.NewTimer(w.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
