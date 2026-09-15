package runtimeslotterminal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
)

type carrierRefiller interface {
	RefillFailedCarriers(context.Context, string, string) (int, string, error)
}

// terminalAndRefill keeps scheduling repair independent of regional slot
// discovery. Failures before registration have no PostgreSQL slot to scan.
// Terminal work always runs first; refill visits one cluster and at most eight
// failed allocations every 30 seconds, within a five-second operation budget.
type terminalAndRefill struct {
	terminal runtimeslotreconciler.Runner
	refiller carrierRefiller
	clusters []string
	cursors  map[string]string
	cluster  int
	nextRun  time.Time
	now      func() time.Time
}

func (r *terminalAndRefill) RunOnce(ctx context.Context) (runtimeslotreconciler.Result, error) {
	result, err := r.terminal.RunOnce(ctx)
	if ctx.Err() != nil || len(r.clusters) == 0 || r.now().Before(r.nextRun) {
		return result, err
	}
	r.nextRun = r.now().Add(30 * time.Second)
	cluster := r.clusters[r.cluster]
	r.cluster = (r.cluster + 1) % len(r.clusters)
	refillCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	count, next, refillErr := r.refiller.RefillFailedCarriers(refillCtx, cluster, r.cursors[cluster])
	result.RefillRequested = count
	if refillErr != nil {
		return result, errors.Join(err, fmt.Errorf("request failed carrier replacements: %w", refillErr))
	}
	r.cursors[cluster] = next
	return result, err
}
