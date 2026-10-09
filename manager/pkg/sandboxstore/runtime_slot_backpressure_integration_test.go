package sandboxstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSlotLockWaitersLeaveConnectionsForCommandReadinessIntegration(t *testing.T) {
	authority := newSandboxStoreIntegrationPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	config := authority.Config().Copy()
	config.MaxConns, config.MinConns = 4, 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store := NewPGSandboxStore(pool)
	blocker, err := authority.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	const attempts = 8
	request := func(i int) *AcquireRuntimeSlotRequest {
		return &AcquireRuntimeSlotRequest{
			OperationID: fmt.Sprintf("blocked-operation-%d", i), ClaimID: fmt.Sprintf("claim-%d", i),
			SandboxID: fmt.Sprintf("sandbox-%d", i), FilesystemID: "filesystem", SourceGenerationID: "generation",
			CompatibilityDigest:       runtimeSlotTestRegistration("unused", "unused").CompatibilityDigest,
			RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32),
			ClaimTTL: time.Minute, Resources: runtimeSlotTestResources(),
		}
	}
	for i := range attempts {
		require.NoError(t, lockRuntimeSlotClaimOperation(ctx, blocker, request(i).OperationID))
	}
	var workers sync.WaitGroup
	defer func() {
		cancel()
		_ = blocker.Rollback(context.Background())
		workers.Wait()
	}()
	results := make(chan error, attempts)
	for i := range attempts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := store.AcquireRuntimeSlot(ctx, request(i))
			results <- err
		}()
	}
	require.Eventually(t, func() bool { return pool.Stat().AcquiredConns() >= 1 }, time.Second, 5*time.Millisecond)
	// Every database operation key is fenced externally. Only one local
	// acquisition should borrow a connection; readiness writes can still run.
	require.Never(t, func() bool { return pool.Stat().AcquiredConns() > 1 }, 200*time.Millisecond, 5*time.Millisecond)
	probeCtx, probeCancel := context.WithTimeout(ctx, time.Second)
	defer probeCancel()
	var marker int
	require.NoError(t, pool.QueryRow(probeCtx, "SELECT 1").Scan(&marker))
	require.Equal(t, 1, marker)
	cancel()
	for range attempts {
		require.ErrorIs(t, <-results, context.Canceled)
	}
	workers.Wait()
	require.NoError(t, blocker.Rollback(context.Background()))
	// Cancellation releases the permit. An unfenced new request reaches the
	// durable admission check and correctly rejects its missing sandbox.
	nextCtx, nextCancel := context.WithTimeout(t.Context(), time.Second)
	defer nextCancel()
	_, err = store.AcquireRuntimeSlot(nextCtx, request(attempts))
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
}
