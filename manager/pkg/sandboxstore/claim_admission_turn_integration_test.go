package sandboxstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestTeamQuotaWaitersLeaveDatabaseConnectionsForUnrelatedWork(t *testing.T) {
	authority := newSandboxStoreIntegrationPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	config := authority.Config().Copy()
	config.MaxConns, config.MinConns = 2, 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store := NewPGSandboxStore(pool)
	blocker, err := authority.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, lockActiveSandboxQuotaTeam(ctx, blocker, "blocked-team"))
	var workers sync.WaitGroup
	defer func() {
		cancel()
		_ = blocker.Rollback(context.Background())
		workers.Wait()
	}()
	const attempts = 16
	results := make(chan error, attempts)
	started := make(chan struct{}, attempts)
	for i := range attempts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			started <- struct{}{}
			_, err := store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
				Record:      rootFSTestSandboxRecord(fmt.Sprintf("queued-sandbox-%d", i), "blocked-team"),
				OperationID: fmt.Sprintf("queued-operation-%d", i), LeaseTTL: 15 * time.Second,
				ActiveSandboxLimit: int64Pointer(attempts),
			})
			results <- err
		}()
	}
	for range attempts {
		<-started
	}
	require.Eventually(t, func() bool { return pool.Stat().AcquiredConns() >= 1 }, time.Second, 5*time.Millisecond)
	// The quota lock is deliberately held by a different database session.
	// Waiting claims must not acquire both connections and starve a readiness
	// query or the writes that let already-admitted sandboxes become ready.
	require.Never(t, func() bool { return pool.Stat().AcquiredConns() > 1 }, 200*time.Millisecond, 5*time.Millisecond)
	probeCtx, probeCancel := context.WithTimeout(ctx, time.Second)
	defer probeCancel()
	var marker int
	require.NoError(t, pool.QueryRow(probeCtx, "SELECT 1").Scan(&marker))
	require.Equal(t, 1, marker)
	require.NoError(t, blocker.Rollback(ctx))
	for range attempts {
		require.NoError(t, <-results)
	}
}
