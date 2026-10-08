package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func batchTestRequest(id, team string) *ReserveSandboxClaimRequest {
	return &ReserveSandboxClaimRequest{Record: rootFSTestSandboxRecord(id, team),
		OperationID: "operation-" + id, LeaseTTL: 15 * time.Second}
}

// Observe actual transaction identities at insertion, rather than asserting
// that requests happened to enter the in-memory batching helper.
func TestReserveSandboxClaimBurstSharesDurableTransactionsIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `
		CREATE TABLE manager.claim_commit_audit (sandbox_id text, transaction_id bigint);
		CREATE FUNCTION manager.audit_claim_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			INSERT INTO manager.claim_commit_audit VALUES (NEW.sandbox_id, txid_current());
			RETURN NEW;
		END $$;
		CREATE TRIGGER audit_claim_commit AFTER INSERT ON manager.sandbox_runtime_claims
		FOR EACH ROW EXECUTE FUNCTION manager.audit_claim_commit();
	`)
	require.NoError(t, err)
	store := NewPGSandboxStore(pool)
	blocker, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	require.NoError(t, lockActiveSandboxQuotaTeam(ctx, blocker, "burst-team"))
	const attempts = 32
	start := make(chan struct{})
	results := make(chan error, attempts)
	var workers sync.WaitGroup
	defer workers.Wait()
	for i := range attempts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			r := batchTestRequest(fmt.Sprintf("burst-%d", i), "burst-team")
			r.ActiveSandboxLimit = int64Pointer(attempts)
			reserved, err := store.ReserveSandboxClaim(ctx, r)
			if err == nil {
				var durable int
				err = pool.QueryRow(ctx, `SELECT count(*) FROM manager.sandbox_runtime_claims WHERE sandbox_id=$1`, reserved.ID).Scan(&durable)
				if err == nil && durable != 1 {
					err = fmt.Errorf("returned an uncommitted reservation")
				}
			}
			results <- err
		}()
	}
	close(start)
	// A different PostgreSQL session holds admission, allowing the whole
	// burst to arrive before any group can complete.
	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'`).Scan(&waiting)
		return err == nil && waiting > 0
	}, time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return len(results) != 0 }, 100*time.Millisecond, 5*time.Millisecond)
	require.NoError(t, blocker.Rollback(ctx))
	for range attempts {
		require.NoError(t, <-results)
	}
	var count, commits int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT transaction_id) FROM manager.claim_commit_audit`).Scan(&count, &commits))
	require.Equal(t, attempts, count)
	require.LessOrEqual(t, commits, 4, "a burst must share durable commits instead of serializing 32 WAL flushes")
}

func TestReserveSandboxClaimBatchPreservesQuotaAcrossManagersIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	stores := []*PGSandboxStore{NewPGSandboxStore(pool), NewPGSandboxStore(pool)}
	const attempts = 40
	start := make(chan struct{})
	results := make(chan error, attempts)
	for i := range attempts {
		go func() {
			<-start
			r := batchTestRequest(fmt.Sprintf("cross-manager-%d", i), "cross-manager-team")
			r.ActiveSandboxLimit = int64Pointer(7)
			_, err := stores[i%2].ReserveSandboxClaim(t.Context(), r)
			results <- err
		}()
	}
	close(start)
	accepted := 0
	for range attempts {
		err := <-results
		if err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, ErrActiveSandboxQuotaExceeded)
		}
	}
	require.Equal(t, 7, accepted)
	count, err := stores[0].CountActiveSandboxes(t.Context(), "cross-manager-team")
	require.NoError(t, err)
	require.Equal(t, int64(7), count)
}

func TestReserveSandboxClaimBatchIsolatesConflictsAndInvalidPeersIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	owner := batchTestRequest("operation-owner", "another-team")
	_, err := store.ReserveSandboxClaim(t.Context(), owner)
	require.NoError(t, err)
	requests := []*ReserveSandboxClaimRequest{
		batchTestRequest("good-1", "mixed-team"), batchTestRequest("conflict", "mixed-team"),
		batchTestRequest("good-2", "mixed-team"), batchTestRequest("invalid", "mixed-team"),
	}
	requests[1].OperationID = "  " + owner.OperationID + "  "
	requests[3].Record.ResourceMemoryMiB = 0
	start := make(chan struct{})
	type result struct {
		index int
		err   error
	}
	results := make(chan result, len(requests))
	for i, r := range requests {
		go func() {
			<-start
			_, err := store.ReserveSandboxClaim(t.Context(), r)
			results <- result{i, err}
		}()
	}
	close(start)
	for range requests {
		r := <-results
		switch r.index {
		case 0, 2:
			require.NoError(t, r.err)
		case 1:
			require.ErrorIs(t, r.err, ErrSandboxClaimReservationConflict)
		case 3:
			require.ErrorContains(t, r.err, "must be positive")
		}
	}
	count, err := store.CountActiveSandboxes(t.Context(), "mixed-team")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	for _, i := range []int{1, 3} {
		record, err := store.GetSandbox(t.Context(), requests[i].Record.ID)
		require.NoError(t, err)
		require.Nil(t, record)
	}
}

func TestReserveSandboxClaimBatchCanceledPeerDoesNotAbortLivePeerIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	blocker, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	require.NoError(t, lockActiveSandboxQuotaTeam(ctx, blocker, "cancel-team"))
	abandoned, abandon := context.WithCancel(ctx)
	defer abandon()
	calls := []*claimReservationCall{
		{ctx: abandoned, request: batchTestRequest("canceled", "cancel-team")},
		{ctx: ctx, request: batchTestRequest("live", "cancel-team")},
	}
	type outcome struct {
		records []claimReservationResult
		err     error
	}
	done := make(chan outcome, 1)
	go func() { r, err := store.reserveFreshSandboxClaimBatch(calls); done <- outcome{r, err} }()
	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'`).Scan(&waiting)
		return err == nil && waiting > 0
	}, time.Second, 5*time.Millisecond)
	abandon()
	require.NoError(t, blocker.Rollback(ctx))
	result := <-done
	require.NoError(t, result.err)
	require.ErrorIs(t, result.records[0].err, context.Canceled)
	require.NoError(t, result.records[1].err)
	require.Equal(t, "live", result.records[1].record.ID)
	count, err := store.CountActiveSandboxes(ctx, "cancel-team")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestReserveSandboxClaimBatchAllCanceledReleasesConnectionIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	blocker, err := pool.BeginTx(t.Context(), pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	require.NoError(t, lockActiveSandboxQuotaTeam(t.Context(), blocker, "abandoned-team"))
	done := make(chan error, 1)
	go func() {
		_, err := store.reserveFreshSandboxClaimBatch([]*claimReservationCall{
			{ctx: ctx, request: batchTestRequest("abandoned-1", "abandoned-team")},
			{ctx: ctx, request: batchTestRequest("abandoned-2", "abandoned-team")},
		})
		done <- err
	}()
	require.Eventually(t, func() bool { return pool.Stat().AcquiredConns() == 2 }, time.Second, 5*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.True(t, errors.Is(err, context.Canceled), "%v", err)
	case <-time.After(time.Second):
		t.Fatal("abandoned group retained its quota lock waiter")
	}
	require.Eventually(t, func() bool { return pool.Stat().AcquiredConns() == 1 }, time.Second, 5*time.Millisecond)
}
