package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

// Controlled node receipts isolate database recovery behavior. Authentication
// and actual CPU collection have separate node-channel and Linux tests.
type cpuWorkNode struct {
	mu       sync.Mutex
	t        *testing.T
	f        *nomadPauseStoreFixture
	calls    map[bool]int
	failRole *bool
	invalid  bool
}

func (n *cpuWorkNode) PreflightMigrationCPU(ctx context.Context, request protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	source := request.IsSource()
	n.calls[source]++
	var payload, receipt []byte
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT cpu_preflight_request,cpu_preflight_source FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.Source.OperationID).Scan(&payload, &receipt))
	var stored protocol.MigrationCPUPreflightRequest
	require.NoError(n.t, json.Unmarshal(payload, &stored), "source command must precede any dispatch")
	if !source {
		var committed protocol.MigrationCPUPreflight
		require.NoError(n.t, json.Unmarshal(receipt, &committed), "target requires committed source CPU history")
		stored.Target, stored.Launch = stored.Destination, &committed.Launch
	}
	require.Equal(n.t, stored, request)
	if n.failRole != nil && *n.failRole == source {
		n.failRole = nil
		return nil, errors.New("lost CPU node response")
	}
	result := migrationCPUStoreResult(n.t, n.f, request)
	if n.invalid {
		result.RequestDigest = "invalid"
	}
	return &result, nil
}

type cpuWorkStore struct {
	*PGSandboxStore
	role     bool
	boundary string
}

func (s *cpuWorkStore) CommitNomadSandboxMigrationCPUPreflight(ctx context.Context, a runtimecontrol.MigrationAssignment, r protocol.MigrationCPUPreflightRequest, p protocol.MigrationCPUPreflight) error {
	inject := s.boundary != "" && r.IsSource() == s.role
	boundary := s.boundary
	if inject {
		s.boundary = ""
	}
	if inject && boundary == "before" {
		return errors.New("before CPU receipt commit")
	}
	if err := s.PGSandboxStore.CommitNomadSandboxMigrationCPUPreflight(ctx, a, r, p); err != nil {
		return err
	}
	if inject && boundary == "after" {
		return errors.New("lost CPU receipt commit response")
	}
	return nil
}

func TestNomadMigrationCPUWorkerRecoversLostResponsesIntegration(t *testing.T) {
	for _, source := range []bool{true, false} {
		role := "target"
		if source {
			role = "source"
		}
		for _, boundary := range []string{"node", "before", "after"} {
			t.Run(role+"-"+boundary, func(t *testing.T) {
				f, a := migrationStoreFixture(t, "cpu-worker-"+role+"-"+boundary)
				migrationReadyTarget(t, f, a.OperationID, "b")
				_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
				require.NoError(t, err)
				n := &cpuWorkNode{t: t, f: f, calls: map[bool]int{}}
				store := &cpuWorkStore{PGSandboxStore: f.store, role: source, boundary: boundary}
				if boundary == "node" {
					n.failRole = &source
					store.boundary = ""
				}
				var deadline time.Time
				failures := 0
				for i := 0; i < 5; i++ {
					// Neither an in-memory cursor nor the previous manager is
					// needed to recover the original command and deadline.
					store.PGSandboxStore = NewPGSandboxStore(f.pool)
					c, err := nomadmigration.NewCPUPreflight(store, n)
					require.NoError(t, err)
					_, err = c.RunOnce(f.ctx)
					if err != nil {
						failures++
					}
					var observed time.Time
					require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT cpu_preflight_requested_at FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&observed))
					if i == 0 {
						deadline = observed
					} else {
						require.Equal(t, deadline, observed)
					}
				}
				require.Equal(t, 1, failures)
				work, err := f.store.GetNomadMigrationCPUPreflightWork(f.ctx, a.OperationID)
				require.NoError(t, err)
				require.Nil(t, work, "completed eligibility leaves this queue")
				for _, isSource := range []bool{true, false} {
					expected := 1
					if isSource == source && boundary != "after" {
						expected++
					}
					require.Equal(t, expected, n.calls[isSource], "committed receipts suppress repeat probes")
				}
				var phase string
				var preparation, capture bool
				var generation int64
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT l.phase,m.preparation_request IS NOT NULL,m.capture_request IS NOT NULL,s.runtime_generation FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id WHERE m.operation_id=$1`, a.OperationID).Scan(&phase, &preparation, &capture, &generation))
				require.Equal(t, "preparing", phase)
				require.False(t, preparation)
				require.False(t, capture)
				require.EqualValues(t, 1, generation)
				retainMigrationStagingFixture(t, f, a)
				_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, migrationSourcePolicy(f.sandboxID, a.Target.TeamID))
				require.NoError(t, err, "both CPU and staging receipts satisfy preparation")
			})
		}
	}
}

func TestNomadMigrationCPUWorkerConcurrentReplicasIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "cpu-worker-concurrent")
	migrationReadyTarget(t, f, a.OperationID, "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	n := &cpuWorkNode{t: t, f: f, calls: map[bool]int{}}
	for pass := 0; pass < 2; pass++ {
		var wg sync.WaitGroup
		results := make([]error, 4)
		for i := range results {
			wg.Go(func() {
				c, e := nomadmigration.NewCPUPreflight(NewPGSandboxStore(f.pool), n)
				if e == nil {
					_, e = c.RunOnce(f.ctx)
				}
				results[i] = e
			})
		}
		wg.Wait()
		for _, e := range results {
			require.NoError(t, e)
		}
	}
	ids, err := f.store.ListNomadMigrationCPUPreflights(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.GreaterOrEqual(t, n.calls[true], 1)
	require.GreaterOrEqual(t, n.calls[false], 1)
}

func TestNomadMigrationCPUWorkerRejectsInvalidAndExpiredWorkIntegration(t *testing.T) {
	for _, reason := range []string{"invalid-receipt", "cpu-expired", "reservation-expired", "aborted", "target-fenced"} {
		t.Run(reason, func(t *testing.T) {
			f, a := migrationStoreFixture(t, "cpu-worker-"+reason)
			target := migrationReadyTarget(t, f, a.OperationID, "b")
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
			require.NoError(t, err)
			n := &cpuWorkNode{t: t, f: f, calls: map[bool]int{}, invalid: reason == "invalid-receipt"}
			switch reason {
			case "cpu-expired":
				_, err = f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
				require.NoError(t, err)
				ageMigrationCPUPreflight(t, f, a.OperationID)
			case "reservation-expired":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET created_at=NOW()-INTERVAL '3 minutes' WHERE txn_id=$1`, a.OperationID)
				require.NoError(t, err)
			case "aborted":
				require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, a.OperationID, "test abort before probe"))
			case "target-fenced":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 minute' WHERE slot_id=$1`, target.SlotID)
				require.NoError(t, err)
			}
			c, err := nomadmigration.NewCPUPreflight(f.store, n)
			require.NoError(t, err)
			result, err := c.RunOnce(f.ctx)
			if reason == "invalid-receipt" || reason == "target-fenced" {
				require.Error(t, err)
				require.Equal(t, 1, result.Failed)
			} else {
				require.NoError(t, err)
				require.Zero(t, result.Candidates)
			}
			if reason != "invalid-receipt" {
				require.Empty(t, n.calls)
			}
			var committed bool
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT cpu_preflight_source IS NOT NULL OR cpu_preflight_destination IS NOT NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&committed))
			require.False(t, committed)
		})
	}
}

func TestNomadMigrationReservationAssignmentIsImmutableIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "cpu-assignment")
	migrationReadyTarget(t, f, a.OperationID, "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	work, err := NewPGSandboxStore(f.pool).GetNomadMigrationCPUPreflightWork(f.ctx, a.OperationID)
	require.NoError(t, err)
	require.Equal(t, a, work.Assignment)
	for _, value := range []any{nil, []byte(`{}`)} {
		_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET assignment_request=$2 WHERE operation_id=$1`, a.OperationID, value)
		require.Error(t, err)
	}
	// Simulate a pre-upgrade reservation without rewriting its digest. Only
	// this isolated test bypasses the immutable column trigger.
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_migrations DISABLE TRIGGER runtime_migration_assignment_guard`)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET assignment_request=NULL WHERE operation_id=$1`, a.OperationID)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_migrations ENABLE TRIGGER runtime_migration_assignment_guard`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(f.ctx))
	work, err = f.store.GetNomadMigrationCPUPreflightWork(f.ctx, a.OperationID)
	require.NoError(t, err)
	require.Nil(t, work)
	payload, err := json.Marshal(a)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET assignment_request=$2 WHERE operation_id=$1`, a.OperationID, payload)
	require.Error(t, err, "legacy assignment cannot be invented after reservation")
	require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, a.OperationID, "legacy reservation cleanup"))
}

type cpuWorkAbortAfterRead struct {
	*PGSandboxStore
}

func (s cpuWorkAbortAfterRead) GetNomadMigrationCPUPreflightWork(ctx context.Context, id string) (*nomadmigration.CPUPreflightWork, error) {
	work, err := s.PGSandboxStore.GetNomadMigrationCPUPreflightWork(ctx, id)
	if err != nil || work == nil {
		return work, err
	}
	if err := s.AbortNomadSandboxMigrationReservation(ctx, work.Assignment.Target.SandboxID, id, "abort after CPU work projection"); err != nil {
		return nil, err
	}
	return work, nil
}

func TestNomadMigrationCPUWorkerStaleProjectionCannotDispatchIntegration(t *testing.T) {
	for _, sourceCommitted := range []bool{false, true} {
		name := "source"
		if sourceCommitted {
			name = "target"
		}
		t.Run(name, func(t *testing.T) {
			f, a := migrationStoreFixture(t, "cpu-worker-stale-"+name)
			migrationReadyTarget(t, f, a.OperationID, "b")
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
			require.NoError(t, err)
			if sourceCommitted {
				r, err := f.store.AuthorizeNomadSandboxMigrationCPUPreflight(f.ctx, a)
				require.NoError(t, err)
				require.NoError(t, f.store.CommitNomadSandboxMigrationCPUPreflight(f.ctx, a, *r, migrationCPUStoreResult(t, f, *r)))
			}
			n := &cpuWorkNode{t: t, f: f, calls: map[bool]int{}}
			c, err := nomadmigration.NewCPUPreflight(cpuWorkAbortAfterRead{f.store}, n)
			require.NoError(t, err)
			result, err := c.RunOnce(f.ctx)
			require.Error(t, err)
			require.Equal(t, 1, result.Failed)
			require.Empty(t, n.calls, "a previously eligible read is never execution authority")
		})
	}
}
