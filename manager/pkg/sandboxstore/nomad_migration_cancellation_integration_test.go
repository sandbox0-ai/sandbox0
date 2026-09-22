package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

type cancellationProcd struct {
	f     *nomadPauseStoreFixture
	t     *testing.T
	fail  bool
	calls int
}

func (p *cancellationProcd) GenerateMigrationToken(r procdapi.RuntimeMigrationRequest) (string, error) {
	return r.Permission()
}
func (p *cancellationProcd) GenerateToken(string, string, string) (string, error) {
	return "", errors.New("cancellation must not probe readiness")
}
func (p *cancellationProcd) ProbeCommandReady(context.Context, string, string) (*procdapi.CommandReadyProbeResult, error) {
	return nil, errors.New("cancellation must not probe readiness")
}
func (p *cancellationProcd) MigrateRuntime(ctx context.Context, address string, r procdapi.RuntimeMigrationRequest, token string) (*procdapi.RuntimeMigrationResponse, error) {
	var payload []byte
	var captured bool
	require.NoError(p.t, p.f.pool.QueryRow(ctx, `SELECT preparation_cancel_request,capture_request IS NOT NULL
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, r.Assignment.OperationID).Scan(&payload, &captured))
	var c nomadmigration.PreparationCancellation
	require.NoError(p.t, json.Unmarshal(payload, &c))
	require.Equal(p.t, c.Address, address)
	require.Equal(p.t, c.Request, r)
	want, err := r.Permission()
	require.NoError(p.t, err)
	require.Equal(p.t, want, token)
	require.False(p.t, captured)
	p.calls++
	if p.fail {
		p.fail = false
		return nil, errors.New("lost source cancellation reply")
	}
	d, err := r.Digest()
	return &procdapi.RuntimeMigrationResponse{InstanceID: r.InstanceID, RequestDigest: d, RuntimeGeneration: r.Assignment.SourceGeneration, State: "ready"}, err
}

type cancellationStore struct {
	*PGSandboxStore
	boundary string
}

func (s *cancellationStore) CommitNomadSandboxMigrationPreparationCancellation(ctx context.Context, c nomadmigration.PreparationCancellation, r procdapi.RuntimeMigrationResponse) error {
	b := s.boundary
	s.boundary = ""
	if b == "before" {
		return errors.New("before cancellation receipt commit")
	}
	if err := s.PGSandboxStore.CommitNomadSandboxMigrationPreparationCancellation(ctx, c, r); err != nil {
		return err
	}
	if b == "after" {
		return errors.New("lost cancellation commit reply")
	}
	return nil
}

func migrationPreparedForCancellation(t *testing.T, name string) (*nomadPauseStoreFixture, runtimecontrol.MigrationAssignment, *procdapi.RuntimeMigrationRequest) {
	t.Helper()
	f, a := migrationStoreFixture(t, name)
	migrationReadyTarget(t, f, name, "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, a)
	p, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, migrationSourcePolicy(f.sandboxID, a.Target.TeamID))
	require.NoError(t, err)
	return f, a, p
}

func expirePreparedMigration(t *testing.T, f *nomadPauseStoreFixture, id string) {
	t.Helper()
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET created_at=NOW()-INTERVAL '3 minutes' WHERE txn_id=$1`, id)
	require.NoError(t, err)
}

func TestNomadMigrationCancellationWorkerRecoversLostRepliesIntegration(t *testing.T) {
	for _, boundary := range []string{"node", "before", "after"} {
		t.Run(boundary, func(t *testing.T) {
			f, a, prepare := migrationPreparedForCancellation(t, "cancel-recovery")
			ids, err := f.store.ListNomadMigrationPreparationCancellations(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids)
			command, err := f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.Nil(t, command, "healthy preparation is not canceled")
			expirePreparedMigration(t, f, a.OperationID)
			command, err = f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.NotNil(t, command)
			_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			release, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.Nil(t, release, "cancellation intent is not a completed source gate release")
			var active int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
			require.Equal(t, 2, active)
			// Retry uses the retained endpoint despite later slot observations.
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET procd_address='http://192.0.2.99:49983' WHERE slot_id=$1`, f.slotID)
			require.NoError(t, err)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating',hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
			require.NoError(t, err)
			node := &cancellationProcd{f: f, t: t, fail: boundary == "node"}
			store := &cancellationStore{PGSandboxStore: f.store, boundary: boundary}
			for i := range 3 {
				store.PGSandboxStore = NewPGSandboxStore(f.pool)
				worker, err := nomadmigration.NewPreparationCancellation(store, node, node)
				require.NoError(t, err)
				_, err = worker.RunOnce(f.ctx)
				if i == 0 {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
			expectedCalls := 2
			if boundary == "after" {
				expectedCalls = 1
			}
			require.Equal(t, expectedCalls, node.calls)
			var phase string
			var generation int64
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT l.phase,s.runtime_generation FROM manager.sandbox_lifecycle_txns l
                JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id WHERE l.txn_id=$1`, a.OperationID).Scan(&phase, &generation))
			require.Equal(t, SandboxLifecyclePhaseAborted, phase)
			require.EqualValues(t, 1, generation)
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
			require.Equal(t, 1, active, "only the unattached destination lease is released")
			filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
			require.NoError(t, err)
			require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
			require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID)
			staging := &stagingWorkNode{f: f, t: t, calls: map[string]int{}}
			for range 2 {
				worker, err := nomadmigration.NewStagingRelease(f.store, staging)
				require.NoError(t, err)
				_, err = worker.RunOnce(f.ctx)
				require.NoError(t, err)
			}
			ids, err = f.store.ListNomadMigrationStagingReleases(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids)
		})
	}
}

func TestNomadMigrationCancellationAndCaptureSerializeIntegration(t *testing.T) {
	for range 4 {
		f, a, prepare := migrationPreparedForCancellation(t, "cancel-race")
		expirePreparedMigration(t, f, a.OperationID)
		var wg sync.WaitGroup
		var captureErr, cancelErr error
		var canceled *nomadmigration.PreparationCancellation
		wg.Go(func() {
			_, captureErr = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
		})
		wg.Go(func() {
			canceled, cancelErr = f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, a.OperationID)
		})
		wg.Wait()
		require.NotEqual(t, captureErr == nil, cancelErr == nil, "exactly one authorization may commit")
		if captureErr == nil {
			require.Nil(t, canceled)
			require.ErrorIs(t, cancelErr, ErrNomadSandboxMigrationConflict)
		} else {
			require.NotNil(t, canceled)
			require.ErrorIs(t, captureErr, ErrNomadSandboxMigrationConflict)
		}
	}
}

func TestNomadMigrationCancellationRejectsChangedEvidenceIntegration(t *testing.T) {
	f, a, _ := migrationPreparedForCancellation(t, "cancel-evidence")
	expirePreparedMigration(t, f, a.OperationID)
	c, err := f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, a.OperationID)
	require.NoError(t, err)
	d, err := c.Request.Digest()
	require.NoError(t, err)
	receipt := procdapi.RuntimeMigrationResponse{InstanceID: c.Request.InstanceID, RequestDigest: d, RuntimeGeneration: 1, State: "ready"}
	wrong := receipt
	wrong.RuntimeGeneration++
	require.Error(t, f.store.CommitNomadSandboxMigrationPreparationCancellation(f.ctx, *c, wrong))
	changed := *c
	changed.Address = "http://192.0.2.100:49983"
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationPreparationCancellation(f.ctx, changed, receipt), ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_cancel_request=NULL,preparation_cancel_digest=NULL WHERE operation_id=$1`, a.OperationID)
	require.Error(t, err)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() {
			errs[i] = NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationPreparationCancellation(f.ctx, *c, receipt)
		})
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationPreparationCancellation(f.ctx, *c, receipt))
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_cancel_receipt=NULL WHERE operation_id=$1`, a.OperationID)
	require.Error(t, err)
}

func TestNomadMigrationCancellationRechecksFailureEligibilityIntegration(t *testing.T) {
	for _, reason := range []string{"cpu-expiry", "hard-ttl", "termination", "target-heartbeat", "target-retired"} {
		t.Run(reason, func(t *testing.T) {
			f, a, _ := migrationPreparedForCancellation(t, "cancel-due")
			var err error
			switch reason {
			case "cpu-expiry":
				ageMigrationCPUPreflight(t, f, a.OperationID)
			case "hard-ttl":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
			case "termination":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating' WHERE sandbox_id=$1`, f.sandboxID)
			case "target-heartbeat":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second'
                    WHERE slot_id=(SELECT target_slot_id FROM manager.sandbox_runtime_migrations WHERE operation_id=$1)`, a.OperationID)
			case "target-retired":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET carrier_retired=true
                    WHERE slot_id=(SELECT target_slot_id FROM manager.sandbox_runtime_migrations WHERE operation_id=$1)`, a.OperationID)
			}
			require.NoError(t, err)
			ids, err := f.store.ListNomadMigrationPreparationCancellations(f.ctx, "", 8)
			require.NoError(t, err)
			require.Equal(t, []string{a.OperationID}, ids)
			c, err := f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.NotNil(t, c)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_cancel_request=jsonb_set(preparation_cancel_request,'{address}','"http://192.0.2.8:49983"') WHERE operation_id=$1`, a.OperationID)
			require.Error(t, err, "cancellation address is immutable")
		})
	}
}
