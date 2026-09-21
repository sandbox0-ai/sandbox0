package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationSourceExecutionFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, runtimecontrol.MigrationAssignment) {
	t.Helper()
	f, a := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), suffix, "")
	migrationReadyTarget(t, f, suffix, "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	return f, a
}

func migrationExecutionSource(t *testing.T, pool *pgxpool.Pool, suffix, node string) (*nomadPauseStoreFixture, runtimecontrol.MigrationAssignment) {
	t.Helper()
	var a runtimecontrol.Assignment
	f := newNomadPauseStoreFixtureOnNode(t, "source-execution-"+suffix, pool, node, func(r *AcquireRuntimeSlotRequest) {
		a = runtimecontrol.Assignment{SandboxID: r.SandboxID, TeamID: "team-slot", RuntimeGeneration: 1, SecurityClass: "standard",
			EnvVars: map[string]string{"MODE": "original"}, EphemeralMounts: []runtimecontrol.EphemeralMount{{MountPath: "/cache", SizeBytes: 8 << 20}}}
		payload, err := json.Marshal(a)
		require.NoError(t, err)
		r.RuntimeAssignmentPayload = string(payload)
		r.RuntimeAssignmentRevision, err = a.Revision()
		require.NoError(t, err)
		r.NetworkPolicy = migrationSourcePolicy(a.SandboxID, a.TeamID)
		r.NetworkPolicyDigest = protocol.NetworkPolicyDigest(r.NetworkPolicy)
	})
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET procd_instance_id=$2,runsc_container_id=$3 WHERE slot_id=$1`, f.slotID, uuid.NewString(), protocol.NomadRunscContainerID(f.slotID))
	require.NoError(t, err)
	revision, err := a.Revision()
	require.NoError(t, err)
	a.RuntimeGeneration++
	assignment := runtimecontrol.MigrationAssignment{OperationID: "source-execution-" + suffix, SourceGeneration: 1, SourceRevision: revision, Target: a}
	return f, assignment
}

// This double checks durable commands at dispatch time in real PostgreSQL.
// Physical checkpoint idempotency remains covered by the driver/Linux tests.
type sourceExecutionNode struct {
	mu                         sync.Mutex
	t                          *testing.T
	f                          *nomadPauseStoreFixture
	prepareCalls, captureCalls int
	fail                       string
	badPrepare                 bool
	uncertain                  bool
	expire                     bool
}

func (n *sourceExecutionNode) GenerateMigrationToken(r procdapi.RuntimeMigrationRequest) (string, error) {
	return r.Permission()
}
func (n *sourceExecutionNode) GenerateToken(string, string, string) (string, error) {
	return "", errors.New("unexpected readiness token")
}
func (n *sourceExecutionNode) ProbeCommandReady(context.Context, string, string) (*procdapi.CommandReadyProbeResult, error) {
	return nil, errors.New("unexpected readiness probe")
}
func (n *sourceExecutionNode) MigrateRuntime(ctx context.Context, address string, r procdapi.RuntimeMigrationRequest, token string) (*procdapi.RuntimeMigrationResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.prepareCalls++
	var payload []byte
	var retained string
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT preparation_request,preparation_address FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, r.Assignment.OperationID).Scan(&payload, &retained))
	var command procdapi.RuntimeMigrationRequest
	require.NoError(n.t, json.Unmarshal(payload, &command))
	require.Equal(n.t, command, r)
	require.Equal(n.t, retained, address)
	require.Equal(n.t, "http://192.0.2.2:49983", address)
	permission, err := r.Permission()
	require.NoError(n.t, err)
	require.Equal(n.t, permission, token)
	if n.fail == "prepare-node" {
		n.fail = ""
		return nil, errors.New("lost prepared response")
	}
	if n.expire {
		ageMigrationCPUPreflight(n.t, n.f, r.Assignment.OperationID)
		n.expire = false
	}
	receipt := preparedMigrationResponse(n.t, r)
	if n.badPrepare {
		receipt.RuntimeGeneration++
	}
	return &receipt, nil
}
func (n *sourceExecutionNode) CaptureMigration(ctx context.Context, r protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.captureCalls++
	var payload []byte
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT capture_request FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, r.OperationID).Scan(&payload))
	var command protocol.MigrationCaptureRequest
	require.NoError(n.t, json.Unmarshal(payload, &command))
	require.Equal(n.t, command, r)
	if n.fail == "capture-node" {
		n.fail = ""
		return nil, errors.New("lost capture response")
	}
	digest, err := r.Digest()
	require.NoError(n.t, err)
	state := protocol.MigrationCaptureIntent
	if n.uncertain {
		state = protocol.MigrationCaptureUncertain
	}
	return &protocol.MigrationCapture{Request: r, RequestDigest: digest, State: state}, nil
}

type sourceExecutionStore struct {
	*PGSandboxStore
	boundary string
}

func (s *sourceExecutionStore) AuthorizeNomadSandboxMigrationPreparation(ctx context.Context, a runtimecontrol.MigrationAssignment, p string) (*procdapi.RuntimeMigrationRequest, error) {
	if s.boundary == "prepare-before" {
		s.boundary = ""
		return nil, errors.New("before prepare authorization")
	}
	r, err := s.PGSandboxStore.AuthorizeNomadSandboxMigrationPreparation(ctx, a, p)
	if err == nil && s.boundary == "prepare-after" {
		s.boundary = ""
		return nil, errors.New("lost prepare authorization reply")
	}
	return r, err
}
func (s *sourceExecutionStore) AuthorizeNomadSandboxMigrationCapture(ctx context.Context, r procdapi.RuntimeMigrationRequest, p procdapi.RuntimeMigrationResponse) (*protocol.MigrationCaptureRequest, error) {
	if s.boundary == "capture-before" {
		s.boundary = ""
		return nil, errors.New("before capture authorization")
	}
	c, err := s.PGSandboxStore.AuthorizeNomadSandboxMigrationCapture(ctx, r, p)
	if err == nil && s.boundary == "capture-after" {
		s.boundary = ""
		return nil, errors.New("lost capture authorization reply")
	}
	return c, err
}

func TestNomadMigrationSourceExecutionRecoversLostResponsesIntegration(t *testing.T) {
	for _, boundary := range []string{"prepare-before", "prepare-after", "prepare-node", "capture-before", "capture-after", "capture-node"} {
		t.Run(boundary, func(t *testing.T) {
			f, a := migrationSourceExecutionFixture(t, boundary)
			retainMigrationEligibilityFixture(t, f, a)
			n := &sourceExecutionNode{t: t, f: f, fail: boundary}
			s := &sourceExecutionStore{PGSandboxStore: f.store, boundary: boundary}
			failures := 0
			for range 6 {
				// Manager restart discards in-memory scheduling state between steps.
				s.PGSandboxStore = NewPGSandboxStore(f.pool)
				w, err := nomadmigration.NewSourceExecution(s, n, n, n)
				require.NoError(t, err)
				r, err := w.RunOnce(f.ctx)
				if err != nil {
					failures++
				} else {
					require.Equal(t, 1, r.Advanced)
				}
				if n.captureCalls > 0 && n.fail != "capture-node" {
					break
				}
			}
			require.Equal(t, 1, failures)
			require.Positive(t, n.captureCalls)
			if boundary == "capture-node" {
				w, err := nomadmigration.NewSourceExecution(s, n, n, n)
				require.NoError(t, err)
				_, err = w.RunOnce(f.ctx)
				require.NoError(t, err)
				require.Equal(t, 2, n.captureCalls)
			}
			work, err := f.store.GetNomadMigrationSourceExecution(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.NotNil(t, work.Capture)
			recovery, err := f.store.GetNomadMigrationSourceRecovery(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.Equal(t, *work.Capture, recovery.Capture)
			fs, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
			require.NoError(t, err)
			require.Equal(t, f.initialGenerationID, fs.HeadGenerationID)
			require.Equal(t, f.writerEpoch, fs.WriterEpoch)
			record, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, int64(1), record.RuntimeGeneration)
		})
	}
}

func TestNomadMigrationSourceExecutionGatesAndEndpointCustodyIntegration(t *testing.T) {
	f, a := migrationSourceExecutionFixture(t, "gates")
	ids, err := f.store.ListNomadMigrationSourceExecutions(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	retainMigrationCPUFixture(t, f, a)
	ids, err = f.store.ListNomadMigrationSourceExecutions(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	retainMigrationStagingFixture(t, f, a)
	n := &sourceExecutionNode{t: t, f: f}
	w, err := nomadmigration.NewSourceExecution(f.store, n, n, n)
	require.NoError(t, err)
	_, err = w.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Zero(t, n.prepareCalls, "authorization precedes dispatch")
	// An updated observation/config cannot change either the original endpoint
	// or exact launch input used by the automatic dispatcher.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET procd_address='http://192.0.2.99:49983' WHERE slot_id=$1`, f.slotID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET config='{"envVars":{"MODE":"changed"}}'::jsonb WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_address='http://192.0.2.99:49983' WHERE operation_id=$1`, a.OperationID)
	require.Error(t, err)
	_, err = w.RunOnce(f.ctx)
	require.NoError(t, err)
	work, err := f.store.GetNomadMigrationSourceExecution(f.ctx, a.OperationID)
	require.NoError(t, err)
	require.Equal(t, "original", work.Assignment.Target.EnvVars["MODE"])
	changed := *work.Capture
	changed.Target.ControlEndpoint = "unix:///run/replaced.sock"
	require.ErrorIs(t, f.store.AuthorizeNomadMigrationSourceCaptureDispatch(f.ctx, changed), ErrNomadSandboxMigrationConflict)
	ageMigrationCPUPreflight(t, f, a.OperationID)
	_, err = w.RunOnce(f.ctx)
	require.NoError(t, err, "authorized capture retry retains original authority")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = w.RunOnce(f.ctx)
	require.Error(t, err)
	require.Equal(t, 1, n.captureCalls, "stale queue cannot dispatch after termination")
}

func TestNomadMigrationSourceExecutionRefusesInvalidOrExpiredAcknowledgementIntegration(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "changed-receipt", true: "cpu-expired"}[expired], func(t *testing.T) {
			f, a := migrationSourceExecutionFixture(t, "bad-ack")
			retainMigrationEligibilityFixture(t, f, a)
			n := &sourceExecutionNode{t: t, f: f, badPrepare: !expired, expire: expired}
			w, err := nomadmigration.NewSourceExecution(f.store, n, n, n)
			require.NoError(t, err)
			_, err = w.RunOnce(f.ctx)
			require.NoError(t, err)
			_, err = w.RunOnce(f.ctx)
			require.Error(t, err)
			require.Zero(t, n.captureCalls)
			var captured bool
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT capture_request IS NOT NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&captured))
			require.False(t, captured)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET procd_address='http://192.0.2.99:49983' WHERE slot_id=$1`, f.slotID)
			require.NoError(t, err)
			expirePreparedMigration(t, f, a.OperationID)
			c, err := f.store.AuthorizeNomadSandboxMigrationPreparationCancellation(f.ctx, a.OperationID)
			require.NoError(t, err)
			require.NotNil(t, c)
			require.Equal(t, "http://192.0.2.2:49983", c.Address)
			_, err = w.RunOnce(f.ctx)
			require.NoError(t, err)
			require.Equal(t, 1, n.prepareCalls, "cancel authority removes source from dispatch queue")
		})
	}
}

func TestNomadMigrationSourceExecutionConcurrentManagersIntegration(t *testing.T) {
	f, a := migrationSourceExecutionFixture(t, "concurrent")
	retainMigrationEligibilityFixture(t, f, a)
	n := &sourceExecutionNode{t: t, f: f}
	for range 4 {
		var group sync.WaitGroup
		for range 4 {
			group.Add(1)
			go func() {
				defer group.Done()
				w, err := nomadmigration.NewSourceExecution(NewPGSandboxStore(f.pool), n, n, n)
				require.NoError(t, err)
				_, _ = w.RunOnce(f.ctx)
			}()
		}
		group.Wait()
	}
	require.Positive(t, n.captureCalls)
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 AND capture_request IS NOT NULL`, a.OperationID).Scan(&count))
	require.Equal(t, 1, count)
	n.uncertain = true
	w, err := nomadmigration.NewSourceExecution(f.store, n, n, n)
	require.NoError(t, err)
	_, err = w.RunOnce(f.ctx)
	require.ErrorContains(t, err, "requires reconciliation")
	var phase string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, a.OperationID).Scan(&phase))
	require.Equal(t, "publishing", phase)
}

func TestNomadMigrationSourceExecutionExcludesLegacyInputsIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "legacy-source-inputs")
	migrationReadyTarget(t, f, "legacy-source-inputs", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, a)
	ids, err := f.store.ListNomadMigrationSourceExecutions(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	work, err := f.store.GetNomadMigrationSourceExecution(f.ctx, a.OperationID)
	require.NoError(t, err)
	require.Nil(t, work)
	// Existing internal authority tests can explicitly prepare legacy fixtures;
	// automatic dispatch cannot invent either launch inputs or old endpoints.
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, migrationSourcePolicy(f.sandboxID, a.Target.TeamID))
	require.NoError(t, err)
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_migrations DISABLE TRIGGER runtime_migration_preparation_address_guard`)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_address=NULL WHERE operation_id=$1`, a.OperationID)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_migrations ENABLE TRIGGER runtime_migration_preparation_address_guard`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(f.ctx))
	ids, err = f.store.ListNomadMigrationSourceExecutions(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_address='http://192.0.2.2:49983' WHERE operation_id=$1`, a.OperationID)
	require.Error(t, err)
}
