package sandboxstore

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationSourcePolicy(sandbox, team string) string {
	payload, _ := json.Marshal(map[string]string{"version": "v1", "sandboxId": sandbox, "teamId": team, "mode": "allow-all"})
	return string(payload)
}

func migrationStoreFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, runtimecontrol.MigrationAssignment) {
	t.Helper()
	f := newNomadPauseStoreFixture(t, "migration-"+suffix)
	source := runtimecontrol.Assignment{SandboxID: f.sandboxID, TeamID: "team-slot", RuntimeGeneration: 1, SecurityClass: "standard"}
	revision, err := source.Revision()
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_runtime_assignment_revision=$2,procd_instance_id=$3,runsc_container_id=$4 WHERE slot_id=$1`, f.slotID, revision, uuid.NewString(), protocol.NomadRunscContainerID(f.slotID))
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_network_policy_digest=$2 WHERE slot_id=$1`, f.slotID, protocol.NetworkPolicyDigest(migrationSourcePolicy(f.sandboxID, source.TeamID)))
	require.NoError(t, err)
	source.RuntimeGeneration = 2
	return f, runtimecontrol.MigrationAssignment{OperationID: "migrate-" + suffix, SourceGeneration: 1, SourceRevision: revision, Target: source}
}

func migrationReadyTarget(t *testing.T, f *nomadPauseStoreFixture, name, node string) *RegisterRuntimeSlotRequest {
	t.Helper()
	registration := runtimeSlotTestRegistration("migration-slot-"+name, "migration-allocation-"+name)
	registration.NodeID = "nomad-node-" + node
	registration.NodeUID = "node-" + node
	registration.NodeBootID = "boot-" + node
	_, err := registerRuntimeSlotWithTestCapacity(t, f.ctx, f.store, registration)
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0xd3}, 32)
	_, err = f.store.ReportRuntimeSlotReady(f.ctx, &ReportRuntimeSlotReadyRequest{SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID, RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof, HeartbeatTTL: time.Minute})
	require.NoError(t, err)
	return registration
}

func preparedMigrationResponse(t *testing.T, request procdapi.RuntimeMigrationRequest) procdapi.RuntimeMigrationResponse {
	t.Helper()
	digest, err := request.Digest()
	require.NoError(t, err)
	return procdapi.RuntimeMigrationResponse{InstanceID: request.InstanceID, RequestDigest: digest,
		RuntimeGeneration: request.Assignment.SourceGeneration, State: "prepared"}
}

func TestNomadMigrationSourceAuthoritySurvivesRestartAndExpiryIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "source-authority")
	migrationReadyTarget(t, f, "source-authority", "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, assignment)
	prepare, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.NoError(t, err)
	require.Equal(t, reservation.SourceProcdInstanceID, prepare.InstanceID)
	require.Equal(t, reservation.Lifecycle.Epoch, prepare.LifecycleEpoch)
	require.Equal(t, procdapi.MigrationPrepare, prepare.Action)
	restarted := NewPGSandboxStore(f.pool)
	observed, err := restarted.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.NoError(t, err)
	require.Equal(t, prepare, observed)
	// Lost prepare responses must not become abandoned capacity reservations.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET created_at=NOW()-INTERVAL '1 hour' WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	count, err := restarted.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.ErrorIs(t, restarted.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, assignment.OperationID, "unsafe abort"), ErrNomadSandboxMigrationConflict)
	capture, err := restarted.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
	require.NoError(t, err)
	require.Equal(t, reservation.SourceSlot.ID, capture.Target.SlotID)
	require.Equal(t, reservation.SourceSlot.NodeUID, capture.Target.NodeUID)
	require.Equal(t, reservation.SourceSlot.NodeBootID, capture.Target.NodeBootID)
	require.Equal(t, assignment.SourceRevision, capture.AssignmentRevision)
	require.NoError(t, capture.Validate())
	// Recovery returns the original command even if a later observation changes.
	// Reconstructing it with this socket would authorize a different endpoint.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint='unix:///run/replacement.sock' WHERE slot_id=$1`, f.slotID)
	require.NoError(t, err)
	again, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
	require.NoError(t, err)
	require.Equal(t, capture, again)
	_, err = restarted.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "capture cannot reopen preparation")
	count, err = restarted.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	var active int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
	require.Equal(t, 2, active)
	target, err := f.store.GetRuntimeSlot(f.ctx, reservation.TargetSlot.ID)
	require.NoError(t, err)
	require.Empty(t, target.WriterGrantID)
	require.Empty(t, target.ResourceLease.LeaseID)
	require.Empty(t, target.SandboxID, "source capture never authorizes destination execution")
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
	require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_request=NULL,capture_digest=NULL WHERE operation_id=$1`, assignment.OperationID)
	require.Error(t, err, "authorized commands cannot lose durable recovery evidence")
	// Even an incorrect generic recovery phase cannot erase dispatch history.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='preparing' WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	count, err = restarted.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.ErrorIs(t, restarted.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, assignment.OperationID, "lost dispatch history"), ErrNomadSandboxMigrationConflict)
}

func TestNomadMigrationCaptureRejectsUnpreparedOrChangedSourceIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "source-reject")
	migrationReadyTarget(t, f, "source-reject", "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	invented := procdapi.RuntimeMigrationRequest{Action: procdapi.MigrationPrepare, Assignment: assignment,
		InstanceID: reservation.SourceProcdInstanceID, LifecycleEpoch: reservation.Lifecycle.Epoch}
	_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, invented, preparedMigrationResponse(t, invented))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "a valid-looking ack needs prior committed preparation authority")
	retainMigrationEligibilityFixture(t, f, assignment)
	prepare, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.NoError(t, err)
	for _, mutate := range []func(*procdapi.RuntimeMigrationResponse){
		func(r *procdapi.RuntimeMigrationResponse) { r.InstanceID = uuid.NewString() },
		func(r *procdapi.RuntimeMigrationResponse) { r.RuntimeGeneration++ },
		func(r *procdapi.RuntimeMigrationResponse) { r.RequestDigest = strings.Repeat("a", 64) },
		func(r *procdapi.RuntimeMigrationResponse) { r.State = "ready" },
	} {
		response := preparedMigrationResponse(t, *prepare)
		mutate(&response)
		_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, response)
		require.Error(t, err)
	}
	changed := *prepare
	changed.LifecycleEpoch++
	_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, changed, preparedMigrationResponse(t, changed))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET procd_instance_id=$2 WHERE slot_id=$1`, f.slotID, uuid.NewString())
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	var phase string
	var captured bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT lifecycle.phase,migration.capture_request IS NOT NULL
		FROM manager.sandbox_lifecycle_txns lifecycle JOIN manager.sandbox_runtime_migrations migration ON migration.operation_id=lifecycle.txn_id
		WHERE lifecycle.txn_id=$1`, assignment.OperationID).Scan(&phase, &captured))
	require.Equal(t, SandboxLifecyclePhaseBarriered, phase)
	require.False(t, captured)
}

func TestNomadMigrationSourceAuthorityConcurrentRetriesIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "source-concurrent")
	migrationReadyTarget(t, f, "source-concurrent", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, assignment)
	const workers = 8
	preparations := make([]*procdapi.RuntimeMigrationRequest, workers)
	errors := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			preparations[i], errors[i] = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
		})
	}
	wg.Wait()
	for i := range workers {
		require.NoError(t, errors[i])
		require.Equal(t, preparations[0], preparations[i])
	}
	response := preparedMigrationResponse(t, *preparations[0])
	captures := make([]*protocol.MigrationCaptureRequest, workers)
	for i := range workers {
		wg.Go(func() {
			captures[i], errors[i] = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *preparations[i], response)
		})
	}
	wg.Wait()
	for i := range workers {
		require.NoError(t, errors[i])
		require.Equal(t, captures[0], captures[i])
	}
}

func TestNomadMigrationPreparationRejectsExpiredOrUnavailableReservationIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "source-expiry")
	target := migrationReadyTarget(t, f, "source-expiry", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, assignment)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, target.SlotID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()+INTERVAL '1 minute' WHERE slot_id=$1`, target.SlotID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET created_at=NOW()-INTERVAL '1 hour' WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	count, err := f.store.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, count, "failed authorization leaves an unused reservation recoverable")
}

func TestNomadMigrationCaptureRetryCannotOutliveSandboxIntegration(t *testing.T) {
	for _, change := range []struct{ name, sql string }{
		{"ttl", `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`},
		{"termination", `UPDATE manager.sandboxes SET desired_state='terminating' WHERE sandbox_id=$1`},
	} {
		t.Run(change.name, func(t *testing.T) {
			f, assignment := migrationStoreFixture(t, "source-"+change.name)
			migrationReadyTarget(t, f, "source-"+change.name, "b")
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
			require.NoError(t, err)
			retainMigrationEligibilityFixture(t, f, assignment)
			prepare, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
			require.NoError(t, err)
			response := preparedMigrationResponse(t, *prepare)
			_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, response)
			require.NoError(t, err)
			_, err = f.pool.Exec(f.ctx, change.sql, f.sandboxID)
			require.NoError(t, err)
			_, err = NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, response)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			var retained bool
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT capture_request IS NOT NULL
				FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&retained))
			require.True(t, retained, "termination still needs the exact source-custody evidence")
		})
	}
}

func TestNomadMigrationReservationKeepsOneExecutingSandboxIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "reservation")
	migrationReadyTarget(t, f, "same-node", "a")
	target := migrationReadyTarget(t, f, "destination", "b")
	_, err := f.store.EnsureRuntimeNodePoolState(f.ctx, "migration-pool", "cluster-a")
	require.NoError(t, err)
	before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	require.Equal(t, target.SlotID, reservation.TargetSlot.ID, "migration must exclude the source physical node")
	require.Equal(t, RuntimeSlotStateActive, reservation.SourceSlot.State)
	require.Equal(t, RuntimeSlotStateFastpathReady, reservation.TargetSlot.State)
	require.Empty(t, reservation.TargetSlot.SandboxID)
	require.Empty(t, reservation.TargetSlot.WriterGrantID)
	require.Empty(t, reservation.TargetSlot.ResourceLease.LeaseID, "capacity reservation must not become execution authorization")
	require.Equal(t, assignment.OperationID, reservation.TargetResourceLease.OperationID)
	require.Equal(t, reservation.SourceSlot.ResourceLease.CPUMillicores, reservation.TargetResourceLease.CPUMillicores)
	require.Equal(t, reservation.SourceSlot.ResourceLease.MemoryBytes, reservation.TargetResourceLease.MemoryBytes)
	require.Equal(t, SandboxLifecycleKindMigrate, reservation.Lifecycle.Kind)
	require.Equal(t, SandboxLifecycleSourceAuto, reservation.Lifecycle.Source)
	require.False(t, reservation.Lifecycle.Cancelable)
	after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, before.RuntimeID, after.RuntimeID)
	require.Equal(t, before.RuntimeGeneration, after.RuntimeGeneration)
	require.Equal(t, before.LifecycleEpoch+1, after.LifecycleEpoch)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
	require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID)
	snapshot, err := f.store.GetRuntimeNodePoolSnapshot(f.ctx, "migration-pool")
	require.NoError(t, err)
	require.Equal(t, 2, snapshot.ClusterActiveLeases)
	require.Equal(t, 1, snapshot.ClusterWorkloadSlots, "temporary migration capacity is not another executing user sandbox")
	require.Equal(t, 1, snapshot.ClusterReadySlots, "reserved destination is not ready claim inventory")
	nodes, err := f.store.ListRuntimeCarrierNodes(f.ctx, "cluster-a")
	require.NoError(t, err)
	for _, node := range nodes {
		if node.NodeID == target.NodeID {
			require.Zero(t, node.Ready)
			require.Empty(t, node.ReadyByCompatibility)
		}
	}
	retry, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	require.Equal(t, reservation.TargetResourceLease, retry.TargetResourceLease)
	require.Equal(t, reservation.Lifecycle.Epoch, retry.Lifecycle.Epoch)
	other := assignment
	other.OperationID = "another-migration"
	_, err = f.store.ReserveNomadSandboxMigration(f.ctx, other)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	other = assignment
	other.Target.EnvVars = map[string]string{"MUTATED": "true"}
	source := other.Target
	source.RuntimeGeneration = 1
	other.SourceRevision, err = source.Revision()
	require.NoError(t, err)
	_, err = f.store.ReserveNomadSandboxMigration(f.ctx, other)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
}

func TestNomadMigrationReservationRollsBackWithoutCapacityIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "no-capacity")
	migrationReadyTarget(t, f, "same-source", "a")
	before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.ErrorIs(t, err, ErrRuntimeSlotUnavailable)
	after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, before.LifecycleEpoch, after.LifecycleEpoch)
	active, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active)
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestNomadMigrationReservationIsExcludedFromNormalClaimsIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "normal-claims")
	target := migrationReadyTarget(t, f, "reserved-only", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	filesystem, generation := runtimeSlotTestGeneration(t, f.store, "another-sandbox", "another-claim")
	_, err = f.store.AcquireRuntimeSlot(f.ctx, &AcquireRuntimeSlotRequest{OperationID: "another-claim", ClaimID: "another-claim-id", SandboxID: "another-sandbox",
		FilesystemID: filesystem.ID, SourceGenerationID: generation.ID, CompatibilityDigest: target.CompatibilityDigest, ClusterID: target.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute, Resources: runtimeSlotTestResources()})
	require.ErrorIs(t, err, ErrRuntimeSlotUnavailable)
	_, err = f.store.HeartbeatRuntimeSlot(f.ctx, &HeartbeatRuntimeSlotRequest{SlotID: target.SlotID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, TTL: time.Minute})
	require.NoError(t, err, "a reserved warm carrier must continue heartbeating")
	_, err = f.store.GetRuntimeSlotBySandboxID(f.ctx, "another-sandbox")
	require.ErrorIs(t, err, ErrRuntimeSlotNotFound)
}

func TestNomadMigrationReservationConcurrentRetriesIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "concurrent")
	migrationReadyTarget(t, f, "concurrent-b", "b")
	migrationReadyTarget(t, f, "concurrent-c", "c")
	const workers = 8
	var wg sync.WaitGroup
	results := make(chan *NomadSandboxMigrationReservation, workers)
	errors := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			value, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
			results <- value
			errors <- err
		})
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var targetID string
	for result := range results {
		if targetID == "" {
			targetID = result.TargetSlot.ID
		}
		require.Equal(t, targetID, result.TargetSlot.ID)
	}
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestNomadMigrationUnusedReservationAbortIsExactIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "abort")
	target := migrationReadyTarget(t, f, "abort-target", "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	require.ErrorIs(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, "wrong-operation", "aborted"), ErrNomadSandboxMigrationConflict)
	for range 2 {
		require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, assignment.OperationID, "source remains running"))
	}
	var state string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT lease_state FROM manager.runtime_resource_leases WHERE lease_id=$1`, reservation.TargetResourceLease.LeaseID).Scan(&state))
	require.Equal(t, RuntimeResourceLeaseReleased, state)
	var retired bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT carrier_retired FROM manager.runtime_slots WHERE slot_id=$1`, target.SlotID).Scan(&retired))
	require.True(t, retired)
	_, err = f.store.HeartbeatRuntimeSlot(f.ctx, &HeartbeatRuntimeSlotRequest{SlotID: target.SlotID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, TTL: time.Minute})
	require.NoError(t, err)
	observed, err := f.store.GetRuntimeSlot(f.ctx, target.SlotID)
	require.NoError(t, err)
	require.False(t, observed.HeartbeatExpiresAt.After(observed.AuthorityObservedAt), "heartbeat must not revive retired reservation")
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateActive, source.State)
	require.Equal(t, RuntimeResourceLeaseActive, source.ResourceLeaseState)
	_, err = f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "aborted operation cannot be reused")
}

func TestNomadMigrationReservationAbortRejectsPreparationAuthorizationIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "authorized")
	migrationReadyTarget(t, f, "authorized-target", "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='barriered' WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	require.ErrorIs(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, assignment.OperationID, "unsafe rollback"), ErrNomadSandboxMigrationConflict)
	var state string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT lease_state FROM manager.runtime_resource_leases WHERE lease_id=$1`, reservation.TargetResourceLease.LeaseID).Scan(&state))
	require.Equal(t, RuntimeResourceLeaseActive, state)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET created_at=NOW()-INTERVAL '1 hour' WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	count, err := f.store.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Zero(t, count, "elapsed time cannot release authorized execution custody")
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT lease_state FROM manager.runtime_resource_leases WHERE lease_id=$1`, reservation.TargetResourceLease.LeaseID).Scan(&state))
	require.Equal(t, RuntimeResourceLeaseActive, state)
}

func TestNomadMigrationReservationRecoveryAfterManagerRestartIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "restart")
	target := migrationReadyTarget(t, f, "restart-target", "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	count, err := f.store.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Zero(t, count, "fresh reservations remain available for preparation")
	// No in-memory work queue or owning manager is needed after a restart.
	restarted := NewPGSandboxStore(f.pool)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET created_at=NOW()-INTERVAL '1 hour' WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	_, err = restarted.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "retry cannot extend an expired reservation")
	// A busy sandbox transaction is skipped, not allowed to stall terminal
	// cleanup for unrelated slots. Its intent remains due after lock release.
	owner, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = owner.Rollback(f.ctx) })
	_, err = lockNomadSandboxClaimRecord(f.ctx, owner, f.sandboxID)
	require.NoError(t, err)
	count, err = restarted.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, owner.Rollback(f.ctx))
	const workers = 8
	var wg sync.WaitGroup
	type result struct {
		count int
		err   error
	}
	results := make(chan result, workers)
	for range workers {
		wg.Go(func() {
			n, err := restarted.RecoverExpiredNomadMigrationReservations(f.ctx, 10)
			results <- result{n, err}
		})
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		count += result.count
	}
	require.Equal(t, 1, count, "concurrent managers retire one reservation exactly once")
	active, err := restarted.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active, "recovery must unblock the sandbox lifecycle")
	observed, err := restarted.GetRuntimeSlot(f.ctx, target.SlotID)
	require.NoError(t, err)
	require.False(t, observed.HeartbeatExpiresAt.After(observed.AuthorityObservedAt))
	var state, reason string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT lease.lease_state,lifecycle.error FROM manager.runtime_resource_leases lease
		JOIN manager.sandbox_lifecycle_txns lifecycle ON lifecycle.txn_id=lease.operation_id WHERE lease.lease_id=$1`, reservation.TargetResourceLease.LeaseID).Scan(&state, &reason))
	require.Equal(t, RuntimeResourceLeaseReleased, state)
	require.Equal(t, "migration_reservation_expired", reason)
	source, err := restarted.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateActive, source.State)
	require.Equal(t, RuntimeResourceLeaseActive, source.ResourceLeaseState)
	require.Equal(t, reservation.SourceWriterGrantID, source.WriterGrantID)
	for _, limit := range []int{0, MaxRuntimeSlotReconcileLimit + 1} {
		_, err := restarted.RecoverExpiredNomadMigrationReservations(f.ctx, limit)
		require.Error(t, err)
	}
}
