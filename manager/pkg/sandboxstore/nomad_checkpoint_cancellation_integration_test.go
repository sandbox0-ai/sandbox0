package sandboxstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type checkpointCancellationNode struct {
	*checkpointPauseWorkerNode
	lostCancel, lostRelease   bool
	unreachable, sourceAbsent bool
	cancelCalls, releaseCalls int
	retireCalls               int
}

func (n *checkpointCancellationNode) RetireTerminalCheckpointSource(ctx context.Context, operation string, target protocol.NodeChannelTarget, namespace string) error {
	_, _, err := n.checkpointPauseWorkerNode.ObserveCheckpointSource(ctx, target, namespace)
	if err != nil || operation != n.checkpoint.Lifecycle.ID {
		return ErrNomadCheckpointConflict
	}
	n.retireCalls++
	n.sourceAbsent = true
	return nil
}

func (n *checkpointCancellationNode) ObserveCheckpointSource(ctx context.Context, target protocol.NodeChannelTarget, namespace string) (bool, []byte, error) {
	_, _, err := n.checkpointPauseWorkerNode.ObserveCheckpointSource(ctx, target, namespace)
	if err != nil || !n.sourceAbsent {
		return false, nil, err
	}
	return true, bytes.Repeat([]byte{0x6a}, 32), nil
}

func (n *checkpointCancellationNode) CheckpointRuntime(ctx context.Context, address string, r procdapi.RuntimeCheckpointRequest, token string) (*procdapi.RuntimeCheckpointResponse, error) {
	require.Equal(n.t, procdapi.MigrationCancel, r.Action)
	require.Equal(n.t, n.checkpoint.Evidence.Address, address)
	digest, err := r.Digest()
	require.NoError(n.t, err)
	require.Equal(n.t, digest, token)
	var authorized, captured bool
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT evidence ? 'cancel_authorized',evidence ? 'capture_authorized'
        FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, r.Capture.OperationID).Scan(&authorized, &captured))
	require.True(n.t, authorized)
	require.False(n.t, captured)
	n.cancelCalls++
	if n.unreachable {
		return nil, errors.New("source procd is unreachable")
	}
	if n.lostCancel {
		n.lostCancel = false
		return nil, errors.New("lost cancellation response")
	}
	return &procdapi.RuntimeCheckpointResponse{InstanceID: r.InstanceID, RequestDigest: digest, RuntimeGeneration: r.Capture.RuntimeGeneration, State: "ready"}, nil
}
func (n *checkpointCancellationNode) ReleaseMigrationStaging(ctx context.Context, r protocol.MigrationStagingRequest) error {
	var authorized, captured, prepared, canceled, absent bool
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT evidence ? 'cancel_authorized',evidence ? 'capture_authorized',evidence ? 'preparation',evidence ? 'canceled',evidence ? 'source_absent_proof'
		FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, r.Source.OperationID).Scan(&authorized, &captured, &prepared, &canceled, &absent))
	require.True(n.t, authorized)
	require.False(n.t, captured)
	require.True(n.t, !prepared || canceled || absent)
	n.releaseCalls++
	if n.lostRelease {
		n.lostRelease = false
		return errors.New("lost staging release response")
	}
	return r.Validate()
}

func checkpointCancellationStep(t *testing.T, n *checkpointCancellationNode) error {
	t.Helper()
	// Every pass discards the coordinator and store objects; only DB evidence survives.
	worker, err := nomadmigration.NewCheckpointPause(NewPGSandboxStore(n.f.pool), n, n, n, n)
	require.NoError(t, err)
	_, err = worker.RunOnce(n.f.ctx)
	return err
}

func TestNomadCheckpointCancellationRecoversUnusedAndPreparedSourcesIntegration(t *testing.T) {
	for _, stage := range []string{"preflight", "staging-requested", "staging-reserved", "prepare-requested"} {
		t.Run(stage, func(t *testing.T) {
			base, id := newCheckpointPauseWorkerFixture(t, "cancel-"+stage)
			n := &checkpointCancellationNode{checkpointPauseWorkerNode: base}
			f := n.f
			switch stage {
			case "staging-requested":
				advanceCheckpointPauseWorker(t, base, 1)
				_, err := f.store.AuthorizeNomadCheckpointStaging(f.ctx, id)
				require.NoError(t, err)
			case "staging-reserved", "prepare-requested":
				advanceCheckpointPauseWorker(t, base, 2)
			}
			if stage == "prepare-requested" {
				_, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, id)
				require.NoError(t, err)
			}
			command, err := f.store.AuthorizeNomadCheckpointCancellation(f.ctx, id)
			require.NoError(t, err)
			require.Nil(t, command, "healthy preparation must continue")
			expirePreparedMigration(t, f, id)
			command, err = f.store.AuthorizeNomadCheckpointCancellation(f.ctx, id)
			require.NoError(t, err)
			require.NotNil(t, command)
			require.Error(t, f.store.CompleteNomadCheckpointCancellation(f.ctx, "unknown-operation"))
			if stage != "preflight" {
				require.ErrorIs(t, f.store.CompleteNomadCheckpointCancellation(f.ctx, id), ErrNomadCheckpointConflict)
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',aborted_at=NOW() WHERE txn_id=$1`, id)
				require.Error(t, err, "SQL cannot omit source/staging receipts")
				n.lostRelease = true
			}
			if command.Preparation != nil {
				prepare := *command.Preparation
				prepare.Action = procdapi.MigrationPrepare
				_, err = f.store.AuthorizeNomadCheckpointCapture(f.ctx, prepare, checkpointPreparedResponse(t, prepare))
				require.ErrorIs(t, err, ErrNomadCheckpointConflict, "cancellation fences delayed prepare acknowledgement")
				n.lostCancel = true
				require.ErrorContains(t, checkpointCancellationStep(t, n), "lost cancellation")
			}
			// Repeated attempts recover each lost response without changing execution.
			for attempt := 0; attempt < 6; attempt++ {
				err := checkpointCancellationStep(t, n)
				if err != nil {
					require.Contains(t, err.Error(), "lost staging release")
				}
			}
			life, err := f.store.GetLifecycleTxn(f.ctx, id)
			require.NoError(t, err)
			require.Equal(t, SandboxLifecyclePhaseAborted, life.Phase)
			require.NoError(t, f.store.CompleteNomadCheckpointCancellation(f.ctx, id), "completion is idempotent")
			pending, err := f.store.ListNomadCheckpointPauses(f.ctx, "", 10)
			require.NoError(t, err)
			require.Empty(t, pending)
			require.Zero(t, n.captureCalls)
			require.Zero(t, n.fenceCalls)
			require.Zero(t, n.publishCalls)
			var active int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
			require.Equal(t, 1, active)
			slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateActive, slot.State)
			require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState)
			fs, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
			require.NoError(t, err)
			require.Equal(t, f.initialGenerationID, fs.HeadGenerationID)
			require.Equal(t, f.writerEpoch, fs.WriterEpoch)
			if stage == "prepare-requested" {
				require.Equal(t, 2, n.cancelCalls)
			}
			if stage != "preflight" {
				require.Equal(t, 2, n.releaseCalls)
				var payload []byte
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT evidence->'staging' FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, id).Scan(&payload))
				var request protocol.MigrationStagingRequest
				require.NoError(t, json.Unmarshal(payload, &request))
				scope, err := request.CaptureUpload.Scope(request.Source)
				require.NoError(t, err)
				collector, err := runtimecheckpoint.NewCollector(objectstore.NewMemoryStore(""))
				require.NoError(t, err)
				gc, err := nomadmigration.NewCheckpointCaptureUploadGC(f.store, collector)
				require.NoError(t, err)
				pass, err := gc.RunOnce(f.ctx)
				require.NoError(t, err)
				require.Equal(t, 1, pass.Advanced)
				require.NoError(t, f.store.CompleteNomadCheckpointCaptureUploadGC(f.ctx, scope))
			}
			fresh, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, id+"-retry", n.source, n.checkpoint.Evidence.Policy)
			require.NoError(t, err)
			require.Greater(t, fresh.Lifecycle.Epoch, life.Epoch)
			require.NoError(t, f.store.CompleteNomadCheckpointCancellation(f.ctx, id), "late completion cannot abort the next operation")
			next, err := f.store.GetLifecycleTxn(f.ctx, fresh.Lifecycle.ID)
			require.NoError(t, err)
			require.Equal(t, SandboxLifecyclePhasePreparing, next.Phase)
		})
	}
}

func TestNomadCheckpointCancellationAfterExactSourceAbsenceIntegration(t *testing.T) {
	base, id := newCheckpointPauseWorkerFixture(t, "cancel-absent")
	advanceCheckpointPauseWorker(t, base, 2)
	f := base.f
	_, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, id)
	require.NoError(t, err)
	expirePreparedMigration(t, f, id)
	n := &checkpointCancellationNode{checkpointPauseWorkerNode: base, unreachable: true}
	require.ErrorContains(t, checkpointCancellationStep(t, n), "source procd is unreachable")
	life, err := f.store.GetLifecycleTxn(f.ctx, id)
	require.NoError(t, err)
	require.NotEqual(t, SandboxLifecyclePhaseAborted, life.Phase, "unreachable procd alone cannot release the source")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET state='quiescing',carrier_retired=true,quiescing_at=NOW() WHERE slot_id=$1`, f.slotID)
	require.NoError(t, err)
	require.NoError(t, checkpointCancellationStep(t, n))
	var proof []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT evidence->'source_absent_proof' FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, id).Scan(&proof))
	require.NotEmpty(t, proof)
	require.ErrorIs(t, f.store.CommitNomadCheckpointSourceAbsent(f.ctx, id, protocol.NodeChannelTarget{SlotID: "wrong"}, bytes.Repeat([]byte{0x6a}, 32)), ErrNomadCheckpointConflict)
	require.NoError(t, checkpointCancellationStep(t, n))
	require.NoError(t, checkpointCancellationStep(t, n))
	life, err = f.store.GetLifecycleTxn(f.ctx, id)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseAborted, life.Phase)
	require.Equal(t, 2, n.cancelCalls, "completed absence skips the unreachable endpoint")
	require.Equal(t, 1, n.retireCalls)
	require.Equal(t, 1, n.releaseCalls)
	slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, slot.State, "terminal reconciliation still owns physical cleanup")
}

func TestNomadCheckpointCancellationAndCaptureHaveOneWinnerIntegration(t *testing.T) {
	n, id := newCheckpointPauseWorkerFixture(t, "cancel-race")
	advanceCheckpointPauseWorker(t, n, 2)
	f := n.f
	prepare, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, id)
	require.NoError(t, err)
	expirePreparedMigration(t, f, id) // The immutable CPU window remains fresh for this race.
	start := make(chan struct{})
	var group sync.WaitGroup
	var captureErr, cancelErr error
	var cancel *nomadmigration.CheckpointCancellation
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		_, captureErr = f.store.AuthorizeNomadCheckpointCapture(f.ctx, *prepare, checkpointPreparedResponse(t, *prepare))
	}()
	go func() {
		defer group.Done()
		<-start
		cancel, cancelErr = f.store.AuthorizeNomadCheckpointCancellation(f.ctx, id)
	}()
	close(start)
	group.Wait()
	var canceled, captured bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT evidence ? 'cancel_authorized',evidence ? 'capture_authorized' FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, id).Scan(&canceled, &captured))
	require.NotEqual(t, canceled, captured)
	if captured {
		require.NoError(t, captureErr)
		require.ErrorIs(t, cancelErr, ErrNomadCheckpointConflict)
		require.Nil(t, cancel)
	} else {
		require.NoError(t, cancelErr)
		require.NotNil(t, cancel)
		require.ErrorIs(t, captureErr, ErrNomadCheckpointConflict)
	}
}

func TestNomadCheckpointCancellationContinuesAfterHardExpiryIntegration(t *testing.T) {
	n, id := newCheckpointPauseWorkerFixture(t, "cancel-expired")
	advanceCheckpointPauseWorker(t, n, 2)
	f := n.f
	_, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, id)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	cancelNode := &checkpointCancellationNode{checkpointPauseWorkerNode: n}
	for range 3 {
		require.NoError(t, checkpointCancellationStep(t, cancelNode))
	}
	life, err := f.store.GetLifecycleTxn(f.ctx, id)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseAborted, life.Phase)
}

func TestNomadCheckpointDeletionWaitsForPreparationCancellationIntegration(t *testing.T) {
	base, id := newCheckpointPauseWorkerFixture(t, "cancel-delete")
	advanceCheckpointPauseWorker(t, base, 2)
	f := base.f
	_, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, id)
	require.NoError(t, err)
	candidate, err := f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete prepared memory source")
	require.NoError(t, err)
	require.Nil(t, candidate, "generic cleanup cannot kill the cancellation endpoint")
	slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateActive, slot.State)
	n := &checkpointCancellationNode{checkpointPauseWorkerNode: base}
	for range 3 {
		require.NoError(t, checkpointCancellationStep(t, n))
	}
	life, err := f.store.GetLifecycleTxn(f.ctx, id)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseAborted, life.Phase)
	require.Equal(t, 1, n.cancelCalls)
	require.Equal(t, 1, n.releaseCalls)
	candidate, err = f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "retry deletion after cancellation")
	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Equal(t, f.slotID, candidate.SlotID)
	require.Zero(t, n.captureCalls)
}
