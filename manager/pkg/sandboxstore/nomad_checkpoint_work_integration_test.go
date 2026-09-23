package sandboxstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type checkpointPauseWorkerNode struct {
	t                                          *testing.T
	f                                          *nomadPauseStoreFixture
	checkpoint                                 *NomadSandboxCheckpoint
	source                                     runtimecontrol.Assignment
	capture                                    *protocol.MigrationCapture
	captureCalls, publishCalls, fenceCalls     int
	lostCapture, lostPublication, invalidFence bool
	recoveryErr                                error
}

func (n *checkpointPauseWorkerNode) CleanupFailedMigrationCapture(ctx context.Context, request protocol.MigrationCaptureFailureRequest) (*protocol.MigrationCaptureFailureProof, error) {
	return (&captureFailureTestNode{t: n.t}).CleanupFailedMigrationCapture(ctx, request)
}
func (n *checkpointPauseWorkerNode) FinalizeFailedMigrationCapture(ctx context.Context, request protocol.MigrationCaptureFailureFinalizeRequest) (*protocol.MigrationCaptureFailureFinalizeProof, error) {
	return (&captureFailureTestNode{t: n.t}).FinalizeFailedMigrationCapture(ctx, request)
}

func (n *checkpointPauseWorkerNode) PreflightMigrationCPU(_ context.Context, request protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error) {
	r := migrationCPUStoreResult(n.t, n.f, request)
	return &r, nil
}
func (n *checkpointPauseWorkerNode) ReleaseMigrationStaging(_ context.Context, request protocol.MigrationStagingRequest) error {
	return request.Validate()
}
func (n *checkpointPauseWorkerNode) ReserveMigrationStaging(_ context.Context, request protocol.MigrationStagingRequest) (*protocol.MigrationStagingReserved, error) {
	d, err := request.Digest()
	return &protocol.MigrationStagingReserved{RequestDigest: d}, err
}
func (n *checkpointPauseWorkerNode) GenerateCheckpointToken(request procdapi.RuntimeCheckpointRequest) (string, error) {
	return request.Digest()
}
func (n *checkpointPauseWorkerNode) ObserveCheckpointSource(_ context.Context, target protocol.NodeChannelTarget, namespace string) (bool, []byte, error) {
	require.Equal(n.t, n.checkpoint.Evidence.Preflight.Source.Target, target)
	require.Equal(n.t, n.checkpoint.Lifecycle.FromRuntimeNamespace, namespace)
	return false, nil, nil
}
func (n *checkpointPauseWorkerNode) RetireTerminalCheckpointSource(_ context.Context, _ string, _ protocol.NodeChannelTarget, _ string) error {
	return errors.New("live checkpoint source cannot be retired")
}
func (n *checkpointPauseWorkerNode) CheckpointRuntime(_ context.Context, address string, request procdapi.RuntimeCheckpointRequest, token string) (*procdapi.RuntimeCheckpointResponse, error) {
	require.Equal(n.t, n.checkpoint.Evidence.Address, address)
	d, err := request.Digest()
	require.NoError(n.t, err)
	require.Equal(n.t, d, token)
	r := checkpointPreparedResponse(n.t, request)
	return &r, nil
}
func (n *checkpointPauseWorkerNode) RecoverMigrationCapture(_ context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	require.Equal(n.t, n.checkpoint.Evidence.Preflight.Source, request)
	if n.recoveryErr != nil {
		return nil, n.recoveryErr
	}
	if n.capture == nil {
		return nil, errdefs.ErrNotFound
	}
	return n.capture, nil
}
func (n *checkpointPauseWorkerNode) CaptureMigration(_ context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	n.captureCalls++
	require.Equal(n.t, 1, n.captureCalls, "capture cannot execute twice after an uncertain response")
	publication := checkpointWorkerPublication(n.t, n.f, n.checkpoint, n.source, request)
	n.capture = &publication.Capture
	if n.lostCapture {
		n.lostCapture = false
		return nil, errors.New("lost capture reply")
	}
	return n.capture, nil
}
func (n *checkpointPauseWorkerNode) PublishMigration(_ context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error) {
	n.publishCalls++
	r := migrationPublicationReceipt(n.t, request)
	if n.lostPublication {
		n.lostPublication = false
		return nil, errors.New("lost publication reply")
	}
	return &r, nil
}
func (n *checkpointPauseWorkerNode) FenceMigrationSource(_ context.Context, request protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error) {
	n.fenceCalls++
	r := migrationSourceFenceStoreProof(n.t, n.f, request)
	if n.invalidFence {
		r.ContainerAbsent = false
	}
	return &r, nil
}

func newCheckpointPauseWorkerFixture(t *testing.T, name string) (*checkpointPauseWorkerNode, string) {
	t.Helper()
	f, source, operation, policy := checkpointStoreFixture(t, name)
	checkpoint, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, operation, source, policy)
	require.NoError(t, err)
	return &checkpointPauseWorkerNode{t: t, f: f, source: source, checkpoint: checkpoint}, operation
}

func runCheckpointPauseWorker(t *testing.T, n *checkpointPauseWorkerNode) (nomadmigration.Result, error) {
	t.Helper()
	// Recreate both store and scheduler at every step, losing all local state.
	worker, err := nomadmigration.NewCheckpointPause(NewPGSandboxStore(n.f.pool), n, n, n, n)
	require.NoError(t, err)
	return worker.RunOnce(n.f.ctx)
}

func advanceCheckpointPauseWorker(t *testing.T, n *checkpointPauseWorkerNode, steps int) {
	t.Helper()
	for range steps {
		r, err := runCheckpointPauseWorker(t, n)
		require.NoError(t, err)
		require.Equal(t, 1, r.Advanced)
		var active int
		require.NoError(t, n.f.pool.QueryRow(n.f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
		require.Equal(t, 1, active, "capture, publication and fence do not prove complete cleanup")
	}
}

func TestNomadCheckpointPauseWorkerRecoversLostRepliesAndDefersCapacityReleaseIntegration(t *testing.T) {
	n, operation := newCheckpointPauseWorkerFixture(t, "worker-recovery")
	advanceCheckpointPauseWorker(t, n, 3)
	n.lostCapture = true
	r, err := runCheckpointPauseWorker(t, n)
	require.ErrorContains(t, err, "lost capture reply")
	require.Equal(t, 1, r.Failed)
	advanceCheckpointPauseWorker(t, n, 1)
	require.Equal(t, 1, n.captureCalls)
	n.lostPublication = true
	_, err = runCheckpointPauseWorker(t, n)
	require.ErrorContains(t, err, "lost publication reply")
	advanceCheckpointPauseWorker(t, n, 1)
	n.invalidFence = true
	_, err = runCheckpointPauseWorker(t, n)
	require.ErrorContains(t, err, "physical proof")
	slot, err := n.f.store.GetRuntimeSlot(n.f.ctx, n.f.slotID)
	require.NoError(t, err)
	require.NotEqual(t, RuntimeSlotStateTerminal, slot.State)
	n.invalidFence = false
	advanceCheckpointPauseWorker(t, n, 1)
	ids, err := n.f.store.ListNomadCheckpointPauses(n.f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids, "fenced source belongs to the terminal reconciler")
	work, err := n.f.store.GetNomadCheckpointPauseWork(n.f.ctx, operation)
	require.NoError(t, err)
	require.Nil(t, work)
	receipt, found, err := n.f.store.GetNomadCheckpointSourceFinalization(n.f.ctx, operation)
	require.NoError(t, err)
	require.True(t, found, "shared reconciler must select checkpoint authority before a receipt exists")
	require.Nil(t, receipt)
	_, err = n.f.store.CompleteNomadSandboxMemoryPause(n.f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	cleanup, err := n.f.store.AuthorizeNomadCheckpointSourceFinalization(n.f.ctx, operation)
	require.NoError(t, err)
	require.NoError(t, n.f.store.CommitNomadCheckpointSourceFinalization(n.f.ctx, *cleanup, migrationFinalizationStoreProof(t, *cleanup)))
	_, err = n.f.store.CompleteNomadSandboxMemoryPause(n.f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	markCheckpointAllocationMissing(t, n.f)
	slot, err = n.f.store.CompleteNomadSandboxMemoryPause(n.f.ctx, operation)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, slot.State)
	require.Equal(t, RuntimeResourceLeaseReleased, slot.ResourceLeaseState)
	receipt, found, err = NewPGSandboxStore(n.f.pool).GetNomadCheckpointSourceFinalization(n.f.ctx, operation)
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, receipt.Validate())
	_, found, err = n.f.store.GetNomadCheckpointSourceFinalization(n.f.ctx, operation+"-missing")
	require.NoError(t, err)
	require.False(t, found)
}

func TestNomadCheckpointPauseWorkerNeverRecapturesIntentOrUnreachableSourceIntegration(t *testing.T) {
	n, _ := newCheckpointPauseWorkerFixture(t, "worker-intent")
	advanceCheckpointPauseWorker(t, n, 3)
	source := n.checkpoint.Evidence.Preflight.Source
	d, err := source.Digest()
	require.NoError(t, err)
	n.capture = &protocol.MigrationCapture{Request: source, RequestDigest: d, State: protocol.MigrationCaptureIntent}
	r, err := runCheckpointPauseWorker(t, n)
	require.NoError(t, err)
	require.Equal(t, 1, r.Skipped)
	n.recoveryErr = errors.New("node unreachable")
	_, err = runCheckpointPauseWorker(t, n)
	require.ErrorContains(t, err, "node unreachable")
	require.Zero(t, n.captureCalls)
	require.Zero(t, n.publishCalls)
	require.Zero(t, n.fenceCalls)
}

func TestNomadCheckpointPauseWorkerDispatchRevalidatesLiveOwnerIntegration(t *testing.T) {
	n, operation := newCheckpointPauseWorkerFixture(t, "worker-dispatch")
	f := n.f
	source := n.checkpoint.Evidence.Preflight.Source
	require.ErrorIs(t, f.store.AuthorizeNomadCheckpointCaptureDispatch(f.ctx, source), ErrNomadCheckpointConflict)
	advanceCheckpointPauseWorker(t, n, 3)
	require.NoError(t, f.store.AuthorizeNomadCheckpointCaptureDispatch(f.ctx, source))
	var before, after string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT xmin::text FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, operation).Scan(&before))
	require.NoError(t, f.store.AuthorizeNomadCheckpointCaptureDispatch(f.ctx, source))
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT xmin::text FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, operation).Scan(&after))
	require.Equal(t, before, after, "read-only dispatch retries must not rewrite the image evidence")
	wrong := source
	wrong.Target.ControlEndpoint = "unix:///other.sock"
	require.ErrorIs(t, f.store.AuthorizeNomadCheckpointCaptureDispatch(f.ctx, wrong), ErrNomadCheckpointConflict)
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = runCheckpointPauseWorker(t, n)
	require.Error(t, err)
	require.Zero(t, n.captureCalls, "expired owner cannot authorize new capture")
	// A capture that already happened remains recoverable after expiration.
	publication := checkpointWorkerPublication(t, f, n.checkpoint, n.source, source)
	n.capture = &publication.Capture
	advanceCheckpointPauseWorker(t, n, 3)
	ids, err := f.store.ListNomadCheckpointPauses(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestNomadCheckpointPauseWorkScanIsBoundedAndUsesExclusiveCursorIntegration(t *testing.T) {
	n, operation := newCheckpointPauseWorkerFixture(t, "worker-list")
	f := n.f
	ids, err := f.store.ListNomadCheckpointPauses(f.ctx, "", 1)
	require.NoError(t, err)
	require.Equal(t, []string{operation}, ids)
	ids, err = f.store.ListNomadCheckpointPauses(f.ctx, operation, 1)
	require.NoError(t, err)
	require.Empty(t, ids)
	for _, limit := range []int{0, -1, MaxRuntimeSlotReconcileLimit + 1} {
		_, err = f.store.ListNomadCheckpointPauses(f.ctx, "", limit)
		require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	}
	_, err = f.store.ListNomadCheckpointPauses(f.ctx, strings.Repeat("x", 257), 1)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
}
