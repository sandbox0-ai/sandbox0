package sandboxstore

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func resizeFixtureRequest(t *testing.T, f *nomadPauseStoreFixture, memory string, cpu, memoryBytes int64) RequestSandboxResourceResize {
	t.Helper()
	r, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	return RequestSandboxResourceResize{SandboxID: f.sandboxID, ExpectedTeamID: r.TeamID, ExpectedGeneration: r.RuntimeGeneration,
		ExpectedMemoryOverride: sandboxMemoryOverride(r), Memory: memory, CPUMillicores: cpu, MemoryBytes: memoryBytes}
}

func TestSandboxResourceResizeReplacesExactLeaseAndPreservesRootFSIntegration(t *testing.T) {
	for _, test := range []struct {
		name, memory     string
		cpu, memoryBytes int64
	}{
		{"increase", "4Gi", 1000, 4 << 30}, {"decrease", "128Mi", 500, 128 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newNomadPauseStoreFixture(t, "resize-"+test.name)
			before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			oldSlot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			request := resizeFixtureRequest(t, f, test.memory, test.cpu, test.memoryBytes)
			r, err := f.store.BeginSandboxResourceResize(f.ctx, request)
			require.NoError(t, err)
			require.Equal(t, SandboxResourceResizePausing, r.Phase)
			// A different manager recovers the same desired operation; changing
			// its target while pending cannot create another restart.
			other := NewPGSandboxStore(f.pool)
			retry, err := other.BeginSandboxResourceResize(f.ctx, request)
			require.NoError(t, err)
			require.Equal(t, r.OperationID, retry.OperationID)
			changed := request
			changed.MemoryBytes++
			_, err = other.BeginSandboxResourceResize(f.ctx, changed)
			require.ErrorIs(t, err, ErrSandboxResourceResizeConflict)
			_, err = other.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceManual)
			require.ErrorIs(t, err, ErrNomadSandboxPauseConflict)
			_, _, err = other.RetryNomadSandboxResume(f.ctx, &RetryNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: before.TeamID})
			require.ErrorIs(t, err, ErrNomadSandboxResumeConflict)
			_, err = other.RequestNomadSandboxResourceResizePause(f.ctx, f.sandboxID, "stale-resize")
			require.ErrorIs(t, err, ErrNomadSandboxPauseConflict)
			pause, err := other.RequestNomadSandboxResourceResizePause(f.ctx, f.sandboxID, r.OperationID)
			require.NoError(t, err)
			require.Equal(t, SandboxLifecycleSourceResourceResize, pause.Source)
			f.publishPlannedPause(t, pause.OperationID)
			// Publishing a head alone must not release the live source lease or
			// expose the new config before exact physical cleanup.
			_, err = other.PrepareSandboxResourceResize(f.ctx, f.sandboxID)
			require.ErrorIs(t, err, ErrNomadSandboxResumeNotReady)
			stillOld, err := other.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, before.Config.Resources, stillOld.Config.Resources)
			terminalizeNomadPauseSlot(t, f, pause)
			r, err = other.PrepareSandboxResourceResize(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, SandboxResourceResizeResuming, r.Phase)
			_, err = other.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: before.TeamID, Memory: true})
			require.ErrorIs(t, err, ErrNomadSandboxResumeConflict)
			resume, err := other.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: before.TeamID})
			require.NoError(t, err)
			require.NotEqual(t, f.initialGenerationID, resume.SourceGenerationID)
			wrong := &AcquireRuntimeSlotRequest{
				OperationID: resume.OperationID, ClaimID: "wrong-resize-claim", SandboxID: f.sandboxID,
				FilesystemID: resume.FilesystemID, SourceGenerationID: resume.SourceGenerationID,
				CompatibilityDigest: oldSlot.CompatibilityDigest, ClusterID: oldSlot.ClusterID,
				RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32),
				ClaimTTL: time.Minute, Resources: runtimeSlotTestResources(),
			}
			wrong.Resources.MemoryBytes = test.memoryBytes + (1 << 20)
			_, err = other.AcquireRuntimeSlot(f.ctx, wrong)
			require.ErrorIs(t, err, ErrRuntimeSlotConflict, "stale planner must not admit another size")
			resources := protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: test.cpu, MemoryBytes: test.memoryBytes, PIDsLimit: protocol.DefaultRuntimePIDsLimit}
			fresh := prepareNomadResumeRuntimeWithResources(t, f, resume, "resize-"+test.name, resources)
			_, err = other.MarkRuntimeSlotCommandReady(f.ctx, &MarkRuntimeSlotCommandReadyRequest{
				SlotID: fresh.claimed.ID, AllocationID: fresh.registration.AllocationID, NodeUID: fresh.registration.NodeUID, NodeBootID: fresh.registration.NodeBootID,
				OperationID: resume.OperationID, ClaimID: fresh.acquire.ClaimID, ProcdInstanceID: "procd-resized", ProcdAddress: "http://192.0.2.10:49983", CommandReadyDigest: bytes.Repeat([]byte{0xa4}, 32),
			})
			require.NoError(t, err)
			completed, err := other.CompleteNomadSandboxResume(f.ctx, &CompleteNomadSandboxResumeRequest{
				SandboxID: f.sandboxID, OperationID: resume.OperationID, SlotID: fresh.claimed.ID, AllocationID: fresh.registration.AllocationID, AllocationNamespace: fresh.registration.AllocationNamespace,
				ResourceLeaseID: fresh.claimed.ResourceLease.LeaseID, ResourceLeaseDigest: fresh.claimed.ResourceLeaseDigest,
			})
			require.NoError(t, err)
			require.Equal(t, before.ID, completed.ID)
			require.Equal(t, before.RuntimeGeneration+1, completed.RuntimeGeneration)
			require.Equal(t, test.cpu, completed.ResourceMillicpu)
			require.Equal(t, test.memoryBytes>>20, completed.ResourceMemoryMiB)
			require.Equal(t, test.memory, completed.Config.Resources.Memory)
			require.NotEqual(t, oldSlot.ResourceLease.LeaseID, fresh.claimed.ResourceLease.LeaseID)
			oldSlot, err = other.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeResourceLeaseReleased, oldSlot.ResourceLeaseState)
			r, err = other.PrepareSandboxResourceResize(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, SandboxResourceResizeApplied, r.Phase)
			pending, err := other.ListPendingSandboxResourceResizes(f.ctx, 100)
			require.NoError(t, err)
			require.Empty(t, pending)
			request = resizeFixtureRequest(t, f, test.memory, test.cpu, test.memoryBytes)
			request.NoChange = true
			noop, err := other.BeginSandboxResourceResize(f.ctx, request)
			require.NoError(t, err)
			require.Nil(t, noop)
		})
	}
}

func TestSandboxResourceResizePausedDoesNotStartComputeIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "resize-paused")
	terminalizeNomadPauseFixture(t, f)
	r, err := f.store.BeginSandboxResourceResize(f.ctx, resizeFixtureRequest(t, f, "4Gi", 1000, 4<<30))
	require.NoError(t, err)
	require.False(t, r.WasActive)
	r, err = f.store.PrepareSandboxResourceResize(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxResourceResizeApplied, r.Phase)
	record, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, record.DesiredState)
	require.Equal(t, "4Gi", record.Config.Resources.Memory)
	require.Equal(t, int64(1), record.RuntimeGeneration)
}

func TestSandboxResourceResizeDeletionWinsIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "resize-delete")
	_, err := f.store.BeginSandboxResourceResize(f.ctx, resizeFixtureRequest(t, f, "4Gi", 1000, 4<<30))
	require.NoError(t, err)
	_, err = f.store.RequestSandboxRuntimeClaimCleanup(context.Background(), f.sandboxID, "delete during resize")
	require.NoError(t, err)
	r, err := f.store.PrepareSandboxResourceResize(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxResourceResizeCanceled, r.Phase)
}

func TestSandboxResourceResizeDiscardsRetainedMemoryWithoutLosingRootFSIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "resize-memory-paused")
	var refs int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&refs))
	require.Equal(t, 1, refs)
	before, err := f.store.GetRootFSFilesystem(f.ctx, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.BeginSandboxResourceResize(f.ctx, resizeFixtureRequest(t, f, "4Gi", 1000, 4<<30))
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID)
	require.Error(t, err, "pending resize alone cannot discard memory before config and physical checks")
	r, err := f.store.PrepareSandboxResourceResize(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxResourceResizeApplied, r.Phase)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&refs))
	require.Zero(t, refs, "automatic memory resume must not restore old processes")
	after, err := f.store.GetRootFSFilesystem(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, before.ID, after.ID)
	require.Equal(t, before.HeadGenerationID, after.HeadGenerationID)
}

func TestSandboxResourceResizeConcurrentAdmissionHasOneOperationIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "resize-concurrent")
	request := resizeFixtureRequest(t, f, "4Gi", 1000, 4<<30)
	const count = 8
	results := make([]*SandboxResourceResize, count)
	errors := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errors[i] = NewPGSandboxStore(f.pool).BeginSandboxResourceResize(f.ctx, request)
		}()
	}
	wg.Wait()
	for i := range count {
		require.NoError(t, errors[i])
		require.Equal(t, results[0].OperationID, results[i].OperationID)
	}
}
