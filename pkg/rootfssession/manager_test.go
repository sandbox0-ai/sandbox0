package session

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func TestManagerEnsureResolveAndReleaseExactlyOnce(t *testing.T) {
	manager, runtime, request := newTestManager(t, "lifecycle")
	mount, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, "bind", mount.Type)
	require.Equal(t, []string{"rbind", "rw", "nosuid", "nodev"}, mount.Options)
	require.Equal(t, []string{"attach", "mount-xfs", "mount-overlay"}, runtime.callsSnapshot())

	retry, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, mount, retry)
	resolved, err := manager.Resolve(request.Parent, request.WithoutWriterGrantToken())
	require.NoError(t, err)
	require.Equal(t, mount, resolved)
	require.Equal(t, []string{"attach", "mount-xfs", "mount-overlay"}, runtime.callsSnapshot())

	require.NoError(t, manager.Release(t.Context(), request.Identity))
	require.NoError(t, manager.Release(t.Context(), request.Identity))
	require.Equal(t, []string{
		"attach", "mount-xfs", "mount-overlay", "unmount-overlay", "unmount-xfs", "close-device",
	}, runtime.callsSnapshot())
	require.Equal(t, []bool{false, false}, runtime.unmountSyncSnapshot(), "crash cleanup must not claim a filesystem barrier")
	stored, err := manager.load(request.Parent)
	require.NoError(t, err)
	require.Equal(t, stateTombstoned, stored.State)
}

func TestManagerCrashFenceIsDurableAndOperationBound(t *testing.T) {
	manager, runtime, request := newTestManager(t, "crash-fence")
	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))

	result, err := manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	require.NoError(t, result.Validate())
	path, err := runtime.ReserveDevice("replacement-allocation")
	require.NoError(t, err)
	retry, err := manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	require.Equal(t, result, retry)
	require.Equal(t, "replacement-allocation", runtime.reservationOwner(path))
	_, err = manager.CrashFence(request.WithoutWriterGrantToken(), "different-operation")
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	require.Equal(t, 1, runtime.fenceInspections)
}

func TestManagerCrashFenceFailsClosedUntilNBDIsDetached(t *testing.T) {
	manager, runtime, request := newTestManager(t, "crash-fence-nbd")
	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))
	runtime.fenceObservation.NBDPID = 42

	_, err = manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	runtime.fenceObservation.NBDPID = 0
	result, err := manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	require.NoError(t, result.Validate())
}

func TestManagerCrashFenceFailsClosedWhileSessionMountRemains(t *testing.T) {
	manager, runtime, request := newTestManager(t, "crash-fence-mount")
	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))
	runtime.fenceObservation.MergedMountAbsent = false

	_, err = manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestManagerCrashFenceProvesPreAttachmentSessionWithEmptyDevicePool(t *testing.T) {
	manager, runtime, request := newTestManager(t, "crash-fence-pre-attachment")
	binding, err := request.WithoutWriterGrantToken().BindingDigest()
	require.NoError(t, err)
	paths := sessionPaths(manager.branchRoot, manager.mountRoot, request.Parent)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, manager.saveNew(record{
		Version: legacySessionSchemaVersion, Parent: request.Parent, BindingDigest: fmt.Sprintf("%x", binding),
		RootFSID: request.Identity.RootFSID, WriterEpoch: request.Identity.WriterEpoch,
		GenerationID: request.InitialGeneration, BranchPath: paths.branch,
		XFSRoot: paths.xfs, MergedRoot: paths.merged, State: stateTombstoned,
		CreatedAt: now, UpdatedAt: now,
	}))

	result, err := manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	require.NoError(t, result.Validate())
	require.False(t, result.DeviceBound)
	require.Empty(t, result.DevicePath)
	require.True(t, result.NBDPoolAbsent)
	require.Equal(t, 1, runtime.unattachedFenceInspections)
}

func TestManagerCrashFenceRejectsPreAttachmentSessionWhenDevicePoolIsBusy(t *testing.T) {
	manager, runtime, request := newTestManager(t, "crash-fence-pre-attachment-busy")
	binding, err := request.WithoutWriterGrantToken().BindingDigest()
	require.NoError(t, err)
	paths := sessionPaths(manager.branchRoot, manager.mountRoot, request.Parent)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, manager.saveNew(record{
		Version: legacySessionSchemaVersion, Parent: request.Parent, BindingDigest: fmt.Sprintf("%x", binding),
		RootFSID: request.Identity.RootFSID, WriterEpoch: request.Identity.WriterEpoch,
		GenerationID: request.InitialGeneration, BranchPath: paths.branch,
		XFSRoot: paths.xfs, MergedRoot: paths.merged, State: stateTombstoned,
		CreatedAt: now, UpdatedAt: now,
	}))
	runtime.fenceObservation.NBDPID = 42

	_, err = manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestManagerPersistsExactDeviceReservationBeforeAttach(t *testing.T) {
	manager, runtime, request := newTestManager(t, "device-reservation-before-attach")
	var reserved record
	runtime.beforeAttach = func(devicePath, allocationID string) {
		stored, err := manager.load(request.Parent)
		require.NoError(t, err)
		require.Equal(t, devicePath, stored.DevicePath)
		require.Equal(t, allocationID, stored.DeviceAllocationID)
		reserved = stored
	}

	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, stateDeviceReserved, reserved.State)
	require.Equal(t, "/dev/fake0", reserved.DevicePath)
	require.NotEmpty(t, reserved.DeviceAllocationID)
	require.False(t, reserved.DeviceReservationReleased)
}

func TestManagerFailedAttachKeepsExactReservationUntilCrashFenceProof(t *testing.T) {
	manager, runtime, request := newTestManager(t, "failed-attach-reservation")
	runtime.failAt = "attach"

	_, err := manager.Ensure(t.Context(), request)
	require.ErrorContains(t, err, "attach block device")
	stored, loadErr := manager.load(request.Parent)
	require.NoError(t, loadErr)
	require.Equal(t, stateFailed, stored.State)
	require.Equal(t, "/dev/fake0", stored.DevicePath)
	require.NotEmpty(t, stored.DeviceAllocationID)
	require.Equal(t, stored.DeviceAllocationID, runtime.reservationOwner(stored.DevicePath))
	require.NoError(t, manager.Release(t.Context(), request.Identity))

	_, err = runtime.ReserveDevice("competing-allocation")
	require.ErrorContains(t, err, "no usable NBD device")
	result, err := manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	require.NoError(t, result.Validate())
	require.Equal(t, 2, runtime.fenceInspections)
	require.Zero(t, runtime.unattachedFenceInspections)
	require.Empty(t, runtime.reservationOwner(stored.DevicePath))

	path, err := runtime.ReserveDevice("next-allocation")
	require.NoError(t, err)
	require.Equal(t, stored.DevicePath, path)
	runtime.ReleaseDeviceReservation(path, "next-allocation")
}

func TestManagerReconcileAdoptsOutstandingDeviceReservation(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	request := testStageRequest(t, objects, "adopt-device-reservation")
	config := Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects,
	}
	firstRuntime := newFakeHostRuntime(objects)
	config.Runtime = firstRuntime
	first, err := New(config)
	require.NoError(t, err)
	_, err = first.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Release(t.Context(), request.Identity))
	stored, err := first.load(request.Parent)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	secondRuntime := newFakeHostRuntime(objects)
	config.Runtime = secondRuntime
	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.ReconcileReleases(t.Context()))
	require.Equal(t, stored.DeviceAllocationID, secondRuntime.reservationOwner(stored.DevicePath))
	_, err = secondRuntime.ReserveDevice("competing-allocation")
	require.ErrorContains(t, err, "no usable NBD device")

	_, err = second.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	path, err := secondRuntime.ReserveDevice("next-allocation")
	require.NoError(t, err)
	require.Equal(t, stored.DevicePath, path)
	secondRuntime.ReleaseDeviceReservation(path, "next-allocation")
}

func TestManagerRestartDisconnectsExactOrphanBeforeTombstone(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	request := testStageRequest(t, objects, "restart-orphan-disconnect")
	config := Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects,
	}
	firstRuntime := newFakeHostRuntime(objects)
	config.Runtime = firstRuntime
	first, err := New(config)
	require.NoError(t, err)
	_, err = first.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	secondRuntime := newFakeHostRuntime(objects)
	config.Runtime = secondRuntime
	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.ReconcileReleases(t.Context()))
	require.NoError(t, second.ReleaseParent(t.Context(), request.Parent, request.Identity))
	require.Equal(t, 1, secondRuntime.orphanRecoveries)
	require.Equal(t, []string{"unmount-overlay", "unmount-xfs", "recover-orphan-device"}, secondRuntime.callsSnapshot())
	stored, err := second.load(request.Parent)
	require.NoError(t, err)
	require.Equal(t, stateTombstoned, stored.State)
	require.Equal(t, stored.DeviceAllocationID, secondRuntime.reservationOwner(stored.DevicePath))

	_, err = second.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	require.Empty(t, secondRuntime.reservationOwner(stored.DevicePath))
}

func TestManagerRestartKeepsReservationWhenOrphanDisconnectFails(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	request := testStageRequest(t, objects, "restart-orphan-disconnect-failure")
	config := Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects,
	}
	firstRuntime := newFakeHostRuntime(objects)
	config.Runtime = firstRuntime
	first, err := New(config)
	require.NoError(t, err)
	_, err = first.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	secondRuntime := newFakeHostRuntime(objects)
	secondRuntime.orphanRecoveryErr = fmt.Errorf("injected orphan disconnect failure")
	config.Runtime = secondRuntime
	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.ReconcileReleases(t.Context()))
	err = second.ReleaseParent(t.Context(), request.Parent, request.Identity)
	require.ErrorContains(t, err, "injected orphan disconnect failure")
	stored, loadErr := second.load(request.Parent)
	require.NoError(t, loadErr)
	require.Equal(t, stateReleasing, stored.State)
	require.Equal(t, stored.DeviceAllocationID, secondRuntime.reservationOwner(stored.DevicePath))
}

func TestManagerLegacyReconcileDoesNotReclaimReusedPathForHistoricalTombstone(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	manager, err := New(Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects, Runtime: runtime,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	readyRequest := testStageRequest(t, objects, "legacy-live")
	tombstoneRequest := testStageRequest(t, objects, "legacy-tombstone")
	for _, candidate := range []struct {
		request rootfshandoff.StageRequest
		state   string
	}{
		{request: readyRequest, state: stateReady},
		{request: tombstoneRequest, state: stateTombstoned},
	} {
		binding, digestErr := candidate.request.WithoutWriterGrantToken().BindingDigest()
		require.NoError(t, digestErr)
		paths := sessionPaths(manager.branchRoot, manager.mountRoot, candidate.request.Parent)
		now := time.Now().UTC().Format(time.RFC3339Nano)
		var crashFence *crashFenceRecord
		if candidate.state == stateTombstoned {
			// Version 2 proofs did not establish pool-wide absence for an
			// unattached session. Revalidating this historical proof under the
			// version 3 contract would incorrectly block startup while another
			// session legitimately owns the reused device path.
			crashFence = &crashFenceRecord{OperationID: "legacy-crash", Result: &rootfshandoff.CrashFenceSessionObservation{
				Parent: candidate.request.Parent, RootFSID: candidate.request.Identity.RootFSID,
				WriterEpoch: candidate.request.Identity.WriterEpoch, OperationID: "legacy-crash",
				BindingDigest: fmt.Sprintf("%x", binding), SessionState: stateTombstoned,
				BranchPath: paths.branch, DeviceBound: false, NBDPoolAbsent: false,
				LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: now,
			}}
		}
		require.NoError(t, manager.saveNew(record{
			Version: legacySessionSchemaVersion, Parent: candidate.request.Parent, BindingDigest: fmt.Sprintf("%x", binding),
			RootFSID: candidate.request.Identity.RootFSID, WriterEpoch: candidate.request.Identity.WriterEpoch,
			GenerationID: candidate.request.InitialGeneration, BranchPath: paths.branch, DevicePath: "/dev/fake0",
			XFSRoot: paths.xfs, MergedRoot: paths.merged, State: candidate.state, CrashFence: crashFence,
			CreatedAt: now, UpdatedAt: now,
		}))
	}

	require.NoError(t, manager.ReconcileReleases(t.Context()))
	ready, err := manager.load(readyRequest.Parent)
	require.NoError(t, err)
	require.Equal(t, sessionSchemaVersion, ready.Version)
	require.NotEmpty(t, ready.DeviceAllocationID)
	require.Equal(t, ready.DeviceAllocationID, runtime.reservationOwner("/dev/fake0"))
	tombstone, err := manager.load(tombstoneRequest.Parent)
	require.NoError(t, err)
	require.Equal(t, legacySessionSchemaVersion, tombstone.Version)
	require.Empty(t, tombstone.DeviceAllocationID)
}

func TestManagerReconcileRejectsUnsupportedSessionSchema(t *testing.T) {
	manager, runtime, request := newTestManager(t, "unsupported-session-schema")
	binding, err := request.WithoutWriterGrantToken().BindingDigest()
	require.NoError(t, err)
	paths := sessionPaths(manager.branchRoot, manager.mountRoot, request.Parent)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, manager.saveNew(record{
		Version: sessionSchemaVersion + 1, Parent: request.Parent, BindingDigest: fmt.Sprintf("%x", binding),
		RootFSID: request.Identity.RootFSID, WriterEpoch: request.Identity.WriterEpoch,
		GenerationID: request.InitialGeneration, BranchPath: paths.branch,
		XFSRoot: paths.xfs, MergedRoot: paths.merged, State: stateReserved,
		CreatedAt: now, UpdatedAt: now,
	}))

	err = manager.ReconcileReleases(t.Context())
	require.ErrorContains(t, err, "unsupported schema version")
	require.Empty(t, runtime.reservationOwner("/dev/fake0"))
}

func TestManagerReconcileRejectsDuplicateLiveDeviceReservations(t *testing.T) {
	manager, runtime, first := newTestManager(t, "duplicate-live-device-first")
	second := testStageRequest(t, newSessionObjectStore(), "duplicate-live-device-second")
	for index, request := range []rootfshandoff.StageRequest{first, second} {
		binding, err := request.WithoutWriterGrantToken().BindingDigest()
		require.NoError(t, err)
		paths := sessionPaths(manager.branchRoot, manager.mountRoot, request.Parent)
		now := time.Now().UTC().Format(time.RFC3339Nano)
		require.NoError(t, manager.saveNew(record{
			Version: sessionSchemaVersion, Parent: request.Parent, BindingDigest: fmt.Sprintf("%x", binding),
			RootFSID: request.Identity.RootFSID, WriterEpoch: request.Identity.WriterEpoch,
			GenerationID: request.InitialGeneration, BranchPath: paths.branch, DevicePath: "/dev/fake0",
			DeviceAllocationID: fmt.Sprintf("duplicate-allocation-%d", index),
			XFSRoot:            paths.xfs, MergedRoot: paths.merged, State: stateReady,
			CreatedAt: now, UpdatedAt: now,
		}))
	}

	err := manager.ReconcileReleases(t.Context())
	require.ErrorContains(t, err, "reserved by both")
	require.Empty(t, runtime.reservationOwner("/dev/fake0"))
}

func TestManagerCurrentSchemaPreAttachmentProofDoesNotScanSharedDevicePool(t *testing.T) {
	manager, runtime, request := newTestManager(t, "current-pre-attachment")
	binding, err := request.WithoutWriterGrantToken().BindingDigest()
	require.NoError(t, err)
	paths := sessionPaths(manager.branchRoot, manager.mountRoot, request.Parent)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	require.NoError(t, manager.saveNew(record{
		Version: sessionSchemaVersion, Parent: request.Parent, BindingDigest: fmt.Sprintf("%x", binding),
		RootFSID: request.Identity.RootFSID, WriterEpoch: request.Identity.WriterEpoch,
		GenerationID: request.InitialGeneration, BranchPath: paths.branch, DeviceAllocationID: "allocation-pre-attach",
		XFSRoot: paths.xfs, MergedRoot: paths.merged, State: stateTombstoned,
		CreatedAt: now, UpdatedAt: now,
	}))
	runtime.fenceObservation.NBDPID = 42

	result, err := manager.CrashFence(request.WithoutWriterGrantToken(), "crash-operation")
	require.NoError(t, err)
	require.NoError(t, result.Validate())
	require.False(t, result.DeviceBound)
	require.True(t, result.NBDPoolAbsent)
	require.Equal(t, 1, runtime.preAttachmentFenceInspections)
	require.Zero(t, runtime.unattachedFenceInspections)
}

func TestManagerConcurrentExactEnsureCreatesOnePhysicalSession(t *testing.T) {
	manager, runtime, request := newTestManager(t, "concurrent")
	const workers = 32
	errorsByWorker := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := manager.Ensure(t.Context(), request)
			errorsByWorker <- err
		}()
	}
	wait.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		require.NoError(t, err)
	}
	require.Equal(t, 1, runtime.count("attach"))
	require.Equal(t, 1, runtime.count("mount-xfs"))
	require.Equal(t, 1, runtime.count("mount-overlay"))
}

func TestManagerReleaseBeforeEnsurePermanentlyFencesLateAttach(t *testing.T) {
	manager, runtime, request := newTestManager(t, "release-before-ensure")
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))

	_, err := manager.Ensure(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	require.Empty(t, runtime.callsSnapshot())
	stored, err := manager.load(request.Parent)
	require.NoError(t, err)
	require.Equal(t, stateTombstoned, stored.State)
}

func TestManagerConcurrentReleaseAndEnsureNeverLeavesALiveSession(t *testing.T) {
	for iteration := range 50 {
		manager, runtime, request := newTestManager(t, fmt.Sprintf("release-race-%d", iteration))
		start := make(chan struct{})
		result := make(chan error, 2)
		go func() {
			<-start
			_, err := manager.Ensure(t.Context(), request)
			result <- err
		}()
		go func() {
			<-start
			result <- manager.ReleaseParent(t.Context(), request.Parent, request.Identity)
		}()
		close(start)
		first, second := <-result, <-result
		require.True(t, first == nil || errdefs.IsAlreadyExists(first), "unexpected result: %v", first)
		require.True(t, second == nil || errdefs.IsAlreadyExists(second), "unexpected result: %v", second)
		require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))
		stored, err := manager.load(request.Parent)
		require.NoError(t, err)
		require.Equal(t, stateTombstoned, stored.State)
		require.LessOrEqual(t, runtime.count("attach"), 1)
	}
}

func TestManagerPlannedRetireSealsCompositeGenerationAfterWriterRevocation(t *testing.T) {
	manager, runtime, request := newTestManager(t, "planned-retire")
	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	manager.mu.Lock()
	branch := manager.live[request.Parent].branch
	manager.mu.Unlock()
	written := bytes.Repeat([]byte{0xa5}, rootfsblock.LogicalBlockSize)
	_, err = branch.WriteAt(written, rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.NoError(t, branch.Flush())

	operationID := "retire-planned"
	require.NoError(t, manager.BeginRetire(request.Parent, request.Identity, operationID))
	_, err = manager.RetireResult(request.Parent, request.Identity, operationID)
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))
	require.Equal(t, []bool{true, true}, runtime.unmountSyncSnapshot(), "planned retire must require both filesystem barriers")

	result, err := manager.RetireResult(request.Parent, request.Identity, operationID)
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DurabilityComposite, result.DurabilityState)
	require.Len(t, result.DetachProof, 32)
	sealed, err := rootfsblock.DecodeDescriptor(result.Descriptor)
	require.NoError(t, err)
	require.Equal(t, request.Generation.CurrentBlockHead, result.CurrentBlockHead)
	updates, _, err := rootfsblock.DecodeCompositeTail(*sealed.CompositeTail, uint64(sealed.LogicalSizeBytes/rootfsblock.LogicalBlockSize))
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, uint64(1), updates[0].Block)
	require.Equal(t, written, updates[0].Data)

	resultRetry, err := manager.RetireResult(request.Parent, request.Identity, operationID)
	require.NoError(t, err)
	require.Equal(t, result, resultRetry)
	path, err := runtime.ReserveDevice("post-retire-allocation")
	require.NoError(t, err)
	require.Equal(t, "/dev/fake0", path)
}

func TestManagerPlannedRetireMaterializesTailThatExceedsPostgresLimit(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	request := testStageRequestWithBlocks(t, objects, "materialized-retire", 16)
	manager, err := New(Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects, Runtime: runtime,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	_, err = manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	manager.mu.Lock()
	branch := manager.live[request.Parent].branch
	manager.mu.Unlock()
	for block := range 12 {
		_, err = branch.WriteAt(bytes.Repeat([]byte{byte(block + 1)}, rootfsblock.LogicalBlockSize), int64(block*rootfsblock.LogicalBlockSize))
		require.NoError(t, err)
	}
	require.NoError(t, branch.Flush())

	require.NoError(t, manager.BeginRetire(request.Parent, request.Identity, "retire-materialized"))
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))
	result, err := manager.RetireResult(request.Parent, request.Identity, "retire-materialized")
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DurabilityS3, result.DurabilityState)
	require.NotEqual(t, request.Generation.CurrentBlockHead, result.CurrentBlockHead)
	sealed, err := rootfsblock.DecodeDescriptor(result.Descriptor)
	require.NoError(t, err)
	require.Nil(t, sealed.CompositeTail)
	reader, err := rootfsblock.NewReader(objects, sealed, rootfsblock.DefaultReadCacheBytes)
	require.NoError(t, err)
	for block := range 12 {
		actual := make([]byte, rootfsblock.LogicalBlockSize)
		_, err = reader.ReadAt(actual, int64(block*rootfsblock.LogicalBlockSize))
		require.NoError(t, err)
		require.Equal(t, byte(block+1), actual[0])
	}
}

func TestManagerPlannedRetireRetriesMaterializationAfterRestart(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	request := testStageRequestWithBlocks(t, objects, "materialized-restart", 16)
	publisher := &failOncePublisher{next: objects}
	config := Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: publisher, Runtime: runtime,
	}
	first, err := New(config)
	require.NoError(t, err)
	_, err = first.Ensure(t.Context(), request)
	require.NoError(t, err)
	first.mu.Lock()
	branch := first.live[request.Parent].branch
	first.mu.Unlock()
	for block := range 12 {
		_, err = branch.WriteAt(bytes.Repeat([]byte{0x80 + byte(block)}, rootfsblock.LogicalBlockSize), int64(block*rootfsblock.LogicalBlockSize))
		require.NoError(t, err)
	}
	require.NoError(t, branch.Flush())
	require.NoError(t, first.BeginRetire(request.Parent, request.Identity, "retire-restart-materialized"))
	err = first.ReleaseParent(t.Context(), request.Parent, request.Identity)
	require.ErrorContains(t, err, "injected immutable publication failure")
	stored, loadErr := first.load(request.Parent)
	require.NoError(t, loadErr)
	require.Equal(t, stateReleasing, stored.State)
	require.NoError(t, first.Close())

	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.ReconcileReleases(t.Context()))
	result, err := second.RetireResult(request.Parent, request.Identity, "retire-restart-materialized")
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DurabilityS3, result.DurabilityState)
}

func TestManagerPlannedRetireRecoversBranchAfterProcessRestart(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	request := testStageRequest(t, objects, "retire-restart")
	config := Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects, Runtime: runtime,
	}
	first, err := New(config)
	require.NoError(t, err)
	_, err = first.Ensure(t.Context(), request)
	require.NoError(t, err)
	first.mu.Lock()
	branch := first.live[request.Parent].branch
	first.mu.Unlock()
	_, err = branch.WriteAt(bytes.Repeat([]byte{0x42}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, branch.Flush())
	require.NoError(t, first.BeginRetire(request.Parent, request.Identity, "retire-restart"))
	require.NoError(t, first.Close())

	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.ReleaseParent(t.Context(), request.Parent, request.Identity))
	result, err := second.RetireResult(request.Parent, request.Identity, "retire-restart")
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DurabilityComposite, result.DurabilityState)
}

func TestManagerPlannedRetireRejectsOperationReplacement(t *testing.T) {
	manager, _, request := newTestManager(t, "retire-conflict")
	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, manager.BeginRetire(request.Parent, request.Identity, "retire-a"))
	err = manager.BeginRetire(request.Parent, request.Identity, "retire-b")
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
}

func TestManagerWriterIdentityCannotBindTwoParents(t *testing.T) {
	manager, runtime, first := newTestManager(t, "writer-first")
	second := testStageRequest(t, runtime.source, "writer-second")
	second.Identity.RootFSID = first.Identity.RootFSID
	second.Identity.WriterEpoch = first.Identity.WriterEpoch
	second.Generation.FilesystemID = first.Generation.FilesystemID
	second.Generation.WriterEpoch = first.Identity.WriterEpoch - 1
	require.NoError(t, first.WithoutWriterGrantToken().ValidateDurableBinding())
	require.NoError(t, second.WithoutWriterGrantToken().ValidateDurableBinding())

	errorsByRequest := make(chan error, 2)
	var wait sync.WaitGroup
	for _, request := range []rootfshandoff.StageRequest{first, second} {
		request := request
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := manager.Ensure(t.Context(), request)
			errorsByRequest <- err
		}()
	}
	wait.Wait()
	close(errorsByRequest)
	successes := 0
	conflicts := 0
	for err := range errorsByRequest {
		if err == nil {
			successes++
		} else if errdefs.IsAlreadyExists(err) {
			conflicts++
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)
	require.Equal(t, 1, runtime.count("attach"))
}

func TestManagerMountFailureIsDurableAndReleasable(t *testing.T) {
	manager, runtime, request := newTestManager(t, "mount-failure")
	runtime.failAt = "mount-xfs"
	_, err := manager.Ensure(t.Context(), request)
	require.ErrorContains(t, err, "mount XFS")
	stored, loadErr := manager.load(request.Parent)
	require.NoError(t, loadErr)
	require.Equal(t, stateFailed, stored.State)
	require.Equal(t, []string{"attach", "mount-xfs", "close-device"}, runtime.callsSnapshot())

	_, err = manager.Ensure(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.NoError(t, manager.Release(t.Context(), request.Identity))
	stored, loadErr = manager.load(request.Parent)
	require.NoError(t, loadErr)
	require.Equal(t, stateTombstoned, stored.State)
}

func TestManagerMountFailurePreservesDeviceError(t *testing.T) {
	manager, runtime, request := newTestManager(t, "mount-device-failure")
	runtime.failAt = "mount-xfs"
	runtime.deviceCloseErr = fmt.Errorf("NBD read offset 0: object range checksum mismatch")

	_, err := manager.Ensure(t.Context(), request)
	require.ErrorContains(t, err, "mount XFS")
	require.ErrorContains(t, err, "object range checksum mismatch")
}

func TestManagerRestartNeverTreatsJournalAsLiveDevice(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	request := testStageRequest(t, objects, "restart")
	config := Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects, Runtime: runtime,
	}
	first, err := New(config)
	require.NoError(t, err)
	_, err = first.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	_, err = second.Ensure(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	_, err = second.Resolve(request.Parent, request.WithoutWriterGrantToken())
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.NoError(t, second.Release(t.Context(), request.Identity))
	stored, err := second.load(request.Parent)
	require.NoError(t, err)
	require.Equal(t, stateTombstoned, stored.State)
}

func TestManagerRejectsSymlinkStatePath(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	require.NoError(t, os.WriteFile(target, nil, 0o600))
	state := filepath.Join(base, "state.db")
	require.NoError(t, os.Symlink(target, state))
	_, err := New(Config{
		StatePath: state, BranchRoot: filepath.Join(base, "branches"), MountRoot: filepath.Join(base, "mounts"),
		Source: newSessionObjectStore(), Publisher: newSessionObjectStore(), Runtime: &fakeHostRuntime{},
	})
	require.ErrorContains(t, err, "symlink")
}

func newTestManager(t *testing.T, name string) (*Manager, *fakeHostRuntime, rootfshandoff.StageRequest) {
	t.Helper()
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	manager, err := New(Config{
		StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects, Runtime: runtime,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	return manager, runtime, testStageRequest(t, objects, name)
}

func testStageRequest(t *testing.T, objects *sessionObjectStore, name string) rootfshandoff.StageRequest {
	return testStageRequestWithBlocks(t, objects, name, 3)
}

func testStageRequestWithBlocks(t *testing.T, objects *sessionObjectStore, name string, blocks int) rootfshandoff.StageRequest {
	t.Helper()
	logical := bytes.Repeat([]byte{byte(len(name) + 1)}, blocks*rootfsblock.LogicalBlockSize)
	built, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(logical), int64(len(logical)), objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	request := rootfshandoff.StageRequest{
		BindingVersion:    rootfshandoff.WriterBindingVersion,
		Parent:            digest.FromString("parent-" + name).String(),
		InitialGeneration: "generation-" + name,
		Identity: rootfshandoff.Identity{
			NodeUID: "node", BootID: "boot", RuntimeGeneration: "runtime", PodUID: "pod-" + name,
			PodSandboxID: "sandbox-" + name, ContainerName: "app", Image: "gate-" + name,
			Snapshotter: "sandbox0-rootfs", RuntimeName: "io.containerd.runsc.v1", SlotNonce: "slot-" + name,
			ClaimID: "claim-" + name, LaunchAttempt: "attempt-" + name, RootFSID: "rootfs-" + name,
			WriterEpoch: 1, WriterGrantID: "grant-" + name, WriterGrantToken: "token-" + name,
		},
	}
	request.Identity.WriterGrantTokenDigest = rootfshandoff.WriterGrantTokenDigest(request.Identity.WriterGrantToken)
	request.ExpectedPolicyToken = rootfshandoff.NetworkPolicyToken{
		PodUID: request.Identity.PodUID, PodSandboxID: request.Identity.PodSandboxID,
		ClaimID: request.Identity.ClaimID, NetworkEpoch: request.Identity.WriterEpoch,
		PolicyDigest: "policy-" + name, PodIP: "10.0.0.2", CtldGeneration: "ctld",
		NetNSIdentity: "netns-" + name,
	}
	request.Generation = &rootfshandoff.GenerationDescriptor{
		Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: request.InitialGeneration,
		FilesystemID: request.Identity.RootFSID, SourceOCIDigest: digest.FromString("oci-" + name).String(),
		BaseArtifactDigest: digest.FromString("artifact-" + name).String(),
		BaseBlockRoot:      built.Descriptor.MappingRoot.RootDigest, CurrentBlockHead: built.Descriptor.MappingRoot.RootDigest,
		WriterEpoch: 0, FormatGeneration: 1, DurabilityState: "s3_materialized", LocatorVersion: 1,
		Descriptor: built.Payload,
	}
	require.NoError(t, request.Validate())
	return request
}

type failOncePublisher struct {
	mu   sync.Mutex
	next rootfsblock.ImmutableObjectPublisher
	fail bool
}

func (p *failOncePublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	p.mu.Lock()
	if !p.fail {
		p.fail = true
		p.mu.Unlock()
		return fmt.Errorf("injected immutable publication failure")
	}
	p.mu.Unlock()
	return p.next.PutImmutable(ctx, key, payload)
}

type sessionObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newSessionObjectStore() *sessionObjectStore {
	return &sessionObjectStore{objects: make(map[string][]byte)}
}

func (s *sessionObjectStore) PutImmutable(_ context.Context, key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, found := s.objects[key]; found && !bytes.Equal(existing, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	s.objects[key] = append([]byte(nil), payload...)
	return nil
}

func (s *sessionObjectStore) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, found := s.objects[key]
	if !found || offset < 0 || length < 0 || offset+length > int64(len(payload)) {
		return nil, fmt.Errorf("range not found")
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

type fakeHostRuntime struct {
	mu                            sync.Mutex
	calls                         []string
	unmountSync                   []bool
	failAt                        string
	devices                       int
	source                        *sessionObjectStore
	deviceCloseErr                error
	fenceObservation              CrashFenceHostObservation
	fenceErr                      error
	fenceInspections              int
	unattachedFenceInspections    int
	preAttachmentFenceInspections int
	orphanRecoveries              int
	orphanRecoveryErr             error
	reservations                  map[string]string
	devicePaths                   []string
	beforeAttach                  func(string, string)
}

func newFakeHostRuntime(objects *sessionObjectStore) *fakeHostRuntime {
	return &fakeHostRuntime{
		source: objects, devicePaths: []string{"/dev/fake0"}, reservations: make(map[string]string),
		fenceObservation: CrashFenceHostObservation{MergedMountAbsent: true, XFSMountAbsent: true},
	}
}

func (r *fakeHostRuntime) InspectCrashFence(_, _, _ string) (CrashFenceHostObservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fenceInspections++
	return r.fenceObservation, r.fenceErr
}

func (r *fakeHostRuntime) InspectUnattachedCrashFence(_, _ string) (CrashFenceHostObservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unattachedFenceInspections++
	observation := r.fenceObservation
	if observation.NBDPID != 0 || len(observation.NBDHolders) != 0 {
		return observation, fmt.Errorf("NBD pool remains owned: %w", errdefs.ErrFailedPrecondition)
	}
	observation.NBDPoolAbsent = true
	return observation, r.fenceErr
}

func (r *fakeHostRuntime) InspectPreAttachmentCrashFence(_, _ string) (CrashFenceHostObservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.preAttachmentFenceInspections++
	return CrashFenceHostObservation{
		NBDPoolAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true,
	}, r.fenceErr
}

func (r *fakeHostRuntime) ReserveDevice(allocationID string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureReservationsLocked()
	for path, owner := range r.reservations {
		if owner == allocationID {
			return path, nil
		}
	}
	for _, path := range r.devicePaths {
		if r.reservations[path] == "" {
			r.reservations[path] = allocationID
			return path, nil
		}
	}
	return "", fmt.Errorf("no usable NBD device is available")
}

func (r *fakeHostRuntime) AdoptDeviceReservation(devicePath, allocationID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureReservationsLocked()
	if owner := r.reservations[devicePath]; owner != "" && owner != allocationID {
		return fmt.Errorf("device already reserved")
	}
	r.reservations[devicePath] = allocationID
	return nil
}

func (r *fakeHostRuntime) ReleaseDeviceReservation(devicePath, allocationID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureReservationsLocked()
	if r.reservations[devicePath] == allocationID {
		delete(r.reservations, devicePath)
	}
}

func (r *fakeHostRuntime) RecoverOrphanDevice(_ context.Context, devicePath, allocationID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureReservationsLocked()
	if r.reservations[devicePath] != allocationID || allocationID == "" {
		return fmt.Errorf("device is not reserved by allocation")
	}
	r.orphanRecoveries++
	r.calls = append(r.calls, "recover-orphan-device")
	return r.orphanRecoveryErr
}

func (r *fakeHostRuntime) reservationOwner(devicePath string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureReservationsLocked()
	return r.reservations[devicePath]
}

func (r *fakeHostRuntime) ensureReservationsLocked() {
	if r.reservations == nil {
		r.reservations = make(map[string]string)
	}
	if len(r.devicePaths) == 0 {
		r.devicePaths = []string{"/dev/fake0"}
	}
}

func (r *fakeHostRuntime) AttachDevice(
	lifetime, readyContext context.Context,
	devicePath, allocationID string,
	_ rootfsblock.WritableBlockDevice,
) (Device, error) {
	if lifetime == nil || readyContext == nil {
		return nil, fmt.Errorf("contexts are required")
	}
	r.mu.Lock()
	r.ensureReservationsLocked()
	owner := r.reservations[devicePath]
	beforeAttach := r.beforeAttach
	r.mu.Unlock()
	if owner != allocationID || owner == "" {
		return nil, fmt.Errorf("device is not reserved by allocation")
	}
	if beforeAttach != nil {
		beforeAttach(devicePath, allocationID)
	}
	r.record("attach")
	if r.failAt == "attach" {
		return nil, fmt.Errorf("injected attach failure")
	}
	return &fakeDevice{path: devicePath, runtime: r, closeErr: r.deviceCloseErr}, nil
}

func (r *fakeHostRuntime) MountXFS(_, _ string) error {
	r.record("mount-xfs")
	if r.failAt == "mount-xfs" {
		return fmt.Errorf("injected XFS mount failure")
	}
	return nil
}

func (r *fakeHostRuntime) MountOverlay(_, _ string) error {
	r.record("mount-overlay")
	if r.failAt == "mount-overlay" {
		return fmt.Errorf("injected OverlayFS mount failure")
	}
	return nil
}

func (r *fakeHostRuntime) UnmountOverlay(_ string, requireSync bool) error {
	r.record("unmount-overlay")
	r.recordUnmountSync(requireSync)
	return nil
}

func (r *fakeHostRuntime) UnmountXFS(_ string, requireSync bool) error {
	r.record("unmount-xfs")
	r.recordUnmountSync(requireSync)
	return nil
}

func (r *fakeHostRuntime) recordUnmountSync(requireSync bool) {
	r.mu.Lock()
	r.unmountSync = append(r.unmountSync, requireSync)
	r.mu.Unlock()
}

func (r *fakeHostRuntime) record(call string) {
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.mu.Unlock()
}

func (r *fakeHostRuntime) callsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *fakeHostRuntime) unmountSyncSnapshot() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.unmountSync...)
}

func (r *fakeHostRuntime) count(call string) int {
	count := 0
	for _, current := range r.callsSnapshot() {
		if current == call {
			count++
		}
	}
	return count
}

type fakeDevice struct {
	path     string
	runtime  *fakeHostRuntime
	closeErr error
	once     sync.Once
}

func (d *fakeDevice) Path() string { return d.path }

func (d *fakeDevice) Close() error {
	d.once.Do(func() { d.runtime.record("close-device") })
	return d.closeErr
}
