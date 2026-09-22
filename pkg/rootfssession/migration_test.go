package session

import (
	"bytes"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func migrationCutRequest(t *testing.T, stage rootfshandoff.StageRequest) rootfshandoff.MigrationRootFSCutRequest {
	t.Helper()
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	return rootfshandoff.MigrationRootFSCutRequest{OperationID: "migration-operation", GenerationID: "migration-generation",
		CaptureRequestDigest: strings.Repeat("ab", 32), SourceBindingDigest: hex.EncodeToString(binding[:])}
}

func TestMigrationRootFSCutFreezesAndRetainsExactState(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-cut")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	branch := manager.live[stage.Parent].branch
	_, err = branch.WriteAt(bytes.Repeat([]byte{0x42}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	cut, err := manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.NoError(t, cut.ValidateFor(stage, request))
	require.Equal(t, stage.Identity.RootFSID, cut.Generation.FilesystemID)
	require.Positive(t, cut.Sequence)
	require.Equal(t, []string{"attach", "mount-xfs", "mount-overlay", "freeze-xfs", "thaw-xfs"}, runtime.callsSnapshot())
	require.NoError(t, manager.ReconcileFreezes(t.Context()))
	require.Equal(t, 1, runtime.count("thaw-xfs"), "ordinary recovery must not reopen a sealed cut")
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, cut.Sequence, *stored.Migration.Sequence)
	recovery, err := manager.RecoverySession(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, RecoveryMigration, recovery.Kind)
	require.ErrorIs(t, manager.Release(t.Context(), stage.Identity), errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, manager.BeginRetire(stage.Parent, stage.Identity, "ordinary-pause"), errdefs.ErrFailedPrecondition)
	_, err = manager.Ensure(t.Context(), stage)
	require.Error(t, err, "cut source may not become an ordinary live writer again")
	// Callers cannot mutate the descriptor retained for an exact retry.
	expected := cloneMigrationRootFSCut(cut)
	cut.Generation.Descriptor[0] ^= 1
	retry, err := manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, expected, retry)
	changed := request
	changed.CaptureRequestDigest = strings.Repeat("cd", 32)
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
}

func TestMigrationRootFSCutNeverPromotesAnUnconfirmedFreeze(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-freeze-failure")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	runtime.failAt = "freeze-xfs"
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.Error(t, err)
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, stored.Migration.Sequence)
	require.Nil(t, stored.Migration.Result)
	runtime.failAt = ""
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.NoError(t, manager.ReconcileFreezes(t.Context()))
	require.NotContains(t, runtime.callsSnapshot(), "thaw-xfs")
	require.ErrorIs(t, manager.Release(t.Context(), stage.Identity), errdefs.ErrFailedPrecondition)
}

func TestMigrationRootFSCutRetriesThawWithoutRecapturing(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-thaw-retry")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	runtime.failAt = "thaw-xfs"
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.ErrorContains(t, err, "thaw sealed migration")
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.NotNil(t, stored.Migration.Result)
	require.Equal(t, request.OperationID, stored.FreezeOperationID)
	expected := cloneMigrationRootFSCut(*stored.Migration.Result)
	runtime.failAt = ""
	cut, err := manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, expected, cut)
	require.Equal(t, 1, runtime.count("freeze-xfs"))
	require.Equal(t, 2, runtime.count("thaw-xfs"))
	stored, err = manager.load(stage.Parent)
	require.NoError(t, err)
	require.Empty(t, stored.FreezeOperationID)
	require.Equal(t, sessionSchemaVersion, stored.Version)
	// A post-cut housekeeping write must not change a durable retry result.
	_, err = manager.live[stage.Parent].branch.WriteAt(bytes.Repeat([]byte{0x71}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	cut, err = manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, expected, cut)
}

func TestMigrationRootFSCutRecoversOnlyTheSyncedWALBoundary(t *testing.T) {
	for _, synced := range []bool{true, false} {
		t.Run(map[bool]string{true: "synced", false: "uncertain"}[synced], func(t *testing.T) {
			base := t.TempDir()
			objects := newSessionObjectStore()
			runtime := newFakeHostRuntime(objects)
			stage := testStageRequest(t, objects, "migration-restart")
			config := Config{StatePath: filepath.Join(base, "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
				MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: objects, Runtime: runtime}
			first, err := New(config)
			require.NoError(t, err)
			_, err = first.Ensure(t.Context(), stage)
			require.NoError(t, err)
			stage = stage.WithoutWriterGrantToken()
			request := migrationCutRequest(t, stage)
			branch := first.live[stage.Parent].branch
			_, err = branch.WriteAt(bytes.Repeat([]byte{0x65}, rootfsblock.LogicalBlockSize), 0)
			require.NoError(t, err)
			cut, err := first.CaptureMigrationRootFS(t.Context(), stage, request)
			require.NoError(t, err)
			// Emulate owner death before the sealed result became durable. The
			// earlier independently persisted sync boundary is the only replay gate.
			stored, err := first.load(stage.Parent)
			require.NoError(t, err)
			stored.Migration.Result = nil
			stored.FreezeOperationID = request.OperationID
			if !synced {
				stored.Migration.Sequence = nil
			}
			require.NoError(t, first.save(stored))
			require.NoError(t, first.Close())
			second, err := New(config)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, second.Close()) })
			require.NoError(t, second.ReconcileFreezes(t.Context()))
			require.Equal(t, 1, runtime.count("thaw-xfs"), "generic recovery does not thaw incomplete custody")
			recovered, err := second.CaptureMigrationRootFS(t.Context(), stage, request)
			if synced {
				require.NoError(t, err)
				require.Equal(t, cut, recovered)
			} else {
				require.ErrorIs(t, err, errdefs.ErrUnavailable)
			}
		})
	}
}

func TestMigrationRootFSCutRejectsWALChangesAfterSync(t *testing.T) {
	manager, _, stage := newTestManager(t, "migration-changed-wal")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	stored.Migration.Result = nil
	require.NoError(t, manager.save(stored))
	// Bypass frozen XFS to simulate an invalid physical writer after the cut.
	_, err = manager.live[stage.Parent].branch.WriteAt(bytes.Repeat([]byte{0x78}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestMigrationRootFSCutRetriesObjectPublicationAfterRestart(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	runtime := newFakeHostRuntime(objects)
	stage := testStageRequest(t, objects, "migration-object-outage")
	config := Config{StatePath: filepath.Join(base, "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
		MountRoot: filepath.Join(base, "mounts"), Source: objects, Publisher: &failOncePublisher{next: objects}, Runtime: runtime}
	first, err := New(config)
	require.NoError(t, err)
	_, err = first.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	_, err = first.live[stage.Parent].branch.WriteAt(bytes.Repeat([]byte{0x57}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	_, err = first.CaptureMigrationRootFS(t.Context(), stage, request)
	require.ErrorContains(t, err, "injected immutable publication failure")
	stored, err := first.load(stage.Parent)
	require.NoError(t, err)
	require.NotNil(t, stored.Migration.Sequence)
	require.Nil(t, stored.Migration.Result)
	require.NoError(t, first.Close())
	config.Publisher = objects
	second, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	cut, err := second.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, *stored.Migration.Sequence, cut.Sequence)
	require.Equal(t, rootfsblock.DurabilityS3, cut.Generation.DurabilityState)
	descriptor, err := rootfsblock.DecodeDescriptor(cut.Generation.Descriptor)
	require.NoError(t, err)
	require.Nil(t, descriptor.CompositeTail, "migration blocks must already be regional immutable objects")
	retry, err := second.CaptureMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, cut, retry)
	require.Equal(t, 1, runtime.count("thaw-xfs"))
}

func TestMigrationRootFSCutPublicationDoesNotBlockConsumerRenewal(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-renewal")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	consumer := ConsumerRegistration{LeaseID: "migration-consumer", ActiveKey: "migration-slot", ContainerID: "migration-runsc",
		StableMount: "/var/lib/nomad/migration/rootfs", HostMountNamespace: "mnt:[4026531841]",
		LeaseExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	require.NoError(t, manager.RegisterConsumer(stage.Parent, stage.Identity, consumer))
	publisher := &blockingPublisher{next: manager.publisher, started: make(chan struct{}), release: make(chan struct{})}
	manager.publisher = publisher
	_, err = manager.live[stage.Parent].branch.WriteAt(bytes.Repeat([]byte{0x34}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := migrationCutRequest(t, stage)
	expiresAt := time.Now().Add(2 * time.Minute).UTC()
	done := make(chan error, 1)
	go func() { _, err := manager.CaptureMigrationRootFS(t.Context(), stage, request); done <- err }()
	defer func() {
		close(publisher.release)
		require.NoError(t, <-done)
		stored, err := manager.load(stage.Parent)
		require.NoError(t, err)
		require.Equal(t, expiresAt.Format(time.RFC3339Nano), stored.Consumer.LeaseExpiresAt,
			"publishing the cut must preserve concurrent consumer renewal")
	}()
	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("migration did not reach immutable publication")
	}
	renewed := make(chan error, 1)
	go func() { renewed <- manager.RenewConsumer(stage.Parent, stage.Identity, consumer.LeaseID, expiresAt) }()
	select {
	case err := <-renewed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("slow immutable publication blocked the carrier lease")
	}
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, request)
	require.ErrorIs(t, err, errdefs.ErrUnavailable, "one cut may have only one publisher")
	require.ErrorIs(t, manager.Release(t.Context(), stage.Identity), errdefs.ErrFailedPrecondition)
	require.NotContains(t, runtime.callsSnapshot(), "thaw-xfs")
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, expiresAt.Format(time.RFC3339Nano), stored.Consumer.LeaseExpiresAt)
}

func TestMigrationRootFSDetachRetainsPublishedCutAndAttestsPhysicalAbsence(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-detach")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	cut, err := manager.CaptureMigrationRootFS(t.Context(), stage, migrationCutRequest(t, stage))
	require.NoError(t, err)
	request := rootfshandoff.MigrationRootFSDetachRequest{OperationID: cut.Request.OperationID, CutDigest: cut.Digest, AuthorizationDigest: strings.Repeat("cd", 32)}
	proof, err := manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(stage, cut, request))
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, stateTombstoned, stored.State)
	require.Equal(t, cut, *stored.Migration.Result, "unmount must not replace the captured filesystem")
	require.Empty(t, stored.SealedDescriptor)
	require.Nil(t, stored.CrashFence)
	require.True(t, stored.DeviceReservationReleased)
	require.Equal(t, []string{"attach", "mount-xfs", "mount-overlay", "freeze-xfs", "thaw-xfs", "unmount-overlay", "unmount-xfs", "close-device"}, runtime.callsSnapshot())
	before := runtime.callsSnapshot()
	again, err := manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, proof, again)
	require.Equal(t, before, runtime.callsSnapshot())
	_, err = manager.CaptureMigrationRootFS(t.Context(), stage, cut.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	_, err = manager.CrashFenceExternal(stage, "ordinary-crash")
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Error(t, manager.ReclaimTerminalArtifacts(stage.Parent, stage.Identity), "custody evidence needs explicit regional completion")
	recovery, err := manager.RecoverySession(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, RecoveryMigration, recovery.Kind)
}

func TestMigrationRootFSDetachRejectsUnconfirmedCutAndChangedAuthorization(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-detach-authority")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	request := rootfshandoff.MigrationRootFSDetachRequest{OperationID: "migration-operation", CutDigest: strings.Repeat("ab", 32), AuthorizationDigest: strings.Repeat("cd", 32)}
	_, err = manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.Error(t, err)
	cut, err := manager.CaptureMigrationRootFS(t.Context(), stage, migrationCutRequest(t, stage))
	require.NoError(t, err)
	_, err = manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	request.CutDigest = cut.Digest
	runtime.failAt = "unmount-overlay"
	_, err = manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.Error(t, err)
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, request, *stored.Migration.DetachRequest)
	require.Nil(t, stored.Migration.DetachProof)
	require.NoError(t, manager.ReconcileFreezes(t.Context()))
	require.NoError(t, manager.ReconcileReleases(t.Context()), "generic recovery cannot reinterpret handoff")
	changed := request
	changed.AuthorizationDigest = strings.Repeat("ef", 32)
	_, err = manager.DetachMigrationRootFS(t.Context(), stage, changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	runtime.failAt = ""
	proof, err := manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(stage, cut, request))
}

func TestMigrationRootFSDetachRequiresInspectionBeforeReleasingDevice(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-detach-inspection")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	cut, err := manager.CaptureMigrationRootFS(t.Context(), stage, migrationCutRequest(t, stage))
	require.NoError(t, err)
	request := rootfshandoff.MigrationRootFSDetachRequest{OperationID: cut.Request.OperationID, CutDigest: cut.Digest, AuthorizationDigest: strings.Repeat("cd", 32)}
	runtime.fenceObservation.NBDPID = 123
	_, err = manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	stored, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, stored.Migration.DetachProof)
	require.False(t, stored.DeviceReservationReleased)
	runtime.fenceObservation.NBDPID = 0
	_, err = manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
}

func TestMigrationRootFSDetachRecoversInterruptedPhysicalRelease(t *testing.T) {
	manager, runtime, stage := newTestManager(t, "migration-detach-restart")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	cut, err := manager.CaptureMigrationRootFS(t.Context(), stage, migrationCutRequest(t, stage))
	require.NoError(t, err)
	request := rootfshandoff.MigrationRootFSDetachRequest{OperationID: cut.Request.OperationID, CutDigest: cut.Digest, AuthorizationDigest: strings.Repeat("cd", 32)}
	runtime.failAt = "unmount-overlay"
	_, err = manager.DetachMigrationRootFS(t.Context(), stage, request)
	require.Error(t, err)
	config := Config{StatePath: manager.db.Path(), BranchRoot: manager.branchRoot, MountRoot: manager.mountRoot, Source: runtime.source, Publisher: runtime.source, Runtime: runtime}
	require.NoError(t, manager.Close())
	runtime.failAt = ""
	restarted, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	require.NoError(t, restarted.ReconcileFreezes(t.Context()))
	require.NoError(t, restarted.ReconcileReleases(t.Context()))
	proof, err := restarted.DetachMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(stage, cut, request))
	stored, err := restarted.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, cut, *stored.Migration.Result)
	require.Positive(t, runtime.orphanRecoveries)
	require.NoError(t, restarted.Close())
	completed, err := New(config)
	require.NoError(t, err, "completed migration proof must allow device reservation recovery")
	t.Cleanup(func() { require.NoError(t, completed.Close()) })
	require.NoError(t, completed.ReconcileReleases(t.Context()))
	again, err := completed.DetachMigrationRootFS(t.Context(), stage, request)
	require.NoError(t, err)
	require.Equal(t, proof, again)
}
