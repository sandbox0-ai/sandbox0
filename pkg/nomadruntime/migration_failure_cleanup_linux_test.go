//go:build linux

package nomadruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func failureDestinationFixture(t *testing.T, kind runtimecontrol.CheckpointRestoreKind) (*nodeRuntime, protocol.MigrationRestoreObservation, *migrationImageDownloadTestRuntime, *migrationSourceTestRunsc) {
	t.Helper()
	if kind != "" {
		d, image, runtime := checkpointDestinationFixture(t, kind)
		return migrationRestoreForImageFixture(t, d, image, runtime)
	}
	return migrationRestoreNodeFixture(t, func(d *nodeRuntime, image protocol.MigrationImagePrepareRequest) {
		resources, err := image.Resources.Digest()
		require.NoError(t, err)
		_, err = d.ReserveMigrationStaging(t.Context(), protocol.MigrationStagingRequest{Target: image.Target, Source: image.Publication.Capture.Request,
			Destination: image.Target, DestinationResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), Bytes: 8 << 20, Inodes: 64})
		require.NoError(t, err)
	})
}

func TestMigrationFailureCleanupRetainsImageAndExternalCrashJournal(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{"", runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		name := string(kind)
		if name == "" {
			name = "migration"
		}
		t.Run(name, func(t *testing.T) {
			for _, attached := range []bool{false, true} {
				t.Run(map[bool]string{false: "canceled-unattached", true: "consumed-attached"}[attached], func(t *testing.T) {
					d, observation, _, runner := failureDestinationFixture(t, kind)
					record, err := d.journal.Get(observation.Request.Image.Target.SlotID)
					require.NoError(t, err)
					r := record.Registration
					observation.Request.Stage.ExpectedPolicyToken.NetNSIdentity = r.NetNSIdentity
					failure := protocol.MigrationFailureRequest{Restore: observation.Request, Reason: protocol.MigrationFailureDestinationUnavailable}
					d.runner = &migrationFenceTestRunsc{migrationSourceTestRunsc: runner}
					stopped, err := d.StopFailedMigrationDestination(t.Context(), failure)
					require.NoError(t, err)
					c := testRuntimeSlotJournalCleanup(r)
					c.NodeUID = d.nodeUID
					c.OperationID = protocol.MigrationFailureCleanupOperationID(failure.Restore.Image.OperationID())
					c.WriterOperationID = c.OperationID
					c.WriterRetireKind = protocol.WriterRetireKindCanceled
					if attached {
						c.WriterRetireKind = protocol.WriterRetireKindCrashAbandon
					}
					c.WriterGrantID = failure.Restore.Stage.Identity.WriterGrantID
					c.WriterAuthorityDigest = stopped.RequestDigest
					c.Resources = failure.Restore.Image.Resources
					resources, err := c.Resources.Digest()
					require.NoError(t, err)
					c.ResourceLeaseDigest = strings.TrimPrefix(resources, "sha256:")
					request := protocol.MigrationFailureCleanupRequest{Failure: protocol.MigrationFailureStopReceipt{Request: failure, Proof: *stopped}, Cleanup: c}
					_, err = request.Digest()
					require.NoError(t, err)
					runtime := &cleanupRootFSRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}}
					if attached {
						namespace, err := os.Readlink("/proc/self/ns/mnt")
						require.NoError(t, err)
						runtime.recovery = []rootfssession.RecoverySession{{Stage: failure.Restore.Stage, Kind: rootfssession.RecoveryCrashAbandon}}
						runtime.proof = testUnboundLocalCrashProof(t, failure.Restore.Stage, c.WriterOperationID, namespace)
					}
					d.runtime = runtime
					d.mounter = &fakeMounter{}
					d.runtimeSlotNetwork = newFakeCtldNetwork(t)
					cgroups := &fakeRuntimeResourceCgroup{}
					d.resourceCgroups = cgroups
					d.config.RootFSConsumerMountRoot = filepath.Dir(r.StableMount)
					d.config.RootFSConsumerNetNSRoot = filepath.Dir(r.NetNSPath)
					require.NoError(t, os.Remove(r.NetNSPath))
					_, err = d.CleanupRuntimeSlot(t.Context(), c)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					_, err = d.CleanupFailedMigrationDestination(t.Context(), request)
					require.ErrorContains(t, err, "resource cgroup remains present")
					record, err = d.journal.Get(c.SlotID)
					require.NoError(t, err)
					require.NotNil(t, record.MigrationDestination.Failure.Cleanup)
					require.NotNil(t, record.Cleanup)
					require.Nil(t, record.Proof)
					cgroups.removeOK = true
					proof, err := d.CleanupFailedMigrationDestination(t.Context(), request)
					require.NoError(t, err)
					require.NoError(t, proof.ValidateFor(request))
					record, err = d.journal.Get(c.SlotID)
					require.NoError(t, err)
					expectedVersion := runtimeSlotFailureCleanupJournalVersion
					if kind != "" {
						expectedVersion = runtimeSlotCheckpointRestoreJournalVersion
					}
					require.Equal(t, expectedVersion, record.Version)
					if kind == "" {
						require.NotNil(t, record.MigrationStaging)
						require.False(t, record.MigrationStaging.Released)
					} else {
						require.Nil(t, record.MigrationStaging)
					}
					require.FileExists(t, filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
					require.Zero(t, runtime.externalReclaims)
					require.Zero(t, runtime.reclaimCalls)
					if attached {
						require.True(t, runtime.recovery[0].ExternalCrash)
						require.Equal(t, c.WriterOperationID, runtime.recovery[0].CrashOperationID)
					}
					path := d.journal.db.Path()
					require.NoError(t, d.journal.Close())
					reopened, err := newRuntimeSlotJournal(path, time.Hour)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, reopened.Close()) })
					d.journal = reopened
					retry, err := d.CleanupFailedMigrationDestination(t.Context(), request)
					require.NoError(t, err)
					require.Equal(t, proof, retry)
					pruned, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
					require.NoError(t, err)
					require.Zero(t, pruned)
					record.Version = runtimeSlotFailureJournalVersion
					payload, err := json.Marshal(record)
					require.NoError(t, err)
					_, err = decodeRuntimeSlotJournalRecord(payload)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					finalizer := &failedTargetFinalizeTestRuntime{cleanupRootFSRuntime: runtime, err: errdefs.ErrPermissionDenied}
					d.runtime = finalizer
					finalization := protocol.MigrationFailureFinalizeRequest{Request: request, Proof: *proof}
					_, err = d.FinalizeFailedMigrationDestination(t.Context(), finalization)
					require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
					require.FileExists(t, filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
					finalizer.err = nil
					finalized, err := d.FinalizeFailedMigrationDestination(t.Context(), finalization)
					require.NoError(t, err)
					require.NoError(t, finalized.ValidateFor(finalization))
					require.NoDirExists(t, record.MigrationDestination.ImageDirectory)
					record, err = d.journal.Get(c.SlotID)
					require.NoError(t, err)
					expectedVersion = runtimeSlotFailureFinalizeJournalVersion
					if kind != "" {
						expectedVersion = runtimeSlotCheckpointRestoreJournalVersion
					}
					require.Equal(t, expectedVersion, record.Version)
					require.Equal(t, finalized, record.MigrationDestination.Failure.Cleanup.Finalization.Proof)
					if kind == "" {
						require.False(t, record.MigrationStaging.Released)
						require.NoError(t, d.ReleaseMigrationStaging(t.Context(), record.MigrationStaging.Request))
					}
					require.NoError(t, reopened.Close())
					reopened, err = newRuntimeSlotJournal(path, time.Hour)
					require.NoError(t, err)
					d.journal = reopened
					finalizer.err = errdefs.ErrUnavailable
					replayed, err := d.FinalizeFailedMigrationDestination(t.Context(), finalization)
					require.NoError(t, err, "lost response retries the durable receipt after journal reopen")
					require.Equal(t, finalized, replayed)
					require.Equal(t, 2, finalizer.calls)
					pruned, err = d.journal.Prune(time.Now().Add(48 * time.Hour))
					require.NoError(t, err)
					require.Zero(t, pruned, "allocation custody remains until regional acknowledgement")
					record.Version = runtimeSlotFailureCleanupJournalVersion
					payload, err = json.Marshal(record)
					require.NoError(t, err)
					_, err = decodeRuntimeSlotJournalRecord(payload)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					gc := protocol.MigrationSourceGCRequest{Target: observation.Request.Image.Target,
						FinalizationDigest: finalized.RequestDigest, CleanupProofDigest: proof.Cleanup.ProofDigest,
						AllocationAbsenceDigest: strings.Repeat("9", 64)}
					changed := gc
					changed.Target.NodeBootID = "another-boot"
					_, err = d.AcknowledgeMigrationSourceGC(t.Context(), changed)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					ack, err := d.AcknowledgeMigrationSourceGC(t.Context(), gc)
					require.NoError(t, err)
					require.NoError(t, ack.ValidateFor(gc))
					record, err = d.journal.Get(c.SlotID)
					require.NoError(t, err)
					expectedVersion = runtimeSlotFailureGCJournalVersion
					if kind != "" {
						expectedVersion = runtimeSlotCheckpointRestoreJournalVersion
					}
					require.Equal(t, expectedVersion, record.Version)
					require.NoError(t, reopened.Close())
					reopened, err = newRuntimeSlotJournal(path, time.Hour)
					require.NoError(t, err)
					d.journal = reopened
					ack, err = d.AcknowledgeMigrationSourceGC(t.Context(), gc)
					require.NoError(t, err)
					require.NoError(t, ack.ValidateFor(gc))
					changed = gc
					changed.AllocationAbsenceDigest = strings.Repeat("8", 64)
					_, err = d.AcknowledgeMigrationSourceGC(t.Context(), changed)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					record.Version = runtimeSlotFailureFinalizeJournalVersion
					payload, err = json.Marshal(record)
					require.NoError(t, err)
					_, err = decodeRuntimeSlotJournalRecord(payload)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					pruned, err = d.journal.Prune(time.Now().Add(48 * time.Hour))
					require.NoError(t, err)
					require.Equal(t, 1, pruned)
					ack, err = d.AcknowledgeMigrationSourceGC(t.Context(), gc)
					require.NoError(t, err, "lost acknowledgement remains retryable after pruning")
					require.NoError(t, ack.ValidateFor(gc))
				})
			}
		})
	}
}

type failedTargetFinalizeTestRuntime struct {
	*cleanupRootFSRuntime
	err   error
	calls int
}

func (r *failedTargetFinalizeTestRuntime) FinalizeFailedMigrationRootFS(context.Context, rootfshandoff.StageRequest) error {
	r.calls++
	return r.err
}
