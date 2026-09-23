//go:build linux

package nomadruntime

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationCaptureFailureFixture(t *testing.T, invalidated bool) (*nodeRuntime, protocol.MigrationCaptureFailureRequest, *cleanupRootFSRuntime) {
	t.Helper()
	return captureFailureFixture(t, invalidated, false)
}

func captureFailureFixture(t *testing.T, invalidated, captureOnly bool) (*nodeRuntime, protocol.MigrationCaptureFailureRequest, *cleanupRootFSRuntime) {
	t.Helper()
	d, capture, runtime, runner := migrationRootFSNodeFixture(t)
	record, err := d.journal.Get(capture.Request.Target.SlotID)
	require.NoError(t, err)
	r := record.Registration
	session := runtime.recoverySessions[0]
	session.Stage.ExpectedPolicyToken.NetNSIdentity = r.NetNSIdentity
	session.Consumer.NetNSIdentity = r.NetNSIdentity
	session.Consumer.NetNSPath = r.NetNSPath
	session.Consumer.NetworkChain = r.NetworkChain
	resources := migrationSourceTestResources(t, session.Stage, capture.Request.Target)
	rd, err := resources.Digest()
	require.NoError(t, err)
	capture.Request.ResourceLeaseDigest = strings.TrimPrefix(rd, "sha256:")
	binding, err := session.Stage.BindingDigest()
	require.NoError(t, err)
	capture.Request.BindingDigest = hex.EncodeToString(binding[:])
	capture.RequestDigest, err = capture.Request.Digest()
	require.NoError(t, err)
	// Use a fresh journal for this fixture's complete physical binding.
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "failure.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	d.journal = journal
	require.NoError(t, journal.Register(r))
	staging := protocol.MigrationStagingRequest{Target: capture.Request.Target, Source: capture.Request,
		Destination:                    protocol.NodeChannelTarget{ClusterID: r.ClusterID, NodeID: "target-node", NodeUID: "target-uid", NodeBootID: "target-boot", SlotID: "target-slot", AllocationID: "target-alloc", ControlEndpoint: "unix:///target.sock"},
		DestinationResourceLeaseDigest: strings.Repeat("b", 64), Bytes: 8 << 20, Inodes: 64}
	if captureOnly {
		staging.CaptureOnly = true
		staging.Destination = protocol.NodeChannelTarget{}
		staging.DestinationResourceLeaseDigest = ""
	}
	_, err = d.ReserveMigrationStaging(t.Context(), staging)
	require.NoError(t, err)
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	capture.State = protocol.MigrationCaptureUncertain
	if invalidated {
		completed := capture
		completed.State = protocol.MigrationCaptureComplete
		require.NoError(t, d.RecordMigrationCapture(t.Context(), completed))
		require.NoError(t, journal.invalidateMigrationExecution(r.SlotID))
	} else {
		require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	}
	c := testRuntimeSlotJournalCleanup(r)
	c.NodeUID = d.nodeUID
	c.OperationID = protocol.MigrationCaptureFailureOperationID(capture.Request.OperationID)
	c.WriterOperationID = c.OperationID
	c.WriterRetireKind = protocol.WriterRetireKindCrashAbandon
	c.WriterGrantID = session.Stage.Identity.WriterGrantID
	c.WriterAuthorityDigest = capture.RequestDigest
	c.Resources, c.ResourceLeaseDigest = resources, capture.Request.ResourceLeaseDigest
	request := protocol.MigrationCaptureFailureRequest{Capture: capture, Cleanup: c}
	_, err = request.Digest()
	require.NoError(t, err)
	cleanup := &cleanupRootFSRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}, recovery: []rootfssession.RecoverySession{session},
		proof: testLocalCrashProof(t, session.Stage, *session.Consumer, c.WriterOperationID, r.MountNamespaceID)}
	d.runtime = cleanup
	d.runner = &migrationFenceTestRunsc{migrationSourceTestRunsc: runner}
	d.mounter = &fakeMounter{}
	d.runtimeSlotNetwork = newFakeCtldNetwork(t)
	d.resourceCgroups = &fakeRuntimeResourceCgroup{}
	d.config.RootFSConsumerMountRoot = filepath.Dir(r.StableMount)
	d.config.RootFSConsumerNetNSRoot = filepath.Dir(r.NetNSPath)
	require.NoError(t, os.Remove(r.NetNSPath))
	record, err = journal.Get(c.SlotID)
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(record.Migration.ImageDirectory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(record.Migration.ImageDirectory, "checkpoint.img"), []byte("unusable"), 0o600))
	return d, request, cleanup
}

func TestMigrationCaptureFailureRetainsInvalidationThroughCleanupAndGC(t *testing.T) {
	for _, captureOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "migration", true: "checkpoint"}[captureOnly], func(t *testing.T) {
			for _, invalidated := range []bool{false, true} {
				t.Run(map[bool]string{false: "uncertain-call", true: "invalidated-completed-call"}[invalidated], func(t *testing.T) {
					d, request, runtime := captureFailureFixture(t, invalidated, captureOnly)
					_, err := d.CleanupRuntimeSlot(t.Context(), request.Cleanup)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					_, err = d.CleanupFailedMigrationCapture(t.Context(), request)
					require.ErrorContains(t, err, "resource cgroup remains present")
					record, err := d.journal.Get(request.Cleanup.SlotID)
					require.NoError(t, err)
					require.NotNil(t, record.Migration.Failure)
					require.True(t, record.Migration.ExecutionInvalidated)
					require.Nil(t, record.Proof)
					require.True(t, runtime.recovery[0].ExternalCrash)
					require.Zero(t, runtime.reclaimCalls)
					require.FileExists(t, filepath.Join(record.Migration.ImageDirectory, "checkpoint.img"))
					require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), record.MigrationStaging.Request), errdefs.ErrFailedPrecondition)
					d.resourceCgroups.(*fakeRuntimeResourceCgroup).removeOK = true
					proof, err := d.CleanupFailedMigrationCapture(t.Context(), request)
					require.NoError(t, err)
					require.NoError(t, proof.ValidateFor(request))
					path := d.journal.db.Path()
					require.NoError(t, d.journal.Close())
					reopened, err := newRuntimeSlotJournal(path, time.Hour)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, reopened.Close()) })
					d.journal = reopened
					replayed, err := d.CleanupFailedMigrationCapture(t.Context(), request)
					require.NoError(t, err)
					require.Equal(t, proof, replayed)
					finalizer := &failedTargetFinalizeTestRuntime{cleanupRootFSRuntime: runtime, err: errdefs.ErrPermissionDenied}
					d.runtime = finalizer
					final := protocol.MigrationCaptureFailureFinalizeRequest{Request: request, Proof: *proof}
					_, err = d.FinalizeFailedMigrationCapture(t.Context(), final)
					require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
					require.FileExists(t, filepath.Join(record.Migration.ImageDirectory, "checkpoint.img"))
					pruned, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
					require.NoError(t, err)
					require.Zero(t, pruned)
					finalizer.err = nil
					finalProof, err := d.FinalizeFailedMigrationCapture(t.Context(), final)
					require.NoError(t, err)
					require.NoError(t, finalProof.ValidateFor(final))
					require.NoDirExists(t, record.Migration.ImageDirectory)
					require.NoError(t, d.ReleaseMigrationStaging(t.Context(), record.MigrationStaging.Request))
					pruned, err = d.journal.Prune(time.Now().Add(48 * time.Hour))
					require.NoError(t, err)
					require.Zero(t, pruned, "allocation absence has not been acknowledged")
					require.NoError(t, reopened.Close())
					reopened, err = newRuntimeSlotJournal(path, time.Hour)
					require.NoError(t, err)
					d.journal = reopened
					finalizer.err = errdefs.ErrUnavailable
					finalRetry, err := d.FinalizeFailedMigrationCapture(t.Context(), final)
					require.NoError(t, err)
					require.Equal(t, finalProof, finalRetry)
					require.Equal(t, 2, finalizer.calls)
					custody, err := d.GetMigrationCapture(t.Context(), request.Cleanup.SlotID)
					require.NoError(t, err)
					require.True(t, custody.ExecutionInvalidated)
					require.True(t, custody.CaptureFailureFinalized())
					require.Equal(t, protocol.MigrationCaptureUncertain, custody.Capture.State)
					require.ErrorIs(t, d.RecordMigrationCapture(t.Context(), request.Capture), errdefs.ErrFailedPrecondition)
					_, err = d.SealMigrationRootFS(t.Context(), request.Capture.Request)
					require.Error(t, err)
					gc := protocol.MigrationSourceGCRequest{Target: request.Capture.Request.Target, FinalizationDigest: finalProof.RequestDigest,
						CleanupProofDigest: proof.Cleanup.ProofDigest, AllocationAbsenceDigest: strings.Repeat("9", 64)}
					changed := gc
					changed.Target.NodeBootID = "successor-boot"
					_, err = d.AcknowledgeMigrationSourceGC(t.Context(), changed)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					ack, err := d.AcknowledgeMigrationSourceGC(t.Context(), gc)
					require.NoError(t, err)
					require.NoError(t, ack.ValidateFor(gc))
					changed = gc
					changed.AllocationAbsenceDigest = strings.Repeat("8", 64)
					_, err = d.AcknowledgeMigrationSourceGC(t.Context(), changed)
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					record, err = d.journal.Get(request.Cleanup.SlotID)
					require.NoError(t, err)
					expectedVersion := runtimeSlotCaptureFailureJournalVersion
					if captureOnly {
						expectedVersion = runtimeSlotCheckpointStagingJournalVersion
					}
					require.Equal(t, expectedVersion, record.Version)
					for version := 1; version < expectedVersion; version++ {
						record.Version = version
						payload, err := json.Marshal(record)
						require.NoError(t, err)
						_, err = decodeRuntimeSlotJournalRecord(payload)
						require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
					}
					pruned, err = d.journal.Prune(time.Now().Add(48 * time.Hour))
					require.NoError(t, err)
					require.Equal(t, 1, pruned)
					ack, err = d.AcknowledgeMigrationSourceGC(t.Context(), gc)
					require.NoError(t, err)
					require.NoError(t, ack.ValidateFor(gc))
				})
			}
		})
	}
}

func TestMigrationCaptureFailureRejectsChangedAuthorityBeforePhysicalEffects(t *testing.T) {
	for _, name := range []string{"writer", "boot", "container", "lease", "binding", "captured", "intent", "partial-rootfs-cut"} {
		t.Run(name, func(t *testing.T) {
			d, request, runtime := migrationCaptureFailureFixture(t, true)
			switch name {
			case "writer":
				request.Cleanup.WriterGrantID = "other-writer"
			case "boot":
				request.Cleanup.NodeBootID = "other-boot"
			case "container":
				request.Cleanup.RunscContainerID = "other-container"
			case "lease":
				request.Cleanup.Resources.LeaseID = "other-lease"
			case "binding":
				request.Capture.Request.BindingDigest = strings.Repeat("a", 64)
				request.Capture.RequestDigest, _ = request.Capture.Request.Digest()
				request.Cleanup.WriterAuthorityDigest = request.Capture.RequestDigest
			case "captured":
				request.Capture.State = protocol.MigrationCaptureComplete
			case "intent":
				request.Capture.State = protocol.MigrationCaptureIntent
			case "partial-rootfs-cut":
				runtime.recovery[0].Kind = rootfssession.RecoveryMigration
			}
			_, err := d.CleanupFailedMigrationCapture(t.Context(), request)
			require.Error(t, err)
			require.Zero(t, runtime.localCalls)
			require.NotContains(t, d.runner.(*migrationFenceTestRunsc).callsSnapshot(), "delete:force")
			record, err := d.journal.Get(request.Capture.Request.Target.SlotID)
			require.NoError(t, err)
			require.Nil(t, record.Migration.Failure)
			require.Nil(t, record.Cleanup)
		})
	}
}

type lostMigrationCutTestRuntime struct {
	*cleanupRootFSRuntime
	calls           int
	err             error
	request         rootfshandoff.MigrationRootFSCutRequest
	writerOperation string
}

func (r *lostMigrationCutTestRuntime) AbandonLostMigrationCut(_ context.Context, _ rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest, writerOperation string) error {
	r.calls++
	r.request, r.writerOperation = request, writerOperation
	return r.err
}

func TestMigrationCaptureFailureRecoversLostCutHandoffAfterJournalRestart(t *testing.T) {
	d, request, runtime := migrationCaptureFailureFixture(t, true)
	runtime.recovery[0].Kind = rootfssession.RecoveryMigration
	runtime.recovery[0].MigrationCutOwnerLost = true
	runtime.recovery[0].Live = false
	lost := &lostMigrationCutTestRuntime{cleanupRootFSRuntime: runtime, err: errdefs.ErrUnavailable}
	d.runtime = lost
	_, err := d.CleanupFailedMigrationCapture(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	record, err := d.journal.Get(request.Cleanup.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.Migration.Failure, "regional failure is durable before RootFS handoff")
	require.Nil(t, record.Cleanup)
	require.Zero(t, runtime.localCalls)
	require.NotContains(t, d.runner.(*migrationFenceTestRunsc).callsSnapshot(), "delete:force")
	require.Equal(t, request.Capture.RequestDigest, lost.request.CaptureRequestDigest)
	require.Equal(t, request.Cleanup.WriterOperationID, lost.writerOperation)
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	reopened, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	d.journal = reopened
	lost.err = nil
	d.resourceCgroups.(*fakeRuntimeResourceCgroup).removeOK = true
	proof, err := d.CleanupFailedMigrationCapture(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	require.Equal(t, 2, lost.calls)
	_, err = d.CleanupFailedMigrationCapture(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, 2, lost.calls, "completed physical proof does not revisit lost RootFS ownership")
}
