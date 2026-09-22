package nomadruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestMigrationSourceFinalizationPrivateRPCRequiresCompletedSessionCleanup(t *testing.T) {
	d, request, runtime, _ := migrationSourceFinalizeNodeFixture(t)
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "ctld.sock"))
	require.NoError(t, err)
	server := &http.Server{Handler: nodeRuntimeRPCHandler(nil, nil, nil, d)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient(listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	pending, err := client.GetMigrationSourceFinalization(t.Context(), request.Cleanup.SlotID)
	require.NoError(t, err)
	require.Nil(t, pending)
	runtime.forgetErr = errors.New("session journal unavailable")
	_, err = d.FinalizeMigrationSource(t.Context(), request)
	require.Error(t, err)
	pending, err = client.GetMigrationSourceFinalization(t.Context(), request.Cleanup.SlotID)
	require.NoError(t, err)
	require.Nil(t, pending, "physical proof alone must not authorize driver closure")
	runtime.forgetErr = nil
	proof, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	receipt, err := client.GetMigrationSourceFinalization(t.Context(), request.Cleanup.SlotID)
	require.NoError(t, err)
	require.Equal(t, &protocol.MigrationSourceFinalizationReceipt{Request: request, Proof: *proof}, receipt)
	require.NoError(t, receipt.Validate())
	_, err = client.GetMigrationSourceFinalization(t.Context(), "another-slot")
	require.ErrorIs(t, err, errdefs.ErrUnavailable, "missing local proof requires regional lookup")
	require.NoError(t, d.journal.invalidateMigrationExecution(request.Cleanup.SlotID))
	_, err = client.GetMigrationSourceFinalization(t.Context(), request.Cleanup.SlotID)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestMigrationFailedSourceFinalizationRequiresExactStoppedDestination(t *testing.T) {
	d, request, runtime, cgroups := migrationSourceFinalizeNodeFixture(t)
	target := request.Adoption.Request.Target
	resources, err := protocol.NewRuntimeResourceLease(request.Adoption.Request.OperationID, request.Adoption.Request.ClaimID,
		target.SlotID, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	image := protocol.MigrationImagePrepareRequest{Target: target, Publication: request.Fence.PublicationRequest, Receipt: request.Fence.Publication, Resources: resources}
	digest, err := image.Digest()
	require.NoError(t, err)
	restore := protocol.MigrationRestoreRequest{Image: image, Fence: request.Fence, Proof: request.SourceProof,
		Prepared: protocol.MigrationImagePrepared{RequestDigest: digest, ManifestDigest: image.Receipt.Reference.ManifestDigest, TotalBytes: 128},
		Stage:    migrationRestoreStageForImage(t, image)}
	failure := protocol.MigrationFailureRequest{Restore: restore, Reason: protocol.MigrationFailureTermination}
	digest, err = failure.Digest()
	require.NoError(t, err)
	request.Adoption = protocol.MigrationAdoptionReceipt{}
	request.Failure = &protocol.MigrationFailureStopReceipt{Request: failure, Proof: protocol.MigrationFailureStopProof{
		RequestDigest: digest, ContainerID: protocol.NomadRunscContainerID(target.SlotID), ContainerAbsent: true}}
	require.NoError(t, request.Validate())
	request.Failure.Proof.ContainerAbsent = false
	_, err = d.FinalizeMigrationSource(t.Context(), request)
	require.Error(t, err)
	require.Zero(t, runtime.finalizeCalls)
	request.Failure.Proof.ContainerAbsent = true
	proof, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	require.Equal(t, 1, runtime.finalizeCalls)
	require.Equal(t, 1, runtime.forgetCalls)
	require.Equal(t, []protocol.RuntimeResourceLease{request.Cleanup.Resources}, cgroups.removedSnapshot())
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	reopened, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	d.journal = reopened
	retry, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, proof, retry)
	receipt, err := d.GetMigrationSourceFinalization(t.Context(), request.Cleanup.SlotID)
	require.NoError(t, err)
	require.Equal(t, request, receipt.Request)
}

type migrationSourceFinalizeTestRuntime struct {
	*migrationFenceTestRuntime
	finalizeCalls, forgetCalls int
	finalizeErr, forgetErr     error
}

func (r *migrationSourceFinalizeTestRuntime) FinalizeMigrationRootFS(_ context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSFinalizeRequest) (rootfshandoff.MigrationRootFSFinalizeProof, error) {
	r.finalizeCalls++
	if r.finalizeErr != nil {
		return rootfshandoff.MigrationRootFSFinalizeProof{}, r.finalizeErr
	}
	binding, _ := stage.BindingDigest()
	proof := rootfshandoff.MigrationRootFSFinalizeProof{Request: request, Parent: stage.Parent, BindingDigest: fmt.Sprintf("%x", binding), BranchAbsent: true, MountDirectoriesAbsent: true}
	proof.Digest, _ = proof.ProofDigest()
	return proof, nil
}
func (r *migrationSourceFinalizeTestRuntime) ForgetFinalizedMigrationRootFS(rootfshandoff.StageRequest, rootfshandoff.MigrationRootFSFinalizeRequest) error {
	r.forgetCalls++
	return r.forgetErr
}

func migrationSourceFinalizeNodeFixture(t *testing.T, beforeCapture ...func(*nodeRuntime, protocol.MigrationCaptureRequest)) (*nodeRuntime, protocol.MigrationSourceFinalizeRequest, *migrationSourceFinalizeTestRuntime, *fakeRuntimeResourceCgroup) {
	t.Helper()
	d, fence, runtime, _ := migrationFenceNodeFixture(t, beforeCapture...)
	proof, err := d.FenceMigrationSource(t.Context(), fence)
	require.NoError(t, err)
	capture := fence.PublicationRequest.Capture.Request
	stage := runtime.recoverySessions[0].Stage
	record, err := d.journal.Get(capture.Target.SlotID)
	require.NoError(t, err)
	cleanup := testRuntimeSlotJournalCleanup(record.Registration)
	cleanup.NodeUID = capture.Target.NodeUID
	cleanup.OperationID = protocol.MigrationSourceCleanupOperationID(capture.OperationID)
	cleanup.WriterOperationID = capture.OperationID
	cleanup.WriterRetireKind = protocol.WriterRetireKindMigration
	cleanup.WriterGrantID = stage.Identity.WriterGrantID
	cleanup.WriterAuthorityDigest = proof.Digest
	cleanup.Resources = migrationSourceTestResources(t, stage, capture.Target)
	cleanup.ResourceLeaseDigest = capture.ResourceLeaseDigest
	adoption := protocol.MigrationAdoptionRequest{Target: protocol.NodeChannelTarget{SlotID: "target-slot", ClusterID: capture.Target.ClusterID, NodeID: "target-node", NodeUID: "target-uid", NodeBootID: "target-boot", AllocationID: "target-allocation", ControlEndpoint: "unix:///target/control.sock"},
		OperationID: capture.OperationID, ClaimID: "target-claim", SandboxID: capture.SandboxID, RuntimeGeneration: fence.PublicationRequest.Assignment.Target.RuntimeGeneration,
		ProcdInstanceID: capture.ProcdInstanceID, RestoreDigest: strings.Repeat("a", 64), CommandReadyDigest: strings.Repeat("b", 64)}
	digest, err := adoption.Digest()
	require.NoError(t, err)
	request := protocol.MigrationSourceFinalizeRequest{Fence: fence, SourceProof: *proof, Cleanup: cleanup, Adoption: protocol.MigrationAdoptionReceipt{Request: adoption, Proof: protocol.MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}}}
	require.NoError(t, request.Validate())
	finalizer := &migrationSourceFinalizeTestRuntime{migrationFenceTestRuntime: runtime}
	d.runtime = finalizer
	d.resourceCgroups = &fakeRuntimeResourceCgroup{removeOK: true}
	d.runtimeSlotNetwork = newFakeCtldNetwork(t)
	d.config.RootFSConsumerNetNSRoot = filepath.Dir(record.Registration.NetNSPath)
	// The fake namespace file is not a Linux namespace. Model the already
	// removed allocation namespace; ctld must still remove its network policy.
	require.NoError(t, os.Remove(record.Registration.NetNSPath))
	return d, request, finalizer, d.resourceCgroups.(*fakeRuntimeResourceCgroup)
}

func TestMigrationSourceFinalizeCompletesJournaledPhysicalCleanup(t *testing.T) {
	d, request, runtime, cgroups := migrationSourceFinalizeNodeFixture(t)
	before, err := d.journal.Get(request.Cleanup.SlotID)
	require.NoError(t, err)
	_, err = d.CleanupRuntimeSlot(t.Context(), request.Cleanup)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "generic cleanup cannot initiate finalization")
	proof, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	require.NoDirExists(t, before.Migration.ImageDirectory)
	require.Equal(t, 1, runtime.finalizeCalls)
	require.Equal(t, 1, runtime.forgetCalls)
	require.Equal(t, []protocol.RuntimeResourceLease{request.Cleanup.Resources}, cgroups.removedSnapshot())
	record, err := d.journal.Get(request.Cleanup.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotFinalizationJournalVersion, record.Version)
	require.Equal(t, proof.Cleanup, *record.Proof)
	require.True(t, record.Migration.Finalization.SessionForgotten)
	// Full receipts survive process restart, without touching the live target.
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	reopened, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	d.journal = reopened
	again, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, proof, again)
	require.Equal(t, 1, runtime.finalizeCalls)
	require.Equal(t, 1, runtime.forgetCalls)
	require.Len(t, cgroups.removedSnapshot(), 1)
	_, err = d.CleanupRuntimeSlot(t.Context(), request.Cleanup)
	require.NoError(t, err)
	replayed := request.Fence.PublicationRequest.Capture
	replayed.RootFS = nil
	require.ErrorIs(t, d.RecordMigrationCapture(t.Context(), replayed), errdefs.ErrFailedPrecondition)
	deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, deleted, "Nomad may still need its completed source receipt")
}

func TestMigrationSourceFinalizeRetainsIntentAcrossFailures(t *testing.T) {
	for _, failure := range []string{"rootfs", "cgroup", "forget"} {
		t.Run(failure, func(t *testing.T) {
			d, request, runtime, cgroups := migrationSourceFinalizeNodeFixture(t)
			switch failure {
			case "rootfs":
				runtime.finalizeErr = errors.New("rootfs unavailable")
			case "cgroup":
				cgroups.err = errors.New("cgroup busy")
			case "forget":
				runtime.forgetErr = errors.New("journal unavailable")
			}
			_, err := d.FinalizeMigrationSource(t.Context(), request)
			require.Error(t, err)
			record, err := d.journal.Get(request.Cleanup.SlotID)
			require.NoError(t, err)
			require.NotNil(t, record.Migration.Finalization)
			if failure == "rootfs" {
				require.DirExists(t, record.Migration.ImageDirectory)
				require.Empty(t, cgroups.removedSnapshot())
			} else {
				require.NoDirExists(t, record.Migration.ImageDirectory)
			}
			if failure == "forget" {
				require.NotNil(t, record.Proof)
			} else {
				require.Nil(t, record.Proof)
			}
			deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
			require.NoError(t, err)
			require.Zero(t, deleted, "unfinished session forgetting must retain the retry handle")
			path := d.journal.db.Path()
			require.NoError(t, d.journal.Close())
			reopened, err := newRuntimeSlotJournal(path, time.Hour)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			d.journal = reopened
			runtime.finalizeErr = nil
			runtime.forgetErr = nil
			cgroups.err = nil
			proof, err := d.FinalizeMigrationSource(t.Context(), request)
			require.NoError(t, err)
			require.NoError(t, proof.ValidateFor(request))
		})
	}
}

func TestMigrationSourceFinalizePreservesInvalidatedEvidence(t *testing.T) {
	d, request, runtime, _ := migrationSourceFinalizeNodeFixture(t)
	runtime.forgetErr = errors.New("session journal unavailable")
	_, err := d.FinalizeMigrationSource(t.Context(), request)
	require.Error(t, err)
	require.NoError(t, d.journal.invalidateMigrationExecution(request.Cleanup.SlotID))
	record, err := d.journal.Get(request.Cleanup.SlotID)
	require.NoError(t, err, "historical evidence must remain readable after invalidation")
	require.NotNil(t, record.Migration.Finalization.RootFS)
	require.NotNil(t, record.Proof)
	_, err = d.FinalizeMigrationSource(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	_, err = d.CleanupRuntimeSlot(t.Context(), request.Cleanup)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	exposed, err := d.GetMigrationCapture(t.Context(), request.Cleanup.SlotID)
	require.NoError(t, err)
	require.Nil(t, exposed.Finalization)
	require.Nil(t, exposed.SourceFenceProof)
	deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, deleted)
}

func TestMigrationSourceFinalizeRejectsChangedWriterAndConflictingReplay(t *testing.T) {
	d, request, runtime, _ := migrationSourceFinalizeNodeFixture(t)
	changed := request
	changed.Cleanup.WriterGrantID = "other-writer"
	_, err := d.FinalizeMigrationSource(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Zero(t, runtime.finalizeCalls)
	_, err = d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	changed = request
	changed.Adoption.Request.CommandReadyDigest = strings.Repeat("c", 64)
	changed.Adoption.Proof.RequestDigest, err = changed.Adoption.Request.Digest()
	require.NoError(t, err)
	_, err = d.FinalizeMigrationSource(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	record, err := d.journal.Get(request.Cleanup.SlotID)
	require.NoError(t, err)
	for _, version := range []int{1, 2, 3, 4} {
		record.Version = version
		payload, err := json.Marshal(record)
		require.NoError(t, err)
		_, err = decodeRuntimeSlotJournalRecord(payload)
		require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	}
}
