package nomadruntime

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationRestoreNodeFixture(t *testing.T, beforeImage ...func(*nodeRuntime, protocol.MigrationImagePrepareRequest)) (*nodeRuntime, protocol.MigrationRestoreObservation, *migrationImageDownloadTestRuntime, *migrationSourceTestRunsc) {
	t.Helper()
	daemon, image, runtime := migrationImageDestinationFixture(t)
	for _, prepare := range beforeImage {
		prepare(daemon, image)
	}
	prepared, err := daemon.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: image.Publication, Publication: image.Receipt}
	detach, err := fence.RootFSRequest()
	require.NoError(t, err)
	cut := image.Publication.Capture.RootFS.Generation
	rootProof, err := rootfshandoff.NewMigrationRootFSDetachProof(detach, rootfshandoff.CrashFenceSessionObservation{
		Parent: digest.FromString("source-parent").String(), RootFSID: cut.FilesystemID, WriterEpoch: cut.WriterEpoch,
		OperationID: detach.OperationID, BindingDigest: image.Publication.Capture.Request.BindingDigest,
		SessionState: rootfshandoff.StateTombstoned, BranchPath: "/private/source.wal", DeviceBound: true, DevicePath: "/dev/fake0",
		LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	require.NoError(t, err)
	fenceDigest, err := fence.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationSourceFenceProof{RequestDigest: fenceDigest, RootFS: rootProof,
		ContainerID: protocol.NomadRunscContainerID(image.Publication.Capture.Request.Target.SlotID), MountNamespaceID: "mnt:source",
		ContainerAbsent: true, StableMountAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	require.NoError(t, err)
	stage := migrationRestoreStageForImage(t, image)
	request := protocol.MigrationRestoreRequest{Image: image, Prepared: *prepared, Fence: fence, Proof: proof, Stage: stage}
	requestDigest, err := request.Digest()
	require.NoError(t, err)
	runner := daemon.runner.(*migrationSourceTestRunsc)
	return daemon, protocol.MigrationRestoreObservation{Request: request, RequestDigest: requestDigest, State: protocol.MigrationRestoreIntent}, runtime, runner
}

func migrationRestoreStageForImage(t *testing.T, image protocol.MigrationImagePrepareRequest) rootfshandoff.StageRequest {
	t.Helper()
	cut := image.Publication.Capture.RootFS.Generation
	stage := testNomadNodeClaimControlRequest(t).Stage.WithoutWriterGrantToken()
	stage.Generation, stage.InitialGeneration = &cut, cut.GenerationID
	stage.Identity.RootFSID, stage.Identity.SourceOCIDigest, stage.Identity.WriterEpoch = cut.FilesystemID, cut.SourceOCIDigest, cut.WriterEpoch+1
	stage.Identity.NodeUID, stage.Identity.BootID = image.Target.NodeUID, image.Target.NodeBootID
	stage.Identity.AllocationID, stage.Identity.SlotNonce, stage.Identity.ClaimID = image.Target.AllocationID, image.Target.SlotID, image.Resources.ClaimID
	stage.Identity.TaskName, stage.Identity.RuntimeClass, stage.Identity.RootFSDriver = protocol.NomadTaskName, "sandbox0-gvisor", "nomad-driver"
	stage.Identity.RuntimeGeneration = strconv.FormatInt(image.Publication.Assignment.Target.RuntimeGeneration, 10)
	stage.ExpectedPolicyToken.AllocationID, stage.ExpectedPolicyToken.ClaimID = stage.Identity.AllocationID, stage.Identity.ClaimID
	revision, err := image.Publication.Assignment.Target.Revision()
	require.NoError(t, err)
	resourceDigest, err := image.Resources.Digest()
	require.NoError(t, err)
	stage.Labels = map[string]string{protocol.RuntimeAssignmentRevisionLabel: revision, protocol.RuntimeResourceLeaseDigestLabel: resourceDigest}
	return stage
}

func TestMigrationRestorePrivateRPCRequiresExactCreatedWriterAndRunningReceipt(t *testing.T) {
	daemon, observation, runtime, runner := migrationRestoreNodeFixture(t)
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "ctld.sock"))
	require.NoError(t, err)
	server := &http.Server{Handler: nodeRuntimeRPCHandler(nil, nil, nil, daemon)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient(listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.RecordMigrationRestore(t.Context(), observation))
	observation.State = protocol.MigrationRestoreComplete
	require.Error(t, client.RecordMigrationRestore(t.Context(), observation), "completion cannot skip execution intent")
	observation.State = protocol.MigrationRestoreExecuting
	require.ErrorIs(t, client.RecordMigrationRestore(t.Context(), observation), errdefs.ErrFailedPrecondition)
	runtime.recoverySessions = []rootfssession.RecoverySession{{Stage: observation.Request.Stage, Live: true}}
	runner.stateErr = nil
	runner.setState("created")
	require.NoError(t, client.RecordMigrationRestore(t.Context(), observation))
	observation.State = protocol.MigrationRestoreComplete
	require.ErrorIs(t, client.RecordMigrationRestore(t.Context(), observation), errdefs.ErrFailedPrecondition)
	runner.setState("running")
	require.NoError(t, client.RecordMigrationRestore(t.Context(), observation))
	custody, err := client.GetMigrationDestination(t.Context(), observation.Request.Image.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, observation, *custody.Restore)
	observation.State = protocol.MigrationRestoreIntent
	require.ErrorIs(t, client.RecordMigrationRestore(t.Context(), observation), errdefs.ErrFailedPrecondition, "restored execution cannot replay")
}

func TestMigrationRestoreRecoveryFencesWithoutDiscardingTargetDirtyState(t *testing.T) {
	daemon, observation, runtime, runner := migrationRestoreNodeFixture(t)
	require.NoError(t, daemon.RecordMigrationRestore(t.Context(), observation))
	session := rootfssession.RecoverySession{Stage: observation.Request.Stage, Live: true}
	runtime.recoverySessions = []rootfssession.RecoverySession{session}
	runner.stateErr = nil
	runner.setState("created")
	observation.State = protocol.MigrationRestoreExecuting
	require.NoError(t, daemon.RecordMigrationRestore(t.Context(), observation))
	// The restore response was lost after execution began. Restart ctld from
	// durable custody, then reconcile without publishing or abandoning writes.
	runner.setState("running")
	path := daemon.journal.db.Path()
	require.NoError(t, daemon.journal.Close())
	reopened, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	daemon.journal = reopened
	require.NoError(t, daemon.reconcile(t.Context(), session))
	require.Contains(t, runner.callsSnapshot(), "kill:KILL")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	require.Zero(t, runtime.crashCalls)
	require.Zero(t, runtime.retireCalls)
	require.Zero(t, runtime.externalReclaims)
	custody, err := daemon.GetMigrationDestination(t.Context(), observation.Request.Image.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
	data, err := os.ReadFile(filepath.Join(custody.ImageDirectory, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "retained-memory", string(data))
	runner.setState("created")
	require.ErrorIs(t, daemon.RecordMigrationRestore(t.Context(), observation), errdefs.ErrFailedPrecondition, "uncertain execution cannot replay")
	record, err := daemon.journal.Get(observation.Request.Image.Target.SlotID)
	require.NoError(t, err)
	_, err = daemon.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestMigrationRestoreRejectsChangedAuthorityAndCorruptedImage(t *testing.T) {
	daemon, observation, runtime, _ := migrationRestoreNodeFixture(t)
	for name, mutate := range map[string]func(*protocol.MigrationRestoreRequest){
		"writer epoch":   func(r *protocol.MigrationRestoreRequest) { r.Stage.Identity.WriterEpoch++ },
		"target boot":    func(r *protocol.MigrationRestoreRequest) { r.Stage.Identity.BootID += "changed" },
		"source absence": func(r *protocol.MigrationRestoreRequest) { r.Proof.ContainerAbsent = false },
		"image receipt":  func(r *protocol.MigrationRestoreRequest) { r.Prepared.TotalBytes = 0 },
		"secret":         func(r *protocol.MigrationRestoreRequest) { r.Stage.Identity.WriterGrantToken = "secret" },
		"filesystem cut": func(r *protocol.MigrationRestoreRequest) { r.Stage.Generation.LocatorVersion++ },
		"assignment": func(r *protocol.MigrationRestoreRequest) {
			r.Stage.Labels[protocol.RuntimeAssignmentRevisionLabel] = "changed"
		},
	} {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(observation.Request)
			require.NoError(t, err)
			var changed protocol.MigrationRestoreRequest
			require.NoError(t, json.Unmarshal(data, &changed))
			mutate(&changed)
			require.Error(t, changed.Validate())
		})
	}
	custody, err := daemon.GetMigrationDestination(t.Context(), observation.Request.Image.Target.SlotID)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(custody.ImageDirectory, "checkpoint.img"), []byte("corrupt!-memory"), 0o600))
	require.Error(t, daemon.RecordMigrationRestore(t.Context(), observation))
	require.Nil(t, custody.Restore)
	require.Empty(t, runtime.recoverySessions)
}

func TestMigrationRestoreJournalRequiresRestoreAwareReader(t *testing.T) {
	daemon, observation, _, _ := migrationRestoreNodeFixture(t)
	require.NoError(t, daemon.RecordMigrationRestore(t.Context(), observation))
	record, err := daemon.journal.Get(observation.Request.Image.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotRestoreJournalVersion, record.Version)
	for _, version := range []int{RuntimeSlotJournalVersion, runtimeSlotMigrationJournalVersion} {
		record.Version = version
		payload, err := json.Marshal(record)
		require.NoError(t, err)
		_, err = decodeRuntimeSlotJournalRecord(payload)
		require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "restore custody cannot masquerade as an older envelope")
	}
}
