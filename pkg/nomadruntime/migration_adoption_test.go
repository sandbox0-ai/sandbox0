package nomadruntime

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationAdoptionNodeFixture(t *testing.T, beforeImage ...func(*nodeRuntime, protocol.MigrationImagePrepareRequest)) (*nodeRuntime, protocol.MigrationAdoptionRequest, *migrationImageDownloadTestRuntime, *migrationSourceTestRunsc) {
	t.Helper()
	d, restored, runtime, runner := migrationRestoreNodeFixture(t, beforeImage...)
	require.NoError(t, d.RecordMigrationRestore(t.Context(), restored))
	runtime.recoverySessions = []rootfssession.RecoverySession{{Stage: restored.Request.Stage, Live: true}}
	runner.stateErr = nil
	runner.setState("created")
	restored.State = protocol.MigrationRestoreExecuting
	require.NoError(t, d.RecordMigrationRestore(t.Context(), restored))
	runner.setState("running")
	restored.State = protocol.MigrationRestoreComplete
	require.NoError(t, d.RecordMigrationRestore(t.Context(), restored))
	image := restored.Request.Image
	request := protocol.MigrationAdoptionRequest{Target: image.Target, OperationID: image.Publication.Assignment.OperationID,
		ClaimID: image.Resources.ClaimID, SandboxID: image.Publication.Assignment.Target.SandboxID,
		RuntimeGeneration: image.Publication.Assignment.Target.RuntimeGeneration, ProcdInstanceID: image.Publication.Capture.Request.ProcdInstanceID,
		RestoreDigest: restored.RequestDigest, CommandReadyDigest: strings.Repeat("a", 64)}
	require.NoError(t, request.ValidateFor(restored))
	return d, request, runtime, runner
}

func TestMigrationAdoptionPrivateRPCReleasesOnlyImageAndPermitsNormalCleanup(t *testing.T) {
	d, request, runtime, runner := migrationAdoptionNodeFixture(t)
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "ctld.sock"))
	require.NoError(t, err)
	server := &http.Server{Handler: nodeRuntimeRPCHandler(nil, nil, nil, d)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient(listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	before, err := client.GetMigrationDestination(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	proof, err := client.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	_, err = os.Lstat(before.ImageDirectory)
	require.True(t, os.IsNotExist(err))
	require.NotContains(t, runner.callsSnapshot(), "kill:KILL")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	require.Zero(t, runtime.crashCalls)
	require.Zero(t, runtime.retireCalls)
	require.Zero(t, runtime.externalReclaims)
	custody, err := client.GetMigrationDestination(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, custody.Adopted())
	require.Equal(t, before.Restore, custody.Restore)
	// Later execution state must not invalidate the already durable receipt.
	runner.setState("stopped")
	retry, err := client.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, proof, retry)
	_, err = d.PrepareMigrationImage(t.Context(), before.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	uncertain := *before.Restore
	uncertain.State = protocol.MigrationRestoreUncertain
	require.ErrorIs(t, client.RecordMigrationRestore(t.Context(), uncertain), errdefs.ErrFailedPrecondition)
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	_, err = d.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
	require.NoError(t, err)
	_, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err, "adoption history coexists with ordinary terminal cleanup")
}

func TestMigrationAdoptionRejectsChangedAuthorityAndUncertainRestore(t *testing.T) {
	d, request, _, runner := migrationAdoptionNodeFixture(t)
	for name, mutate := range map[string]func(*protocol.MigrationAdoptionRequest){
		"node":       func(r *protocol.MigrationAdoptionRequest) { r.Target.NodeUID += "changed" },
		"boot":       func(r *protocol.MigrationAdoptionRequest) { r.Target.NodeBootID += "changed" },
		"claim":      func(r *protocol.MigrationAdoptionRequest) { r.ClaimID += "changed" },
		"operation":  func(r *protocol.MigrationAdoptionRequest) { r.OperationID += "changed" },
		"sandbox":    func(r *protocol.MigrationAdoptionRequest) { r.SandboxID += "changed" },
		"generation": func(r *protocol.MigrationAdoptionRequest) { r.RuntimeGeneration++ },
		"process":    func(r *protocol.MigrationAdoptionRequest) { r.ProcdInstanceID += "changed" },
		"restore":    func(r *protocol.MigrationAdoptionRequest) { r.RestoreDigest = strings.Repeat("b", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			_, err := d.AdoptMigrationDestination(t.Context(), changed)
			require.Error(t, err)
		})
	}
	runner.setState("stopped")
	_, err := d.AdoptMigrationDestination(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	runner.setState("running")
	custody, err := d.GetMigrationDestination(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	uncertain := *custody.Restore
	uncertain.State = protocol.MigrationRestoreUncertain
	require.NoError(t, d.RecordMigrationRestore(t.Context(), uncertain))
	_, err = d.AdoptMigrationDestination(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.DirExists(t, custody.ImageDirectory)
}

func TestMigrationAdoptionRecoveryCompletesIntentAfterImageRemoval(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before removal", true: "after removal"}[removed], func(t *testing.T) {
			d, request, runtime, runner := migrationAdoptionNodeFixture(t)
			require.NoError(t, d.journal.recordMigrationAdoption(request, nil))
			custody, err := d.GetMigrationDestination(t.Context(), request.Target.SlotID)
			require.NoError(t, err)
			if removed {
				require.NoError(t, os.RemoveAll(custody.ImageDirectory))
			}
			path := d.journal.db.Path()
			require.NoError(t, d.journal.Close())
			reopened, err := newRuntimeSlotJournal(path, time.Hour)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			d.journal = reopened
			runner.setState("stopped")
			handled, err := d.fenceMigrationDestination(t.Context(), runtime.recoverySessions[0])
			require.NoError(t, err)
			require.False(t, handled, "normal crash recovery must own the adopted writer")
			custody, err = d.GetMigrationDestination(t.Context(), request.Target.SlotID)
			require.NoError(t, err)
			require.True(t, custody.Adopted())
			require.NoError(t, custody.Adoption.Proof.ValidateFor(request))
			require.Equal(t, protocol.MigrationRestoreComplete, custody.Restore.State)
			changed := request
			changed.CommandReadyDigest = strings.Repeat("c", 64)
			_, err = d.AdoptMigrationDestination(t.Context(), changed)
			require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
		})
	}
}

func TestMigrationAdoptionJournalRequiresAdoptionAwareReader(t *testing.T) {
	d, request, _, _ := migrationAdoptionNodeFixture(t)
	require.NoError(t, d.journal.recordMigrationAdoption(request, nil))
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotAdoptionJournalVersion, record.Version)
	for _, version := range []int{RuntimeSlotJournalVersion, runtimeSlotMigrationJournalVersion, runtimeSlotRestoreJournalVersion} {
		record.Version = version
		payload, err := json.Marshal(record)
		require.NoError(t, err)
		_, err = decodeRuntimeSlotJournalRecord(payload)
		require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	}
}

func TestMigrationAdoptionRetainsHistoryThroughSubsequentMigration(t *testing.T) {
	d, request, _, _ := migrationAdoptionNodeFixture(t)
	_, err := d.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	restore := record.MigrationDestination.Restore.Request
	binding, err := restore.Stage.BindingDigest()
	require.NoError(t, err)
	revision, err := restore.Image.Publication.Assignment.Target.Revision()
	require.NoError(t, err)
	resourceDigest, err := restore.Image.Resources.Digest()
	require.NoError(t, err)
	capture := protocol.MigrationCapture{State: protocol.MigrationCaptureIntent, Request: protocol.MigrationCaptureRequest{
		Target: request.Target, OperationID: "next-migration", LifecycleEpoch: 4, SandboxID: request.SandboxID, SourceGeneration: request.RuntimeGeneration,
		ProcdInstanceID: request.ProcdInstanceID, AssignmentRevision: revision, BindingDigest: fmt.Sprintf("%x", binding), ResourceLeaseDigest: strings.TrimPrefix(resourceDigest, "sha256:")}}
	capture.RequestDigest, err = capture.Request.Digest()
	require.NoError(t, err)
	require.NoError(t, d.journal.RecordMigrationCapture(capture))
	record, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.Migration)
	require.True(t, record.MigrationDestination.Adopted())
	require.Equal(t, runtimeSlotAdoptionJournalVersion, record.Version)
	_, err = d.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "new source custody still needs its own physical handoff")
}

func TestMigrationAdoptionImageRemovalNeverFollowsSymlink(t *testing.T) {
	d, request, _, _ := migrationAdoptionNodeFixture(t)
	custody, err := d.GetMigrationDestination(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	unrelated := t.TempDir()
	file := filepath.Join(unrelated, "keep")
	require.NoError(t, os.WriteFile(file, []byte("unrelated"), 0600))
	require.NoError(t, os.RemoveAll(custody.ImageDirectory))
	require.NoError(t, os.Symlink(unrelated, custody.ImageDirectory))
	_, err = d.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	require.FileExists(t, file)
}
