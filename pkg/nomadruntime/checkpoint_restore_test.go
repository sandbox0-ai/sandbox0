package nomadruntime

import (
	"encoding/hex"
	"encoding/json"
	"maps"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointDestinationFixture(t *testing.T, kind runtimecontrol.CheckpointRestoreKind) (*nodeRuntime, protocol.MigrationImagePrepareRequest, *migrationImageDownloadTestRuntime) {
	t.Helper()
	source, publication, images := checkpointImageNodeFixture(t)
	receipt, err := source.PublishMigration(t.Context(), publication)
	require.NoError(t, err)
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment(publication.Capture.Request.OperationID, *publication.CheckpointSource)
	require.NoError(t, err)
	target := *publication.CheckpointSource
	target.RuntimeGeneration++
	if kind == runtimecontrol.CheckpointFork {
		target.SandboxID, target.RuntimeGeneration = "memory-child", 1
		target.EnvVars = maps.Clone(target.EnvVars)
		if _, ok := target.EnvVars[runtimecontrol.EnvSandboxID]; ok {
			target.EnvVars[runtimecontrol.EnvSandboxID] = target.SandboxID
		}
	}
	authority := &protocol.CheckpointRestoreAuthority{
		Assignment:     runtimecontrol.CheckpointRestoreAssignment{OperationID: "restore-" + string(kind), Capture: capture, Kind: kind, Target: target},
		LifecycleEpoch: 7,
		Retained:       protocol.CheckpointRetained{CheckpointID: capture.OperationID, PublicationRequestDigest: receipt.RequestDigest, Reference: receipt.Reference},
	}
	return migrationImageDestinationForPublication(t, publication, *receipt, images, authority)
}

func TestCheckpointDestinationUsesIndependentAuthorityAndSharesImmutableImage(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) {
			d, image, runtime := checkpointDestinationFixture(t, kind)
			require.Equal(t, image.Target.NodeUID, image.Publication.Capture.Request.Target.NodeUID, "new carrier on the same node is eligible")
			require.NotEqual(t, image.OperationID(), image.Publication.Capture.Request.OperationID)
			require.Equal(t, image.Receipt.Reference, image.Checkpoint.Retained.Reference)
			for _, mutate := range []func(*protocol.MigrationImagePrepareRequest){
				func(r *protocol.MigrationImagePrepareRequest) { r.Checkpoint = nil },
				func(r *protocol.MigrationImagePrepareRequest) { r.Checkpoint.LifecycleEpoch = 0 },
				func(r *protocol.MigrationImagePrepareRequest) { r.Checkpoint.Assignment.Target.TeamID = "another-team" },
				func(r *protocol.MigrationImagePrepareRequest) {
					r.Checkpoint.Assignment.OperationID = r.Publication.Capture.Request.OperationID
				},
				func(r *protocol.MigrationImagePrepareRequest) {
					r.Checkpoint.Retained.Reference.ManifestDigest = digest.FromString("other-image").String()
				},
				func(r *protocol.MigrationImagePrepareRequest) { r.Target = r.Publication.Capture.Request.Target },
			} {
				copy := image
				authority := *image.Checkpoint
				copy.Checkpoint = &authority
				mutate(&copy)
				_, err := d.PrepareMigrationImage(t.Context(), copy)
				require.Error(t, err)
			}
			d, observation, runtime, runner := migrationRestoreForImageFixture(t, d, image, runtime)
			retryEpoch := observation.Request
			retryEpoch.Stage.Identity.WriterEpoch += 2
			require.NoError(t, retryEpoch.Validate(), "retained image permits a separately fenced later writer attempt")
			retryEpoch.Stage.Identity.WriterEpoch = image.Publication.Capture.RootFS.Generation.WriterEpoch
			require.Error(t, retryEpoch.Validate(), "captured writer authority cannot be reused")
			require.Equal(t, 1, runtime.downloadCalls)
			if kind == runtimecontrol.CheckpointFork {
				require.Equal(t, "memory-child", observation.Request.Stage.Generation.FilesystemID)
				require.Equal(t, image.Publication.Capture.RootFS.Generation.CurrentBlockHead, observation.Request.Stage.Generation.CurrentBlockHead)
			}
			wrong := observation.Request
			g := *wrong.Stage.Generation
			g.CurrentBlockHead = digest.FromString("different-disk-cut").String()
			wrong.Stage.Generation = &g
			require.Error(t, wrong.Validate(), "memory cannot be combined with another disk cut")
			require.NoError(t, d.RecordMigrationRestore(t.Context(), observation))
			runtime.recoverySessions = []rootfssession.RecoverySession{{Stage: observation.Request.Stage, Live: true}}
			runner.stateErr = nil
			runner.setState("created")
			observation.State = protocol.MigrationRestoreExecuting
			require.NoError(t, d.RecordMigrationRestore(t.Context(), observation))
			runner.setState("running")
			observation.State = protocol.MigrationRestoreComplete
			require.NoError(t, d.RecordMigrationRestore(t.Context(), observation))
			launch, err := protocol.BindMigrationCPURestore(observation.Request, image.Publication.CPULaunch.Observation, image.Publication.CPULaunch.Observation)
			require.NoError(t, err)
			require.NoError(t, launch.ValidateRestore(observation.Request))
			require.Equal(t, image.RuntimeAssignment().SandboxID, launch.SandboxID)
			require.Equal(t, kind, launch.Restored.CheckpointKind)
			require.Equal(t, image.Publication.CPULaunch.GuestCPUProfile(), launch.GuestCPUProfile())
			stripped := *launch.Clone()
			stripped.Restored.CheckpointKind = ""
			stripped.Restored.CheckpointRestoreDigest = ""
			require.Error(t, stripped.ValidateRestore(observation.Request))
			checkpointDigest, err := image.Checkpoint.Assignment.Digest()
			require.NoError(t, err)
			adoption := protocol.MigrationAdoptionRequest{Target: image.Target, OperationID: image.OperationID(), ClaimID: image.Resources.ClaimID,
				SandboxID: image.RuntimeAssignment().SandboxID, RuntimeGeneration: image.RuntimeAssignment().RuntimeGeneration,
				ProcdInstanceID: image.Publication.Capture.Request.ProcdInstanceID, RestoreDigest: observation.RequestDigest,
				CommandReadyDigest: strings.Repeat("a", 64), CheckpointRestoreDigest: checkpointDigest}
			require.NoError(t, adoption.ValidateFor(observation))
			unbound := adoption
			unbound.CheckpointRestoreDigest = ""
			require.Error(t, unbound.ValidateFor(observation))
			proof, err := d.AdoptMigrationDestination(t.Context(), adoption)
			require.NoError(t, err)
			require.NoError(t, proof.ValidateFor(adoption))
			require.NotContains(t, runner.callsSnapshot(), "kill:KILL")
			binding, err := observation.Request.Stage.BindingDigest()
			require.NoError(t, err)
			resources, err := image.Resources.Digest()
			require.NoError(t, err)
			revision, err := image.RuntimeAssignment().Revision()
			require.NoError(t, err)
			nextCapture := protocol.MigrationCaptureRequest{Target: image.Target, OperationID: "next-memory-pause", LifecycleEpoch: 8,
				SandboxID: adoption.SandboxID, SourceGeneration: adoption.RuntimeGeneration, AssignmentRevision: revision,
				BindingDigest: hex.EncodeToString(binding[:]), ResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), ProcdInstanceID: adoption.ProcdInstanceID}
			_, err = d.ReserveMigrationStaging(t.Context(), protocol.MigrationStagingRequest{CaptureOnly: true,
				Target: image.Target, Source: nextCapture, Bytes: 8 << 20, Inodes: 64})
			require.NoError(t, err)
			nextDigest, err := nextCapture.Digest()
			require.NoError(t, err)
			require.NoError(t, d.journal.RecordMigrationCapture(protocol.MigrationCapture{Request: nextCapture, RequestDigest: nextDigest, State: protocol.MigrationCaptureIntent}),
				"an adopted fork or resume can become a new source without losing its independent restore history")
			_, err = runtime.store.Download(t.Context(), image.Receipt.Binding, image.Receipt.Reference, filepath.Join(t.TempDir(), "retained"))
			require.NoError(t, err, "adoption must retain the shared regional image")
			record, err := d.journal.Get(image.Target.SlotID)
			require.NoError(t, err)
			require.Equal(t, runtimeSlotCheckpointRestoreJournalVersion, record.Version)
			for _, version := range []int{runtimeSlotMigrationJournalVersion, runtimeSlotRestoreJournalVersion, runtimeSlotAdoptionJournalVersion, runtimeSlotCheckpointStagingJournalVersion} {
				record.Version = version
				payload, err := json.Marshal(record)
				require.NoError(t, err)
				_, err = decodeRuntimeSlotJournalRecord(payload)
				require.Error(t, err, "older envelopes cannot forget independent restore identity")
			}
			path := d.journal.db.Path()
			require.NoError(t, d.journal.Close())
			d.journal, err = newRuntimeSlotJournal(path, time.Hour)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
			retry, err := d.AdoptMigrationDestination(t.Context(), adoption)
			require.NoError(t, err)
			require.Equal(t, proof, retry)
			_, err = d.PrepareMigrationImage(t.Context(), image)
			require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "an adopted carrier cannot restore twice")
		})
	}
}
