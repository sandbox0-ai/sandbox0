package nomadruntime

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointSourceLaunchFixture(t *testing.T, c protocol.MigrationCaptureRequest, stage rootfshandoff.StageRequest) *protocol.MigrationCPULaunch {
	t.Helper()
	resources := migrationSourceTestResources(t, stage, c.Target)
	// Production reads launch history from the authenticated driver. This fake
	// supplies deterministic evidence without claiming to measure a host CPU.
	launch := &protocol.MigrationCPULaunch{Version: protocol.MigrationCPULaunchVersion,
		ExecutableDigest: digest.FromString("runsc-fixture").String(), Target: c.Target, SandboxID: c.SandboxID,
		RuntimeGeneration: c.SourceGeneration, LaunchAttempt: stage.Identity.LaunchAttempt,
		BindingDigest: c.BindingDigest, ResourceLeaseDigest: c.ResourceLeaseDigest, Resources: resources,
		AssignmentRevision: c.AssignmentRevision, Observation: protocol.MigrationCPUObservation{
			CPUSet: resources.CPUSetCPUs, Profile: protocol.MigrationCPUProfile{
				Version: protocol.MigrationCPUProfileVersion, Architecture: "amd64", RunscVersion: "test",
				Features: []string{"fpu", "sse2"}, CacheLineBytes: 64, XStateLayoutDigest: digest.FromString("xstate-fixture").String(),
			},
		}}
	require.NoError(t, launch.ValidateCapture(c, stage.Identity.LaunchAttempt, resources))
	return launch
}

func checkpointImageNodeFixture(t *testing.T) (*nodeRuntime, protocol.MigrationPublicationRequest, *migrationImageTestRuntime) {
	t.Helper()
	d, publication, runtime := migrationImageNodeFixture(t, func(d *nodeRuntime, source protocol.MigrationCaptureRequest) {
		request := protocol.MigrationStagingRequest{CaptureOnly: true, Target: source.Target, Source: source, Bytes: 8 << 20, Inodes: 64}
		_, err := d.ReserveMigrationStaging(t.Context(), request)
		require.NoError(t, err)
	})
	source, err := publication.SourceAssignment()
	require.NoError(t, err)
	publication.CheckpointSource = &source
	publication.Assignment = runtimecontrol.MigrationAssignment{}
	publication.CPULaunch = checkpointSourceLaunchFixture(t, publication.Capture.Request, runtime.recoverySessions[0].Stage)
	publication.CPUFeaturesDigest, err = publication.CPULaunch.GuestCPUProfile().Digest()
	require.NoError(t, err)
	_, err = publication.Digest()
	require.NoError(t, err)
	return d, publication, runtime
}

func checkpointFinalizeFixture(t *testing.T) (*nodeRuntime, protocol.MigrationSourceFinalizeRequest, *migrationSourceFinalizeTestRuntime, *fakeRuntimeResourceCgroup) {
	t.Helper()
	d, publication, runtime := checkpointImageNodeFixture(t)
	d, fence, fencer, _ := migrationFencePublicationFixture(t, d, publication, runtime)
	return migrationFinalizeFenceFixture(t, d, fence, fencer)
}

func TestCheckpointSourceCleanupRequiresRetainedImageAndPhysicalProof(t *testing.T) {
	d, request, runtime, cgroups := checkpointFinalizeFixture(t)
	require.Empty(t, request.Destination(), "paused memory consumes no target placement")
	require.NotNil(t, request.Checkpoint)
	require.NoError(t, request.Checkpoint.ValidateFor(request.Fence.PublicationRequest, request.Fence.Publication))
	source := request.Fence.PublicationRequest.Capture.Request
	root, err := request.RootFSRequest()
	require.NoError(t, err)
	require.Equal(t, source.OperationID, root.OperationID)
	for _, mutate := range []func(*protocol.MigrationSourceFinalizeRequest){
		func(r *protocol.MigrationSourceFinalizeRequest) { r.Checkpoint = nil },
		func(r *protocol.MigrationSourceFinalizeRequest) { r.Checkpoint.CheckpointID = "" },
		func(r *protocol.MigrationSourceFinalizeRequest) {
			r.Checkpoint.PublicationRequestDigest = strings.Repeat("a", 64)
		},
		func(r *protocol.MigrationSourceFinalizeRequest) {
			r.Checkpoint.Reference.ManifestDigest = digest.FromString("other-image").String()
		},
		func(r *protocol.MigrationSourceFinalizeRequest) { r.Adoption.Request.OperationID = source.OperationID },
		func(r *protocol.MigrationSourceFinalizeRequest) { r.Failure = &protocol.MigrationFailureStopReceipt{} },
		func(r *protocol.MigrationSourceFinalizeRequest) {
			r.Cleanup.ResourceLeaseDigest = strings.Repeat("a", 64)
		},
		func(r *protocol.MigrationSourceFinalizeRequest) { r.SourceProof.ContainerAbsent = false },
	} {
		copy := request
		retained := *request.Checkpoint
		copy.Checkpoint = &retained
		mutate(&copy)
		_, err := d.FinalizeMigrationSource(t.Context(), copy)
		require.Error(t, err)
	}
	require.Zero(t, runtime.finalizeCalls)
	require.Empty(t, cgroups.removedSnapshot())
	before, err := d.journal.Get(source.Target.SlotID)
	require.NoError(t, err)
	runtime.forgetErr = errors.New("session journal unavailable")
	_, err = d.FinalizeMigrationSource(t.Context(), request)
	require.ErrorContains(t, err, "session journal unavailable")
	pending, err := d.GetMigrationSourceFinalization(t.Context(), source.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, pending, "incomplete cleanup cannot authorize regional lease release")
	runtime.forgetErr = nil
	proof, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	require.NoDirExists(t, before.Migration.ImageDirectory)
	require.Equal(t, []protocol.RuntimeResourceLease{request.Cleanup.Resources}, cgroups.removedSnapshot())

	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	d.journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
	retry, err := d.FinalizeMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, proof, retry)
	record, err := d.journal.Get(source.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotCheckpointStagingJournalVersion, record.Version)
	bad := record
	bad.Version = runtimeSlotFinalizationJournalVersion
	payload, err := json.Marshal(bad)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.ErrorContains(t, err, "non-downgradable")
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), before.MigrationStaging.Request))
	_, err = d.ReserveMigrationStaging(t.Context(), before.MigrationStaging.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	// Local cleanup does not remove the durable image.
	_, err = runtime.store.Download(t.Context(), request.Fence.Publication.Binding,
		request.Fence.Publication.Reference, filepath.Join(t.TempDir(), "retained-image"))
	require.NoError(t, err)
}
