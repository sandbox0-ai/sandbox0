package nomadruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationImageDownloadTestRuntime struct {
	*fakeRootFSRuntime
	store           *runtimecheckpoint.Store
	fail            bool
	calls           int
	downloadCalls   int
	verifyCalls     int
	started, resume chan struct{}
}

func (r *migrationImageDownloadTestRuntime) PrepareMigrationImageFiles(ctx context.Context, binding runtimecheckpoint.Binding, ref runtimecheckpoint.Reference, directory string, verify bool, admit func(int64, uint64) error) (runtimecheckpoint.Manifest, error) {
	r.calls++
	if verify {
		r.verifyCalls++
	} else {
		r.downloadCalls++
	}
	if r.started != nil {
		close(r.started)
		select {
		case <-r.resume:
		case <-ctx.Done():
			return runtimecheckpoint.Manifest{}, ctx.Err()
		}
	}
	if r.fail {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return runtimecheckpoint.Manifest{}, err
		}
		if err := os.WriteFile(filepath.Join(directory, "partial.img"), []byte("partial"), 0o600); err != nil {
			return runtimecheckpoint.Manifest{}, err
		}
		return runtimecheckpoint.Manifest{}, errors.New("interrupted image transfer")
	}
	if verify {
		return r.store.VerifyLocal(ctx, binding, ref, directory)
	}
	return r.store.DownloadWithAdmission(ctx, binding, ref, directory, admit)
}

func migrationImageDestinationFixture(t *testing.T, beforePublish ...func(*nodeRuntime, *protocol.MigrationPublicationRequest, *migrationImageTestRuntime)) (*nodeRuntime, protocol.MigrationImagePrepareRequest, *migrationImageDownloadTestRuntime) {
	t.Helper()
	source, publication, images := migrationImageNodeFixture(t)
	for _, prepare := range beforePublish {
		prepare(source, &publication, images)
	}
	receipt, err := source.PublishMigration(t.Context(), publication)
	require.NoError(t, err)
	return migrationImageDestinationForPublication(t, publication, *receipt, images, nil)
}

func migrationImageDestinationForPublication(t *testing.T, publication protocol.MigrationPublicationRequest, receipt protocol.MigrationPublication, images *migrationImageTestRuntime, checkpoint *protocol.CheckpointRestoreAuthority) (*nodeRuntime, protocol.MigrationImagePrepareRequest, *migrationImageDownloadTestRuntime) {
	t.Helper()
	registration := testRuntimeSlotJournalRegistration(t, "target-slot")
	registration.NodeID, registration.NodeBootID, registration.AllocationID = "target-node", "target-boot", "target-allocation"
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "target.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	operation, uid := publication.Assignment.OperationID, "target-uid"
	if checkpoint != nil {
		operation = checkpoint.Assignment.OperationID
		registration.NodeID, registration.NodeBootID = publication.Capture.Request.Target.NodeID, publication.Capture.Request.Target.NodeBootID
		uid = publication.Capture.Request.Target.NodeUID
	}
	require.NoError(t, journal.Register(registration))
	resources, err := protocol.NewRuntimeResourceLease(operation, "target-claim", registration.SlotID,
		registration.ClusterID, registration.NodeID, uid, registration.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	request := protocol.MigrationImagePrepareRequest{Target: protocol.NodeChannelTarget{SlotID: registration.SlotID, ClusterID: registration.ClusterID,
		NodeID: registration.NodeID, NodeUID: resources.NodeUID, NodeBootID: registration.NodeBootID, AllocationID: registration.AllocationID,
		ControlEndpoint: "unix:///private/target.sock"}, Publication: publication, Receipt: receipt, Resources: resources, Checkpoint: checkpoint}
	require.NoError(t, request.Validate())
	runtime := &migrationImageDownloadTestRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}, store: images.store}
	runner := &migrationSourceTestRunsc{fakeRunsc: newFakeRunsc()}
	runner.stateErr = errdefs.ErrNotFound
	parent, cancel := context.WithCancel(t.Context())
	target := &nodeRuntime{migrationStaging: testMigrationStagingGuard{}, runtime: runtime, runner: runner, journal: journal, migrationContext: parent,
		clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: resources.NodeUID}
	t.Cleanup(func() { cancel(); target.wg.Wait() })
	return target, request, runtime
}

func TestMigrationImageDestinationRetriesPartialDownloadAcrossRestart(t *testing.T) {
	daemon, request, runtime := migrationImageDestinationFixture(t)
	runtime.fail = true
	_, err := daemon.PrepareMigrationImage(t.Context(), request)
	require.ErrorContains(t, err, "interrupted image transfer")
	record, err := daemon.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotMigrationJournalVersion, record.Version)
	require.NotNil(t, record.MigrationDestination)
	require.Nil(t, record.MigrationDestination.Prepared)
	_, err = daemon.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "generic cleanup cannot discard image custody")
	changed := request
	changed.Target.ControlEndpoint = "unix:///private/replacement.sock"
	_, err = daemon.PrepareMigrationImage(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	path := daemon.journal.db.Path()
	require.NoError(t, daemon.journal.Close())
	reopened, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	daemon.journal = reopened
	runtime.fail = false
	prepared, err := daemon.PrepareMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(request))
	require.Equal(t, int64(len("retained-memory")), prepared.TotalBytes)
	_, err = os.Stat(filepath.Join(record.MigrationDestination.ImageDirectory, "partial.img"))
	require.ErrorIs(t, err, os.ErrNotExist)
	again, err := daemon.PrepareMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, prepared, again)
	// A journaled receipt cannot repair or bless corruption after restart.
	require.NoError(t, os.WriteFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"), []byte("corrupt!-memory"), 0o600))
	_, err = daemon.PrepareMigrationImage(t.Context(), request)
	require.Error(t, err)
	data, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "corrupt!-memory", string(data), "completed custody must never be silently replaced")
}

func TestMigrationImageDestinationSurvivesCallerDisconnect(t *testing.T) {
	daemon, request, runtime := migrationImageDestinationFixture(t)
	runtime.started, runtime.resume = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := daemon.PrepareMigrationImage(ctx, request); done <- err }()
	select {
	case <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	record, err := daemon.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	request.Publication.Capture.RootFS.Sequence++ // Worker must own nested data.
	close(runtime.resume)
	daemon.wg.Wait()
	record, err = daemon.journal.Get(record.Registration.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.MigrationDestination.Prepared)
	require.Equal(t, 1, runtime.calls)
}

func TestMigrationImageDestinationRejectsWrongBootAndExistingExecution(t *testing.T) {
	daemon, request, runtime := migrationImageDestinationFixture(t)
	changed := request
	changed.Target.NodeBootID = "replacement-boot"
	changed.Resources.NodeBootID = "replacement-boot"
	_, err := daemon.PrepareMigrationImage(t.Context(), changed)
	require.Error(t, err)
	runner := daemon.runner.(*migrationSourceTestRunsc)
	runner.stateErr = nil
	runner.setState("running")
	_, err = daemon.PrepareMigrationImage(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Zero(t, runtime.calls)
	require.NotContains(t, runner.calls, "kill:KILL")
}
