package nomadruntime

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationFenceTestRuntime struct {
	*migrationImageTestRuntime
	detachCalls int
	detachErr   error
	afterDetach func()
}

func (r *migrationFenceTestRuntime) DetachMigrationRootFS(_ context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSDetachRequest) (rootfshandoff.MigrationRootFSDetachProof, error) {
	r.detachCalls++
	if r.detachErr != nil {
		return rootfshandoff.MigrationRootFSDetachProof{}, r.detachErr
	}
	if r.afterDetach != nil {
		r.afterDetach()
	}
	binding, _ := stage.BindingDigest()
	return rootfshandoff.NewMigrationRootFSDetachProof(request, rootfshandoff.CrashFenceSessionObservation{
		Parent: stage.Parent, RootFSID: stage.Identity.RootFSID, WriterEpoch: stage.Identity.WriterEpoch, OperationID: request.OperationID,
		BindingDigest: hex.EncodeToString(binding[:]), SessionState: rootfshandoff.StateTombstoned, BranchPath: "/private/source.wal",
		DeviceBound: true, DevicePath: "/dev/fake0", LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

type migrationFenceTestRunsc struct{ *migrationSourceTestRunsc }

func (r *migrationFenceTestRunsc) Delete(ctx context.Context, id string, force bool) error {
	if err := r.fakeRunsc.Delete(ctx, id, force); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stateErr = errdefs.ErrNotFound
	return nil
}

func migrationFenceNodeFixture(t *testing.T, beforeCapture ...func(*nodeRuntime, protocol.MigrationCaptureRequest)) (*nodeRuntime, protocol.MigrationSourceFenceRequest, *migrationFenceTestRuntime, *migrationFenceTestRunsc) {
	t.Helper()
	daemon, publication, runtime := migrationImageNodeFixture(t, beforeCapture...)
	receipt, err := daemon.PublishMigration(t.Context(), publication)
	require.NoError(t, err)
	fencer := &migrationFenceTestRuntime{migrationImageTestRuntime: runtime}
	runner := &migrationFenceTestRunsc{migrationSourceTestRunsc: daemon.runner.(*migrationSourceTestRunsc)}
	daemon.runtime, daemon.runner, daemon.mounter = fencer, runner, &fakeMounter{}
	record, err := daemon.journal.Get(publication.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	daemon.config.RootFSConsumerMountRoot = filepath.Dir(record.Registration.StableMount)
	return daemon, protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: *receipt}, fencer, runner
}

func TestMigrationSourceFenceRetainsCustodyAcrossInterruptedDetach(t *testing.T) {
	daemon, request, runtime, runner := migrationFenceNodeFixture(t)
	runtime.detachErr = errors.New("NBD still owned")
	_, err := daemon.FenceMigrationSource(t.Context(), request)
	require.ErrorContains(t, err, "NBD still owned")
	record, err := daemon.journal.Get(request.PublicationRequest.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.Migration.SourceFenceRequest)
	require.Nil(t, record.Migration.SourceFenceProof)
	require.Contains(t, runner.callsSnapshot(), "delete:force")
	require.NotContains(t, runner.callsSnapshot(), "kill:KILL")
	path := daemon.journal.db.Path()
	require.NoError(t, daemon.journal.Close())
	reopened, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	daemon.journal = reopened
	runtime.detachErr = nil
	proof, err := daemon.FenceMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	again, err := daemon.FenceMigrationSource(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, proof, again)
	require.Equal(t, 2, runtime.detachCalls)
	record, err = daemon.journal.Get(request.PublicationRequest.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.Migration.Publication)
	require.Nil(t, record.Cleanup, "source fence is not final carrier/resource cleanup")
	require.Nil(t, record.Proof)
}

func TestMigrationSourceFenceRejectsLateExecutionAndMismatchedReceipt(t *testing.T) {
	daemon, request, runtime, runner := migrationFenceNodeFixture(t)
	changed := request
	changed.Publication.RequestDigest = request.PublicationRequest.Capture.RequestDigest
	_, err := daemon.FenceMigrationSource(t.Context(), changed)
	require.Error(t, err)
	runner.setState("running")
	_, err = daemon.FenceMigrationSource(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Zero(t, runtime.detachCalls)
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	runner.setState("stopped")
	_, err = daemon.FenceMigrationSource(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestMigrationSourceFenceRejectsExecutionReappearingDuringDetach(t *testing.T) {
	daemon, request, runtime, runner := migrationFenceNodeFixture(t)
	runtime.afterDetach = func() {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		runner.stateErr = nil
		runner.state = "running"
	}
	_, err := daemon.FenceMigrationSource(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	record, err := daemon.journal.Get(request.PublicationRequest.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.Migration.ExecutionInvalidated)
	require.Nil(t, record.Migration.SourceFenceProof)
	require.NotContains(t, runner.callsSnapshot(), "kill:KILL", "later execution cannot repair the captured image")
}

func TestMigrationSourceFenceRejectsChangedConsumerMountIdentity(t *testing.T) {
	for _, field := range []string{"missing", "namespace", "path"} {
		t.Run(field, func(t *testing.T) {
			daemon, request, runtime, runner := migrationFenceNodeFixture(t)
			session := &runtime.recoverySessions[0]
			consumer := *session.Consumer
			session.Consumer = &consumer
			switch field {
			case "missing":
				session.Consumer = nil
			case "namespace":
				consumer.HostMountNamespace = "mnt:[different]"
			case "path":
				consumer.StableMount += "-different"
			}
			_, err := daemon.FenceMigrationSource(t.Context(), request)
			require.Error(t, err)
			require.Zero(t, runtime.detachCalls)
			require.NotContains(t, runner.callsSnapshot(), "delete:force")
		})
	}
}
