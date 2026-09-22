package nomadruntime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type failureStopRunsc struct {
	*migrationFenceTestRunsc
	err error
}

func (r *failureStopRunsc) Delete(ctx context.Context, id string, force bool) error {
	if r.err != nil {
		return r.err
	}
	return r.migrationFenceTestRunsc.Delete(ctx, id, force)
}

func TestMigrationFailureStopRetainsDataAndSurvivesRestart(t *testing.T) {
	d, adoption, runtime, originalRunner := migrationAdoptionNodeFixture(t, func(d *nodeRuntime, image protocol.MigrationImagePrepareRequest) {
		resources, err := image.Resources.Digest()
		require.NoError(t, err)
		_, err = d.ReserveMigrationStaging(t.Context(), protocol.MigrationStagingRequest{Target: image.Target,
			Source: image.Publication.Capture.Request, Destination: image.Target,
			DestinationResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), Bytes: 8 << 20, Inodes: 64})
		require.NoError(t, err)
	})
	custody, err := d.GetMigrationDestination(t.Context(), adoption.Target.SlotID)
	require.NoError(t, err)
	request := protocol.MigrationFailureRequest{Restore: custody.Restore.Request, Reason: protocol.MigrationFailureDestinationUnavailable}
	runner := &failureStopRunsc{migrationFenceTestRunsc: &migrationFenceTestRunsc{migrationSourceTestRunsc: originalRunner}, err: errors.New("injected runsc stop failure")}
	d.runner = runner
	_, err = d.StopFailedMigrationDestination(t.Context(), request)
	require.ErrorContains(t, err, "injected runsc stop failure")
	retained, err := d.GetMigrationDestination(t.Context(), adoption.Target.SlotID)
	require.NoError(t, err)
	require.NotNil(t, retained.Failure)
	require.Nil(t, retained.Failure.Proof)
	_, err = d.AdoptMigrationDestination(t.Context(), adoption)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, d.RecordMigrationRestore(t.Context(), *custody.Restore), errdefs.ErrFailedPrecondition)
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	reopened, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	d.journal = reopened
	runner.err = nil
	proof, err := d.StopFailedMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	retry, err := d.StopFailedMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, proof, retry)
	record, err := d.journal.Get(adoption.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotFailureJournalVersion, record.Version)
	require.NotNil(t, record.MigrationStaging)
	require.False(t, record.MigrationStaging.Released)
	require.Nil(t, record.Cleanup)
	require.Nil(t, record.Proof, "execution stop cannot substitute for full carrier cleanup")
	_, err = d.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	data, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "retained-memory", string(data))
	require.Zero(t, runtime.crashCalls)
	require.Zero(t, runtime.retireCalls)
	require.Zero(t, runtime.externalReclaims)
	// Old journal envelopes cannot silently discard the new execution fence.
	record.Version = runtimeSlotRestoreJournalVersion
	payload, err := json.Marshal(record)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	changed := request
	changed.Reason = protocol.MigrationFailureTermination
	_, err = d.StopFailedMigrationDestination(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	bad := *proof
	bad.ContainerAbsent = false
	require.Error(t, bad.ValidateFor(request))
	command, err := protocol.NewNodeChannelMigrationFailureStopCommand(request)
	require.NoError(t, err)
	result := protocol.NodeChannelResult{Version: protocol.NodeChannelVersion, Kind: command.Kind, RequestID: command.RequestID, MigrationFailureStop: proof}
	require.NoError(t, result.ValidateFor(command))
	result.MigrationFailureStop = &bad
	require.Error(t, result.ValidateFor(command))
}

func TestMigrationFailureStopBeforeLocalRestoreBlocksLateExecution(t *testing.T) {
	d, restore, _, originalRunner := migrationRestoreNodeFixture(t)
	d.runner = &migrationFenceTestRunsc{migrationSourceTestRunsc: originalRunner}
	request := protocol.MigrationFailureRequest{Restore: restore.Request, Reason: protocol.MigrationFailureTermination}
	_, err := d.StopFailedMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	require.ErrorIs(t, d.RecordMigrationRestore(t.Context(), restore), errdefs.ErrFailedPrecondition)
	custody, err := d.GetMigrationDestination(t.Context(), restore.Request.Image.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, custody.Restore)
	require.NotNil(t, custody.Failure.Proof)
}

func TestMigrationFailureStopRejectsAdoptedDestination(t *testing.T) {
	d, adoption, _, _ := migrationAdoptionNodeFixture(t)
	custody, err := d.GetMigrationDestination(t.Context(), adoption.Target.SlotID)
	require.NoError(t, err)
	_, err = d.AdoptMigrationDestination(t.Context(), adoption)
	require.NoError(t, err)
	_, err = d.StopFailedMigrationDestination(t.Context(), protocol.MigrationFailureRequest{
		Restore: custody.Restore.Request, Reason: protocol.MigrationFailureTermination})
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}
