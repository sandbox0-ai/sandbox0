package nomadruntime

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestMigrationRecoveryCannotInitiateCapture(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	registration := testRuntimeSlotJournalRegistration(t, "uncaptured")
	require.NoError(t, journal.Register(registration))
	capture := migrationJournalRequest(t, registration)
	d := &nodeRuntime{journal: journal, clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: capture.Request.Target.NodeUID}
	executor := &nodeRuntimeChannelExecutor{cleaner: d, clusterID: d.clusterID, nodeID: d.nodeID, nodeUID: d.nodeUID}
	_, err = executor.RecoverMigrationCapture(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	record, err := journal.Get(registration.SlotID)
	require.NoError(t, err)
	require.Nil(t, record.Migration, "recovery cannot create first capture intent")
}

func TestMigrationRecoverySealsCompletedCheckpointAfterJournalReopen(t *testing.T) {
	d, capture, runtime, runner := migrationRootFSNodeFixture(t)
	observed, err := d.RecoverMigrationCapture(t.Context(), capture.Request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureIntent, observed.State)
	require.Zero(t, runtime.sealCalls)
	require.Empty(t, runner.callsSnapshot())
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	journal, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	d.journal = journal
	executor := &nodeRuntimeChannelExecutor{cleaner: d, clusterID: d.clusterID, nodeID: d.nodeID, nodeUID: d.nodeUID}
	observed, err = executor.RecoverMigrationCapture(t.Context(), capture.Request)
	require.NoError(t, err, "no driver control socket is needed after completed capture")
	require.NotNil(t, observed.RootFS)
	require.NoError(t, observed.Validate())
	require.Equal(t, 1, runtime.sealCalls)
	require.Equal(t, []string{"state"}, runner.callsSnapshot(), "recovery neither starts nor checkpoints nor kills execution")
	again, err := executor.RecoverMigrationCapture(t.Context(), capture.Request)
	require.NoError(t, err)
	require.Equal(t, observed, again)
	require.Equal(t, 1, runtime.sealCalls)
	changed := capture.Request
	changed.Target.NodeBootID = "different-boot"
	_, err = executor.RecoverMigrationCapture(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	changed = capture.Request
	changed.Target.NodeUID = "different-node"
	_, err = executor.RecoverMigrationCapture(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
	require.Equal(t, 1, runtime.sealCalls)
}

func TestMigrationRecoveryInvalidatesUnexpectedSourceExecution(t *testing.T) {
	d, capture, runtime, runner := migrationRootFSNodeFixture(t)
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	runner.setState("running")
	_, err := d.RecoverMigrationCapture(t.Context(), capture.Request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Zero(t, runtime.sealCalls)
	require.Equal(t, []string{"state"}, runner.callsSnapshot())
	observed, err := d.RecoverMigrationCapture(t.Context(), capture.Request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureUncertain, observed.State)
	require.Nil(t, observed.RootFS)
	require.Zero(t, runtime.sealCalls)
}
