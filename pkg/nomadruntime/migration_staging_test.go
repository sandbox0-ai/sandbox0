package nomadruntime

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type testMigrationStagingGuard struct {
	verify func(string) error
	admit  func(string) error
	budget func(string, int64, uint64) error
}

func (g testMigrationStagingGuard) Verify(path string) error {
	if g.verify != nil {
		return g.verify(path)
	}
	return nil
}
func (g testMigrationStagingGuard) Admit(path string) error {
	if g.admit != nil {
		return g.admit(path)
	}
	return nil
}

func (g testMigrationStagingGuard) AdmitBudget(path string, bytes int64, inodes uint64) error {
	if g.budget != nil {
		return g.budget(path, bytes, inodes)
	}
	return nil
}

func TestMigrationStagingRefusesNewSourceButPreservesOutcomeRecovery(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	r := testRuntimeSlotJournalRegistration(t, "quota-source")
	require.NoError(t, journal.Register(r))
	capture := migrationJournalRequest(t, r)
	d := &nodeRuntime{journal: journal, clusterID: r.ClusterID, nodeID: r.NodeID, nodeUID: capture.Request.Target.NodeUID}
	require.ErrorIs(t, d.RecordMigrationCapture(t.Context(), capture), errdefs.ErrUnavailable)
	stored, err := journal.Get(r.SlotID)
	require.NoError(t, err)
	require.Nil(t, stored.Migration)
	d.migrationStaging = testMigrationStagingGuard{}
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture))
	d.migrationStaging = nil
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, d.RecordMigrationCapture(t.Context(), capture), "quota failure cannot erase a completed checkpoint observation")
	result, err := d.GetMigrationCapture(t.Context(), r.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureComplete, result.Capture.State)
}

func TestMigrationStagingDestinationChecksBeforeWriteAndRecoversFullPartialImage(t *testing.T) {
	d, request, runtime := migrationImageDestinationFixture(t)
	d.migrationStaging = nil
	_, err := d.PrepareMigrationImage(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.Zero(t, runtime.calls)
	d.migrationStaging = testMigrationStagingGuard{}
	runtime.fail = true
	_, err = d.PrepareMigrationImage(t.Context(), request)
	require.Error(t, err)
	stored, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	partial := filepath.Join(stored.MigrationDestination.ImageDirectory, "partial.img")
	require.FileExists(t, partial)
	d.migrationStaging = testMigrationStagingGuard{admit: func(string) error {
		_, err := os.Stat(partial)
		require.ErrorIs(t, err, os.ErrNotExist, "only an unused destination may discard its exact partial image before readmitting disk")
		return nil
	}}
	runtime.fail = false
	_, err = d.PrepareMigrationImage(t.Context(), request)
	require.NoError(t, err)
	d.migrationStaging = testMigrationStagingGuard{admit: func(string) error { return errdefs.ErrResourceExhausted }}
	_, err = d.PrepareMigrationImage(t.Context(), request)
	require.NoError(t, err, "an exact prepared-image verification does not allocate new image bytes")
}

func TestMigrationStagingUsesVerifiedImageFootprintBeforeDownload(t *testing.T) {
	d, request, _ := migrationImageDestinationFixture(t)
	calls := 0
	d.migrationStaging = testMigrationStagingGuard{budget: func(root string, bytes int64, inodes uint64) error {
		calls++
		require.Equal(t, d.journal.migrationRoot, root)
		require.EqualValues(t, 8192, bytes)
		require.EqualValues(t, 2, inodes)
		return errdefs.ErrResourceExhausted
	}}
	_, err := d.PrepareMigrationImage(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrResourceExhausted)
	require.Equal(t, 1, calls)
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.MigrationDestination, "intent remains available for exact retry")
	require.Nil(t, record.MigrationDestination.Prepared)
	require.NoDirExists(t, record.MigrationDestination.ImageDirectory)
	d.migrationStaging = testMigrationStagingGuard{}
	prepared, err := d.PrepareMigrationImage(t.Context(), request)
	require.NoError(t, err)
	d.migrationStaging = testMigrationStagingGuard{budget: func(string, int64, uint64) error {
		t.Fatal("prepared verification must not reserve new disk")
		return nil
	}}
	again, err := d.PrepareMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, prepared, again)
}
