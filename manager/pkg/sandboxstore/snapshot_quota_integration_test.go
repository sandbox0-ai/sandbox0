package sandboxstore

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/stretchr/testify/require"
)

func snapshotQuotaSandbox(t *testing.T, store *PGSandboxStore, id, team string) *RootFSFilesystem {
	t.Helper()
	record := rootFSTestSandboxRecord(id, team)
	record.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(t.Context(), record))
	artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, _, err := store.EnsureInitialRootFSGeneration(t.Context(), &EnsureInitialRootFSGenerationRequest{
		SandboxID: id, TeamID: team, SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	return filesystem
}

func setSnapshotQuota(t *testing.T, store *PGSandboxStore, team string, limit int64) {
	t.Helper()
	require.NoError(t, quota.NewRepository(store.pool).PutPolicy(t.Context(), &quota.Policy{
		TeamID: team, Dimension: quota.DimensionSnapshotsPerSandbox, LimitValue: limit,
	}))
}

func TestSnapshotQuotaDefaultRetainsNewestAndExcludesInternalAndExpiredIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	snapshotQuotaSandbox(t, store, "snapshot-source", "team-1")
	for i := range 12 {
		_, err := store.CreateRootFSSnapshot(t.Context(), &CreateRootFSSnapshotRequest{
			SandboxID: "snapshot-source", SnapshotID: fmt.Sprintf("public-%02d", i),
		})
		require.NoError(t, err)
	}
	_, err := store.GetRootFSSnapshot(t.Context(), "public-00", "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
	_, err = store.GetRootFSSnapshot(t.Context(), "public-01", "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
	for i := 2; i < 12; i++ {
		_, err := store.GetRootFSSnapshot(t.Context(), fmt.Sprintf("public-%02d", i), "team-1")
		require.NoError(t, err)
	}
	for _, id := range []string{"template-build-retained", "expired-public"} {
		request := &CreateRootFSSnapshotRequest{SandboxID: "snapshot-source", SnapshotID: id}
		if id == "expired-public" {
			request.ExpiresAt = time.Now().Add(-time.Minute)
		}
		_, err := store.CreateRootFSSnapshot(t.Context(), request)
		require.NoError(t, err)
	}
	current, err := store.MaxSnapshotsPerSandbox(t.Context(), "team-1")
	require.NoError(t, err)
	require.Equal(t, int64(10), current)
	_, err = store.GetRootFSSnapshot(t.Context(), "template-build-retained", "team-1")
	require.NoError(t, err)
	_, err = store.GetRootFSSnapshot(t.Context(), "public-02", "team-1")
	require.NoError(t, err, "expired and internal snapshots must not evict live public snapshots")
}

func TestSnapshotQuotaOverridesArePerSandboxAndMaintenanceAppliesReductionsIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	require.NoError(t, quota.NewRepository(store.pool).SyncDefaultPolicies(t.Context(), "test", []quota.DefaultLimit{
		{Dimension: quota.DimensionSnapshotsPerSandbox, LimitValue: 2},
	}))
	setSnapshotQuota(t, store, "team-1", 4)
	for _, id := range []string{"source-a", "source-b", "other-team"} {
		team := "team-1"
		if id == "other-team" {
			team = "team-2"
		}
		snapshotQuotaSandbox(t, store, id, team)
		for i := range 4 {
			_, err := store.CreateRootFSSnapshot(t.Context(), &CreateRootFSSnapshotRequest{
				SandboxID: id, SnapshotID: fmt.Sprintf("%s-%02d", id, i),
			})
			require.NoError(t, err)
		}
	}
	current, err := store.MaxSnapshotsPerSandbox(t.Context(), "team-1")
	require.NoError(t, err)
	require.Equal(t, int64(4), current, "each sandbox has four; the team total is eight")
	current, err = store.MaxSnapshotsPerSandbox(t.Context(), "team-2")
	require.NoError(t, err)
	require.Equal(t, int64(2), current, "other teams inherit the regional quota")
	setSnapshotQuota(t, store, "team-1", 2)
	deleted, err := store.PruneExcessRootFSSnapshots(t.Context(), "team-1", 1)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "maintenance honors its deletion batch budget")
	_, err = store.GetRootFSSnapshot(t.Context(), "source-a-00", "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound, "the oldest excess snapshot is removed first")
	for range 4 {
		_, err := store.PruneExcessRootFSSnapshots(t.Context(), "team-1", 1)
		require.NoError(t, err)
	}
	for _, id := range []string{"source-a", "source-b"} {
		snapshots, err := store.ListRootFSSnapshots(t.Context(), &ListRootFSSnapshotsRequest{SandboxID: id, TeamID: "team-1"})
		require.NoError(t, err)
		require.Len(t, snapshots, 2)
		_, err = store.GetRootFSSnapshot(t.Context(), id+"-03", "team-1")
		require.NoError(t, err)
	}
}

func TestSnapshotQuotaSerializesConcurrentCreationIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	snapshotQuotaSandbox(t, store, "concurrent-source", "team-1")
	setSnapshotQuota(t, store, "team-1", 3)
	const attempts = 16
	errs := make([]error, attempts)
	var group sync.WaitGroup
	for i := range attempts {
		group.Go(func() {
			_, errs[i] = store.CreateRootFSSnapshot(t.Context(), &CreateRootFSSnapshotRequest{
				SandboxID: "concurrent-source", SnapshotID: fmt.Sprintf("concurrent-%02d", i),
			})
		})
	}
	group.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	current, err := store.MaxSnapshotsPerSandbox(t.Context(), "team-1")
	require.NoError(t, err)
	require.Equal(t, int64(3), current)
}

func TestSnapshotQuotaDeletionFailureRollsBackCreationIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	snapshotQuotaSandbox(t, store, "rollback-source", "team-1")
	setSnapshotQuota(t, store, "team-1", 1)
	_, err := store.CreateRootFSSnapshot(t.Context(), &CreateRootFSSnapshotRequest{SandboxID: "rollback-source", SnapshotID: "old"})
	require.NoError(t, err)
	_, err = store.pool.Exec(t.Context(), `
		CREATE FUNCTION manager.fail_snapshot_pruning() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'simulated snapshot pruning failure'; END; $$;
		CREATE TRIGGER fail_snapshot_pruning BEFORE DELETE ON manager.rootfs_snapshots
		FOR EACH ROW EXECUTE FUNCTION manager.fail_snapshot_pruning();
	`)
	require.NoError(t, err)
	_, err = store.CreateRootFSSnapshot(t.Context(), &CreateRootFSSnapshotRequest{SandboxID: "rollback-source", SnapshotID: "new"})
	require.ErrorContains(t, err, "simulated snapshot pruning failure")
	_, err = store.GetRootFSSnapshot(t.Context(), "old", "team-1")
	require.NoError(t, err)
	_, err = store.GetRootFSSnapshot(t.Context(), "new", "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
}

func TestSnapshotQuotaRunningCaptureReleasesPinAndPreservesDerivedSandboxIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "snapshot-quota-running")
	source, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	setSnapshotQuota(t, f.store, source.TeamID, 1)
	for i := range 3 {
		request := &NomadRunningRootFSCaptureRequest{
			OperationID: fmt.Sprintf("quota-capture-%d", i), SourceSandboxID: source.ID,
			TeamID: source.TeamID, SnapshotID: fmt.Sprintf("quota-running-%d", i),
			CaptureKind: NomadRunningRootFSCaptureKindSnapshot,
		}
		candidate, err := f.store.RequestNomadRunningRootFSCapture(f.ctx, request)
		require.NoError(t, err)
		publication := nomadTemplateCaptureCheckpointRequest(t, f, source, candidate)
		_, err = f.store.ForkRunningRootFSFilesystem(f.ctx, publication)
		require.NoError(t, err)
		completed, err := f.store.RequestNomadRunningRootFSCapture(f.ctx, request)
		require.NoError(t, err, "a retained snapshot can be retried without another eviction")
		require.True(t, completed.Completed)
		if i == 0 {
			snapshotQuotaSandbox(t, f.store, "derived-sandbox", source.TeamID)
			_, err := f.store.RestoreRootFSFromSnapshot(f.ctx, &RestoreRootFSFromSnapshotRequest{
				SandboxID: "derived-sandbox", SnapshotID: request.SnapshotID,
				TeamID: source.TeamID, OperationID: "derived-restore",
			})
			require.NoError(t, err)
		}
	}
	_, err = f.store.GetRootFSSnapshot(f.ctx, "quota-running-0", source.TeamID)
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
	var reason string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT cancel_reason FROM manager.rootfs_running_template_captures WHERE operation_id='quota-capture-0'`).Scan(&reason))
	require.Equal(t, "snapshot retention quota", reason)
	deleted, err := f.store.DeleteReleasedNomadRunningRootFSCaptures(f.ctx, source.TeamID, 10)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "the evicted capture without a derived owner is collected")
	_, err = f.store.GetRootFSSnapshot(f.ctx, "quota-running-2", source.TeamID)
	require.NoError(t, err, "the newest running snapshot remains usable")
	_, err = f.store.DeleteUnreferencedRootFSGenerations(f.ctx, source.TeamID, 10)
	require.NoError(t, err)
	derived, err := f.store.GetRootFSFilesystem(f.ctx, "derived-sandbox")
	require.NoError(t, err)
	generation, err := f.store.GetRootFSGeneration(f.ctx, derived.HeadGenerationID)
	require.NoError(t, err)
	require.NotNil(t, generation, "snapshot pruning cannot collect data retained by a restored sandbox")
	loadedSource, err := f.store.GetSandbox(f.ctx, source.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, loadedSource.DesiredState)
	require.Equal(t, source.RuntimeID, loadedSource.RuntimeID)
}
