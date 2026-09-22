package nomadruntime

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
)

func TestMigrationInventoryIsBoundedAndConsumedWithoutAuthorizingChangedBytes(t *testing.T) {
	d, request, fixture := migrationImageNodeFixture(t)
	custody, err := d.GetMigrationCapture(t.Context(), request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	binding, err := request.Binding()
	require.NoError(t, err)
	r := &rootfsRuntime{checkpoints: fixture.store}
	directory := custody.ImageDirectory
	require.NoError(t, r.PrimeMigrationImageInventory(t.Context(), directory))
	original, err := fixture.store.PlanLocal(t.Context(), binding, directory)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "checkpoint.img"), []byte("replaced-memory"), 0o600))
	plan, err := r.PlanMigrationImage(t.Context(), binding, directory)
	require.NoError(t, err)
	require.Equal(t, original, plan, "consume the completed inventory without another scan")
	require.Empty(t, r.migrationInventories)
	_, err = r.PublishPlannedMigrationImage(t.Context(), binding, plan, directory)
	require.Error(t, err, "a hash cache hit cannot publish changed bytes")
	require.Error(t, r.WritePlannedMigrationPeerImage(t.Context(), binding, plan, directory, io.Discard))
	fresh, err := r.PlanMigrationImage(t.Context(), binding, directory)
	require.NoError(t, err)
	require.NotEqual(t, plan.Reference, fresh.Reference, "consumed entries fall back to a full scan")
	for i := 0; i < maxMigrationImageCustodies+2; i++ {
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "state"), []byte("state"), 0o600))
		require.NoError(t, r.PrimeMigrationImageInventory(t.Context(), dir))
		require.LessOrEqual(t, len(r.migrationInventories), maxMigrationImageCustodies)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, r.PrimeMigrationImageInventory(ctx, directory), context.Canceled)
	_, found := r.migrationInventories[directory]
	require.False(t, found)
	r.migrationInventories = map[string]runtimecheckpoint.LocalImageInventory{}
	_, err = r.PlanMigrationImage(t.Context(), binding, directory)
	require.NoError(t, err, "daemon restart or cache loss uses the normal planning path")
}
