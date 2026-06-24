package service

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveRootFSStateWritesCheckpointAndFilesystemHeadOnly(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("SELECT 1"),
		},
	}
	state := rootFSTestState()
	state.LayerID = "layer-1"

	err := saveRootFSState(context.Background(), exec, state)

	require.NoError(t, err)
	require.Len(t, exec.sqls, 2)
	assert.Contains(t, exec.sqls[0], "INSERT INTO manager.rootfs_layers")
	assert.Contains(t, exec.sqls[1], "INSERT INTO manager.rootfs_filesystems")
	for _, sql := range exec.sqls {
		assert.NotContains(t, sql, "INSERT INTO manager.rootfs_objects")
		assert.NotContains(t, sql, "INSERT INTO manager.sandbox_rootfs_states")
		assert.NotContains(t, sql, "INSERT INTO manager.sandbox_rootfs_heads")
	}
}

func TestSaveRootFSStateRequiresLayerID(t *testing.T) {
	exec := &recordingRootFSStateExecutor{}
	state := rootFSTestState()
	state.LayerID = ""

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorContains(t, err, "layer_id is required")
	assert.Empty(t, exec.sqls)
}

func TestSaveRootFSStateWritesS0FSLayerWithoutObjectInventory(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("SELECT 1"),
		},
	}
	state := rootFSTestState()
	state.LayerID = "layer-s0fs"
	state.StorageEngine = ctldapi.RootFSStorageEngineS0FS
	state.S0FSVolumeID = "fs-1"
	state.S0FSManifestKey = "manifests/00000000000000000007.json"
	state.S0FSManifestSeq = 7
	state.S0FSCheckpointSeq = 3

	err := saveRootFSState(context.Background(), exec, state)

	require.NoError(t, err)
	require.Len(t, exec.sqls, 2)
	assert.NotContains(t, exec.sqls[0], "INSERT INTO manager.rootfs_objects")
	assert.Contains(t, exec.sqls[0], "INSERT INTO manager.rootfs_layers")
	assert.Contains(t, exec.sqls[1], "INSERT INTO manager.rootfs_filesystems")
	require.Len(t, exec.args, 2)
	assert.Equal(t, ctldapi.RootFSStorageEngineS0FS, exec.args[0][12])
	assert.Equal(t, "fs-1", exec.args[0][13])
	assert.Equal(t, "manifests/00000000000000000007.json", exec.args[0][14])
	assert.Equal(t, int64(7), exec.args[0][15])
	assert.Equal(t, int64(3), exec.args[0][16])
}

func TestSaveRootFSStateRequiresS0FSManifest(t *testing.T) {
	exec := &recordingRootFSStateExecutor{}
	state := rootFSTestState()
	state.LayerID = "layer-s0fs"
	state.StorageEngine = ctldapi.RootFSStorageEngineS0FS
	state.S0FSVolumeID = "fs-1"
	state.S0FSManifestKey = ""

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorContains(t, err, "s0fs_manifest_key is required")
	assert.Empty(t, exec.sqls)
}

func TestSaveRootFSStateMapsHeadCASMissToConflict(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("SELECT 0"),
		},
	}
	state := rootFSTestState()
	state.LayerID = "layer-child"
	state.ParentLayerID = "layer-stale"

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorIs(t, err, ErrRootFSHeadConflict)
	require.Len(t, exec.sqls, 2)
}

func TestSaveRootFSStateUsesExpectedHeadLayerIDWhenParentDiffers(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("SELECT 1"),
		},
	}
	state := rootFSTestState()
	state.LayerID = "layer-full"
	state.ParentLayerID = ""
	state.ExpectedHeadLayerID = "layer-parent"

	err := saveRootFSState(context.Background(), exec, state)

	require.NoError(t, err)
	require.Len(t, exec.args, 2)
	assert.Equal(t, "layer-parent", exec.args[1][3])
}

func TestTrimRootFSS0FSGarbageCollectionPlanCapsObjectDeletes(t *testing.T) {
	plan := &s0fs.GarbageCollectionPlan{
		Segments:  []string{"segments/a.bin", "segments/b.bin"},
		Manifests: []string{"manifests/a.json", "manifests/b.json"},
	}

	trimRootFSS0FSGarbageCollectionPlan(plan, 3)

	assert.Equal(t, []string{"segments/a.bin", "segments/b.bin"}, plan.Segments)
	assert.Equal(t, []string{"manifests/a.json"}, plan.Manifests)
}

type recordingRootFSStateExecutor struct {
	sqls []string
	args [][]any
	tags []pgconn.CommandTag
	err  error
}

func (e *recordingRootFSStateExecutor) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	e.sqls = append(e.sqls, strings.Join(strings.Fields(sql), " "))
	e.args = append(e.args, args)
	if e.err != nil {
		return pgconn.CommandTag{}, e.err
	}
	if len(e.tags) == 0 {
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	tag := e.tags[0]
	e.tags = e.tags[1:]
	return tag, nil
}
