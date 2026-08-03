package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveRootFSStateWritesLayerAndFilesystemHeadOnly(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("DELETE 0"),
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("SELECT 1"),
		},
	}
	state := rootFSTestState()
	state.LayerID = "layer-1"
	state.PlatformOS = "linux"
	state.PlatformArchitecture = "arm64"
	state.PlatformVariant = "v8"

	err := saveRootFSState(context.Background(), exec, state)

	require.NoError(t, err)
	require.Len(t, exec.sqls, 4)
	assert.Contains(t, exec.sqls[0], "INSERT INTO manager.rootfs_objects")
	assert.Contains(t, exec.sqls[1], "DELETE FROM manager.rootfs_object_deletions")
	assert.Contains(t, exec.sqls[2], "INSERT INTO manager.rootfs_layers")
	assert.Contains(t, exec.sqls[3], "INSERT INTO manager.rootfs_filesystems")
	for _, sql := range exec.sqls {
		assert.NotContains(t, sql, "INSERT INTO manager.sandbox_rootfs_states")
		assert.NotContains(t, sql, "INSERT INTO manager.sandbox_rootfs_heads")
	}
	assert.Equal(t, state.DiffID, exec.args[2][13])
	assert.Equal(t, state.PlatformOS, exec.args[2][17])
	assert.Equal(t, state.PlatformArchitecture, exec.args[2][18])
	assert.Equal(t, state.PlatformVariant, exec.args[2][19])
}

func TestSaveRootFSStateRequiresLayerID(t *testing.T) {
	exec := &recordingRootFSStateExecutor{}
	state := rootFSTestState()

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorContains(t, err, "layer_id is required")
	assert.Empty(t, exec.sqls)
}

func TestSaveRootFSStateMapsHeadCASMissToConflict(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("DELETE 0"),
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("SELECT 0"),
		},
	}
	state := rootFSTestState()
	state.LayerID = "layer-child"
	state.ParentLayerID = "layer-stale"

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorIs(t, err, ErrRootFSHeadConflict)
	require.Len(t, exec.sqls, 4)
}

func TestSaveRootFSStateUsesExpectedHeadLayerIDWhenParentDiffers(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("DELETE 0"),
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
	require.Len(t, exec.args, 4)
	assert.Equal(t, "layer-parent", exec.args[3][3])
}

func TestSaveRootFSStateMapsObjectMetadataConflict(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 0"),
		},
	}
	state := rootFSTestState()
	state.LayerID = "layer-conflict"

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorIs(t, err, ErrRootFSObjectConflict)
	require.Len(t, exec.sqls, 1)
	assert.Contains(t, exec.sqls[0], "INSERT INTO manager.rootfs_objects")
}

func TestDeleteRootFSObjectsDedupesAndSkipsEmptyKeys(t *testing.T) {
	deleter := &recordingRootFSObjectDeleter{}

	deleted, err := DeleteRootFSObjects(context.Background(), deleter, []*SandboxRootFSLayer{
		{DiffObjectKey: " rootfs/a.tar "},
		nil,
		{DiffObjectKey: ""},
		{DiffObjectKey: "rootfs/a.tar"},
		{DiffObjectKey: "rootfs/b.tar"},
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"rootfs/a.tar", "rootfs/b.tar"}, deleted)
	assert.Equal(t, []string{"rootfs/a.tar", "rootfs/b.tar"}, deleter.keys)
}

func TestDeleteRootFSObjectsReturnsDeletedKeysBeforeFailure(t *testing.T) {
	deleteErr := errors.New("delete failed")
	deleter := &recordingRootFSObjectDeleter{failKey: "rootfs/b.tar", err: deleteErr}

	deleted, err := DeleteRootFSObjects(context.Background(), deleter, []*SandboxRootFSLayer{
		{DiffObjectKey: "rootfs/a.tar"},
		{DiffObjectKey: "rootfs/b.tar"},
		{DiffObjectKey: "rootfs/c.tar"},
	})

	require.ErrorIs(t, err, deleteErr)
	assert.Equal(t, []string{"rootfs/a.tar"}, deleted)
	assert.Equal(t, []string{"rootfs/a.tar", "rootfs/b.tar"}, deleter.keys)
}

func TestDeleteRootFSObjectsHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deleter := &recordingRootFSObjectDeleter{}

	deleted, err := DeleteRootFSObjects(ctx, deleter, []*SandboxRootFSLayer{
		{DiffObjectKey: "rootfs/a.tar"},
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, deleted)
	assert.Empty(t, deleter.keys)
}

type recordingRootFSObjectDeleter struct {
	keys    []string
	failKey string
	err     error
}

func (d *recordingRootFSObjectDeleter) Delete(key string) error {
	d.keys = append(d.keys, key)
	if key == d.failKey {
		return d.err
	}
	return nil
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
