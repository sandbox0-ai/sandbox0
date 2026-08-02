package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveRootFSStateWritesLayerAndFilesystemHeadOnly(t *testing.T) {
	exec := &recordingRootFSStateExecutor{}
	state := rootFSTestState()
	state.LayerID = "layer-1"
	state.PlatformOS = "linux"
	state.PlatformArchitecture = "arm64"
	state.PlatformVariant = "v8"

	err := saveRootFSState(context.Background(), exec, state)

	require.NoError(t, err)
	require.Len(t, exec.sqls, 3)
	assert.Contains(t, exec.sqls[0], "INSERT INTO manager.rootfs_layers")
	assert.Contains(t, exec.sqls[1], "INSERT INTO manager.rootfs_objects")
	assert.Contains(t, exec.sqls[1], "DELETE FROM manager.rootfs_object_deletions")
	assert.Contains(t, exec.sqls[1], "INSERT INTO manager.rootfs_layer_objects")
	assert.Contains(t, exec.sqls[2], "INSERT INTO manager.rootfs_filesystems")
	for _, sql := range exec.sqls {
		assert.NotContains(t, sql, "INSERT INTO manager.sandbox_rootfs_states")
		assert.NotContains(t, sql, "INSERT INTO manager.sandbox_rootfs_heads")
	}
	assert.Equal(t, state.DiffID, exec.args[0][13])
	assert.Equal(t, state.PlatformOS, exec.args[0][23])
	assert.Equal(t, state.PlatformArchitecture, exec.args[0][24])
	assert.Equal(t, state.PlatformVariant, exec.args[0][25])
}

func TestSaveRootFSStateRequiresLayerID(t *testing.T) {
	exec := &recordingRootFSStateExecutor{}
	state := rootFSTestState()

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorContains(t, err, "layer_id is required")
	assert.Empty(t, exec.sqls)
}

func TestSaveRootFSStateRejectsUnsupportedObjectMediaTypeBeforeWriting(t *testing.T) {
	exec := &recordingRootFSStateExecutor{}
	state := rootFSTestState()
	state.LayerID = "layer-invalid-object"
	state.Objects = []rootfshead.Object{{
		Key:       "sandbox-rootfs/untrusted-object",
		Digest:    rootFSTestDiffDigest,
		Size:      12,
		MediaType: "application/octet-stream",
	}}

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorContains(t, err, "unsupported media type")
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
	require.Len(t, exec.sqls, 3)
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
	require.Len(t, exec.args, 3)
	assert.Equal(t, "layer-parent", exec.args[2][3])
}

func TestSaveRootFSStateMapsObjectMetadataConflict(t *testing.T) {
	exec := &recordingRootFSStateExecutor{
		tags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
		},
		objectCounts: [][2]int64{{2, 1}},
	}
	state := rootFSTestState()
	state.LayerID = "layer-conflict"

	err := saveRootFSState(context.Background(), exec, state)

	require.ErrorIs(t, err, ErrRootFSObjectConflict)
	require.Len(t, exec.sqls, 2)
	assert.Contains(t, exec.sqls[1], "INSERT INTO manager.rootfs_objects")
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
	sqls         []string
	args         [][]any
	tags         []pgconn.CommandTag
	objectCounts [][2]int64
	err          error
}

func (e *recordingRootFSStateExecutor) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	e.sqls = append(e.sqls, strings.Join(strings.Fields(sql), " "))
	e.args = append(e.args, args)
	if e.err != nil {
		return recordingRootFSRow{err: e.err}
	}
	if len(e.objectCounts) > 0 {
		counts := e.objectCounts[0]
		e.objectCounts = e.objectCounts[1:]
		return recordingRootFSRow{values: counts}
	}
	var objects []rootfshead.Object
	if len(args) == 0 {
		return recordingRootFSRow{err: fmt.Errorf("rootfs object inventory argument is required")}
	}
	payload, ok := args[0].([]byte)
	if !ok {
		return recordingRootFSRow{err: fmt.Errorf("rootfs object inventory argument has type %T", args[0])}
	}
	if err := json.Unmarshal(payload, &objects); err != nil {
		return recordingRootFSRow{err: err}
	}
	count := int64(len(objects))
	return recordingRootFSRow{values: [2]int64{count, count}}
}

type recordingRootFSRow struct {
	values [2]int64
	err    error
}

func (r recordingRootFSRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan rootfs row into %d destinations", len(dest))
	}
	for i := range dest {
		value, ok := dest[i].(*int64)
		if !ok {
			return fmt.Errorf("rootfs row destination %d has type %T", i, dest[i])
		}
		*value = r.values[i]
	}
	return nil
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
