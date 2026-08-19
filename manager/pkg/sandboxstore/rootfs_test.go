package sandboxstore

import (
	"context"
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
	assert.Contains(t, exec.sqls[3], "manager.rootfs_filesystems.writer_epoch = 0")
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

func TestRootFSStateFromLayerChainKeepsCurrentSandboxID(t *testing.T) {
	state := rootFSStateFromLayerChain("child-sandbox", []*SandboxRootFSLayer{
		{
			ID:              "layer-parent",
			SourceSandboxID: "parent-sandbox",
			TeamID:          "team-1",
			DiffDigest:      "sha256:parent",
			DiffObjectKey:   "rootfs/parent.tar",
		},
		{
			ID:              "layer-child",
			ParentLayerID:   "layer-parent",
			SourceSandboxID: "parent-sandbox",
			TeamID:          "team-1",
			DiffDigest:      "sha256:child",
			DiffObjectKey:   "rootfs/child.tar",
		},
	})

	if state == nil {
		t.Fatal("state is nil")
	}
	if state.SandboxID != "child-sandbox" {
		t.Fatalf("SandboxID = %q, want child-sandbox", state.SandboxID)
	}
	if state.LayerID != "layer-child" || state.ParentLayerID != "layer-parent" {
		t.Fatalf("head = %q parent = %q, want layer-child/layer-parent", state.LayerID, state.ParentLayerID)
	}
	if len(state.LayerChain) != 2 {
		t.Fatalf("LayerChain len = %d, want 2", len(state.LayerChain))
	}
}

func rootFSTestState() *SandboxRootFSState {
	return &SandboxRootFSState{
		SandboxID:           "sandbox-1",
		TeamID:              "team-1",
		RuntimeGeneration:   3,
		Runtime:             "runc",
		RuntimeHandler:      "io.containerd.runc.v2",
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     "sha256:base",
		Snapshotter:         "overlayfs",
		SnapshotParent:      "parent-1",
		SnapshotParentChain: []string{"parent-1", "parent-0"},
		DiffDigest:          "sha256:diff",
		DiffID:              "sha256:diff",
		DiffMediaType:       "application/vnd.oci.image.layer.v1.tar",
		DiffSize:            123,
		DiffObjectKey:       "sandbox-rootfs/team-1/sandbox-1/3/sha256/diff.tar",
	}
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
