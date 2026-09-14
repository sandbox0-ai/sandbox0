package session

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

type offlineReadCacheSource struct{}

func (offlineReadCacheSource) Get(string, int64, int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("object source is offline")
}

func TestSessionNodeCacheSurvivesManagerReplacementAndIsolatesWrites(t *testing.T) {
	base := t.TempDir()
	objects := newSessionObjectStore()
	first := testStageRequest(t, objects, "cache-a")
	second := testStageRequest(t, objects, "cache-b")
	cacheDirectory := filepath.Join(base, "read-cache")
	open := func(name string, source rootfsblock.RangeSource) *Manager {
		manager, err := New(Config{
			StatePath:  filepath.Join(base, name, "state", "sessions.db"),
			BranchRoot: filepath.Join(base, name, "branches"), MountRoot: filepath.Join(base, name, "mounts"),
			Source: source, Publisher: objects, Runtime: newFakeHostRuntime(objects),
			ReadCacheBytes: 1024,
			ReadDiskCache:  rootfsblock.DiskCacheConfig{Directory: cacheDirectory, MaxBytes: 1 << 20},
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, manager.Close()) })
		return manager
	}
	manager := open("first-process", objects)
	_, err := manager.Ensure(t.Context(), first)
	require.NoError(t, err)
	branch := manager.live[first.Parent].branch
	original := make([]byte, 3*rootfsblock.LogicalBlockSize)
	_, err = branch.ReadAt(original, 0)
	require.NoError(t, err)
	private := bytes.Repeat([]byte{0xf5}, rootfsblock.LogicalBlockSize)
	_, err = branch.WriteAt(private, 0)
	require.NoError(t, err)
	require.NoError(t, manager.Close())
	require.Positive(t, manager.ReadCacheStats().DiskWrites)

	replacement := open("second-process", offlineReadCacheSource{})
	_, err = replacement.Ensure(t.Context(), second)
	require.NoError(t, err)
	actual := make([]byte, len(original))
	_, err = replacement.live[second.Parent].branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, original, actual, "a new sandbox sees immutable template bytes, not another sandbox's dirty tail")
	require.Positive(t, replacement.ReadCacheStats().DiskHits)
}
