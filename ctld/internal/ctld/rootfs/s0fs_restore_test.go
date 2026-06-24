package rootfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/stretchr/testify/require"
)

func TestRestoreS0FSStateToHostTreeAppliesChangedSet(t *testing.T) {
	upper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "tmp", "state"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "tmp", "state", "file.txt"), []byte("value"), 0o644))
	require.NoError(t, os.Symlink("file.txt", filepath.Join(upper, "tmp", "state", "link.txt")))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "tmp", "state", ".wh.old.txt"), nil, 0o644))
	state, err := s0fs.ImportHostTree(context.Background(), upper, s0fs.HostImportOptions{})
	require.NoError(t, err)

	target := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(target, "tmp", "state"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(target, "tmp", "state", "old.txt"), []byte("old"), 0o644))

	require.NoError(t, restoreS0FSStateToHostTree(context.Background(), state, nil, target, nil))

	got, err := os.ReadFile(filepath.Join(target, "tmp", "state", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, "value", string(got))
	link, err := os.Readlink(filepath.Join(target, "tmp", "state", "link.txt"))
	require.NoError(t, err)
	require.Equal(t, "file.txt", link)
	_, err = os.Stat(filepath.Join(target, "tmp", "state", "old.txt"))
	require.True(t, os.IsNotExist(err), "old file should be removed by whiteout")
}

func TestRestoreS0FSStateToHostTreeSkipsExcludedPaths(t *testing.T) {
	upper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "config"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "tmp"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "config", "internal_jwt_public.key"), []byte("runtime"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(upper, "tmp", "user.txt"), []byte("user"), 0o644))
	state, err := s0fs.ImportHostTree(context.Background(), upper, s0fs.HostImportOptions{})
	require.NoError(t, err)

	target := t.TempDir()
	require.NoError(t, restoreS0FSStateToHostTree(context.Background(), state, nil, target, []string{"/config/internal_jwt_public.key"}))

	_, err = os.Stat(filepath.Join(target, "config", "internal_jwt_public.key"))
	require.True(t, os.IsNotExist(err), "excluded runtime mount path should not be restored")
	got, err := os.ReadFile(filepath.Join(target, "tmp", "user.txt"))
	require.NoError(t, err)
	require.Equal(t, "user", string(got))
}

func TestRestoreS0FSStateToHostTreePreservesSparseFiles(t *testing.T) {
	upper := t.TempDir()
	sourcePath := filepath.Join(upper, "tmp", "sparse.img")
	require.NoError(t, os.MkdirAll(filepath.Dir(sourcePath), 0o755))
	source, err := os.OpenFile(sourcePath, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	const sparseSize = 64 << 20
	require.NoError(t, source.Truncate(sparseSize))
	_, err = source.WriteAt([]byte("tail"), sparseSize-4)
	require.NoError(t, err)
	require.NoError(t, source.Close())

	state, err := s0fs.ImportHostTree(context.Background(), upper, s0fs.HostImportOptions{})
	require.NoError(t, err)
	target := t.TempDir()
	require.NoError(t, restoreS0FSStateToHostTree(context.Background(), state, nil, target, nil))

	restoredPath := filepath.Join(target, "tmp", "sparse.img")
	info, err := os.Stat(restoredPath)
	require.NoError(t, err)
	require.Equal(t, int64(sparseSize), info.Size())
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.Less(t, stat.Blocks, int64(4096), "restored sparse file should not allocate hole blocks")
	tail := make([]byte, 4)
	restored, err := os.Open(restoredPath)
	require.NoError(t, err)
	_, err = restored.ReadAt(tail, sparseSize-4)
	require.NoError(t, err)
	require.NoError(t, restored.Close())
	require.True(t, bytes.Equal(tail, []byte("tail")))
}
