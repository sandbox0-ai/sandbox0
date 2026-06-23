package rootfs

import (
	"context"
	"os"
	"path/filepath"
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
