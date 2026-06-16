package rootfs

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestRebasePath(t *testing.T) {
	got, ok := rebasePath("/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/42/fs", "/var/lib/containerd", "/host-var-lib/containerd")
	require.True(t, ok)
	assert.Equal(t, "/host-var-lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/42/fs", got)

	_, ok = rebasePath("/var/lib/containerd-old/snapshots/42/fs", "/var/lib/containerd", "/host-var-lib/containerd")
	assert.False(t, ok)
}

func TestWriteOverlayUpperDiffIncludesUpperEntries(t *testing.T) {
	upperdir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "etc", "config"), []byte("value"), 0o644))
	require.NoError(t, os.Symlink("config", filepath.Join(upperdir, "etc", "config-link")))

	desc, reader, err := writeOverlayUpperDiff(context.Background(), upperdir)
	require.NoError(t, err)
	defer reader.Close()
	require.NotEmpty(t, desc.Digest)
	require.Positive(t, desc.Size)

	entries := readTarEntries(t, reader)
	assert.Contains(t, entries, "etc/")
	assert.Contains(t, entries, "etc/config")
	assert.Contains(t, entries, "etc/config-link")
}

func TestWriteOverlayUpperDiffConvertsWhiteouts(t *testing.T) {
	upperdir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "etc"), 0o755))
	whiteout := filepath.Join(upperdir, "etc", "removed")
	if err := unix.Mknod(whiteout, unix.S_IFCHR, 0); err != nil {
		if err == unix.EPERM || err == unix.EACCES {
			t.Skipf("creating overlay whiteout device is not permitted: %v", err)
		}
		require.NoError(t, err)
	}

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir)
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.Contains(t, entries, "etc/.wh.removed")
	assert.NotContains(t, entries, "etc/removed")
}

func TestWriteOverlayUpperDiffConvertsOpaqueDirectories(t *testing.T) {
	upperdir := t.TempDir()
	dir := filepath.Join(upperdir, "var")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	if err := unix.Lsetxattr(dir, "user.overlay.opaque", []byte{'y'}, 0); err != nil {
		if err == unix.EPERM || err == unix.EACCES || err == unix.ENOTSUP || err == unix.EOPNOTSUPP {
			t.Skipf("setting overlay opaque xattr is not supported: %v", err)
		}
		require.NoError(t, err)
	}

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir)
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.Contains(t, entries, "var/.wh..wh..opq")
	assert.Contains(t, entries, "var/")
}

func TestWriteOverlayUpperDiffFromBaselineAppliesAsChildDelta(t *testing.T) {
	ctx := context.Background()
	baseline := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(baseline, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(baseline, "etc", "config"), []byte("parent"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(baseline, "etc", "removed"), []byte("removed"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(baseline, "etc", "same"), []byte("same"), 0o644))

	current := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(current, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(current, "etc", "config"), []byte("child"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(current, "etc", "added"), []byte("added"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(current, "etc", "same"), []byte("same"), 0o644))

	desc, reader, err := writeOverlayUpperDiffFromBaseline(ctx, baseline, current)
	require.NoError(t, err)
	defer reader.Close()
	require.NotEmpty(t, desc.Digest)
	require.Positive(t, desc.Size)

	applied := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(applied, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(applied, "etc", "config"), []byte("parent"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(applied, "etc", "removed"), []byte("removed"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(applied, "etc", "same"), []byte("same"), 0o644))

	_, err = archive.Apply(ctx, applied, reader)
	require.NoError(t, err)
	assertFileContent(t, filepath.Join(applied, "etc", "config"), "child")
	assertFileContent(t, filepath.Join(applied, "etc", "added"), "added")
	assertFileContent(t, filepath.Join(applied, "etc", "same"), "same")
	_, err = os.Stat(filepath.Join(applied, "etc", "removed"))
	assert.True(t, os.IsNotExist(err))
}

func readTarEntries(t *testing.T, reader io.Reader) []string {
	t.Helper()
	tarReader := tar.NewReader(reader)
	var entries []string
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return entries
		}
		require.NoError(t, err)
		entries = append(entries, header.Name)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}
