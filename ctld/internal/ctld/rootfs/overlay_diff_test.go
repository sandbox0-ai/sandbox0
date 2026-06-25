package rootfs

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
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

	desc, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, nil, nil)
	require.NoError(t, err)
	defer reader.Close()
	require.NotEmpty(t, desc.Digest)
	require.Positive(t, desc.Size)

	entries := readTarEntries(t, reader)
	assert.Contains(t, entries, "etc/")
	assert.Contains(t, entries, "etc/config")
	assert.Contains(t, entries, "etc/config-link")
}

func TestWriteOverlayUpperDiffExcludesRuntimePaths(t *testing.T) {
	upperdir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "procd", "bin"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "procd", "bin", "procd"), []byte("runtime"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "procd", "bin", "python-runner"), []byte("runtime"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, strings.TrimPrefix(volumeportal.WebhookStateMountPath, "/"), "webhook-outbox"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, strings.TrimPrefix(volumeportal.WebhookStateMountPath, "/"), "webhook-outbox", "evt.json"), []byte("runtime"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "workspace"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "workspace", "state"), []byte("value"), 0o644))

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, nil, nil)
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "procd/")
	assert.NotContains(t, entries, "procd/bin/")
	assert.NotContains(t, entries, "procd/bin/procd")
	assert.NotContains(t, entries, "procd/bin/python-runner")
	assert.Contains(t, entries, "var/lib/sandbox0/procd/")
	assert.Contains(t, entries, "var/lib/sandbox0/procd/webhook-outbox/")
	assert.Contains(t, entries, "var/lib/sandbox0/procd/webhook-outbox/evt.json")
	assert.Contains(t, entries, "workspace/")
	assert.Contains(t, entries, "workspace/state")
}

func TestWriteOverlayUpperDiffExcludesConfiguredWebhookStatePath(t *testing.T) {
	upperdir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, strings.TrimPrefix(volumeportal.WebhookStateMountPath, "/"), "webhook-outbox"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, strings.TrimPrefix(volumeportal.WebhookStateMountPath, "/"), "webhook-outbox", "evt.json"), []byte("runtime"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "workspace"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "workspace", "state"), []byte("value"), 0o644))

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, []string{volumeportal.WebhookStateMountPath}, nil)
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "var/lib/sandbox0/procd/")
	assert.NotContains(t, entries, "var/lib/sandbox0/procd/webhook-outbox/")
	assert.NotContains(t, entries, "var/lib/sandbox0/procd/webhook-outbox/evt.json")
	assert.Contains(t, entries, "workspace/")
	assert.Contains(t, entries, "workspace/state")
}

func TestWriteOverlayUpperDiffExcludesConfiguredVolumePaths(t *testing.T) {
	upperdir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "workspace", "data", "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "workspace", "data", "nested", "volume.txt"), []byte("volume"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "workspace", "database"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "workspace", "database", "rootfs.txt"), []byte("rootfs"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "workspace", "other"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "workspace", "other", "state"), []byte("value"), 0o644))

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, []string{
		" /workspace/data/ ",
		"/workspace/data",
		"/",
		"",
	}, nil)
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "workspace/data/")
	assert.NotContains(t, entries, "workspace/data/nested/")
	assert.NotContains(t, entries, "workspace/data/nested/volume.txt")
	assert.Contains(t, entries, "workspace/database/")
	assert.Contains(t, entries, "workspace/database/rootfs.txt")
	assert.Contains(t, entries, "workspace/other/state")
}

func TestWriteOverlayUpperDiffAppendsUnboundPortalBackingAsVisiblePath(t *testing.T) {
	upperdir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upperdir, "workspace", "cache"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "workspace", "cache", "upper.txt"), []byte("upper"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(upperdir, "workspace", "state"), []byte("rootfs"), 0o644))

	backing := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(backing, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backing, "nested", "portal.txt"), []byte("portal"), 0o644))

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, nil, []ctldapi.RootFSPortalPath{{
		PortalName:  "cache",
		MountPath:   "/workspace/cache",
		BackingPath: backing,
	}})
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.Contains(t, entries, "workspace/state")
	assert.Contains(t, entries, "workspace/cache/")
	assert.Contains(t, entries, "workspace/cache/nested/")
	assert.Contains(t, entries, "workspace/cache/nested/portal.txt")
	assert.NotContains(t, entries, "workspace/cache/upper.txt")
}

func TestWriteOverlayUpperDiffExcludesOpaqueWhiteoutAffectingVolumePath(t *testing.T) {
	upperdir := t.TempDir()
	workspace := filepath.Join(upperdir, "workspace")
	require.NoError(t, os.MkdirAll(filepath.Join(workspace, "database"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(workspace, "database", "rootfs.txt"), []byte("rootfs"), 0o644))
	markOverlayOpaqueForTest(t, workspace)

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, []string{"/workspace/data"}, nil)
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "workspace/.wh..wh..opq")
	assert.Contains(t, entries, "workspace/")
	assert.Contains(t, entries, "workspace/database/rootfs.txt")
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

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, nil, nil)
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
	markOverlayOpaqueForTest(t, dir)

	_, reader, err := writeOverlayUpperDiff(context.Background(), upperdir, nil, nil)
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

	desc, reader, err := writeOverlayUpperDiffFromBaseline(ctx, baseline, current, nil, nil)
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

func TestWriteOverlayUpperDiffFromBaselineExcludesConfiguredVolumePaths(t *testing.T) {
	ctx := context.Background()
	baseline := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(baseline, "workspace", "data"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(baseline, "workspace", "data", "old-volume.txt"), []byte("old"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(baseline, "workspace", "database"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(baseline, "workspace", "database", "same"), []byte("same"), 0o644))

	current := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(current, "workspace", "data"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(current, "workspace", "data", "new-volume.txt"), []byte("new"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(current, "workspace", "database"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(current, "workspace", "database", "same"), []byte("changed"), 0o644))

	_, reader, err := writeOverlayUpperDiffFromBaseline(ctx, baseline, current, []string{"/workspace/data"}, nil)
	require.NoError(t, err)
	defer reader.Close()

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "workspace/data/")
	assert.NotContains(t, entries, "workspace/data/old-volume.txt")
	assert.NotContains(t, entries, "workspace/data/new-volume.txt")
	assert.Contains(t, entries, "workspace/database/same")
}

func TestFilterRootFSDiffTarForApplyRestoresPortalBacking(t *testing.T) {
	var buf bytes.Buffer
	tarWriter := tar.NewWriter(&buf)
	writeTarEntry(t, tarWriter, "workspace/cache/", nil, 0o755)
	writeTarEntry(t, tarWriter, "workspace/cache/old.txt", []byte("new"), 0o644)
	writeTarEntry(t, tarWriter, "workspace/root.txt", []byte("rootfs"), 0o644)
	require.NoError(t, tarWriter.Close())

	backing := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(backing, "stale.txt"), []byte("stale"), 0o644))

	desc, reader, err := filterRootFSDiffTarForApply(rootFSDiffDescriptorForTest(), bytes.NewReader(buf.Bytes()), nil, []ctldapi.RootFSPortalPath{{
		PortalName:  "cache",
		MountPath:   "/workspace/cache",
		BackingPath: backing,
	}})
	require.NoError(t, err)
	defer reader.Close()
	require.NotEmpty(t, desc.Digest)

	entries := readTarEntries(t, reader)
	assert.Contains(t, entries, "workspace/root.txt")
	assert.NotContains(t, entries, "workspace/cache/")
	assert.NotContains(t, entries, "workspace/cache/old.txt")
	assertFileContent(t, filepath.Join(backing, "old.txt"), "new")
	_, err = os.Stat(filepath.Join(backing, "stale.txt"))
	assert.True(t, os.IsNotExist(err))
}

func TestFilterRootFSDiffTarExcludesRuntimePaths(t *testing.T) {
	var buf bytes.Buffer
	tarWriter := tar.NewWriter(&buf)
	writeTarEntry(t, tarWriter, "procd/bin/python-runner", []byte("runtime"), 0o755)
	writeTarEntry(t, tarWriter, "procd/.wh..wh..opq", nil, 0o000)
	writeTarEntry(t, tarWriter, "var/lib/sandbox0/procd/webhook-outbox/evt.json", []byte("runtime"), 0o644)
	writeTarEntry(t, tarWriter, "var/lib/sandbox0/procd/.wh..wh..opq", nil, 0o000)
	writeTarEntry(t, tarWriter, "workspace/state", []byte("value"), 0o644)
	require.NoError(t, tarWriter.Close())

	desc, reader, err := filterRootFSDiffTar(rootFSDiffDescriptorForTest(), bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer reader.Close()
	require.NotEmpty(t, desc.Digest)
	require.Positive(t, desc.Size)

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "procd/bin/python-runner")
	assert.NotContains(t, entries, "procd/.wh..wh..opq")
	assert.Contains(t, entries, "var/lib/sandbox0/procd/webhook-outbox/evt.json")
	assert.Contains(t, entries, "var/lib/sandbox0/procd/.wh..wh..opq")
	assert.Contains(t, entries, "workspace/state")
}

func TestFilterRootFSDiffTarExcludesConfiguredWebhookStatePath(t *testing.T) {
	var buf bytes.Buffer
	tarWriter := tar.NewWriter(&buf)
	writeTarEntry(t, tarWriter, "var/lib/sandbox0/procd/webhook-outbox/evt.json", []byte("runtime"), 0o644)
	writeTarEntry(t, tarWriter, "var/lib/sandbox0/procd/.wh..wh..opq", nil, 0o000)
	writeTarEntry(t, tarWriter, "workspace/state", []byte("value"), 0o644)
	require.NoError(t, tarWriter.Close())

	desc, reader, err := filterRootFSDiffTar(rootFSDiffDescriptorForTest(), bytes.NewReader(buf.Bytes()), []string{volumeportal.WebhookStateMountPath})
	require.NoError(t, err)
	defer reader.Close()
	require.NotEmpty(t, desc.Digest)
	require.Positive(t, desc.Size)

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "var/lib/sandbox0/procd/webhook-outbox/evt.json")
	assert.NotContains(t, entries, "var/lib/sandbox0/procd/.wh..wh..opq")
	assert.Contains(t, entries, "workspace/state")
}

func TestFilterRootFSDiffTarExcludesConfiguredVolumePathsAndWhiteouts(t *testing.T) {
	var buf bytes.Buffer
	tarWriter := tar.NewWriter(&buf)
	writeTarEntry(t, tarWriter, "workspace/data/file", []byte("volume"), 0o644)
	writeTarEntry(t, tarWriter, "workspace/.wh.data", nil, 0o000)
	writeTarEntry(t, tarWriter, "workspace/.wh..wh..opq", nil, 0o000)
	writeTarEntry(t, tarWriter, "workspace/database/file", []byte("rootfs"), 0o644)
	writeTarEntry(t, tarWriter, "workspace/other/file", []byte("rootfs"), 0o644)
	require.NoError(t, tarWriter.Close())

	desc, reader, err := filterRootFSDiffTar(rootFSDiffDescriptorForTest(), bytes.NewReader(buf.Bytes()), []string{
		"/workspace/data",
		"/",
	})
	require.NoError(t, err)
	defer reader.Close()
	require.NotEmpty(t, desc.Digest)
	require.Positive(t, desc.Size)

	entries := readTarEntries(t, reader)
	assert.NotContains(t, entries, "workspace/data/file")
	assert.NotContains(t, entries, "workspace/.wh.data")
	assert.NotContains(t, entries, "workspace/.wh..wh..opq")
	assert.Contains(t, entries, "workspace/database/file")
	assert.Contains(t, entries, "workspace/other/file")
}

func TestRootFSPathFilterDoesNotExcludeSimilarPrefixes(t *testing.T) {
	filter := newRootFSPathFilter([]string{"/workspace/data"})

	assert.True(t, filter.Excludes("/workspace/data"))
	assert.True(t, filter.Excludes("/workspace/data/file"))
	assert.False(t, filter.Excludes("/workspace/database/file"))
	assert.False(t, filter.Excludes("/workspace/data-old/file"))
	assert.True(t, filter.ExcludesTarHeader("workspace/.wh.data"))
	assert.False(t, filter.ExcludesTarHeader("workspace/.wh.database"))
	assert.True(t, filter.ExcludesTarHeader("workspace/.wh..wh..opq"))
	assert.False(t, filter.ExcludesTarHeader("workspace/database/.wh..wh..opq"))
	target, opaque, ok := rootFSChangeWhiteoutTargetPath("/workspace/.wh..opq")
	require.True(t, ok)
	assert.True(t, opaque)
	assert.Equal(t, "/workspace", target)
	assert.True(t, filter.AffectsOpaquePreservedPath(target))
}

func rootFSDiffDescriptorForTest() ctldapi.RootFSDiffDescriptor {
	return ctldapi.RootFSDiffDescriptor{MediaType: "application/vnd.oci.image.layer.v1.tar"}
}

func writeTarEntry(t *testing.T, writer *tar.Writer, name string, data []byte, mode int64) {
	t.Helper()
	header := &tar.Header{
		Name: name,
		Mode: mode,
		Size: int64(len(data)),
	}
	require.NoError(t, writer.WriteHeader(header))
	if len(data) > 0 {
		_, err := writer.Write(data)
		require.NoError(t, err)
	}
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

func markOverlayOpaqueForTest(t *testing.T, dir string) {
	t.Helper()
	if err := unix.Lsetxattr(dir, "user.overlay.opaque", []byte{'y'}, 0); err != nil {
		if err == unix.EPERM || err == unix.EACCES || err == unix.ENOTSUP || err == unix.EOPNOTSUPP {
			t.Skipf("setting overlay opaque xattr is not supported: %v", err)
		}
		require.NoError(t, err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}
