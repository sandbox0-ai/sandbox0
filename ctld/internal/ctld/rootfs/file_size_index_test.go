package rootfs

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSFileSizeIndexTracksFilteredTarLayers(t *testing.T) {
	parentTar := bytes.NewBuffer(nil)
	parentWriter := tar.NewWriter(parentTar)
	writeTarEntry(t, parentWriter, "workspace/removed.bin", make([]byte, 10), 0o644)
	writeTarEntry(t, parentWriter, "workspace/kept.bin", make([]byte, 20), 0o644)
	require.NoError(t, parentWriter.Close())

	childTar := bytes.NewBuffer(nil)
	childWriter := tar.NewWriter(childTar)
	writeTarEntry(t, childWriter, "workspace/.wh.removed.bin", nil, 0o000)
	writeTarEntry(t, childWriter, "workspace/added.bin", make([]byte, 30), 0o644)
	require.NoError(t, childWriter.Close())

	index := make(rootFSFileSizeIndex)
	for _, layer := range [][]byte{parentTar.Bytes(), childTar.Bytes()} {
		_, reader, changes, err := filterRootFSDiffTarForApply(rootFSDiffDescriptorForTest(), bytes.NewReader(layer), nil, nil)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.True(t, index.Apply(changes))
	}
	assert.Equal(t, rootFSFileSizeIndex{
		"/workspace/added.bin": 30,
		"/workspace/kept.bin":  20,
	}, index)
}

func TestRootFSFileSizeIndexReplacesPortalSnapshotAcrossLayers(t *testing.T) {
	portal := ctldapi.RootFSPortalPath{
		PortalName:  "workspace",
		MountPath:   "/workspace",
		BackingPath: t.TempDir(),
	}
	parentTar := bytes.NewBuffer(nil)
	parentWriter := tar.NewWriter(parentTar)
	writeTarEntry(t, parentWriter, "workspace/", nil, 0o755)
	writeTarEntry(t, parentWriter, "workspace/removed.bin", make([]byte, 10), 0o644)
	writeTarEntry(t, parentWriter, "workspace/kept.bin", make([]byte, 20), 0o644)
	require.NoError(t, parentWriter.Close())

	childTar := bytes.NewBuffer(nil)
	childWriter := tar.NewWriter(childTar)
	writeTarEntry(t, childWriter, "workspace/", nil, 0o755)
	writeTarEntry(t, childWriter, "workspace/kept.bin", make([]byte, 20), 0o644)
	writeTarEntry(t, childWriter, "workspace/added.bin", make([]byte, 30), 0o644)
	require.NoError(t, childWriter.Close())

	index := make(rootFSFileSizeIndex)
	for _, layer := range [][]byte{parentTar.Bytes(), childTar.Bytes()} {
		_, reader, changes, err := filterRootFSDiffTarForApply(
			rootFSDiffDescriptorForTest(),
			bytes.NewReader(layer),
			nil,
			[]ctldapi.RootFSPortalPath{portal},
		)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.True(t, index.Apply(changes))
	}

	assert.Equal(t, rootFSFileSizeIndex{
		"/workspace/added.bin": 30,
		"/workspace/kept.bin":  20,
	}, index)
}

func TestRootFSFileSizeIndexApplyPreservesSameLayerEntriesAcrossWhiteouts(t *testing.T) {
	tests := []struct {
		name    string
		changes []rootFSFileChange
		want    rootFSFileSizeIndex
	}{
		{
			name: "opaque marker before entries",
			changes: []rootFSFileChange{
				{Path: "/workspace", Delete: true, Opaque: true},
				{Path: "/workspace/new.bin", Regular: true, Size: 30},
			},
			want: rootFSFileSizeIndex{"/workspace/new.bin": 30},
		},
		{
			name: "opaque marker after entries",
			changes: []rootFSFileChange{
				{Path: "/workspace/new.bin", Regular: true, Size: 30},
				{Path: "/workspace", Delete: true, Opaque: true},
			},
			want: rootFSFileSizeIndex{"/workspace/new.bin": 30},
		},
		{
			name: "file whiteout before replacement",
			changes: []rootFSFileChange{
				{Path: "/workspace/old.bin", Delete: true},
				{Path: "/workspace/old.bin", Regular: true, Size: 30},
			},
			want: rootFSFileSizeIndex{
				"/workspace/old.bin":    30,
				"/workspace/nested.bin": 20,
			},
		},
		{
			name: "file whiteout after replacement",
			changes: []rootFSFileChange{
				{Path: "/workspace/old.bin", Regular: true, Size: 30},
				{Path: "/workspace/old.bin", Delete: true},
			},
			want: rootFSFileSizeIndex{
				"/workspace/nested.bin": 20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := rootFSFileSizeIndex{
				"/workspace/old.bin":    10,
				"/workspace/nested.bin": 20,
			}
			index.Apply(tt.changes)

			assert.Equal(t, tt.want, index)
		})
	}
}

func TestRootFSFileSizeIndexApplyTracksTypeReplacementAndHardlink(t *testing.T) {
	index := rootFSFileSizeIndex{
		"/workspace/tree/file.bin": 10,
		"/workspace/plain.bin":     20,
	}
	index.Apply([]rootFSFileChange{
		{Path: "/workspace/tree", Regular: true, Size: 30},
		{Path: "/workspace/plain.bin", Directory: true},
		{Path: "/workspace/tree-link", Regular: true, LinkPath: "/workspace/tree"},
	})

	assert.Equal(t, rootFSFileSizeIndex{
		"/workspace/tree":      30,
		"/workspace/tree-link": 30,
	}, index)
}

func TestRootFSFileSizeIndexApplyInvalidatesUnknownHardlinkTarget(t *testing.T) {
	index := make(rootFSFileSizeIndex)
	assert.False(t, index.Apply([]rootFSFileChange{{
		Path:     "/workspace/link",
		Regular:  true,
		LinkPath: "/workspace/missing",
	}}))
}

func TestRootFSFileChangeFromTarHeader(t *testing.T) {
	tests := []struct {
		name   string
		header *tar.Header
		want   rootFSFileChange
	}{
		{name: "regular", header: &tar.Header{Name: "workspace/file", Typeflag: tar.TypeReg, Size: 10}, want: rootFSFileChange{Path: "/workspace/file", Regular: true, Size: 10}},
		{name: "hardlink", header: &tar.Header{Name: "workspace/link", Typeflag: tar.TypeLink, Linkname: "workspace/file"}, want: rootFSFileChange{Path: "/workspace/link", Regular: true, LinkPath: "/workspace/file"}},
		{name: "file whiteout", header: &tar.Header{Name: "workspace/.wh.file"}, want: rootFSFileChange{Path: "/workspace/file", Delete: true}},
		{name: "opaque whiteout", header: &tar.Header{Name: "workspace/.wh..wh..opq"}, want: rootFSFileChange{Path: "/workspace", Delete: true, Opaque: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := rootFSFileChangeFromTarHeader(tt.header)
			require.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRootFSFileSizeIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	index := rootFSFileSizeIndex{
		"/workspace/file.bin": 10,
		"relative/path":       20,
		"/ignored-zero":       0,
	}
	tmp, err := writeRootFSFileSizeIndexTemp(dir, index)
	require.NoError(t, err)
	target := filepath.Join(dir, "index.json")
	require.NoError(t, os.Rename(tmp, target))

	got, err := loadRootFSFileSizeIndex(target)
	require.NoError(t, err)
	assert.Equal(t, rootFSFileSizeIndex{
		"/workspace/file.bin": 10,
		"/relative/path":      20,
	}, got)

	missing, err := loadRootFSFileSizeIndex(filepath.Join(dir, "missing.json"))
	require.NoError(t, err)
	assert.Nil(t, missing)
}
