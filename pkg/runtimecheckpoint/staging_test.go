package runtimecheckpoint

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type stagingReadStore struct {
	objectstore.ContextConditionalStore
	reads []string
}

func (s *stagingReadStore) GetContext(ctx context.Context, key string, offset, limit int64) (io.ReadCloser, error) {
	s.reads = append(s.reads, key)
	return s.ContextConditionalStore.GetContext(ctx, key, offset, limit)
}

func TestCheckpointStagingAdmissionPrecedesFilesAndChunkReads(t *testing.T) {
	raw := &stagingReadStore{ContextConditionalStore: objectstore.NewMemoryStore("").(objectstore.ContextConditionalStore)}
	store, err := New(raw, ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"a/empty": {}, "a/state": []byte("one"), "b/pages": make([]byte, 4097)})
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	raw.reads = nil
	destination := filepath.Join(t.TempDir(), "image")
	refusal := errors.New("known footprint exceeds staging pool")
	calls := 0
	admit := func(bytes int64, inodes uint64) error {
		calls++
		require.EqualValues(t, 24576, bytes, "three directories plus rounded data extents")
		require.EqualValues(t, 6, inodes, "three files including empty file plus three directories")
		require.NoDirExists(t, destination)
		return refusal
	}
	_, err = store.DownloadWithAdmission(t.Context(), testBinding(), ref, destination, admit)
	require.ErrorIs(t, err, refusal)
	require.Equal(t, 1, calls)
	require.Equal(t, []string{manifestKey(ref.BindingDigest)}, raw.reads)
	require.NoDirExists(t, destination)
	invalid := ref
	invalid.ManifestDigest = digest.FromString("another manifest").String()
	_, err = store.DownloadWithAdmission(t.Context(), testBinding(), invalid, destination, admit)
	require.Error(t, err)
	require.Equal(t, 1, calls, "unverified manifests cannot reserve capacity")
	_, err = store.DownloadWithAdmission(t.Context(), testBinding(), ref, destination, nil)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	_, err = store.DownloadWithAdmission(ctx, testBinding(), ref, destination, func(int64, uint64) error { cancel(); return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.NoDirExists(t, destination)
	_, err = store.DownloadWithAdmission(t.Context(), testBinding(), ref, destination, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(destination, "b/pages"))
	_, err = os.Stat(filepath.Join(destination, "a/empty"))
	require.NoError(t, err)
}

func TestCheckpointRejectsManifestDirectoryExplosion(t *testing.T) {
	m := Manifest{Version: ManifestVersion, Binding: testBinding()}
	for i := 0; i < 8; i++ {
		name := string(rune('a'+i)) + "/" + strings.Repeat("x/", 300) + "state"
		m.Files = append(m.Files, File{Path: name, Size: 1, Chunks: []Chunk{{Digest: digest.FromString("x").String(), Size: 1}}})
	}
	require.ErrorContains(t, m.Validate(MaxImageBytes), "inode bound")
	_, _, err := m.StagingFootprint()
	require.Error(t, err)
}
