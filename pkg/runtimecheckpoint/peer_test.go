package runtimecheckpoint

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func TestPeerImageTransfersLocalChunksWithoutRegionalDataReads(t *testing.T) {
	objects := objectstore.NewMemoryStore("")
	store, err := New(objects, 3*ChunkBytes)
	require.NoError(t, err)
	files := map[string][]byte{
		"checkpoint.img": []byte("process state"), "empty": {},
		"nested/pages.img": bytes.Repeat([]byte("uncompressible-for-transport-test"), ChunkBytes/32+137),
	}
	source := privateImage(t, files)
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	manifest, err := store.loadManifest(t.Context(), testBinding(), ref)
	require.NoError(t, err)
	// A peer transfer must not fetch any data chunks from regional storage.
	// Keep only the small committed manifest for the source's binding check.
	for _, file := range manifest.Files {
		for _, chunk := range file.Chunks {
			require.NoError(t, objects.Delete(chunkKey(ref.BindingDigest, chunk.Digest)))
		}
	}
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	sent := make(chan error, 1)
	go func() {
		err := store.WritePeerImage(t.Context(), testBinding(), ref, source, writer)
		_ = writer.CloseWithError(err)
		sent <- err
	}()
	destination := filepath.Join(t.TempDir(), "image")
	admissions := 0
	got, err := ReceivePeerImage(t.Context(), testBinding(), ref, destination, reader, 3*ChunkBytes, func(size int64, inodes uint64) error {
		admissions++
		wantSize, wantInodes, err := manifest.StagingFootprint()
		require.NoError(t, err)
		require.Equal(t, wantSize, size)
		require.Equal(t, wantInodes, inodes)
		require.NoDirExists(t, destination)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, <-sent)
	require.Equal(t, manifest, got)
	require.Equal(t, 1, admissions)
	for name, data := range files {
		got, err := os.ReadFile(filepath.Join(destination, name))
		require.NoError(t, err)
		require.Equal(t, data, got)
	}
}

func peerFixture(t *testing.T) (*Store, string, Reference, []byte) {
	t.Helper()
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"state": []byte("retained execution image")})
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, store.WritePeerImage(t.Context(), testBinding(), ref, source, &wire))
	return store, source, ref, wire.Bytes()
}

func TestPeerImageRejectsIncompleteCorruptAndUnboundStreams(t *testing.T) {
	_, _, ref, wire := peerFixture(t)
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"truncated header", func(b []byte) []byte { return b[:8] }},
		{"unknown version", func(b []byte) []byte { b[7]++; return b }},
		{"oversized manifest", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:12], MaxManifestBytes+1); return b }},
		{"truncated manifest", func(b []byte) []byte { return b[:16] }},
		{"corrupt manifest", func(b []byte) []byte { b[16] ^= 1; return b }},
		{"truncated data", func(b []byte) []byte { return b[:len(b)-1] }},
		{"corrupt data", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }},
		{"trailing data", func(b []byte) []byte { return append(b, 1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReceivePeerImage(t.Context(), testBinding(), ref, filepath.Join(t.TempDir(), "image"),
				bytes.NewReader(test.mutate(bytes.Clone(wire))), ChunkBytes, func(int64, uint64) error { return nil })
			require.Error(t, err)
		})
	}
	binding := testBinding()
	binding.OperationID = "other-operation"
	destination := filepath.Join(t.TempDir(), "image")
	_, err := ReceivePeerImage(t.Context(), binding, ref, destination, bytes.NewReader(wire), ChunkBytes, func(int64, uint64) error { return nil })
	require.Error(t, err)
	require.NoDirExists(t, destination)
}

func TestPeerImageAdmissionPrecedesAllDataWrites(t *testing.T) {
	_, _, ref, wire := peerFixture(t)
	destination := filepath.Join(t.TempDir(), "image")
	reader := bytes.NewReader(wire)
	denied := errors.New("no staging capacity")
	_, err := ReceivePeerImage(t.Context(), testBinding(), ref, destination, reader, ChunkBytes, func(int64, uint64) error { return denied })
	require.ErrorIs(t, err, denied)
	require.NoDirExists(t, destination)
	require.Equal(t, len("retained execution image"), reader.Len())
	_, err = ReceivePeerImage(t.Context(), testBinding(), ref, destination, bytes.NewReader(wire), ChunkBytes, nil)
	require.Error(t, err)
	require.NoDirExists(t, destination)
}

func TestPeerImageRejectsChangedSourceAndCanceledTransfer(t *testing.T) {
	store, source, ref, wire := peerFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(source, "state"), []byte("changed execution image!"), 0o600))
	require.Error(t, store.WritePeerImage(t.Context(), testBinding(), ref, source, io.Discard))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	destination := filepath.Join(t.TempDir(), "image")
	_, err := ReceivePeerImage(ctx, testBinding(), ref, destination, bytes.NewReader(wire), ChunkBytes, func(int64, uint64) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.NoDirExists(t, destination)
}
