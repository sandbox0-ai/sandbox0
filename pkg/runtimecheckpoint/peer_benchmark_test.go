package runtimecheckpoint

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func benchmarkPeerImage(b *testing.B) (string, Manifest, Reference) {
	b.Helper()
	const size = 128 << 20
	source := b.TempDir()
	require.NoError(b, os.Chmod(source, 0o700))
	input, err := os.Create(filepath.Join(source, "pages.img"))
	require.NoError(b, err)
	data := bytes.Repeat([]byte{0x73}, ChunkBytes)
	file := File{Path: "pages.img", Size: size}
	for offset := 0; offset < size; offset += len(data) {
		_, err := input.Write(data)
		require.NoError(b, err)
		file.Chunks = append(file.Chunks, Chunk{Digest: digest.FromBytes(data).String(), Size: int64(len(data))})
	}
	require.NoError(b, input.Close())
	manifest := Manifest{Version: ManifestVersion, Binding: testBinding(), Files: []File{file}}
	payload, err := manifest.Encode(size)
	require.NoError(b, err)
	bindingDigest, err := manifest.Binding.Digest()
	require.NoError(b, err)
	ref := Reference{BindingDigest: bindingDigest, ManifestDigest: digest.FromBytes(payload).String()}
	return source, manifest, ref
}

// This measures streaming, both hash checks and destination fsync on one host.
// It deliberately does not stand in for a cross-host migration SLO.
func BenchmarkPeerImageMaterialization(b *testing.B) {
	const size = 128 << 20
	source, manifest, ref := benchmarkPeerImage(b)
	destination := filepath.Join(b.TempDir(), "image")
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reader, writer := io.Pipe()
		sent := make(chan error, 1)
		go func() {
			err := writePeerImage(b.Context(), manifest, source, writer, size)
			_ = writer.CloseWithError(err)
			sent <- err
		}()
		_, err := ReceivePeerImage(b.Context(), manifest.Binding, ref, destination, reader, size, func(int64, uint64) error { return nil })
		_ = reader.CloseWithError(err)
		sendErr := <-sent
		require.NoError(b, err)
		require.NoError(b, sendErr)
		b.StopTimer()
		require.NoError(b, os.RemoveAll(destination))
		b.StartTimer()
	}
}

// Planning adds a bounded local read before publication and peer transfer can
// overlap. Measure that cost explicitly instead of treating it as free setup.
func BenchmarkLocalImagePlan(b *testing.B) {
	const size = 128 << 20
	source, manifest, ref := benchmarkPeerImage(b)
	store, err := New(objectstore.NewMemoryStore(""), size)
	require.NoError(b, err)
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		plan, err := store.PlanLocal(b.Context(), manifest.Binding, source)
		require.NoError(b, err)
		require.Equal(b, ref, plan.Reference)
	}
}

// Restore admission must recheck retained bytes even after a successful peer
// receive. Keep this CPU/read cost separate from transport and destination sync.
func BenchmarkVerifyLocal(b *testing.B) {
	const size = 128 << 20
	source, manifest, _ := benchmarkPeerImage(b)
	store, err := New(objectstore.NewMemoryStore(""), size)
	require.NoError(b, err)
	ref, err := store.Publish(b.Context(), manifest.Binding, source)
	require.NoError(b, err)
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := store.VerifyLocal(b.Context(), manifest.Binding, ref, source)
		require.NoError(b, err)
	}
}
