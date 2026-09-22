package runtimecheckpoint

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsobjectstore"
	"github.com/stretchr/testify/require"
)

func testBinding() Binding {
	return Binding{
		OperationID: "migration-1", SandboxID: "sandbox-1", TeamID: "team-1",
		SourceBindingDigest:        digest.FromString("source-writer").String(),
		RuntimeCompatibilityDigest: digest.FromString("runsc-execution-shape").String(),
		AssignmentRevision:         digest.FromString("assignment").String(),
		CPUFeaturesDigest:          digest.FromString("cpu-features").String(),
		RootFSGenerationID:         "generation-1", RootFSDescriptorDigest: digest.FromString("rootfs").String(),
	}
}

func privateImage(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	for name, data := range files {
		file := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o700))
		require.NoError(t, os.WriteFile(file, data, 0o600))
	}
	return dir
}

func TestCheckpointImageRoundTripAcrossChunks(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	store, err := New(raw, 2*ChunkBytes)
	require.NoError(t, err)
	files := map[string][]byte{
		"checkpoint.img": []byte("sentry and task state"),
		"pages.img":      bytes.Repeat([]byte{0x42}, ChunkBytes+139),
		"pages_meta.img": []byte("memory layout"),
		"empty":          {}, "nested/image": []byte("nested state"),
	}
	source := privateImage(t, files)
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	retry, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	require.Equal(t, ref, retry)
	destination := filepath.Join(t.TempDir(), "restored")
	manifest, err := store.Download(t.Context(), testBinding(), ref, destination)
	require.NoError(t, err)
	require.Len(t, manifest.Files, len(files))
	for name, expected := range files {
		actual, err := os.ReadFile(filepath.Join(destination, name))
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		info, err := os.Stat(filepath.Join(destination, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	_, err = store.Download(t.Context(), testBinding(), ref, destination)
	require.ErrorIs(t, err, os.ErrExist, "a retry must not consume a partial image directory")
}

func TestCheckpointImagesReuseRegionalEnvelopeEncryption(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "test-key.pem")
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key),
	}), 0o600))
	raw := objectstore.NewMemoryStore("")
	encrypted, err := rootfsobjectstore.WrapEncryption(raw, config.RootFSObjectStorageConfig{
		ObjectEncryptionEnabled: true, ObjectEncryptionKeyPath: keyPath,
		ObjectEncryptionAlgo: "aes256gcm-rsa",
	})
	require.NoError(t, err)
	store, err := New(encrypted, ChunkBytes)
	require.NoError(t, err)
	secret := []byte("workload-memory-secret-only-for-this-test")
	ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"checkpoint.img": secret}))
	require.NoError(t, err)
	reader, err := raw.Get(chunkKey(ref.BindingDigest, digest.FromBytes(secret).String()), 0, -1)
	require.NoError(t, err)
	ciphertext, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.NotContains(t, string(ciphertext), string(secret))
	retry, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"checkpoint.img": secret}))
	require.NoError(t, err)
	require.Equal(t, ref, retry)
	destination := filepath.Join(t.TempDir(), "restored")
	_, err = store.Download(t.Context(), testBinding(), ref, destination)
	require.NoError(t, err)
	actual, err := os.ReadFile(filepath.Join(destination, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, secret, actual)
}

func TestCheckpointRejectsChangedSourceAndCorruptObjects(t *testing.T) {
	for _, corrupt := range []string{"manifest", "chunk", "oversize chunk", "missing chunk"} {
		t.Run(corrupt, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			store, err := New(raw, ChunkBytes)
			require.NoError(t, err)
			data := []byte("saved state")
			source := privateImage(t, map[string][]byte{"checkpoint.img": data})
			ref, err := store.Publish(t.Context(), testBinding(), source)
			require.NoError(t, err)
			key := chunkKey(ref.BindingDigest, digest.FromBytes(data).String())
			switch corrupt {
			case "manifest":
				key = manifestKey(ref.BindingDigest)
				require.NoError(t, raw.Put(key, strings.NewReader("{}")))
			case "chunk":
				require.NoError(t, raw.Put(key, strings.NewReader("other state")))
			case "oversize chunk":
				require.NoError(t, raw.Put(key, strings.NewReader("saved state extra bytes")))
			case "missing chunk":
				require.NoError(t, raw.Delete(key))
			}
			_, err = store.Download(t.Context(), testBinding(), ref, filepath.Join(t.TempDir(), "restore"))
			require.Error(t, err)
		})
	}
	raw := objectstore.NewMemoryStore("")
	store, err := New(raw, ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"checkpoint.img": []byte("original state")})
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "checkpoint.img"), []byte("modified state"), 0o600))
	_, err = store.Publish(t.Context(), testBinding(), source)
	require.ErrorContains(t, err, "collision")
	destination := filepath.Join(t.TempDir(), "original")
	_, err = store.Download(t.Context(), testBinding(), ref, destination)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(destination, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "original state", string(data))
}

func TestCheckpointRejectsCrossOperationAndCrossTenantRestore(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"checkpoint.img": []byte("state")}))
	require.NoError(t, err)
	for _, mutate := range []func(*Binding){
		func(b *Binding) { b.TeamID = "other-team" },
		func(b *Binding) { b.SandboxID = "other-sandbox" },
		func(b *Binding) { b.OperationID = "other-operation" },
		func(b *Binding) { b.RootFSGenerationID = "another-generation" },
		func(b *Binding) { b.SourceBindingDigest = digest.FromString("successor-boot").String() },
		func(b *Binding) { b.AssignmentRevision = digest.FromString("next-runtime").String() },
		func(b *Binding) { b.CPUFeaturesDigest = digest.FromString("other-cpu").String() },
	} {
		binding := testBinding()
		mutate(&binding)
		destination := filepath.Join(t.TempDir(), "restore")
		_, err := store.Download(t.Context(), binding, ref, destination)
		require.ErrorContains(t, err, "binding")
		_, err = os.Stat(destination)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}

type interruptedStore struct {
	objectstore.ContextConditionalStore
	writes atomic.Int32
	failAt int
}

func (s *interruptedStore) PutIfAbsentContext(ctx context.Context, key string, reader io.Reader) (bool, error) {
	if int(s.writes.Add(1)) == s.failAt {
		return false, errors.New("injected publication interruption")
	}
	return s.ContextConditionalStore.PutIfAbsentContext(ctx, key, reader)
}

func TestCheckpointPublishRecoversInterruptedUploadWithoutPublishingPartialManifest(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	faulty := &interruptedStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), failAt: 2}
	store, err := New(faulty, 2*ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"pages.img": bytes.Repeat([]byte{1}, ChunkBytes+19)})
	_, err = store.Publish(t.Context(), testBinding(), source)
	require.ErrorContains(t, err, "interruption")
	binding, err := testBinding().Digest()
	require.NoError(t, err)
	_, err = raw.Head(manifestKey(binding))
	require.Error(t, err, "partial upload must not publish a manifest")
	faulty.failAt = 0
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	_, err = store.Download(t.Context(), testBinding(), ref, filepath.Join(t.TempDir(), "restored"))
	require.NoError(t, err)
}

func TestCheckpointRejectsOversizeImagesSymlinksAndCanceledTransfers(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), 8)
	require.NoError(t, err)
	_, err = store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"image": []byte("too large")}))
	require.Error(t, err)
	source := privateImage(t, map[string][]byte{"image": []byte("state")})
	require.NoError(t, os.Symlink("image", filepath.Join(source, "alias")))
	_, err = store.Publish(t.Context(), testBinding(), source)
	require.ErrorContains(t, err, "special file")
	require.NoError(t, os.Remove(filepath.Join(source, "alias")))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = store.Publish(ctx, testBinding(), source)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCheckpointManifestRejectsTraversalDuplicatesAndSizeConfusion(t *testing.T) {
	valid := Manifest{Version: ManifestVersion, Binding: testBinding(), Files: []File{{
		Path: "image", Size: 3, Chunks: []Chunk{{Digest: digest.FromString("abc").String(), Size: 3}},
	}}}
	for _, name := range []string{"../escape", "/absolute", "a/../image", "a\\b", ".", "a\x00b"} {
		m := valid
		m.Files = append([]File(nil), valid.Files...)
		m.Files[0].Path = name
		require.Error(t, m.Validate(100), "path %q", name)
	}
	m := valid
	m.Files = append(append([]File(nil), valid.Files...), valid.Files[0])
	require.Error(t, m.Validate(100))
	m.Files[1].Path = "image/child"
	require.Error(t, m.Validate(100))
	m = valid
	m.Files = []File{{Path: "image", Size: MaxImageBytes, Chunks: valid.Files[0].Chunks}}
	require.Error(t, m.Validate(MaxImageBytes))
	payload, err := valid.Encode(100)
	require.NoError(t, err)
	_, err = Decode(payload, 100)
	require.NoError(t, err)
	for _, invalid := range [][]byte{
		append(append([]byte(nil), payload...), []byte("{}")...),
		bytes.Replace(payload, []byte(`"version":1`), []byte(`"version":2,"version":1`), 1),
		bytes.Replace(payload, []byte(`"version":1`), []byte(`"version":1,"unknown":true`), 1),
	} {
		_, err = Decode(invalid, 100)
		require.Error(t, err)
	}
	// Decoding a forged manifest with internally inconsistent sizes must fail
	// even if its outer digest was supplied by a compromised object writer.
	m = valid
	m.Files = []File{{Path: "image", Size: 2, Chunks: valid.Files[0].Chunks}}
	payload, err = json.Marshal(m)
	require.NoError(t, err)
	_, err = Decode(payload, 100)
	require.Error(t, err)
}

func TestCheckpointVerifyLocalRejectsChangedInventoryAndChunks(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
	require.NoError(t, err)
	files := map[string][]byte{"checkpoint.img": []byte("state"), "pages.img": bytes.Repeat([]byte{0x35}, ChunkBytes+21), "empty": {}}
	ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, files))
	require.NoError(t, err)
	for _, mutation := range []string{"none", "corrupt", "extra", "missing", "symlink"} {
		t.Run(mutation, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "image")
			_, err := store.Download(t.Context(), testBinding(), ref, directory)
			require.NoError(t, err)
			switch mutation {
			case "corrupt":
				require.NoError(t, os.WriteFile(filepath.Join(directory, "checkpoint.img"), []byte("other"), 0o600))
			case "extra":
				require.NoError(t, os.WriteFile(filepath.Join(directory, "extra"), []byte("other"), 0o600))
			case "missing":
				require.NoError(t, os.Remove(filepath.Join(directory, "pages.img")))
			case "symlink":
				require.NoError(t, os.Remove(filepath.Join(directory, "pages.img")))
				require.NoError(t, os.Symlink("checkpoint.img", filepath.Join(directory, "pages.img")))
			}
			_, err = store.VerifyLocal(t.Context(), testBinding(), ref, directory)
			if mutation == "none" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestCheckpointVerifyLocalChecksEveryParallelChunk(t *testing.T) {
	data := bytes.Repeat([]byte{0x35}, publicationConcurrency*ChunkBytes+17)
	store, err := New(objectstore.NewMemoryStore(""), int64(len(data)))
	require.NoError(t, err)
	directory := privateImage(t, map[string][]byte{"pages.img": data})
	ref, err := store.Publish(t.Context(), testBinding(), directory)
	require.NoError(t, err)
	file, err := os.OpenFile(filepath.Join(directory, "pages.img"), os.O_RDWR, 0)
	require.NoError(t, err)
	defer file.Close()
	// Cover every worker and a second chunk owned by the first worker, including
	// the short final chunk. No worker may report overall success prematurely.
	for index := 0; index <= publicationConcurrency; index++ {
		offset := int64(index * ChunkBytes)
		_, err := file.WriteAt([]byte{0x36}, offset)
		require.NoError(t, err)
		_, err = store.VerifyLocal(t.Context(), testBinding(), ref, directory)
		require.Error(t, err, "corruption in chunk %d", index)
		_, err = file.WriteAt([]byte{0x35}, offset)
		require.NoError(t, err)
	}
	_, err = store.VerifyLocal(t.Context(), testBinding(), ref, directory)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = store.VerifyLocal(canceled, testBinding(), ref, directory)
	require.ErrorIs(t, err, context.Canceled)
}
