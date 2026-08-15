package rootfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cacheTestEncryptor struct{}

func (cacheTestEncryptor) Encrypt(value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (cacheTestEncryptor) Decrypt(value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func TestObjectCacheReusesValidatedFileIdentityAndRevalidatesChanges(t *testing.T) {
	payload := "rootfs-cache-payload"
	desc := objectCacheDescriptor("rootfs/cache.tar", payload)
	cache := NewObjectCache(ObjectCacheConfig{Dir: t.TempDir(), MaxBytes: 1 << 20})
	require.NoError(t, cache.Put(context.Background(), desc, strings.NewReader(payload)))

	path, err := cache.pathForDescriptor(desc)
	require.NoError(t, err)
	cache.mu.Lock()
	_, validatedOnPut := cache.validated[path]
	cache.mu.Unlock()
	assert.True(t, validatedOnPut)

	reader, ok, err := cache.Open(desc)
	require.NoError(t, err)
	require.True(t, ok)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, payload, string(got))

	badPayload := strings.Repeat("x", len(payload))
	require.NoError(t, os.WriteFile(path, []byte(badPayload), 0o600))
	future := time.Now().Add(time.Second)
	require.NoError(t, os.Chtimes(path, future, future))

	reader, ok, err = cache.Open(desc)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, reader)
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestObjectCacheRevalidatesExistingFileAfterRestart(t *testing.T) {
	payload := "restart-cache-payload"
	desc := objectCacheDescriptor("rootfs/restart.tar", payload)
	cacheDir := t.TempDir()
	cache := NewObjectCache(ObjectCacheConfig{Dir: cacheDir, MaxBytes: 1 << 20})
	require.NoError(t, cache.Put(context.Background(), desc, strings.NewReader(payload)))

	path, err := cache.pathForDescriptor(desc)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("z", len(payload))), 0o600))

	restarted := NewObjectCache(ObjectCacheConfig{Dir: cacheDir, MaxBytes: 1 << 20})
	reader, ok, err := restarted.Open(desc)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, reader)
}

func TestObjectCachePutFileValidatesBeforePublishing(t *testing.T) {
	payload := "prepared-rootfs"
	desc := objectCacheDescriptor("rootfs/prepared.tar", payload)
	cache := NewObjectCache(ObjectCacheConfig{Dir: t.TempDir(), MaxBytes: 1 << 20})
	source := t.TempDir() + "/prepared.tar"
	require.NoError(t, os.WriteFile(source, []byte(strings.Repeat("x", len(payload))), 0o600))

	err := cache.PutFile(context.Background(), desc, source)
	require.ErrorContains(t, err, "digest mismatch")

	require.NoError(t, os.WriteFile(source, []byte(payload), 0o600))
	require.NoError(t, cache.PutFile(context.Background(), desc, source))
	reader, ok, err := cache.Open(desc)
	require.NoError(t, err)
	require.True(t, ok)
	defer reader.Close()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, payload, string(got))
}

func TestEncryptedObjectCacheHidesPlaintextAndReopensByDigest(t *testing.T) {
	payload := "encrypted-rootfs-cache-payload"
	desc := objectCacheDescriptor("rootfs/first.tar", payload)
	cacheDir := t.TempDir()
	encryption := objectstore.EncryptionConfig{
		Enabled:      true,
		Algorithm:    objectstore.EncryptionAlgoAES256GCMRSA,
		KeyEncryptor: cacheTestEncryptor{},
	}
	cache := NewObjectCache(ObjectCacheConfig{Dir: cacheDir, MaxBytes: 1 << 20, Encryption: encryption})
	require.NoError(t, cache.Put(context.Background(), desc, strings.NewReader(payload)))

	path, err := cache.pathForDescriptor(desc)
	require.NoError(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.False(t, bytes.Contains(raw, []byte(payload)))
	encrypted, err := objectstore.HasEncryptedObjectHeader(bytes.NewReader(raw))
	require.NoError(t, err)
	assert.True(t, encrypted)

	restarted := NewObjectCache(ObjectCacheConfig{Dir: cacheDir, MaxBytes: 1 << 20, Encryption: encryption})
	desc.ObjectKey = "rootfs/same-digest-different-key.tar"
	reader, ok, err := restarted.Open(desc)
	require.NoError(t, err)
	require.True(t, ok)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, payload, string(got))
}

func TestEncryptedObjectCacheRejectsLegacyPlaintextEntry(t *testing.T) {
	payload := "legacy-plaintext-rootfs-cache"
	desc := objectCacheDescriptor("rootfs/legacy.tar", payload)
	cache := NewObjectCache(ObjectCacheConfig{
		Dir:      t.TempDir(),
		MaxBytes: 1 << 20,
		Encryption: objectstore.EncryptionConfig{
			Enabled:      true,
			Algorithm:    objectstore.EncryptionAlgoAES256GCMRSA,
			KeyEncryptor: cacheTestEncryptor{},
		},
	})
	path, err := cache.pathForDescriptor(desc)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(payload), 0o600))

	reader, ok, err := cache.Open(desc)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, reader)
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func objectCacheDescriptor(objectKey, payload string) ctldapi.RootFSDiffDescriptor {
	return ctldapi.RootFSDiffDescriptor{
		MediaType: "application/vnd.oci.image.layer.v1.tar",
		Digest:    digest.FromString(payload).String(),
		DiffID:    digest.FromString(payload).String(),
		Size:      int64(len(payload)),
		ObjectKey: objectKey,
	}
}
