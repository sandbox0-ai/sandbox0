package rootfs

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func objectCacheDescriptor(objectKey, payload string) ctldapi.RootFSDiffDescriptor {
	return ctldapi.RootFSDiffDescriptor{
		MediaType: "application/vnd.oci.image.layer.v1.tar",
		Digest:    digest.FromString(payload).String(),
		DiffID:    digest.FromString(payload).String(),
		Size:      int64(len(payload)),
		ObjectKey: objectKey,
	}
}
