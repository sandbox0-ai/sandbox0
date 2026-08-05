package rootfs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectCacheReusesValidatedFileIdentityAndRevalidatesChanges(t *testing.T) {
	payload := "rootfs-cache-payload"
	desc := objectCacheDescriptor(t, "cache-team", payload)
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
	desc := objectCacheDescriptor(t, "restart-team", payload)
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

func TestObjectCacheCoalescesConcurrentFillsAcrossReaders(t *testing.T) {
	payload := strings.Repeat("shared chunk", 1024)
	desc := objectCacheDescriptor(t, "shared-team", payload)
	base := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, base.Put(desc.Key, strings.NewReader(payload)))
	store := &countingGetStore{Store: base, delay: 20 * time.Millisecond}
	cache := NewObjectCache(ObjectCacheConfig{Dir: t.TempDir(), MaxBytes: 1 << 20})

	const readers = 16
	start := make(chan struct{})
	errs := make(chan error, readers)
	var group sync.WaitGroup
	for range readers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			reader, _, err := cache.GetOrFetchObject(context.Background(), store, desc)
			if err == nil {
				_, err = io.ReadAll(reader)
				err = errors.Join(err, reader.Close())
			}
			errs <- err
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), store.gets.Load())
}

func TestObjectCacheSeparatesIdenticalContentAcrossTeams(t *testing.T) {
	payload := strings.Repeat("tenant-scoped chunk", 128)
	first := objectCacheDescriptor(t, "team-one", payload)
	second := objectCacheDescriptor(t, "team-two", payload)
	base := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, base.Put(first.Key, strings.NewReader(payload)))
	require.NoError(t, base.Put(second.Key, strings.NewReader(payload)))
	store := &countingGetStore{Store: base}
	cache := NewObjectCache(ObjectCacheConfig{Dir: t.TempDir(), MaxBytes: 1 << 20})

	for _, desc := range []rootfshead.Object{first, second} {
		reader, _, err := cache.GetOrFetchObject(context.Background(), store, desc)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, reader)
		require.NoError(t, errors.Join(err, reader.Close()))
	}
	assert.Equal(t, int64(2), store.gets.Load())
	firstPath, err := cache.pathForDescriptor(first)
	require.NoError(t, err)
	secondPath, err := cache.pathForDescriptor(second)
	require.NoError(t, err)
	assert.NotEqual(t, firstPath, secondPath)
}

func TestObjectCacheSweepPreservesActiveTempAndRemovesStaleTemp(t *testing.T) {
	payload := "active cache write"
	desc := objectCacheDescriptor(t, "temp-team", payload)
	cache := NewObjectCache(ObjectCacheConfig{Dir: t.TempDir(), MaxBytes: 1 << 20})
	path, err := cache.pathForDescriptor(desc)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))

	active, err := os.CreateTemp(filepath.Dir(path), ".object-*")
	require.NoError(t, err)
	activePath := active.Name()
	require.NoError(t, active.Close())
	stale, err := os.CreateTemp(filepath.Dir(path), ".object-*")
	require.NoError(t, err)
	stalePath := stale.Name()
	require.NoError(t, stale.Close())
	old := time.Now().Add(-staleObjectCacheTempAge - time.Hour)
	require.NoError(t, os.Chtimes(activePath, old, old))
	require.NoError(t, os.Chtimes(stalePath, old, old))
	cache.registerTemp(activePath)
	t.Cleanup(func() { cache.unregisterTemp(activePath) })

	require.NoError(t, cache.Sweep())
	_, err = os.Stat(activePath)
	require.NoError(t, err)
	_, err = os.Stat(stalePath)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestObjectCachePutDoesNotWaitForScheduledSweep(t *testing.T) {
	payload := "nonblocking cache fill"
	desc := objectCacheDescriptor(t, "sweep-team", payload)
	cache := NewObjectCache(ObjectCacheConfig{
		Dir: t.TempDir(), MaxBytes: 1 << 20, SweepInterval: time.Hour,
	})

	cache.sweepMu.Lock()
	locked := true
	defer func() {
		if locked {
			cache.sweepMu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- cache.Put(context.Background(), desc, strings.NewReader(payload))
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cache fill blocked on a scheduled directory sweep")
	}
	cache.mu.Lock()
	assert.True(t, cache.sweepScheduled)
	cache.mu.Unlock()

	cache.sweepMu.Unlock()
	locked = false
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return !cache.sweepScheduled
	}, time.Second, 10*time.Millisecond)
}

type countingGetStore struct {
	objectstore.Store
	gets  atomic.Int64
	delay time.Duration
}

func (s *countingGetStore) Get(key string, offset, limit int64) (io.ReadCloser, error) {
	s.gets.Add(1)
	time.Sleep(s.delay)
	return s.Store.Get(key, offset, limit)
}

func objectCacheDescriptor(t *testing.T, teamID, payload string) rootfshead.Object {
	t.Helper()
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	require.NoError(t, err)
	value := digest.FromString(payload).String()
	key, err := rootfshead.ObjectKey(prefix, rootfshead.ChunkMediaType, value)
	require.NoError(t, err)
	return rootfshead.Object{
		Key:       key,
		Digest:    value,
		Size:      int64(len(payload)),
		MediaType: rootfshead.ChunkMediaType,
	}
}
