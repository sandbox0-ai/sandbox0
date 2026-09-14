package rootfsblock

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func testDiskConfig(t testing.TB, budget int64) DiskCacheConfig {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return DiskCacheConfig{Directory: directory, MaxBytes: budget}
}

func openTestDiskCache(t testing.TB, memory int64, config DiskCacheConfig) *ReadCache {
	t.Helper()
	cache, err := NewReadCacheWithDisk(memory, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	return cache
}

type unavailableCacheSource struct{}

func (unavailableCacheSource) Get(string, int64, int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("source is unavailable")
}

func waitDiskWrites(t *testing.T, cache *ReadCache) {
	t.Helper()
	require.Eventually(t, func() bool { return cache.Stats().QueuedWriteBytes == 0 }, 5*time.Second, time.Millisecond)
}

func TestDiskCacheReadsWholeGenerationAfterRestartWithoutSource(t *testing.T) {
	for _, version := range []int{DescriptorVersion, CompressedFormatVersion} {
		for _, chunk := range []int{4096, coalescedReadBytes} {
			t.Run(fmt.Sprintf("format-%d/read-%d", version, chunk), func(t *testing.T) {
				payload := make([]byte, coalescedReadBytes)
				_, err := rand.New(rand.NewSource(928)).Read(payload)
				require.NoError(t, err)
				// Mixed compressible/raw content is unrelated to any benchmark image.
				copy(payload[CompressedDataRangeBytes:], bytes.Repeat([]byte{0x91}, CompressedDataRangeBytes))
				store := newBuildTestStore()
				built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), store,
					BuildOptions{FormatVersion: version, DataRangeBytes: CompressedDataRangeBytes, PageEntries: 4})
				require.NoError(t, err)
				config := testDiskConfig(t, 4<<20)
				cache := openTestDiskCache(t, 1024, config)
				reader, err := NewReaderWithCache(store, built.Descriptor, cache)
				require.NoError(t, err)
				actual := make([]byte, len(payload))
				for offset := 0; offset < len(actual); offset += chunk {
					_, err = reader.ReadAt(actual[offset:offset+chunk], int64(offset))
					require.NoError(t, err)
				}
				require.Equal(t, payload, actual)
				require.NoError(t, cache.Close())
				require.Zero(t, cache.Stats().DiskWriteDrops)
				require.Greater(t, cache.Stats().DiskWrites, uint64(0))
				// New cache and Reader simulate a new ctld process, with no RAM
				// entries and an object source that cannot satisfy even one GET.
				recovered := openTestDiskCache(t, 0, config)
				reader, err = NewReaderWithCache(unavailableCacheSource{}, built.Descriptor, recovered)
				require.NoError(t, err)
				clear(actual)
				for offset := 0; offset < len(actual); offset += chunk {
					_, err = reader.ReadAt(actual[offset:offset+chunk], int64(offset))
					require.NoError(t, err)
				}
				require.Equal(t, payload, actual)
				require.Greater(t, recovered.Stats().DiskHits, uint64(0))
			})
		}
	}

}

func TestDiskCacheReusesContentAcrossObjectLocationsAndConcurrentReaders(t *testing.T) {
	store := newRangeTestStore()
	payload := bytes.Repeat([]byte{0x37}, LogicalBlockSize)
	object := store.put("tenant-a/immutable/data", payload)
	cache := openTestDiskCache(t, 0, testDiskConfig(t, 1<<20))
	first := &Reader{source: store, cache: cache}
	actual, err := first.readRange(object)
	require.NoError(t, err)
	require.Equal(t, payload, actual)
	waitDiskWrites(t, cache)
	object.Key = "tenant-b/immutable/data"
	var wg sync.WaitGroup
	errors := make(chan error, 100)
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader := &Reader{source: unavailableCacheSource{}, cache: cache}
			got, err := reader.readRange(object)
			if err == nil && !bytes.Equal(got, payload) {
				err = fmt.Errorf("content differs")
			}
			errors <- err
		}()
	}
	wg.Wait()
	for range 100 {
		require.NoError(t, <-errors)
	}
	require.Equal(t, 1, store.count("tenant-a/immutable/data"))
	require.Equal(t, uint64(100), cache.Stats().DiskHits)
}

func TestDiskCacheCorruptionFallsBackToAuthenticatedSource(t *testing.T) {
	for _, kind := range []string{"checksum", "truncated", "oversized", "symlink", "missing"} {
		t.Run(kind, func(t *testing.T) {
			store := newRangeTestStore()
			payload := bytes.Repeat([]byte{0x65}, LogicalBlockSize)
			object := store.put("immutable/data", payload)
			config := testDiskConfig(t, 1<<20)
			cache := openTestDiskCache(t, 0, config)
			reader := &Reader{source: store, cache: cache}
			_, err := reader.readRange(object)
			require.NoError(t, err)
			waitDiskWrites(t, cache)
			name, _ := diskCacheName(rangeCacheKey(object))
			path := filepath.Join(config.Directory, name)
			switch kind {
			case "checksum":
				require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0x44}, len(payload)), 0o600))
			case "truncated":
				require.NoError(t, os.Truncate(path, int64(len(payload)-1)))
			case "oversized":
				require.NoError(t, os.Truncate(path, int64(len(payload)+1)))
			case "symlink":
				require.NoError(t, os.Remove(path))
				outside := filepath.Join(t.TempDir(), "private")
				require.NoError(t, os.WriteFile(outside, payload, 0o600))
				require.NoError(t, os.Symlink(outside, path))
			case "missing":
				require.NoError(t, os.Remove(path))
			}
			actual, err := reader.readRange(object)
			require.NoError(t, err)
			require.Equal(t, payload, actual)
			require.Equal(t, 2, store.count(object.Key))
			require.Positive(t, cache.Stats().DiskErrors)
		})
	}
}

func TestDiskCacheEnforcesBudgetAndRecoversIncompleteWrite(t *testing.T) {
	config := testDiskConfig(t, 2*LogicalBlockSize)
	cache := openTestDiskCache(t, 0, config)
	keys := make([]readCacheKey, 3)
	for index := range keys {
		payload := bytes.Repeat([]byte{byte(index + 1)}, LogicalBlockSize)
		keys[index] = readCacheKey{digest.FromBytes(payload).String(), int64(len(payload))}
		cache.addVerified(keys[index], payload)
		waitDiskWrites(t, cache)
	}
	require.Equal(t, int64(2*LogicalBlockSize), cache.Stats().DiskBytes)
	_, ok := cache.get(keys[0])
	require.False(t, ok, "least recently used range must be evicted")
	require.NoError(t, cache.Close())
	require.NoError(t, os.WriteFile(filepath.Join(config.Directory, ".pending"), []byte("torn write"), 0o600))
	config.MaxBytes = LogicalBlockSize
	recovered := openTestDiskCache(t, 0, config)
	require.Equal(t, int64(LogicalBlockSize), recovered.Stats().DiskBytes)
	require.Equal(t, 1, recovered.Stats().DiskEntries)
	_, err := os.Stat(filepath.Join(config.Directory, ".pending"))
	require.ErrorIs(t, err, os.ErrNotExist)
	files, err := os.ReadDir(config.Directory)
	require.NoError(t, err)
	require.Len(t, files, 2, "only one range and the owner lock remain")
}

func TestDiskCacheRequiresExclusivePrivateDirectory(t *testing.T) {
	config := testDiskConfig(t, 1<<20)
	cache := openTestDiskCache(t, 0, config)
	_, err := NewReadCacheWithDisk(0, config)
	require.ErrorContains(t, err, "lock node read cache")
	info, err := os.Stat(config.Directory)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	link := filepath.Join(t.TempDir(), "linked")
	require.NoError(t, os.Symlink(config.Directory, link))
	_, err = NewReadCacheWithDisk(0, DiskCacheConfig{Directory: link, MaxBytes: 1 << 20})
	require.ErrorContains(t, err, "symlinks")
	require.NoError(t, cache.Close())
	for _, invalid := range []DiskCacheConfig{
		{Directory: config.Directory}, {MaxBytes: 10}, {Directory: "relative", MaxBytes: 10},
		{Directory: "/", MaxBytes: 10}, {Directory: config.Directory, MaxBytes: -1},
	} {
		_, err := NewReadCacheWithDisk(0, invalid)
		require.Error(t, err)
	}
}

func TestDiskCacheRejectsUnverifiedBytesAndBoundsQueuedMemory(t *testing.T) {
	cache := openTestDiskCache(t, 0, testDiskConfig(t, 1<<20))
	payload := bytes.Repeat([]byte{0x12}, LogicalBlockSize)
	key := readCacheKey{digest.FromBytes(payload).String(), int64(len(payload))}
	cache.disk.enqueue(key, bytes.Repeat([]byte{0x99}, len(payload)))
	waitDiskWrites(t, cache)
	require.Zero(t, cache.Stats().DiskBytes)
	require.Equal(t, uint64(1), cache.Stats().DiskErrors)
	oversized := make([]byte, len(payload), diskCacheQueueBytes+1)
	copy(oversized, payload)
	cache.disk.enqueue(key, oversized)
	require.Zero(t, cache.Stats().QueuedWriteBytes)
	require.Equal(t, uint64(1), cache.Stats().DiskWriteDrops)
	cache.disk.enqueue(readCacheKey{"sha256:../../escape", int64(len(payload))}, payload)
	require.Zero(t, cache.Stats().DiskBytes)
}

func TestDiskCacheHitStillValidatesMappingParent(t *testing.T) {
	store, descriptor, leaf := mappingCacheFixture(t, 2)
	config := testDiskConfig(t, 1<<20)
	cache := openTestDiskCache(t, 0, config)
	reader, err := NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), 0)
	require.NoError(t, err)
	require.NoError(t, cache.Close())

	wrongParent, err := EncodeMappingPage(MappingPage{
		Level: 1, StartBlock: 0, BlockCount: 3,
		Entries: []MappingEntry{{LogicalStart: 1, BlockCount: 2, Kind: MappingEntryChild, Object: leaf}},
	})
	require.NoError(t, err)
	root := store.put("maps/wrong-parent", wrongParent)
	recovered := openTestDiskCache(t, 0, config)
	reader, err = NewReaderWithCache(store, testReaderDescriptor(root, 3), recovered)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), LogicalBlockSize)
	require.ErrorContains(t, err, "does not match")
	require.Equal(t, 1, store.count(leaf.Key), "cached child bytes must not bypass the new parent binding")
	require.Positive(t, recovered.Stats().DiskHits)
}

func TestDiskCacheRejectsDirectoryOwnedByAnotherUser(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing file ownership requires root")
	}
	config := testDiskConfig(t, 1<<20)
	require.NoError(t, os.Chown(config.Directory, 1, -1))
	t.Cleanup(func() { require.NoError(t, os.Chown(config.Directory, 0, -1)) })
	_, err := NewReadCacheWithDisk(0, config)
	require.ErrorContains(t, err, "owned by the node daemon")
}
