package rootfsblock

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func assertMappingPolicyAccounting(t *testing.T, c *ReadCache) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var total, protected int64
	count := 0
	for element := c.order.Front(); element != nil; element = element.Next() {
		entry := element.Value.(rangeCacheEntry)
		require.False(t, entry.protected)
		require.Same(t, element, c.items[entry.key])
		total += entry.bytes
		count++
	}
	for element := c.mappingOrder.Front(); element != nil; element = element.Next() {
		entry := element.Value.(rangeCacheEntry)
		require.True(t, entry.protected)
		require.NotNil(t, entry.page)
		require.Same(t, element, c.items[entry.key])
		total += entry.bytes
		protected += entry.bytes
		count++
	}
	require.Equal(t, len(c.items), count)
	require.Equal(t, c.bytes, total)
	require.Equal(t, c.mappingBytes, protected)
	require.LessOrEqual(t, protected, c.mappingBudget)
	require.LessOrEqual(t, total, c.maxBytes)
}

func TestMappingProtectionSurvivesDataScanWithinExistingBudget(t *testing.T) {
	store, descriptor, leaf := mappingCacheFixture(t, 16)
	cache, err := NewReadCache(128 << 10)
	require.NoError(t, err)
	reader, err := NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), 0)
	require.NoError(t, err)
	for index := range 64 {
		payload := bytes.Repeat([]byte{byte(index + 32)}, 8192)
		cache.addVerified(readCacheKey{checksum: digest.FromBytes(payload).String(), length: int64(len(payload))}, payload)
		assertMappingPolicyAccounting(t, cache)
	}
	reader, err = NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, 1, store.count(leaf.Key))
	require.Equal(t, uint64(2), cache.decodes.Load())
	assertMappingPolicyAccounting(t, cache)
}

func TestMappingProtectionDemotesWithoutPinningOrDuplicateAccounting(t *testing.T) {
	cache, err := NewReadCache(32 << 10)
	require.NoError(t, err)
	keys := make([]readCacheKey, 32)
	for index := range keys {
		page := MappingPage{StartBlock: uint64(index * 4), BlockCount: 4}
		payload, err := EncodeMappingPage(page)
		require.NoError(t, err)
		keys[index] = readCacheKey{checksum: digest.FromBytes(payload).String(), length: int64(len(payload))}
		cache.addVerified(keys[index], payload)
		cache.addPage(keys[index], payload, page)
		cache.addPage(keys[index], payload, page)
		cache.get(keys[index])
		cache.getPage(keys[index])
		assertMappingPolicyAccounting(t, cache)
	}
	require.Less(t, cache.mappingOrder.Len(), len(keys))
	require.Positive(t, cache.order.Len(), "displaced metadata may borrow spare ordinary-cache capacity")
	newest := keys[len(keys)-1]
	for index := range 32 {
		payload := bytes.Repeat([]byte{byte(index + 64)}, 2048)
		cache.addVerified(readCacheKey{checksum: digest.FromBytes(payload).String()}, payload)
		assertMappingPolicyAccounting(t, cache)
	}
	_, found := cache.getPage(newest)
	require.True(t, found)
	_, found = cache.getPage(keys[0])
	require.False(t, found, "old metadata must not be pinned indefinitely")
}

func TestMappingProtectionConcurrentUpgradesStayBounded(t *testing.T) {
	store, descriptor, _ := mappingCacheFixture(t, 32)
	cache, err := NewReadCache(1 << 20)
	require.NoError(t, err)
	var wait sync.WaitGroup
	for owner := range 18 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range 32 {
				reader, err := NewReaderWithCache(store, descriptor, cache)
				require.NoError(t, err)
				output := make([]byte, LogicalBlockSize)
				_, err = reader.ReadAt(output, int64(index*LogicalBlockSize))
				require.NoError(t, err)
				require.Equal(t, byte(index+1), output[0])
				payload := bytes.Repeat([]byte(fmt.Sprintf("%04d-%04d", owner, index)), 1024)
				cache.addVerified(readCacheKey{checksum: digest.FromBytes(payload).String()}, payload)
				assertMappingPolicyAccounting(t, cache)
			}
		}()
	}
	wait.Wait()
	assertMappingPolicyAccounting(t, cache)
	require.Equal(t, uint64(2), cache.decodes.Load())
}
