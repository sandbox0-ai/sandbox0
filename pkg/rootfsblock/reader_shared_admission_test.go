package rootfsblock

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// This fixture counts distinct source loads, not HTTP requests, RSS, or claim
// latency. Every generation and range has different content to prevent cache
// hits or singleflight from hiding an aggregate admission bug.
type sharedAdmissionSource struct {
	RangeSource
	started chan int64
	finish  chan struct{}
}

func (s *sharedAdmissionSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if strings.HasPrefix(key, "packs/shared-admission/") {
		s.started <- length
		<-s.finish
	}
	return s.RangeSource.Get(key, offset, length)
}

func sharedAdmissionReaders(t *testing.T, width int, cache *ReadCache) (*sharedAdmissionSource, []*Reader, [][]byte) {
	t.Helper()
	store := newRangeTestStore()
	source := &sharedAdmissionSource{RangeSource: store, started: make(chan int64, width), finish: make(chan struct{})}
	readers := make([]*Reader, width)
	payloads := make([][]byte, width)
	for owner := range width {
		payload := make([]byte, bulkReadThreshold)
		for at := 0; at < len(payload); at += 2 {
			payload[at] = byte(owner + 1)
			payload[at+1] = byte(at/adaptiveRangeBytes + 1)
		}
		pack := store.put(fmt.Sprintf("packs/shared-admission/%d", owner), payload)
		page := MappingPage{StartBlock: 0, BlockCount: uint64(len(payload) / LogicalBlockSize)}
		for index := range 2 {
			part := store.put(fmt.Sprintf("fixture/%d/%d", owner, index), payload[index*adaptiveRangeBytes:(index+1)*adaptiveRangeBytes])
			part.Key, part.Offset = pack.Key, int64(index*adaptiveRangeBytes)
			page.Entries = append(page.Entries, MappingEntry{LogicalStart: uint64(index * 16), BlockCount: 16, Kind: MappingEntryData, Object: part})
		}
		encoded, err := EncodeMappingPage(page)
		require.NoError(t, err)
		root := store.put(fmt.Sprintf("maps/shared-admission/%d", owner), encoded)
		readers[owner], err = NewReaderWithCache(source, testReaderDescriptor(root, int64(page.BlockCount)), cache)
		require.NoError(t, err)
		payloads[owner] = payload
	}
	return source, readers, payloads
}

func TestSharedReadAdmissionBoundsDistinctGenerations(t *testing.T) {
	for _, width := range []int{1, 8, 64} {
		for _, budget := range []int64{0, DefaultReadCacheBytes} {
			for _, demand := range []int{1, bulkReadThreshold} {
				t.Run(fmt.Sprintf("readers%d/cache%d/demand%d", width, budget, demand), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						cache, err := NewReadCache(budget)
						require.NoError(t, err)
						source, readers, expected := sharedAdmissionReaders(t, width, cache)
						var wg sync.WaitGroup
						var unblock sync.Once
						defer func() { unblock.Do(func() { close(source.finish) }); wg.Wait() }()
						results := make(chan error, width)
						for index, reader := range readers {
							wg.Add(1)
							go func() {
								defer wg.Done()
								actual := make([]byte, demand)
								n, err := reader.ReadAt(actual, 0)
								if err == nil && (n != len(actual) || !bytes.Equal(actual, expected[index][:demand])) {
									err = fmt.Errorf("generation %d returned incorrect bytes", index)
								}
								results <- err
							}()
						}
						synctest.Wait()
						require.Len(t, source.started, min(width, maxConcurrentSourceReads), "all generation readers must share the node's source budget")
						unblock.Do(func() { close(source.finish) })
						wg.Wait()
						for range width {
							require.NoError(t, <-results)
						}
						require.Len(t, source.started, width)
					})
				})
			}
		}
	}
}

func TestSharedReadAdmissionPreservesSingleflightAndCacheHitBypass(t *testing.T) {
	for _, demand := range []int{1, bulkReadThreshold} {
		t.Run(fmt.Sprint(demand), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cache, err := NewReadCache(DefaultReadCacheBytes)
				require.NoError(t, err)
				source, readers, expected := sharedAdmissionReaders(t, 1, cache)
				var wg sync.WaitGroup
				var unblock sync.Once
				defer func() { unblock.Do(func() { close(source.finish) }); wg.Wait() }()
				results := make(chan error, 32)
				for range 32 {
					reader, err := NewReaderWithCache(source, readers[0].descriptor, cache)
					require.NoError(t, err)
					wg.Add(1)
					go func() {
						defer wg.Done()
						actual := make([]byte, demand)
						_, err := reader.ReadAt(actual, 0)
						if err == nil && !bytes.Equal(actual, expected[0][:demand]) {
							err = fmt.Errorf("shared content differs")
						}
						results <- err
					}()
				}
				synctest.Wait()
				require.Len(t, source.started, 1)
				requireReadAdmissionUsage(t, &cache.sourceSlots, 1, 0, 0)
				unblock.Do(func() { close(source.finish) })
				wg.Wait()
				for range 32 {
					require.NoError(t, <-results)
				}
				holdReadAdmission(t, cache)
				go func() {
					reader, err := NewReaderWithCache(source, readers[0].descriptor, cache)
					if err == nil {
						actual := make([]byte, demand)
						_, err = reader.ReadAt(actual, 0)
						if err == nil && !bytes.Equal(actual, expected[0][:demand]) {
							err = fmt.Errorf("cached content differs")
						}
					}
					results <- err
				}()
				synctest.Wait()
				require.Len(t, results, 1, "cached mappings and data bypass saturated source admission")
				require.NoError(t, <-results)
				require.Len(t, source.started, 1)
				requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 0, 0)
			})
		})
	}
}
