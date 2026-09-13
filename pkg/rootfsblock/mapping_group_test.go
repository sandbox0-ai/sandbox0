package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func groupCandidate() bool { return true }

// Full-sized mapping pages, not a populated filesystem or startup benchmark.
// Four independently encoded pages contain 1024 valid data locators each.
func groupFixture(t testing.TB, encoded bool, prefix string) (*buildTestStore, Descriptor, []MappingEntry, string) {
	t.Helper()
	store := newBuildTestStore()
	var group, data []byte
	var children []MappingEntry
	rng := rand.New(rand.NewPCG(731, 19))
	dataKey := "packs/" + prefix + "/" + strings.Repeat("a", 80)
	for pageIndex := range 4 {
		start := uint64(pageIndex) << 20
		page := MappingPage{Version: CompressedFormatVersion, StartBlock: start, BlockCount: 1 << 20}
		for entryIndex := range 1024 {
			payload := make([]byte, LogicalBlockSize)
			for i := range payload {
				payload[i] = byte(rng.Uint32())
			}
			object := ObjectRange{Key: dataKey, Offset: int64(len(data)), Length: LogicalBlockSize, Checksum: digest.FromBytes(payload).String()}
			data = append(data, payload...)
			page.Entries = append(page.Entries, MappingEntry{Kind: MappingEntryData, LogicalStart: start + uint64(entryIndex), BlockCount: 1, Object: object})
		}
		raw, err := EncodeMappingPage(page)
		require.NoError(t, err)
		stored := raw
		object := ObjectRange{Length: int64(len(raw)), Checksum: digest.FromBytes(raw).String()}
		if encoded {
			stored, object, err = encodeRangePayload(t.Context(), raw)
			require.NoError(t, err)
			require.Equal(t, RangeEncodingZstd, object.Encoding)
		}
		object.Offset = int64(len(group))
		group = append(group, stored...)
		children = append(children, MappingEntry{Kind: MappingEntryChild, LogicalStart: start, BlockCount: 1 << 20, Object: object})
	}
	pack := "maps/" + prefix + "/" + digest.FromBytes(group).Encoded()
	for i := range children {
		children[i].Object.Key = pack
	}
	require.NoError(t, store.PutImmutable(t.Context(), pack, group))
	require.NoError(t, store.PutImmutable(t.Context(), dataKey, data))
	root := MappingPage{Version: CompressedFormatVersion, Level: 1, BlockCount: 1 << 28, Entries: children}
	raw, err := EncodeMappingPage(root)
	require.NoError(t, err)
	stored, object, err := encodeRangePayload(t.Context(), raw)
	require.NoError(t, err)
	object.Key = "maps/" + prefix + "/" + digest.FromBytes(stored).Encoded()
	require.NoError(t, store.PutImmutable(t.Context(), object.Key, stored))
	d := testReaderDescriptor(object, 1<<28)
	d.Version = CompressedFormatVersion
	d.MappingRoot.Version = CompressedFormatVersion
	return store, d, children, pack
}

type groupProbeSource struct {
	RangeSource
	pack   string
	finish chan struct{}
	mu     sync.Mutex
	calls  []adaptiveGet
	err    error
}

func (s *groupProbeSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	return s.GetContext(context.Background(), key, offset, length)
}
func (s *groupProbeSource) GetContext(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if key == s.pack {
		s.mu.Lock()
		s.calls = append(s.calls, adaptiveGet{offset, length})
		s.mu.Unlock()
		if s.finish != nil {
			select {
			case <-s.finish:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if s.err != nil {
			return nil, s.err
		}
	}
	return s.RangeSource.Get(key, offset, length)
}
func (s *groupProbeSource) requests() []adaptiveGet {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]adaptiveGet(nil), s.calls...)
}
func (s *groupProbeSource) reset() { s.mu.Lock(); defer s.mu.Unlock(); s.calls = nil }
func groupRead(reader *Reader, parent MappingPage, block uint64) error {
	entry, _, found, err := reader.resolve(parent, block)
	if err == nil && (!found || entry.Kind != MappingEntryData || entry.LogicalStart != block) {
		return fmt.Errorf("incorrect mapping at %d", block)
	}
	return err
}

func TestMappingGroupProbeSequentialAndCache(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		store, d, children, pack := groupFixture(t, encoded, "sequential")
		for _, budget := range []int64{0, 1 << 20, DefaultReadCacheBytes} {
			t.Run(fmt.Sprintf("encoded-%t/cache-%d", encoded, budget), func(t *testing.T) {
				source := &groupProbeSource{RangeSource: store, pack: pack}
				reader, err := NewReader(source, d, budget)
				require.NoError(t, err)
				before := reader.cache.decodes.Load()
				for _, child := range children {
					require.NoError(t, groupRead(reader, reader.root, child.LogicalStart))
				}
				calls := source.requests()
				want := 4
				if groupCandidate() && budget == DefaultReadCacheBytes {
					want = 1
				}
				require.Len(t, calls, want)
				var bytes int64
				for _, c := range calls {
					bytes += c.length
				}
				require.Equal(t, int64(len(store.objects[pack])), bytes)
				require.EqualValues(t, 4, reader.cache.decodes.Load()-before)
				reader.cache.mu.Lock()
				require.LessOrEqual(t, reader.cache.bytes, budget)
				for _, child := range children {
					if e, ok := reader.cache.items[rangeCacheKey(child.Object)]; ok {
						payload := e.Value.(rangeCacheEntry).payload
						require.LessOrEqual(t, int64(cap(payload)), child.Object.Length+1, "no cached page may retain a group backing buffer")
					}
				}
				reader.cache.mu.Unlock()
				if budget == DefaultReadCacheBytes {
					source.reset()
					for _, child := range children {
						require.NoError(t, groupRead(reader, reader.root, child.LogicalStart))
					}
					require.Empty(t, source.requests())
				}
				t.Logf("source_calls=%d source_bytes=%d page_decodes=4", len(calls), bytes)
			})
		}
	}
}

func TestMappingGroupProbeConcurrentParents(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		store, d, children, pack := groupFixture(t, encoded, "concurrent")
		for _, subset := range []bool{false, true} {
			t.Run(fmt.Sprintf("encoded-%t/subsets-%t", encoded, subset), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					source := &groupProbeSource{RangeSource: store, pack: pack, finish: make(chan struct{})}
					cache, err := NewReadCache(DefaultReadCacheBytes)
					require.NoError(t, err)
					var readers []*Reader
					for range 8 {
						r, err := NewReaderWithCache(source, d, cache)
						require.NoError(t, err)
						readers = append(readers, r)
					}
					before := cache.decodes.Load()
					results := make(chan error, 8)
					for i, r := range readers {
						go func() {
							p := r.root
							index := i % 4
							if subset {
								if i%2 == 0 {
									p.Entries = children[:3]
									index = 0
								} else {
									p.Entries = children[1:]
									index = 3
								}
							}
							results <- groupRead(r, p, children[index].LogicalStart)
						}()
					}
					synctest.Wait()
					calls := source.requests()
					want := 4
					if subset {
						want = 2
					}
					if groupCandidate() && !subset {
						want = 1
					}
					require.Len(t, calls, want)
					requireReadAdmissionUsage(t, &cache.sourceSlots, want, 0, 0)
					close(source.finish)
					for range 8 {
						require.NoError(t, <-results)
					}
					var loaded int64
					for _, c := range source.requests() {
						loaded += c.length
					}
					var expected int64
					if !subset {
						expected = int64(len(store.objects[pack]))
					} else if groupCandidate() {
						for _, c := range children[:3] {
							expected += c.Object.StoredLength()
						}
						for _, c := range children[1:] {
							expected += c.Object.StoredLength()
						}
					} else {
						expected = children[0].Object.StoredLength() + children[3].Object.StoredLength()
					}
					require.Equal(t, expected, loaded)
					t.Logf("source_calls=%d source_bytes=%d page_decodes=%d", len(calls), loaded, cache.decodes.Load()-before)
					requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
				})
			})
		}
	}
}

func TestMappingGroupProbeErrorsAndCancellation(t *testing.T) {
	store, d, children, pack := groupFixture(t, true, "errors")
	t.Run("transport-bounded-exact-fallback", func(t *testing.T) {
		source := &groupProbeSource{RangeSource: store, pack: pack, err: io.ErrUnexpectedEOF}
		r, err := NewReader(source, d, DefaultReadCacheBytes)
		require.NoError(t, err)
		require.ErrorIs(t, groupRead(r, r.root, 0), io.ErrUnexpectedEOF)
		want := 1
		if groupCandidate() {
			want = 2
		}
		require.Len(t, source.requests(), want)
		requireReadAdmissionUsage(t, &r.cache.sourceSlots, 0, 0, 0)
	})
	t.Run("active-cancellation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := &groupProbeSource{RangeSource: store, pack: pack, finish: make(chan struct{})}
			cache, err := NewReadCache(DefaultReadCacheBytes)
			require.NoError(t, err)
			r, err := NewReaderWithCacheContext(ctx, source, d, cache)
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() { result <- groupRead(r, r.root, 0) }()
			synctest.Wait()
			require.Len(t, source.requests(), 1)
			cancel()
			require.ErrorIs(t, <-result, context.Canceled)
			requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
		})
	})
	t.Run("neighbor-checksum", func(t *testing.T) {
		copyStore := newBuildTestStore()
		for k, v := range store.objects {
			copyStore.objects[k] = bytes.Clone(v)
		}
		copyStore.objects[pack][children[3].Object.Offset] ^= 1
		r, err := NewReader(copyStore, d, DefaultReadCacheBytes)
		require.NoError(t, err)
		require.NoError(t, groupRead(r, r.root, 0))
		require.Error(t, groupRead(r, r.root, children[3].LogicalStart))
		_, ok := r.cache.getPage(rangeCacheKey(children[3].Object))
		require.False(t, ok)
	})
}

func BenchmarkMappingGroupProbe(b *testing.B) {
	for _, encoded := range []bool{false, true} {
		store, d, children, pack := groupFixture(b, encoded, "allocation")
		for _, budget := range []int64{0, 1 << 20, DefaultReadCacheBytes} {
			for _, demand := range []int{1, 4} {
				b.Run(fmt.Sprintf("encoded-%t/cache-%d/demand-%d", encoded, budget, demand), func(b *testing.B) {
					source := &groupProbeSource{RangeSource: store, pack: pack}
					var totalCalls int64
					var totalBytes int64
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						r, err := NewReader(source, d, budget)
						if err != nil {
							b.Fatal(err)
						}
						source.reset()
						for _, child := range children[:demand] {
							if err := groupRead(r, r.root, child.LogicalStart); err != nil {
								b.Fatal(err)
							}
						}
						for _, c := range source.requests() {
							totalCalls++
							totalBytes += c.length
						}
					}
					b.ReportMetric(float64(totalCalls)/float64(b.N), "source_calls/op")
					b.ReportMetric(float64(totalBytes)/float64(b.N), "source_bytes/op")
				})
			}
		}
	}
}
