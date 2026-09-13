package rootfsblock

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type layoutCountingSource struct {
	RangeSource
	dataCalls int
}

func (s *layoutCountingSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if strings.Contains(key, "/packs/") {
		s.dataCalls++
	}
	return s.RangeSource.Get(key, offset, length)
}

func TestDataRangeLayoutSharesContentWithoutDisablingCoalescingOrCOW(t *testing.T) {
	const unit = CompressedDataRangeBytes
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			data := make([]byte, 8*unit)
			_, err := rand.New(rand.NewSource(91)).Read(data)
			require.NoError(t, err)
			if compressed {
				for i := range data {
					data[i] = byte(i/unit + 1)
				}
			}
			cache, err := NewReadCache(4 << 20)
			require.NoError(t, err)
			for lane, shift := range []int64{3 * 4096, 7 * 4096} {
				disk := bytes.Repeat([]byte{0x7b}, 10*unit)
				copy(disk[shift:], data)
				plan, err := NewDataRangeLayout(int64(len(disk)), unit, []DataRangeSpan{{Start: shift, End: shift + int64(len(data))}})
				require.NoError(t, err)
				store := newBuildTestStore()
				built, err := BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(disk), int64(len(disk)), store, BuildOptions{FormatVersion: 2, ObjectPrefix: fmt.Sprintf("lane%d", lane)}, plan)
				require.NoError(t, err)
				source := &layoutCountingSource{RangeSource: store}
				r, err := NewReaderWithCache(source, built.Descriptor, cache)
				require.NoError(t, err)
				for _, e := range r.root.Entries {
					require.True(t, fullDataRange(e))
					require.Zero(t, e.DataOffset)
				}
				got := make([]byte, len(data))
				_, err = r.ReadAt(got, shift)
				require.NoError(t, err)
				require.Equal(t, data, got)
				if lane == 0 {
					require.Equal(t, 1, source.dataCalls, "arbitrarily aligned complete units still coalesce")
				} else {
					require.Zero(t, source.dataCalls, "different image offsets share the existing decoded cache")
				}
				updates := []BlockUpdate{{Block: uint64(shift/4096) + 1, Data: bytes.Repeat([]byte{0x42}, 4096)}, {Block: uint64(shift/4096) + 15, Data: bytes.Repeat([]byte{0x43}, 4096)}, {Block: uint64(shift/4096) + 16, Data: bytes.Repeat([]byte{0x44}, 4096)}}
				count := &layoutCountingSource{RangeSource: store}
				next, err := BuildIncrementalGeneration(t.Context(), count, built.Descriptor, updates, store, BuildOptions{ObjectPrefix: fmt.Sprintf("edit%d", lane)})
				require.NoError(t, err)
				if compressed {
					require.Zero(t, count.dataCalls)
				} else {
					require.Equal(t, 2, count.dataCalls, "only two touched raw ranges require subrange checksums")
				}
				for _, u := range updates {
					copy(disk[int(u.Block)*4096:], u.Data)
				}
				rr, err := NewReader(store, next.Descriptor, 0)
				require.NoError(t, err)
				whole := make([]byte, len(disk))
				_, err = rr.ReadAt(whole, 0)
				require.NoError(t, err)
				require.Equal(t, disk, whole)
				var dirtyBytes int64
				for _, ref := range next.References {
					if ref.Kind == ObjectKindDataPack {
						dirtyBytes += ref.Size
					}
				}
				require.LessOrEqual(t, dirtyBytes, int64(len(updates)*4096), "unchanged data is not republished")
				old, err := NewReader(store, built.Descriptor, 0)
				require.NoError(t, err)
				_, err = old.ReadAt(got, shift)
				require.NoError(t, err)
				require.Equal(t, data, got)
				var target MappingEntry
				for _, e := range old.root.Entries {
					if int64(e.LogicalStart)*4096 == shift {
						target = e
						break
					}
				}
				require.NotEmpty(t, target.Object.Key)
				store.objects[target.Object.Key][target.Object.Offset] ^= 0x80
				bad, err := NewReader(store, built.Descriptor, 0)
				require.NoError(t, err)
				sentinel := bytes.Repeat([]byte{0xaa}, 4096)
				n, err := bad.ReadAt(sentinel, shift)
				require.Error(t, err)
				require.Zero(t, n)
				require.Equal(t, bytes.Repeat([]byte{0xaa}, 4096), sentinel)
			}
		})
	}
}

func TestDataRangeLayoutEncryptedRawAndCompressedUnits(t *testing.T) {
	const unit = CompressedDataRangeBytes
	disk := bytes.Repeat([]byte{0x51}, 4*unit)
	_, err := rand.New(rand.NewSource(29)).Read(disk[3*4096 : 3*4096+unit])
	require.NoError(t, err)
	plan, err := NewDataRangeLayout(int64(len(disk)), unit, []DataRangeSpan{{Start: 3 * 4096, End: 3*4096 + 2*unit}})
	require.NoError(t, err)
	plain := newBuildTestStore()
	built, err := BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(disk), int64(len(disk)), plain, BuildOptions{FormatVersion: 2}, plan)
	require.NoError(t, err)
	rawReader, err := NewReader(plain, built.Descriptor, 0)
	require.NoError(t, err)
	var raw, compressed int
	for _, e := range rawReader.root.Entries {
		if e.Object.Encoding == "" {
			raw++
		} else {
			compressed++
		}
	}
	require.Positive(t, raw)
	require.Positive(t, compressed)
	store := objectstore.EncryptingImmutable(objectstore.NewMemoryStore(t.Name()), objectstore.EncryptionConfig{Enabled: true, KeyEncryptor: diagnosticKeyWrapper{}, ChunkSize: 16 << 10}, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 32, MaxBytes: 1 << 20})
	for key, payload := range plain.objects {
		require.NoError(t, store.Put(key, bytes.NewReader(payload)))
	}
	r, err := NewReader(store, built.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, len(disk))
	_, err = r.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, disk, actual)
}
