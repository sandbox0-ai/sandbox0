package rootfsblock

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

type groupStressSource struct {
	RangeSource
	finish chan struct{}
	mu     sync.Mutex
	calls  int
}

func (s *groupStressSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if strings.HasPrefix(key, "maps/group-stress/") {
		s.mu.Lock()
		s.calls++
		s.mu.Unlock()
		<-s.finish
	}
	return s.RangeSource.Get(key, offset, length)
}

func TestMappingGroupStressNodeWideLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newBuildTestStore()
		source := &groupStressSource{RangeSource: store, finish: make(chan struct{})}
		cache, err := NewReadCache(DefaultReadCacheBytes)
		require.NoError(t, err)
		var readers []*Reader
		for owner := range 64 {
			data := bytes.Repeat([]byte{byte(owner + 1)}, LogicalBlockSize)
			dataKey := fmt.Sprintf("packs/group-stress/%d", owner)
			require.NoError(t, store.PutImmutable(t.Context(), dataKey, data))
			var group []byte
			var children []MappingEntry
			pack := fmt.Sprintf("maps/group-stress/%d", owner)
			for i := range 2 {
				leaf := MappingPage{Version: CompressedFormatVersion, StartBlock: uint64(i), BlockCount: 1, Entries: []MappingEntry{{Kind: MappingEntryData, LogicalStart: uint64(i), BlockCount: 1, Object: ObjectRange{Key: dataKey, Length: LogicalBlockSize, Checksum: digest.FromBytes(data).String()}}}}
				raw, err := EncodeMappingPage(leaf)
				require.NoError(t, err)
				children = append(children, MappingEntry{Kind: MappingEntryChild, LogicalStart: uint64(i), BlockCount: 1, Object: ObjectRange{Key: pack, Offset: int64(len(group)), Length: int64(len(raw)), Checksum: digest.FromBytes(raw).String()}})
				group = append(group, raw...)
			}
			require.NoError(t, store.PutImmutable(t.Context(), pack, group))
			d := publishMappingPackRoot(t, store, children, 2)
			r, err := NewReaderWithCache(source, d, cache)
			require.NoError(t, err)
			readers = append(readers, r)
		}
		results := make(chan error, len(readers))
		for _, r := range readers {
			go func() { results <- groupRead(r, r.root, 0) }()
		}
		synctest.Wait()
		source.mu.Lock()
		calls := source.calls
		source.mu.Unlock()
		require.Equal(t, 8, calls)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 56, 56)
		close(source.finish)
		for range readers {
			require.NoError(t, <-results)
		}
		requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
		source.mu.Lock()
		calls = source.calls
		source.mu.Unlock()
		require.Equal(t, 64, calls)
		require.EqualValues(t, 128, cache.decodes.Load(), "64 roots and one demanded page per reader; optional pages stay unparsed")
	})
}
