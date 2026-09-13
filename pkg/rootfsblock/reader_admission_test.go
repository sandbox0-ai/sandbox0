package rootfsblock

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

type readerAdmissionSource struct {
	RangeSource
	pack    string
	started chan int64
	finish  chan struct{}
}

func (s *readerAdmissionSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if key == s.pack {
		s.started <- offset
		<-s.finish
	}
	return s.RangeSource.Get(key, offset, length)
}

func TestReaderBoundsDistinctSourceLoadsAfterCoalescing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ranges = maxConcurrentSourceReads + 1
		payload := make([]byte, ranges*LogicalBlockSize)
		for i := range ranges {
			copy(payload[i*LogicalBlockSize:], bytes.Repeat([]byte{byte(i + 1)}, LogicalBlockSize))
		}
		store := newBuildTestStore()
		built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), store, BuildOptions{DataRangeBytes: LogicalBlockSize})
		require.NoError(t, err)
		var pack string
		for _, ref := range built.References {
			if ref.Kind == ObjectKindDataPack {
				pack = ref.Key
			}
		}
		require.NotEmpty(t, pack)
		source := &readerAdmissionSource{RangeSource: store, pack: pack, started: make(chan int64, ranges), finish: make(chan struct{}, ranges)}
		reader, err := NewReader(source, built.Descriptor, DefaultReadCacheBytes)
		require.NoError(t, err)
		var readers sync.WaitGroup
		t.Cleanup(func() { close(source.finish); readers.Wait() })
		read := func(block int) {
			readers.Add(1)
			go func() {
				defer readers.Done()
				actual := make([]byte, 1)
				_, err := reader.ReadAt(actual, int64(block*LogicalBlockSize))
				require.NoError(t, err)
				require.Equal(t, []byte{byte(block + 1)}, actual)
			}()
		}
		// More coalesced readers than source slots must still consume just one.
		for range 32 {
			read(0)
		}
		synctest.Wait()
		require.Len(t, source.started, 1)
		for block := 1; block < ranges; block++ {
			read(block)
		}
		synctest.Wait()
		require.Len(t, source.started, maxConcurrentSourceReads)
		for range maxConcurrentSourceReads {
			<-source.started
		}
		source.finish <- struct{}{}
		synctest.Wait()
		require.Len(t, source.started, 1, "one completed source load admits one distinct waiting range")
		for range maxConcurrentSourceReads {
			source.finish <- struct{}{}
		}
		readers.Wait()
	})
}
