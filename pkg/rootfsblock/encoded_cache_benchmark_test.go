package rootfsblock

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

type cacheBenchmarkSource struct {
	RangeSource
	data  map[string]bool
	calls atomic.Int64
	bytes atomic.Int64
}

func (s *cacheBenchmarkSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if s.data[key] {
		s.calls.Add(1)
		s.bytes.Add(length)
	}
	return s.RangeSource.Get(key, offset, length)
}

// This is an in-memory range/CPU benchmark, not node-v, network timing or a
// sandbox startup measurement. Distinct seeded units prevent fake cache wins
// caused by a repeated constant payload or repeated content across images.
func BenchmarkEncodedCacheWorkingSet(b *testing.B) {
	for _, scenario := range []struct {
		name     string
		images   int
		imageMiB int
		random   int
	}{
		{"decoded-fits", 1, 8, 26 << 10},
		{"encoded-fits", 3, 8, 26 << 10},
		{"encoded-overflows", 3, 16, 26 << 10},
		{"incompressible", 3, 8, CompressedDataRangeBytes},
	} {
		for _, width := range []int{1, 18} {
			b.Run(fmt.Sprintf("%s/width%d", scenario.name, width), func(b *testing.B) {
				store := newBuildTestStore()
				source := &cacheBenchmarkSource{RangeSource: store, data: make(map[string]bool)}
				descriptors := make([]Descriptor, scenario.images)
				payloads := make([][]byte, scenario.images)
				for image := range descriptors {
					payload := make([]byte, scenario.imageMiB<<20)
					for offset := 0; offset < len(payload); offset += CompressedDataRangeBytes {
						unit := payload[offset : offset+CompressedDataRangeBytes]
						seed := int64(1000000*image + offset/CompressedDataRangeBytes)
						_, _ = rand.New(rand.NewSource(seed)).Read(unit[:scenario.random])
						for index := scenario.random; index < len(unit); index++ {
							unit[index] = unit[index%scenario.random]
						}
					}
					built, err := BuildMaterializedGeneration(b.Context(), bytes.NewReader(payload), int64(len(payload)), store,
						BuildOptions{FormatVersion: CompressedFormatVersion, PageEntries: 1024})
					if err != nil {
						b.Fatal(err)
					}
					for _, ref := range built.References {
						if ref.Kind == ObjectKindDataPack {
							source.data[ref.Key] = true
						}
					}
					descriptors[image], payloads[image] = built.Descriptor, payload
				}
				cache, err := NewReadCache(16 << 20)
				if err != nil {
					b.Fatal(err)
				}
				read := func(image int) error {
					reader, err := NewReaderWithCacheContext(b.Context(), source, descriptors[image], cache)
					if err != nil {
						return err
					}
					actual := make([]byte, bulkReadThreshold)
					for offset := 0; offset < len(payloads[image]); offset += len(actual) {
						_, err := reader.ReadAt(actual, int64(offset))
						if err != nil {
							return err
						}
						if !bytes.Equal(actual, payloads[image][offset:offset+len(actual)]) {
							return fmt.Errorf("readback mismatch")
						}
					}
					return nil
				}
				for image := range descriptors {
					if err := read(image); err != nil {
						b.Fatal(err)
					}
				}
				source.calls.Store(0)
				source.bytes.Store(0)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if width == 1 {
						for image := range descriptors {
							if err := read(image); err != nil {
								b.Fatal(err)
							}
						}
						continue
					}
					var wait sync.WaitGroup
					start := make(chan struct{})
					for lane := range width {
						wait.Add(1)
						go func() {
							defer wait.Done()
							<-start
							if err := read(lane % len(descriptors)); err != nil {
								b.Error(err)
							}
						}()
					}
					close(start)
					wait.Wait()
				}
				b.StopTimer()
				b.ReportMetric(float64(source.calls.Load())/float64(b.N), "data-GETs/cohort")
				b.ReportMetric(float64(source.bytes.Load())/float64(b.N), "source-B/cohort")
				b.ReportMetric(float64(cache.bytes), "cache-B")
			})
		}
	}
}
