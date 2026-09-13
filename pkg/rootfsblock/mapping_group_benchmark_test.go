package rootfsblock

import (
	"fmt"
	"io"
	"testing"
)

type groupBenchSource struct {
	RangeSource
	pack    string
	started chan adaptiveGet
	finish  chan struct{}
}

func (s *groupBenchSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if key == s.pack {
		s.started <- adaptiveGet{offset, length}
		<-s.finish
	}
	return s.RangeSource.Get(key, offset, length)
}

// A deterministic source barrier makes both subset leaders overlap; no sleeps
// or simulated remote timings. Eight readers are not a production-width claim.
func BenchmarkMappingGroupParallelProbe(b *testing.B) {
	for _, encoded := range []bool{false, true} {
		store, d, children, pack := groupFixture(b, encoded, "parallel-allocation")
		for _, subset := range []bool{false, true} {
			b.Run(fmt.Sprintf("encoded-%t/subsets-%t/readers-8", encoded, subset), func(b *testing.B) {
				var totalCalls, totalBytes int64
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					source := &groupBenchSource{RangeSource: store, pack: pack, started: make(chan adaptiveGet, 8), finish: make(chan struct{})}
					cache, err := NewReadCache(DefaultReadCacheBytes)
					if err != nil {
						b.Fatal(err)
					}
					readers := make([]*Reader, 8)
					for i := range readers {
						readers[i], err = NewReaderWithCache(source, d, cache)
						if err != nil {
							b.Fatal(err)
						}
					}
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
					want := 4
					if subset {
						want = 2
					} else if groupCandidate() {
						want = 1
					}
					for range want {
						call := <-source.started
						totalCalls++
						totalBytes += call.length
					}
					close(source.finish)
					for range readers {
						if err := <-results; err != nil {
							b.Fatal(err)
						}
					}
					if len(source.started) != 0 {
						b.Fatal("unexpected extra source attempt")
					}
				}
				b.ReportMetric(float64(totalCalls)/float64(b.N), "source_calls/op")
				b.ReportMetric(float64(totalBytes)/float64(b.N), "source_bytes/op")
			})
		}
	}
}
