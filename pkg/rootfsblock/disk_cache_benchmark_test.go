package rootfsblock

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkNodeDiskCacheWorkingSet measures local read overhead and source
// traffic, not network latency or sandbox TTI. Three distinct seeded images
// overflow the same memory budget in both variants; only the disk tier differs.
func BenchmarkNodeDiskCacheWorkingSet(b *testing.B) {
	for _, tier := range []string{"memory", "memory-and-disk"} {
		b.Run(tier, func(b *testing.B) {
			store := newBuildTestStore()
			source := &cacheBenchmarkSource{RangeSource: store, data: make(map[string]bool)}
			descriptors := make([]Descriptor, 3)
			payloads := make([][]byte, len(descriptors))
			for image := range descriptors {
				payload := make([]byte, 512<<10)
				_, err := rand.New(rand.NewSource(int64(902 + image))).Read(payload)
				if err != nil {
					b.Fatal(err)
				}
				built, err := BuildMaterializedGeneration(b.Context(), bytes.NewReader(payload), int64(len(payload)), store,
					BuildOptions{FormatVersion: CompressedFormatVersion, PageEntries: 64})
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
			var config DiskCacheConfig
			if tier == "memory-and-disk" {
				config = testDiskConfig(b, 4<<20)
			}
			cache := openTestDiskCache(b, 128<<10, config)
			read := func() error {
				for image, descriptor := range descriptors {
					reader, err := NewReaderWithCache(source, descriptor, cache)
					if err != nil {
						return err
					}
					actual := make([]byte, LogicalBlockSize)
					for offset := 0; offset < len(payloads[image]); offset += len(actual) {
						if _, err := reader.ReadAt(actual, int64(offset)); err != nil {
							return err
						}
						if !bytes.Equal(actual, payloads[image][offset:offset+len(actual)]) {
							return fmt.Errorf("image %d differs at offset %d", image, offset)
						}
					}
				}
				return nil
			}
			if err := read(); err != nil {
				b.Fatal(err)
			}
			if err := cache.Close(); err != nil {
				b.Fatal(err)
			}
			cache = openTestDiskCache(b, 128<<10, config)
			source.calls.Store(0)
			source.bytes.Store(0)
			b.ReportAllocs()
			b.SetBytes(int64(len(payloads) * len(payloads[0])))
			b.ResetTimer()
			for range b.N {
				if err := read(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(source.calls.Load())/float64(b.N), "data-GETs/cycle")
			b.ReportMetric(float64(source.bytes.Load())/float64(b.N), "source-B/cycle")
			b.ReportMetric(float64(cache.Stats().DiskBytes), "disk-B")
		})
	}
}
