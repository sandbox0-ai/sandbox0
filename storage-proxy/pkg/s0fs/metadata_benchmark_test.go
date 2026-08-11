package s0fs

import (
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkOpenLargeNamespaceMetadata(b *testing.B) {
	state := sqliteMetadataFixture(50_000)
	benchmarks := []struct {
		name       string
		input      int
		configured int
		encryption *EncryptionConfig
	}{
		{name: "encrypted_v1_migration", input: StateFormatV1, configured: StateFormatV2, encryption: testEncryptionConfig(64 << 10)},
		{name: "encrypted_v2_paged", input: StateFormatV2, configured: StateFormatV2, encryption: testEncryptionConfig(64 << 10)},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			var peakDelta uint64
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				dir := b.TempDir()
				cfg := Config{
					VolumeID: "vol-large-namespace", WALPath: filepath.Join(dir, "engine.wal"),
					MetadataPath: filepath.Join(dir, "metadata.sqlite"), StateFormatVersion: benchmark.configured,
					Encryption: benchmark.encryption,
				}
				if err := saveSnapshotState(headStatePath(cfg.WALPath), cfg.VolumeID, "head", state, benchmark.encryption, benchmark.input); err != nil {
					b.Fatal(err)
				}
				runtime.GC()
				var baseline runtime.MemStats
				runtime.ReadMemStats(&baseline)
				var peak atomic.Uint64
				peak.Store(baseline.Alloc)
				done := make(chan struct{})
				var sampler sync.WaitGroup
				sampler.Add(1)
				go func() {
					defer sampler.Done()
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							var stats runtime.MemStats
							runtime.ReadMemStats(&stats)
							for observed := peak.Load(); stats.Alloc > observed && !peak.CompareAndSwap(observed, stats.Alloc); observed = peak.Load() {
							}
						case <-done:
							return
						}
					}
				}()
				b.StartTimer()
				engine, err := Open(context.Background(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(engine.EstimatedMemoryBytes()), "steady_metadata_bytes")
				b.StopTimer()
				close(done)
				sampler.Wait()
				if observed := peak.Load(); observed > baseline.Alloc && observed-baseline.Alloc > peakDelta {
					peakDelta = observed - baseline.Alloc
				}
				if err := engine.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(peakDelta), "peak_heap_delta_bytes")
		})
	}
}
