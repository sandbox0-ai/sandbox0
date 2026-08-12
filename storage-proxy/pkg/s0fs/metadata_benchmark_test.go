package s0fs

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
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

func BenchmarkSQLiteReadDirPlus(b *testing.B) {
	store, err := newSQLiteMetadataStore(context.Background(), filepath.Join(b.TempDir(), "metadata.sqlite"), sqliteMetadataFixture(50_000), 8<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	const pageSize = uint32(128)
	const offsetRange = uint64(49_000)

	b.Run("legacy_n_plus_one", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			offset := uint64(iteration*int(pageSize)) % offsetRange
			page, _, ok := store.DirectoryPage(RootInode, offset, pageSize)
			if !ok || len(page) != int(pageSize) {
				b.Fatalf("DirectoryPage() = %d entries, ok %v", len(page), ok)
			}
			for _, entry := range page {
				if _, ok := store.Node(entry.Inode); !ok {
					b.Fatalf("Node(%d) missing", entry.Inode)
				}
			}
		}
		b.ReportMetric(float64(pageSize), "entries/op")
	})

	b.Run("batched_page", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			offset := uint64(iteration*int(pageSize)) % offsetRange
			page, _, ok := store.DirectoryPageWithNodes(RootInode, offset, pageSize)
			if !ok || len(page) != int(pageSize) {
				b.Fatalf("DirectoryPageWithNodes() = %d entries, ok %v", len(page), ok)
			}
		}
		b.ReportMetric(float64(pageSize), "entries/op")
	})
}

func BenchmarkSQLiteWALMutation(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		batched bool
	}{
		{name: "legacy_autocommit"},
		{name: "single_transaction", batched: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			store, err := newSQLiteMetadataStore(context.Background(), filepath.Join(b.TempDir(), "metadata.sqlite"), sqliteMetadataFixture(0), 8<<20)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			apply := func() error {
				store.PutNode(42, &Node{Inode: 42, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 4096})
				store.PutChild(RootInode, "file", 42)
				store.PutData(42, make([]byte, 4096))
				return store.Err()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if benchmark.batched {
					err = store.ApplyMutation(apply)
				} else {
					err = apply()
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEngineSmallFileLifecycle(b *testing.B) {
	for _, benchmark := range []struct {
		name     string
		parallel bool
	}{
		{name: "serial"},
		{name: "parallel", parallel: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			base := objectstore.NewMemoryStore("s0fs-small-file-bench")
			store := objectstore.Prefix(base, "sandboxvolumes/team-a/vol-small-file-bench/s0fs/")
			engine, err := Open(context.Background(), Config{
				VolumeID: "vol-small-file-bench", WALPath: filepath.Join(b.TempDir(), "engine.wal"),
				MetadataPath: filepath.Join(b.TempDir(), "metadata.sqlite"), ObjectStore: store, HeadStore: newMemoryHeadStore(),
			})
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()
			payload := make([]byte, 4096)
			var next atomic.Uint64
			operation := func() {
				index := next.Add(1)
				node, err := engine.CreateFile(RootInode, fmt.Sprintf("file-%09d", index), 0o644)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := engine.Write(node.Inode, 0, payload); err != nil {
					b.Fatal(err)
				}
				if err := engine.Fsync(node.Inode); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			if benchmark.parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						operation()
					}
				})
			} else {
				for iteration := 0; iteration < b.N; iteration++ {
					operation()
				}
			}
		})
	}
}
