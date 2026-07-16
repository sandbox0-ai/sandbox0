package portal

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestPersistProcessLocalS0FSHandleStateReplacesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles.json")
	initial := volume.HandleState{
		NextHandleID: 1,
		FileHandles:  map[uint64]uint64{1: 10},
	}
	if err := persistS0FSHandleState(path, "vol-1", initial); err != nil {
		t.Fatalf("persistS0FSHandleState() error = %v", err)
	}

	want := volume.HandleState{
		NextHandleID:  3,
		FileHandles:   map[uint64]uint64{2: 20},
		DirHandles:    map[uint64]uint64{3: 30},
		UnlinkedFiles: []uint64{20},
	}
	if err := persistProcessLocalS0FSHandleState(path, "vol-1", want); err != nil {
		t.Fatalf("persistProcessLocalS0FSHandleState() error = %v", err)
	}

	got, err := loadS0FSHandleState(path, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loadS0FSHandleState() = %#v, want %#v", got, want)
	}
}

func BenchmarkPersistS0FSHandleState(b *testing.B) {
	handles := volume.HandleState{
		NextHandleID: 64,
		FileHandles:  make(map[uint64]uint64, 32),
		DirHandles:   make(map[uint64]uint64, 32),
	}
	for handle := uint64(1); handle <= 32; handle++ {
		handles.FileHandles[handle] = 1000 + handle
		handles.DirHandles[handle+32] = 2000 + handle
	}

	benchmarks := []struct {
		name    string
		persist func(string, string, volume.HandleState) error
	}{
		{name: "Durable", persist: persistS0FSHandleState},
		{name: "ProcessLocal", persist: persistProcessLocalS0FSHandleState},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "handles.json")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := benchmark.persist(path, "vol-1", handles); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
