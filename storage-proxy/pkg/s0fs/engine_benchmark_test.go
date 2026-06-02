package s0fs

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkEngineRuntimeSequentialWrite4KiB(b *testing.B) {
	engine, err := Open(context.Background(), Config{
		VolumeID: "bench-rootfs",
		WALPath:  filepath.Join(b.TempDir(), "volume.wal"),
	})
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	node, err := engine.CreateFile(RootInode, "runtime.dat", 0o644)
	if err != nil {
		b.Fatalf("CreateFile() error = %v", err)
	}
	payload := bytes.Repeat([]byte("x"), 4*1024)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.Write(node.Inode, uint64(i*len(payload)), payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineLifecycleCreateWriteFsync4KiB(b *testing.B) {
	engine, err := Open(context.Background(), Config{
		VolumeID: "bench-rootfs",
		WALPath:  filepath.Join(b.TempDir(), "volume.wal"),
	})
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	payload := bytes.Repeat([]byte("x"), 4*1024)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node, err := engine.CreateFile(RootInode, fmt.Sprintf("file-%08d.dat", i), 0o644)
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
}
