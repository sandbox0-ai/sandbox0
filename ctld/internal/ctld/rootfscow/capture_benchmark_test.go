package rootfscow

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func BenchmarkCaptureLargeRegularFileCold(b *testing.B) {
	const fileSize = 64 << 20
	root := b.TempDir()
	filePath := filepath.Join(root, "large-file")
	writeBenchmarkFile(b, filePath, fileSize)

	for _, chunkSize := range []int{1 << 20, 4 << 20} {
		chunkSize := chunkSize
		b.Run(byteSizeName(chunkSize), func(b *testing.B) {
			capture := newBenchmarkCapture(b, root, chunkSize)
			b.SetBytes(fileSize)
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				before := benchmarkFileStat(b, filePath)
				capture.ForgetFile(fileVersionFromStat(before))
				b.StartTimer()
				_, err := capture.captureFile(context.Background(), filePath, before)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCaptureUnchangedLargeFileManifestReuse(b *testing.B) {
	const fileSize = 64 << 20
	root := b.TempDir()
	filePath := filepath.Join(root, "unchanged-large-file")
	writeBenchmarkFile(b, filePath, fileSize)
	capture := newBenchmarkCapture(b, root, 1<<20)
	before := benchmarkFileStat(b, filePath)
	_, err := capture.captureFile(context.Background(), filePath, before)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := capture.captureFile(context.Background(), filePath, before)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCaptureAfterSmallRandomOverwrite(b *testing.B) {
	const fileSize = 64 << 20
	root := b.TempDir()
	filePath := filepath.Join(root, "random-overwrite")
	writeBenchmarkFile(b, filePath, fileSize)
	payload := make([]byte, 4096)

	for _, chunkSize := range []int{1 << 20, 4 << 20} {
		chunkSize := chunkSize
		b.Run(byteSizeName(chunkSize), func(b *testing.B) {
			capture := newBenchmarkCapture(b, root, chunkSize)
			file, err := os.OpenFile(filePath, os.O_WRONLY, 0)
			require.NoError(b, err)
			defer file.Close()
			b.SetBytes(fileSize)
			b.ReportAllocs()
			for index := range b.N {
				b.StopTimer()
				binary.LittleEndian.PutUint64(payload, uint64(index+1))
				offset := int64((index * (1 << 20)) % (fileSize - len(payload)))
				_, err := file.WriteAt(payload, offset)
				if err != nil {
					b.Fatal(err)
				}
				before := benchmarkFileStat(b, filePath)
				b.StartTimer()
				_, err = capture.captureFile(context.Background(), filePath, before)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCaptureSmallFileTree(b *testing.B) {
	const (
		fileCount = 1000
		fileSize  = 1024
	)
	root := b.TempDir()
	for index := range fileCount {
		directory := filepath.Join(root, fmt.Sprintf("dir-%02d", index/100))
		require.NoError(b, os.MkdirAll(directory, 0o755))
		payload := bytes.Repeat([]byte{byte(index), byte(index >> 8)}, fileSize/2)
		require.NoError(b, os.WriteFile(filepath.Join(directory, fmt.Sprintf("file-%04d", index)), payload, 0o644))
	}
	b.SetBytes(fileCount * fileSize)
	b.ReportAllocs()
	b.ReportMetric(fileCount, "files/op")
	b.ResetTimer()
	for range b.N {
		capture := newBenchmarkCapture(b, root, 1<<20)
		if err := capture.CaptureTree(context.Background()); err != nil {
			b.Fatal(err)
		}
		if _, err := capture.editor.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchmarkCapture(b *testing.B, root string, chunkSize int) *Capture {
	b.Helper()
	store := objectstore.NewMemoryStore(b.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "capture-benchmark-team")
	require.NoError(b, err)
	editor, err := NewEditor(store, writer, nil)
	require.NoError(b, err)
	capture, err := NewCapture(CaptureConfig{
		Root:         root,
		GenerationID: "capture-benchmark-generation",
		ChunkSize:    chunkSize,
		Editor:       editor,
		Writer:       writer,
	})
	require.NoError(b, err)
	return capture
}

func benchmarkFileStat(b *testing.B, path string) *syscall.Stat_t {
	b.Helper()
	info, err := os.Lstat(path)
	require.NoError(b, err)
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(b, ok)
	return stat
}

func writeBenchmarkFile(b *testing.B, path string, size int) {
	b.Helper()
	file, err := os.Create(path)
	require.NoError(b, err)
	block := make([]byte, 1<<20)
	for offset := 0; offset < size; offset += len(block) {
		binary.LittleEndian.PutUint64(block, uint64(offset))
		_, err := file.Write(block)
		require.NoError(b, err)
	}
	require.NoError(b, file.Close())
}

func byteSizeName(value int) string {
	if value%(1<<20) == 0 {
		return fmt.Sprintf("%dMiB", value/(1<<20))
	}
	return "custom"
}
