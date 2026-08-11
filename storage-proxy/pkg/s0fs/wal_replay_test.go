package s0fs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestWALCheckpointRetainsConcurrentSuffix(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	w, err := openWAL(walPath, "vol-1", nil, nil, walReplayStats{})
	if err != nil {
		t.Fatalf("openWAL() error = %v", err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		appendPreparedWALRecord(t, w, walRecord{Seq: seq, Op: "chmod", Inode: RootInode, Mode: uint32(seq), TimeUnix: time.Now().UnixNano()})
	}
	checkpoint, err := w.checkpoint(2)
	if err != nil {
		t.Fatalf("checkpoint() error = %v", err)
	}
	appendPreparedWALRecord(t, w, walRecord{Seq: 3, Op: "chmod", Inode: RootInode, Mode: 3, TimeUnix: time.Now().UnixNano()})
	if err := w.discardThrough(checkpoint); err != nil {
		t.Fatalf("discardThrough() error = %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}

	replay, err := openWALReplay(walPath, "vol-1", nil)
	if err != nil {
		t.Fatalf("openWALReplay() error = %v", err)
	}
	defer replay.Close()

	first, ok, err := replay.Peek(context.Background())
	if err != nil || !ok || first.Seq != 3 {
		t.Fatalf("Peek() = seq %d, ok %v, err %v; want seq 3", first.Seq, ok, err)
	}
	if stats := replay.Stats(); stats.RecordsScanned != 1 {
		t.Fatalf("stats after Peek() = %+v, want one scanned record", stats)
	}

	var sequences []uint64
	for {
		record, ok, err := replay.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if !ok {
			break
		}
		sequences = append(sequences, record.Seq)
	}
	if got, want := sequences, []uint64{3}; !slices.Equal(got, want) {
		t.Fatalf("replayed sequences = %v, want %v", got, want)
	}
	stats := replay.Stats()
	if stats.RecordsScanned != 1 || stats.ActiveFirstSeq != 3 || stats.ActiveLastSeq != 3 {
		t.Fatalf("final replay stats = %+v", stats)
	}
}

func TestWALCheckpointRetainsEncryptedSuffix(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	encryption := testEncryptionConfig(16)
	w, err := openWAL(walPath, "vol-encrypted", encryption, nil, walReplayStats{})
	if err != nil {
		t.Fatalf("openWAL() error = %v", err)
	}
	prefix := walRecord{
		Seq:      1,
		Op:       "create",
		Inode:    RootInode + 1,
		Parent:   RootInode,
		Name:     "secret-name.txt",
		Type:     TypeFile,
		Mode:     0o600,
		TimeUnix: time.Now().UnixNano(),
	}
	appendPreparedWALRecord(t, w, prefix)
	checkpoint, err := w.checkpoint(prefix.Seq)
	if err != nil {
		t.Fatalf("checkpoint() error = %v", err)
	}
	suffix := prefix
	suffix.Seq = 2
	suffix.Name = "retained-secret.txt"
	appendPreparedWALRecord(t, w, suffix)
	if err := w.discardThrough(checkpoint); err != nil {
		t.Fatalf("discardThrough() error = %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}
	payload, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("ReadFile(WAL) error = %v", err)
	}
	if bytes.Contains(payload, []byte(suffix.Name)) {
		t.Fatal("rewritten encrypted WAL contains the plaintext record name")
	}

	replay, err := openWALReplay(walPath, "vol-encrypted", encryption)
	if err != nil {
		t.Fatalf("openWALReplay() error = %v", err)
	}
	defer replay.Close()
	got, ok, err := replay.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("Next() = ok %v, err %v", ok, err)
	}
	if got.Seq != suffix.Seq || got.Name != suffix.Name {
		t.Fatalf("decrypted record = %+v, want seq %d name %q", got, suffix.Seq, suffix.Name)
	}
}

func TestWALReplayStopsWhenContextIsCanceled(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "chmod", Inode: RootInode, Mode: 1, TimeUnix: time.Now().UnixNano()},
		walRecord{Seq: 2, Op: "chmod", Inode: RootInode, Mode: 2, TimeUnix: time.Now().UnixNano()},
	)
	replay, err := openWALReplay(walPath, "vol-1", nil)
	if err != nil {
		t.Fatalf("openWALReplay() error = %v", err)
	}
	defer replay.Close()

	if record, ok, err := replay.Next(context.Background()); err != nil || !ok || record.Seq != 1 {
		t.Fatalf("first Next() = seq %d, ok %v, err %v", record.Seq, ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := replay.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(canceled) error = %v, want context.Canceled", err)
	}
	if got := replay.Stats().RecordsScanned; got != 1 {
		t.Fatalf("records scanned after cancellation = %d, want 1", got)
	}
}

func TestReadBoundedWALLineRejectsOversizedRecord(t *testing.T) {
	reader := bufio.NewReaderSize(strings.NewReader("123456789\n"), 4)
	if _, _, _, err := readBoundedWALLineLimit(context.Background(), reader, 8); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("readBoundedWALLineLimit() error = %v, want ErrInvalidInput", err)
	}
}

func TestReadBoundedWALLineChecksCancellationBetweenChunks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := bufio.NewReaderSize(&cancelAfterRead{
		reader: strings.NewReader(strings.Repeat("x", 128<<10) + "\n"),
		cancel: cancel,
	}, 64<<10)
	_, _, bytesRead, err := readBoundedWALLine(ctx, reader)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("readBoundedWALLine() error = %v, want context.Canceled", err)
	}
	if bytesRead <= 0 || bytesRead > 64<<10 {
		t.Fatalf("bytes read before cancellation = %d, want at most one 64 KiB chunk", bytesRead)
	}
}

func TestOpenStopsBeforeReplayingPeekedRecordWhenCanceled(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "create", Inode: RootInode + 1, Parent: RootInode, Name: "first.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
		walRecord{Seq: 2, Op: "create", Inode: RootInode + 2, Parent: RootInode, Name: "second.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
	)

	ctx, cancel := context.WithCancel(context.Background())
	var completed OpenObservation
	_, err := Open(ctx, Config{
		VolumeID: "vol-1",
		WALPath:  walPath,
		OpenObserver: func(observation OpenObservation) {
			if observation.Phase == "state_load" && observation.Source == "local" {
				cancel()
			}
			if observation.Phase == "complete" {
				completed = observation
			}
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v, want context.Canceled", err)
	}
	if completed.WALRecords != 0 || completed.WALRecordsScanned != 1 {
		t.Fatalf("completed observation = %+v, want one peeked and zero applied records", completed)
	}
	if !errors.Is(completed.Err, context.Canceled) {
		t.Fatalf("completed observation error = %v, want context.Canceled", completed.Err)
	}

	retried, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(retry) error = %v", err)
	}
	defer retried.Close()
	for _, name := range []string{"first.txt", "second.txt"} {
		if _, err := retried.Lookup(RootInode, name); err != nil {
			t.Fatalf("Lookup(%s) after canceled retry error = %v", name, err)
		}
	}
}

func TestOpenSkipsWALRecordsOlderThanHead(t *testing.T) {
	walPath, inode := createHeadWithFile(t, "base")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "create", Inode: inode, Parent: RootInode, Name: "data.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
		walRecord{Seq: 2, Op: "write", Inode: inode, Offset: 0, Data: []byte("stale"), TimeUnix: time.Now().UnixNano()},
	)

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.Lookup(RootInode, "data.txt")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	data, err := engine.Read(node.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte("base")) {
		t.Fatalf("data = %q, want base", data)
	}
	if engine.dirty {
		t.Fatal("engine dirty after replaying only stale WAL records")
	}
}

func TestOpenAppliesWALSuffixNewerThanHead(t *testing.T) {
	walPath, inode := createHeadWithFile(t, "base")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "create", Inode: inode, Parent: RootInode, Name: "data.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
		walRecord{Seq: 2, Op: "write", Inode: inode, Offset: 0, Data: []byte("stale"), TimeUnix: time.Now().UnixNano()},
		walRecord{Seq: 3, Op: "write", Inode: inode, Offset: 4, Data: []byte("++"), TimeUnix: time.Now().UnixNano()},
	)

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.Lookup(RootInode, "data.txt")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	data, err := engine.Read(node.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte("base++")) {
		t.Fatalf("data = %q, want base++", data)
	}
	if !engine.dirty {
		t.Fatal("engine not dirty after applying WAL suffix")
	}
}

func TestOpenRejectsWALGapAfterHead(t *testing.T) {
	walPath, inode := createHeadWithFile(t, "base")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 4, Op: "write", Inode: inode, Offset: 4, Data: []byte("gap"), TimeUnix: time.Now().UnixNano()},
	)

	if _, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Open() error = %v, want ErrInvalidInput", err)
	}
}

func TestOpenRejectsCorruptWALRecord(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "create", Inode: RootInode + 1, Parent: RootInode, Name: "data.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
	)
	appendPlainWALTail(t, walPath, []byte("not-json\n"))
	if _, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath}); err == nil {
		t.Fatal("Open() accepted a corrupt complete WAL record")
	}
}

func TestOpenQuarantinesUnprovenWALAndUsesCommittedManifest(t *testing.T) {
	store, heads, inode := createCommittedWALBase(t, "vol-unproven-wal")
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "create", Inode: inode, Parent: RootInode, Name: "conflicting.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
	)

	engine, err := Open(context.Background(), Config{
		VolumeID:    "vol-unproven-wal",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	payload, err := engine.Read(inode, 0, 16)
	if err != nil || string(payload) != "base" {
		t.Fatalf("Read(committed manifest) = %q, %v; want base", payload, err)
	}
	if _, err := engine.Lookup(RootInode, "conflicting.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(untrusted WAL entry) error = %v, want ErrNotFound", err)
	}
	evidence, err := filepath.Glob(walPath + ".untrusted-*")
	if err != nil {
		t.Fatalf("Glob(untrusted evidence) error = %v", err)
	}
	if len(evidence) == 0 {
		t.Fatal("unproven WAL was not preserved as recovery evidence")
	}
}

func TestOpenUsesCommittedStateForProvenWALSuffix(t *testing.T) {
	const volumeID = "vol-proven-wal"
	store, heads, inode := createCommittedWALBase(t, volumeID)
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	head, err := heads.LoadCommittedHead(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("LoadCommittedHead() error = %v", err)
	}
	if err := saveRecoveryBinding(walBaseBindingPath(walPath), volumeID, head, ""); err != nil {
		t.Fatalf("saveRecoveryBinding() error = %v", err)
	}
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 3, Op: "write", Inode: inode, Offset: 4, Data: []byte("++"), TimeUnix: time.Now().UnixNano()},
	)

	engine, err := Open(context.Background(), Config{
		VolumeID:    volumeID,
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	payload, err := engine.Read(inode, 0, 16)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := string(payload); got != "base++" {
		t.Fatalf("Read() = %q, want base++", got)
	}
}

func TestOpenIgnoresPartialWALTail(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "create", Inode: RootInode + 1, Parent: RootInode, Name: "data.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
	)
	appendPlainWALTail(t, walPath, []byte(`{"seq":2,"op":"create"`))

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	if _, err := engine.Lookup(RootInode, "data.txt"); err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if _, err := engine.Lookup(RootInode, "partial.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(partial) error = %v, want ErrNotFound", err)
	}
}

func TestOpenTruncatesPartialWALTailBeforeAppending(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	appendPlainWALRecords(t, walPath,
		walRecord{Seq: 1, Op: "create", Inode: RootInode + 1, Parent: RootInode, Name: "first.txt", Type: TypeFile, Mode: 0o644, TimeUnix: time.Now().UnixNano()},
	)
	appendPlainWALTail(t, walPath, []byte(`{"seq":2,"op":"create"`))

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	if _, err := engine.CreateFile(RootInode, "second.txt", 0o644); err != nil {
		t.Fatalf("CreateFile(second) error = %v", err)
	}

	payload, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("ReadFile(WAL) error = %v", err)
	}
	lines := bytes.Split(bytes.TrimSpace(payload), []byte{'\n'})
	if len(lines) != 2 {
		t.Fatalf("complete WAL lines = %d, want 2; payload = %q", len(lines), payload)
	}
	for index, line := range lines {
		var record walRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode WAL line %d: %v", index, err)
		}
		if record.Seq != uint64(index+1) {
			t.Fatalf("WAL line %d sequence = %d, want %d", index, record.Seq, index+1)
		}
	}
}

func BenchmarkWALReplayStreaming(b *testing.B) {
	for _, recordCount := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("records_%d", recordCount), func(b *testing.B) {
			benchmarkWALReplayStreaming(b, recordCount)
		})
	}
}

func benchmarkWALReplayStreaming(b *testing.B, recordCount int) {
	walPath := filepath.Join(b.TempDir(), "volume.wal")
	file, err := os.Create(walPath)
	if err != nil {
		b.Fatal(err)
	}
	writer := bufio.NewWriterSize(file, 64<<10)
	encoder := json.NewEncoder(writer)
	for index := range recordCount {
		if err := encoder.Encode(walRecord{
			Seq:      uint64(index + 1),
			Op:       "chmod",
			Inode:    RootInode,
			Mode:     uint32(index),
			TimeUnix: int64(index + 1),
		}); err != nil {
			b.Fatal(err)
		}
	}
	if err := writer.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(walPath)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	peakHeapBytes := baseline.HeapAlloc
	b.ResetTimer()

	for range b.N {
		replay, err := openWALReplay(walPath, "vol-benchmark", nil)
		if err != nil {
			b.Fatal(err)
		}
		for scanned := 0; ; scanned++ {
			_, ok, err := replay.Next(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			if scanned%1024 == 0 {
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				if current.HeapAlloc > peakHeapBytes {
					peakHeapBytes = current.HeapAlloc
				}
			}
		}
		if err := replay.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if peakHeapBytes < baseline.HeapAlloc {
		peakHeapBytes = baseline.HeapAlloc
	}
	b.ReportMetric(float64(peakHeapBytes-baseline.HeapAlloc), "peak_heap_bytes/op")
}

func createHeadWithFile(t *testing.T, payload string) (string, uint64) {
	t.Helper()

	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte(payload)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return walPath, node.Inode
}

func createCommittedWALBase(t *testing.T, volumeID string) (*recordingStore, *memoryHeadStore, uint64) {
	t.Helper()
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, volumeID)
	heads := newMemoryHeadStore()
	engine, err := Open(ctx, Config{
		VolumeID:    volumeID,
		WALPath:     filepath.Join(t.TempDir(), "committed.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(committed base) error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(committed base) error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("base")); err != nil {
		t.Fatalf("Write(committed base) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(committed base) error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(committed base) error = %v", err)
	}
	return store, heads, node.Inode
}

func appendPlainWALRecords(t testing.TB, walPath string, records ...walRecord) {
	t.Helper()

	var payload []byte
	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshal WAL record: %v", err)
		}
		payload = append(payload, line...)
		payload = append(payload, '\n')
	}
	appendPlainWALTail(t, walPath, payload)
}

func appendPlainWALTail(t testing.TB, walPath string, payload []byte) {
	t.Helper()

	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer file.Close()
	if _, err := file.Write(payload); err != nil {
		t.Fatalf("append WAL payload: %v", err)
	}
}

func appendPreparedWALRecord(t testing.TB, w *wal, record walRecord) {
	t.Helper()
	payload, err := w.prepare(record)
	if err != nil {
		t.Fatalf("prepare() error = %v", err)
	}
	if err := w.appendPrepared(record, payload); err != nil {
		t.Fatalf("appendPrepared() error = %v", err)
	}
}

type cancelAfterRead struct {
	reader io.Reader
	cancel context.CancelFunc
	read   bool
}

func (r *cancelAfterRead) Read(payload []byte) (int, error) {
	n, err := r.reader.Read(payload)
	if !r.read {
		r.read = true
		r.cancel()
	}
	return n, err
}
