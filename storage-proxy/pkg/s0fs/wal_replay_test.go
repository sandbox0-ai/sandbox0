package s0fs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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

func appendPlainWALRecords(t *testing.T, walPath string, records ...walRecord) {
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

func appendPlainWALTail(t *testing.T, walPath string, payload []byte) {
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
