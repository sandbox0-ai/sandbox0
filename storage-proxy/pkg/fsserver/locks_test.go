package fsserver

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func TestFileLockManagerConflictUnlockAndRelease(t *testing.T) {
	locks := newFileLockManager()
	read := &pb.FileLock{Start: 0, End: 99, Typ: uint32(syscall.F_RDLCK), Pid: 10}
	if err := locks.set(context.Background(), "vol", 2, 11, 101, read, false); err != nil {
		t.Fatal(err)
	}
	if err := locks.set(context.Background(), "vol", 2, 12, 102, read, false); err != nil {
		t.Fatalf("second shared read lock error = %v", err)
	}
	write := &pb.FileLock{Start: 50, End: 60, Typ: uint32(syscall.F_WRLCK), Pid: 20}
	if err := locks.set(context.Background(), "vol", 2, 13, 103, write, false); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("conflicting write error = %v, want EAGAIN", err)
	}
	conflict := locks.get("vol", 2, 13, 103, write)
	if conflict.Typ != uint32(syscall.F_RDLCK) || conflict.Pid != 10 {
		t.Fatalf("GetLk conflict = %+v", conflict)
	}

	unlock := &pb.FileLock{Start: 0, End: 99, Typ: uint32(syscall.F_UNLCK)}
	if err := locks.set(context.Background(), "vol", 2, 11, 101, unlock, false); err != nil {
		t.Fatal(err)
	}
	if err := locks.set(context.Background(), "vol", 2, 12, 102, unlock, false); err != nil {
		t.Fatal(err)
	}
	if err := locks.set(context.Background(), "vol", 2, 13, 103, write, false); err != nil {
		t.Fatalf("write after unlock error = %v", err)
	}
	locks.releaseHandle("vol", 13)
	if got := locks.get("vol", 2, 14, 104, write); got.Typ != uint32(syscall.F_UNLCK) {
		t.Fatalf("lock after close = %+v, want unlocked", got)
	}
}

func TestFileLockManagerBlockingCancellation(t *testing.T) {
	locks := newFileLockManager()
	write := &pb.FileLock{Start: 0, End: 1, Typ: uint32(syscall.F_WRLCK)}
	if err := locks.set(context.Background(), "vol", 2, 1, 1, write, false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := locks.set(ctx, "vol", 2, 2, 2, write, true)
	if fserror.CodeOf(err) != fserror.Canceled {
		t.Fatalf("blocking canceled lock error = %v code = %v", err, fserror.CodeOf(err))
	}
}
