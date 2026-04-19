package portal

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func TestLocalSessionReadIntoUsesMountedS0FS(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	})
	session := newLocalSession("vol-1", mgr, nil)

	buf := bytes.Repeat([]byte{0xff}, 8)
	n, eof, err := session.ReadInto(context.Background(), &pb.ReadRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Offset:   1,
		Size:     3,
	}, buf)
	if err != nil {
		t.Fatalf("ReadInto() error = %v", err)
	}
	if n != 3 {
		t.Fatalf("ReadInto() n = %d, want 3", n)
	}
	if eof {
		t.Fatal("ReadInto() eof = true, want false")
	}
	if !bytes.Equal(buf[:3], []byte("bcd")) {
		t.Fatalf("ReadInto() data = %q, want bcd", buf[:3])
	}
	if !bytes.Equal(buf[3:], bytes.Repeat([]byte{0xff}, 5)) {
		t.Fatalf("ReadInto() modified bytes past requested size: %#v", buf)
	}
}

func TestLocalSessionOpenUsesMountedS0FS(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	}
	mgr.add(volCtx)
	session := newLocalSession("vol-1", mgr, nil)

	resp, err := session.Open(context.Background(), &pb.OpenRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if resp.HandleId == 0 {
		t.Fatal("Open() handle = 0, want non-zero")
	}
	if got, ok := volCtx.HandleInode(resp.HandleId); !ok || got != node.Inode {
		t.Fatalf("HandleInode() = %d, %v; want %d, true", got, ok, node.Inode)
	}
	if len(session.readOnlyHandles) != 1 {
		t.Fatalf("read-only handle count = %d, want 1", len(session.readOnlyHandles))
	}
	if err := engine.Unlink(s0fs.RootInode, "data.txt"); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if _, err := session.Release(context.Background(), &pb.ReleaseRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		HandleId: resp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if _, ok := volCtx.HandleInode(resp.HandleId); ok {
		t.Fatal("HandleInode() still tracks read-only handle after Release")
	}
	if len(session.readOnlyHandles) != 0 {
		t.Fatalf("read-only handle count after Release = %d, want 0", len(session.readOnlyHandles))
	}
	if _, err := engine.GetAttr(node.Inode); !errors.Is(err, s0fs.ErrNotFound) {
		t.Fatalf("GetAttr() after unlinked read-only Release err = %v, want ErrNotFound", err)
	}
}

func TestLocalSessionOpenUsesFSServerPermissions(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "private.txt", 0o600)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if err := engine.SetOwner(node.Inode, 0, 0); err != nil {
		t.Fatalf("SetOwner() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	})
	session := newLocalSession("vol-1", mgr, nil)

	_, err = session.Open(context.Background(), &pb.OpenRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Actor:    &pb.PosixActor{Uid: 1000, Gids: []uint32{1000}},
	})
	if fserror.CodeOf(err) != fserror.PermissionDenied {
		t.Fatalf("Open() code = %v, want %v (err=%v)", fserror.CodeOf(err), fserror.PermissionDenied, err)
	}
	if len(session.readOnlyHandles) != 0 {
		t.Fatalf("read-only handle count = %d, want 0 after denied open", len(session.readOnlyHandles))
	}
}
