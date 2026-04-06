package storageproxy

import (
	"context"
	"encoding/base64"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	storagegrpc "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/grpc"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

func TestVolumeSyncApplyUsesDefaultPosixIdentity(t *testing.T) {
	volCtx := newMountedIntegrationVolumeContext(t, "vol-1", "team-1")
	uid := int64(1234)
	gid := int64(2345)
	applier := volsync.NewVolumeChangeApplier(&integrationMountedVolumeManager{
		volumes: map[string]*volume.VolumeContext{"vol-1": volCtx},
	}, logrus.New())
	content := base64.StdEncoding.EncodeToString([]byte("package main\n"))

	err := applier.ApplyChange(context.Background(), &db.SandboxVolume{
		ID:              "vol-1",
		TeamID:          "team-1",
		UserID:          "user-1",
		DefaultPosixUID: &uid,
		DefaultPosixGID: &gid,
		AccessMode:      string(volume.AccessModeRWX),
	}, volsync.ChangeRequest{
		EventType:     db.SyncEventCreate,
		Path:          "/sync/main.go",
		EntryKind:     "file",
		ContentBase64: &content,
	})
	if err != nil {
		t.Fatalf("ApplyChange() error = %v", err)
	}

	var dirIno meta.Ino
	var dirAttr meta.Attr
	if errno := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "sync", &dirIno, &dirAttr, false); errno != 0 {
		t.Fatalf("Lookup(sync) errno = %v, want 0", syscall.Errno(errno))
	}
	var inode meta.Ino
	var attr meta.Attr
	if errno := volCtx.Meta.Lookup(meta.Background(), dirIno, "main.go", &inode, &attr, false); errno != 0 {
		t.Fatalf("Lookup(main.go) errno = %v, want 0", syscall.Errno(errno))
	}
	if attr.Uid != uint32(uid) {
		t.Fatalf("attr.Uid = %d, want %d", attr.Uid, uid)
	}
	if attr.Gid != uint32(gid) {
		t.Fatalf("attr.Gid = %d, want %d", attr.Gid, gid)
	}
	if got := string(readMountedFile(t, volCtx, "/sync/main.go")); got != "package main\n" {
		t.Fatalf("file content = %q, want %q", got, "package main\n")
	}

	rootAttr := &meta.Attr{}
	if errno := volCtx.Meta.GetAttr(meta.Background(), meta.RootInode, rootAttr); errno != 0 {
		t.Fatalf("GetAttr(root) errno = %v, want 0", syscall.Errno(errno))
	}
	if rootAttr.Uid != uint32(uid) || rootAttr.Gid != uint32(gid) {
		t.Fatalf("root owner = %d:%d, want %d:%d", rootAttr.Uid, rootAttr.Gid, uid, gid)
	}
}

func TestVolumeSyncApplyRequiresDefaultPosixIdentity(t *testing.T) {
	volCtx := newMountedIntegrationVolumeContext(t, "vol-1", "team-1")
	applier := volsync.NewVolumeChangeApplier(&integrationMountedVolumeManager{
		volumes: map[string]*volume.VolumeContext{"vol-1": volCtx},
	}, logrus.New())
	content := base64.StdEncoding.EncodeToString([]byte("hello"))

	err := applier.ApplyChange(context.Background(), &db.SandboxVolume{
		ID:         "vol-1",
		TeamID:     "team-1",
		UserID:     "user-1",
		AccessMode: string(volume.AccessModeRWX),
	}, volsync.ChangeRequest{
		EventType:     db.SyncEventCreate,
		Path:          "/sync/main.go",
		EntryKind:     "file",
		ContentBase64: &content,
	})
	if err == nil {
		t.Fatalf("ApplyChange() error = nil, want default posix identity error")
	}
}

func TestFileSystemCreateUsesActorToLazilyInitializeRoot(t *testing.T) {
	volCtx := newMountedIntegrationVolumeContext(t, "vol-1", "team-1")
	fsServer := storagegrpc.NewFileSystemServer(&integrationMountedVolumeManager{
		volumes: map[string]*volume.VolumeContext{"vol-1": volCtx},
	}, nil, nil, nil, logrus.New(), nil, nil)
	actor := &pb.PosixActor{Pid: 4321, Uid: 1234, Gids: []uint32{2345}}
	ctx := internalauth.WithClaims(context.Background(), &internalauth.Claims{TeamID: "team-1", SandboxID: "sb-1"})

	if _, err := fsServer.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(meta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Actor:    actor,
	}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	rootAttr := &meta.Attr{}
	if errno := volCtx.Meta.GetAttr(meta.Background(), meta.RootInode, rootAttr); errno != 0 {
		t.Fatalf("GetAttr(root) errno = %v, want 0", syscall.Errno(errno))
	}
	if rootAttr.Uid != actor.Uid || rootAttr.Gid != actor.Gids[0] {
		t.Fatalf("root owner = %d:%d, want %d:%d", rootAttr.Uid, rootAttr.Gid, actor.Uid, actor.Gids[0])
	}

	var inode meta.Ino
	var attr meta.Attr
	if errno := volCtx.Meta.Lookup(meta.Background(), meta.RootInode, "hello.txt", &inode, &attr, false); errno != 0 {
		t.Fatalf("Lookup(hello.txt) errno = %v, want 0", syscall.Errno(errno))
	}
	if attr.Uid != actor.Uid || attr.Gid != actor.Gids[0] {
		t.Fatalf("file owner = %d:%d, want %d:%d", attr.Uid, attr.Gid, actor.Uid, actor.Gids[0])
	}
}
