package storageproxy

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
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

	dirNode, err := volCtx.S0FS.Lookup(uint64(fsmeta.RootInode), "sync")
	if err != nil {
		t.Fatalf("Lookup(sync) error = %v", err)
	}
	attr, err := volCtx.S0FS.Lookup(dirNode.Inode, "main.go")
	if err != nil {
		t.Fatalf("Lookup(main.go) error = %v", err)
	}
	if attr.UID != uint32(uid) {
		t.Fatalf("attr.Uid = %d, want %d", attr.UID, uid)
	}
	if attr.GID != uint32(gid) {
		t.Fatalf("attr.Gid = %d, want %d", attr.GID, gid)
	}
	if got := string(readMountedFile(t, volCtx, "/sync/main.go")); got != "package main\n" {
		t.Fatalf("file content = %q, want %q", got, "package main\n")
	}

	rootAttr, err := volCtx.S0FS.GetAttr(uint64(fsmeta.RootInode))
	if err != nil {
		t.Fatalf("GetAttr(root) error = %v", err)
	}
	if rootAttr.UID != uint32(uid) || rootAttr.GID != uint32(gid) {
		t.Fatalf("root owner = %d:%d, want %d:%d", rootAttr.UID, rootAttr.GID, uid, gid)
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
		Parent:   uint64(fsmeta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
		Actor:    actor,
	}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	rootAttr, err := volCtx.S0FS.GetAttr(uint64(fsmeta.RootInode))
	if err != nil {
		t.Fatalf("GetAttr(root) error = %v", err)
	}
	if rootAttr.UID != actor.Uid || rootAttr.GID != actor.Gids[0] {
		t.Fatalf("root owner = %d:%d, want %d:%d", rootAttr.UID, rootAttr.GID, actor.Uid, actor.Gids[0])
	}
	attr, err := volCtx.S0FS.Lookup(uint64(fsmeta.RootInode), "hello.txt")
	if err != nil {
		t.Fatalf("Lookup(hello.txt) error = %v", err)
	}
	if attr.UID != actor.Uid || attr.GID != actor.Gids[0] {
		t.Fatalf("file owner = %d:%d, want %d:%d", attr.UID, attr.GID, actor.Uid, actor.Gids[0])
	}
}
