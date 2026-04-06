package storageproxy

import (
	"context"
	"encoding/base64"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

func setRootGroupWritable(t *testing.T, volCtx *volume.VolumeContext, gid int64) {
	t.Helper()

	rootAttr := &meta.Attr{Gid: uint32(gid), Mode: 0o775}
	if errno := volCtx.Meta.SetAttr(meta.Background(), meta.RootInode, meta.SetAttrGID|meta.SetAttrMode, 0, rootAttr); errno != 0 {
		t.Fatalf("SetAttr(root) errno = %v, want 0", syscall.Errno(errno))
	}
}

func TestVolumeSyncApplyUsesDefaultPosixIdentity(t *testing.T) {
	volCtx := newMountedIntegrationVolumeContext(t, "vol-1", "team-1")
	uid := int64(1234)
	gid := int64(2345)
	setRootGroupWritable(t, volCtx, gid)
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
