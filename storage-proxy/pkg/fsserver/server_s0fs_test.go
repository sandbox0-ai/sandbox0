package fsserver

import (
	"bytes"
	"context"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/router"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"google.golang.org/protobuf/proto"
)

func TestS0FSFileLifecycle(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "hello.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if createResp.HandleId == 0 {
		t.Fatal("Create() returned empty handle")
	}

	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("hello"),
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: "vol-1",
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}

	readResp, err := server.Read(ctx, &pb.ReadRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Size:     16,
		HandleId: createResp.HandleId,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(readResp.Data, []byte("hello")) {
		t.Fatalf("Read() data = %q, want hello", readResp.Data)
	}

	if _, err := server.Rename(ctx, &pb.RenameRequest{
		VolumeId:  "vol-1",
		OldParent: 1,
		OldName:   "hello.txt",
		NewParent: 1,
		NewName:   "renamed.txt",
	}); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}

	if _, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "renamed.txt",
	}); err != nil {
		t.Fatalf("Lookup(renamed) error = %v", err)
	}

	if _, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "renamed.txt",
	}); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}

	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestS0FSOpenTruncatesExistingFile(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "truncate.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("abcdef"),
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Write(initial) error = %v", err)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release(initial) error = %v", err)
	}

	openResp, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	})
	if err != nil {
		t.Fatalf("Open(O_TRUNC) error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("xy"),
		HandleId: openResp.HandleId,
	}); err != nil {
		t.Fatalf("Write(after truncate) error = %v", err)
	}

	readResp, err := server.Read(ctx, &pb.ReadRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Size:     16,
		HandleId: openResp.HandleId,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(readResp.Data) != "xy" {
		t.Fatalf("Read() data = %q, want xy", readResp.Data)
	}
	attrResp, err := server.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: "vol-1", Inode: createResp.Inode})
	if err != nil {
		t.Fatalf("GetAttr() error = %v", err)
	}
	if attrResp.Size != 2 {
		t.Fatalf("GetAttr() size = %d, want 2", attrResp.Size)
	}
}

func TestS0FSFlushSyncsDirtyWrites(t *testing.T) {
	t.Parallel()

	volCtx, syncs := newMountedS0FSVolumeContextWithSyncCounter(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "flush.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("hello"),
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("sync count after Write() = %d, want 0", got)
	}
	if _, err := server.Flush(ctx, &pb.FlushRequest{
		VolumeId: "vol-1",
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("sync count after Flush() = %d, want 1", got)
	}
	if _, err := server.Flush(ctx, &pb.FlushRequest{
		VolumeId: "vol-1",
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("second Flush() error = %v", err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("sync count after second Flush() = %d, want 1", got)
	}
}

func TestS0FSReleaseSyncsDirtyWritesWithoutExplicitFsync(t *testing.T) {
	t.Parallel()

	volCtx, syncs := newMountedS0FSVolumeContextWithSyncCounter(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "release.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("hello"),
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("sync count after Write() = %d, want 0", got)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("sync count after Release() = %d, want 1", got)
	}
}

func TestS0FSWatchEventsIncludePaths(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	broadcaster := &recordingBroadcaster{}
	server := NewFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil, broadcaster, nil, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "hello.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("hello"),
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	var sawCreate, sawWrite bool
	for _, event := range broadcaster.events {
		switch event.EventType {
		case pb.WatchEventType_WATCH_EVENT_TYPE_CREATE:
			if event.Path == "/hello.txt" {
				sawCreate = true
			}
		case pb.WatchEventType_WATCH_EVENT_TYPE_WRITE:
			if event.Path == "/hello.txt" {
				sawWrite = true
			}
		}
	}
	if !sawCreate || !sawWrite {
		t.Fatalf("events = %+v, want create and write for /hello.txt", broadcaster.events)
	}
}

func TestS0FSDirectoryAndSetAttr(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	dirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "dir",
		Mode:     0o755,
	})
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	fileResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   dirResp.Inode,
		Name:     "child.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create(child) error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    fileResp.Inode,
		Offset:   0,
		Data:     []byte("abcdef"),
		HandleId: fileResp.HandleId,
	}); err != nil {
		t.Fatalf("Write(child) error = %v", err)
	}

	listResp, err := server.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: "vol-1",
		Inode:    dirResp.Inode,
	})
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(listResp.Entries) != 1 || listResp.Entries[0].Name != "child.txt" {
		t.Fatalf("ReadDir() entries = %+v", listResp.Entries)
	}

	setResp, err := server.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: "vol-1",
		Inode:    fileResp.Inode,
		Valid:    uint32(fsmeta.SetAttrMode | fsmeta.SetAttrUID | fsmeta.SetAttrGID | fsmeta.SetAttrSize),
		Attr: &pb.GetAttrResponse{
			Mode: 0o600,
			Uid:  1000,
			Gid:  1001,
			Size: 3,
		},
	})
	if err != nil {
		t.Fatalf("SetAttr() error = %v", err)
	}
	if setResp.Attr.Mode&0o7777 != 0o600 || setResp.Attr.Uid != 1000 || setResp.Attr.Gid != 1001 || setResp.Attr.Size != 3 {
		t.Fatalf("SetAttr() attr = %+v", setResp.Attr)
	}

	if _, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: "vol-1",
		Parent:   dirResp.Inode,
		Name:     "child.txt",
	}); err != nil {
		t.Fatalf("Unlink(child) error = %v", err)
	}
	if _, err := server.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "dir",
	}); err != nil {
		t.Fatalf("Rmdir() error = %v", err)
	}
}

func TestS0FSReadDirHonorsOffset(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if _, err := server.Create(ctx, &pb.CreateRequest{
			VolumeId: "vol-1",
			Parent:   1,
			Name:     name,
			Mode:     0o644,
		}); err != nil {
			t.Fatalf("Create(%s) error = %v", name, err)
		}
	}

	first, err := server.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: "vol-1",
		Inode:    1,
		Offset:   0,
	})
	if err != nil {
		t.Fatalf("ReadDir(offset 0) error = %v", err)
	}
	assertEntryNames(t, first.Entries, []string{"a.txt", "b.txt", "c.txt"})
	if first.Entries[0].Offset != 1 || first.Entries[1].Offset != 2 || first.Entries[2].Offset != 3 {
		t.Fatalf("ReadDir(offset 0) offsets = [%d %d %d], want [1 2 3]", first.Entries[0].Offset, first.Entries[1].Offset, first.Entries[2].Offset)
	}

	second, err := server.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: "vol-1",
		Inode:    1,
		Offset:   1,
	})
	if err != nil {
		t.Fatalf("ReadDir(offset 1) error = %v", err)
	}
	assertEntryNames(t, second.Entries, []string{"b.txt", "c.txt"})

	done, err := server.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: "vol-1",
		Inode:    1,
		Offset:   3,
	})
	if err != nil {
		t.Fatalf("ReadDir(offset 3) error = %v", err)
	}
	if len(done.Entries) != 0 {
		t.Fatalf("ReadDir(offset 3) entries = %+v, want empty", done.Entries)
	}
}

func TestS0FSUnlinkAfterOpenUntilRelease(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "temp.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("payload"),
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "temp.txt",
	}); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if _, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "temp.txt",
	}); err == nil {
		t.Fatal("Lookup() after unlink returned nil error")
	}
	readResp, err := server.Read(ctx, &pb.ReadRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Size:     16,
		HandleId: createResp.HandleId,
	})
	if err != nil {
		t.Fatalf("Read() on unlinked open file error = %v", err)
	}
	if !bytes.Equal(readResp.Data, []byte("payload")) {
		t.Fatalf("Read() on unlinked open file = %q, want payload", readResp.Data)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if _, err := volCtx.S0FS.GetAttr(createResp.Inode); err == nil {
		t.Fatal("GetAttr() after final release returned nil error")
	}
}

func TestS0FSUnlinkWithoutOpenForgetsFile(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "temp.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if _, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "temp.txt",
	}); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if _, err := volCtx.S0FS.GetAttr(createResp.Inode); err == nil {
		t.Fatal("GetAttr() after unlink without open handle returned nil error")
	}
}

func TestS0FSOpenAndAccessCheckPermissions(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	node, err := volCtx.S0FS.CreateFile(1, "private.txt", 0o640)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if err := volCtx.S0FS.SetOwner(node.Inode, 1000, 2000); err != nil {
		t.Fatalf("SetOwner() error = %v", err)
	}

	owner := &pb.PosixActor{Uid: 1000, Gids: []uint32{2000}}
	groupReader := &pb.PosixActor{Uid: 1001, Gids: []uint32{2000}}
	other := &pb.PosixActor{Uid: 1001, Gids: []uint32{1001}}

	resp, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: "vol-1",
		Inode:    node.Inode,
		Flags:    uint32(syscall.O_RDONLY),
		Actor:    owner,
	})
	if err != nil {
		t.Fatalf("Open(owner read) error = %v", err)
	}
	if _, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    node.Inode,
		HandleId: resp.HandleId,
	}); err != nil {
		t.Fatalf("Release(owner read) error = %v", err)
	}

	if _, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: "vol-1",
		Inode:    node.Inode,
		Flags:    uint32(syscall.O_RDONLY),
		Actor:    groupReader,
	}); err != nil {
		t.Fatalf("Open(group read) error = %v", err)
	}
	if _, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: "vol-1",
		Inode:    node.Inode,
		Flags:    uint32(syscall.O_WRONLY),
		Actor:    groupReader,
	}); fserror.CodeOf(err) != fserror.PermissionDenied {
		t.Fatalf("Open(group write) code = %v, want %v (err=%v)", fserror.CodeOf(err), fserror.PermissionDenied, err)
	}
	if _, err := server.Access(ctx, &pb.AccessRequest{
		VolumeId: "vol-1",
		Inode:    node.Inode,
		Mask:     4,
		Actor:    other,
	}); fserror.CodeOf(err) != fserror.PermissionDenied {
		t.Fatalf("Access(other read) code = %v, want %v (err=%v)", fserror.CodeOf(err), fserror.PermissionDenied, err)
	}
	if _, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: "vol-1",
		Inode:    node.Inode,
		Flags:    uint32(syscall.O_WRONLY),
		Actor:    &pb.PosixActor{Uid: 0, Gids: []uint32{0}},
	}); err != nil {
		t.Fatalf("Open(root write) error = %v", err)
	}
}

func TestS0FSMutationRedirectsWhenRemotePrimary(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	volumeRouter := router.NewVolumeRouter()
	volumeRouter.SetRoute(router.Route{
		VolumeID:      "vol-1",
		PrimaryNodeID: "node-b",
		PrimaryAddr:   "10.0.0.2:8080",
		Epoch:         9,
		LocalPrimary:  false,
	})
	server.SetVolumeRouter(volumeRouter)

	_, err := server.Create(authContext("team-a", ""), &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "blocked.txt",
		Mode:     0o644,
	})
	if got := fserror.CodeOf(err); got != fserror.FailedPrecondition {
		t.Fatalf("Create() code = %v, want %v (err=%v)", got, fserror.FailedPrecondition, err)
	}
}

func TestS0FSLinkCreatesHardLink(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	ctx := authContext("team-a", "")

	createResp, err := server.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "source.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Offset:   0,
		Data:     []byte("payload"),
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	linkResp, err := server.Link(ctx, &pb.LinkRequest{
		VolumeId:  "vol-1",
		Inode:     createResp.Inode,
		NewParent: 1,
		NewName:   "linked.txt",
	})
	if err != nil {
		t.Fatalf("Link() error = %v", err)
	}
	if linkResp.Inode != createResp.Inode {
		t.Fatalf("Link() inode = %d, want source inode %d", linkResp.Inode, createResp.Inode)
	}

	lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId: "vol-1",
		Parent:   1,
		Name:     "linked.txt",
	})
	if err != nil {
		t.Fatalf("Lookup(linked) error = %v", err)
	}
	if lookupResp.Inode != createResp.Inode {
		t.Fatalf("Lookup(linked) inode = %d, want source inode %d", lookupResp.Inode, createResp.Inode)
	}
	if lookupResp.Attr == nil || lookupResp.Attr.Nlink != 2 {
		t.Fatalf("Lookup(linked) nlink = %#v, want 2", lookupResp.Attr)
	}

	readResp, err := server.Read(ctx, &pb.ReadRequest{
		VolumeId: "vol-1",
		Inode:    lookupResp.Inode,
		Offset:   0,
		Size:     32,
	})
	if err != nil {
		t.Fatalf("Read(linked) error = %v", err)
	}
	if string(readResp.Data) != "payload" {
		t.Fatalf("Read(linked) data = %q, want payload", readResp.Data)
	}
}

func newMountedS0FSVolumeContext(t *testing.T, volumeID, teamID string) *volume.VolumeContext {
	t.Helper()

	volCtx, _ := newMountedS0FSVolumeContextWithSyncCounter(t, volumeID, teamID)
	return volCtx
}

type walSyncCounter struct {
	count atomic.Int64
}

func (c *walSyncCounter) Hook() {
	c.count.Add(1)
}

func (c *walSyncCounter) Load() int64 {
	return c.count.Load()
}

func newMountedS0FSVolumeContextWithSyncCounter(t *testing.T, volumeID, teamID string) (*volume.VolumeContext, *walSyncCounter) {
	t.Helper()

	counter := &walSyncCounter{}
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:    volumeID,
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		WALSyncHook: counter.Hook,
	})
	if err != nil {
		t.Fatalf("open s0fs engine: %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})

	return &volume.VolumeContext{
		VolumeID:  volumeID,
		TeamID:    teamID,
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		MountedAt: time.Now(),
		RootInode: 1,
		RootPath:  "/",
	}, counter
}

type recordingBroadcaster struct {
	events []*pb.WatchEvent
}

func (b *recordingBroadcaster) Publish(_ context.Context, event *pb.WatchEvent) {
	if event == nil {
		return
	}
	clone, ok := proto.Clone(event).(*pb.WatchEvent)
	if !ok {
		return
	}
	b.events = append(b.events, clone)
}

func assertEntryNames(t *testing.T, entries []*pb.DirEntry, want []string) {
	t.Helper()
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name)
	}
	if len(got) != len(want) {
		t.Fatalf("ReadDir() names = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("ReadDir() names = %v, want %v", got, want)
		}
	}
}
