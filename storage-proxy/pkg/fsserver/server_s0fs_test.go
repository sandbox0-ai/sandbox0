package fsserver

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/router"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
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

func newMountedS0FSVolumeContext(t *testing.T, volumeID, teamID string) *volume.VolumeContext {
	t.Helper()

	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: volumeID,
		WALPath:  filepath.Join(t.TempDir(), "engine.wal"),
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
	}
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
