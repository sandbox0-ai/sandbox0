package nodefs

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type muxTestSession struct {
	volumefuse.Session

	mu sync.Mutex

	lastLookup        *pb.LookupRequest
	lastGetAttr       *pb.GetAttrRequest
	lastSetAttr       *pb.SetAttrRequest
	lastCreate        *pb.CreateRequest
	lastOpen          *pb.OpenRequest
	lastOpenDir       *pb.OpenDirRequest
	lastReadDir       *pb.ReadDirRequest
	lastFlush         *pb.FlushRequest
	lastCopyFileRange *pb.CopyFileRangeRequest
	lastStatFS        *pb.StatFsRequest
	lastRename        *pb.RenameRequest
	lastLink          *pb.LinkRequest
	lastReadInto      *pb.ReadRequest
}

func (s *muxTestSession) Lookup(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	s.mu.Lock()
	s.lastLookup = req
	s.mu.Unlock()
	return &pb.NodeResponse{
		Inode:      11,
		Generation: 3,
		Attr:       &pb.GetAttrResponse{Ino: 11, Mode: uint32(syscall.S_IFREG | 0o644)},
		HandleId:   21,
	}, nil
}

func (s *muxTestSession) GetAttr(_ context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	s.mu.Lock()
	s.lastGetAttr = req
	s.mu.Unlock()
	return &pb.GetAttrResponse{Ino: req.Inode, Mode: uint32(syscall.S_IFDIR | 0o755)}, nil
}

func (s *muxTestSession) SetAttr(_ context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	s.mu.Lock()
	s.lastSetAttr = req
	s.mu.Unlock()
	return &pb.SetAttrResponse{Attr: &pb.GetAttrResponse{Ino: req.Inode, Mode: uint32(syscall.S_IFREG | 0o600)}}, nil
}

func (s *muxTestSession) Create(_ context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	s.mu.Lock()
	s.lastCreate = req
	s.mu.Unlock()
	return &pb.NodeResponse{
		Inode:    13,
		Attr:     &pb.GetAttrResponse{Ino: 13, Mode: uint32(syscall.S_IFREG | 0o644)},
		HandleId: 23,
	}, nil
}

func (s *muxTestSession) Open(_ context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	s.mu.Lock()
	s.lastOpen = req
	s.mu.Unlock()
	return &pb.OpenResponse{HandleId: 24}, nil
}

func (s *muxTestSession) OpenDir(_ context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	s.mu.Lock()
	s.lastOpenDir = req
	s.mu.Unlock()
	return &pb.OpenDirResponse{HandleId: 25}, nil
}

func (s *muxTestSession) ReadDir(_ context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	s.mu.Lock()
	s.lastReadDir = req
	s.mu.Unlock()
	return &pb.ReadDirResponse{Entries: []*pb.DirEntry{{
		Inode: 14,
		Name:  "child",
		Attr:  &pb.GetAttrResponse{Ino: 14, Mode: uint32(syscall.S_IFREG | 0o644)},
	}}}, nil
}

func (s *muxTestSession) Flush(_ context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	s.mu.Lock()
	s.lastFlush = req
	s.mu.Unlock()
	return &pb.Empty{}, nil
}

func (s *muxTestSession) CopyFileRange(_ context.Context, req *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	s.mu.Lock()
	s.lastCopyFileRange = req
	s.mu.Unlock()
	return &pb.CopyFileRangeResponse{BytesCopied: req.Length}, nil
}

func (s *muxTestSession) StatFs(_ context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	s.mu.Lock()
	s.lastStatFS = req
	s.mu.Unlock()
	return &pb.StatFsResponse{Blocks: 99, Bsize: 4096}, nil
}

func (s *muxTestSession) Rename(_ context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	s.mu.Lock()
	s.lastRename = req
	s.mu.Unlock()
	return &pb.Empty{}, nil
}

func (s *muxTestSession) Link(_ context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	s.mu.Lock()
	s.lastLink = req
	s.mu.Unlock()
	return &pb.NodeResponse{Inode: 15, Attr: &pb.GetAttrResponse{Ino: 15}}, nil
}

func (s *muxTestSession) ReadInto(_ context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	s.mu.Lock()
	s.lastReadInto = req
	s.mu.Unlock()
	return copy(dest, []byte("mux")), true, nil
}

func (s *muxTestSession) OpenFlagsForHandle(handleID uint64) (uint32, bool) {
	return fuse.FOPEN_DIRECT_IO, handleID == 24
}

func registerMuxTestPortal(t *testing.T, mux *SessionMux, session volumefuse.Session, name string, slot Slot) {
	t.Helper()
	if err := mux.RegisterPortal(PortalSpec{
		Name:      name,
		Slot:      slot,
		VolumeID:  "volume-" + name,
		RootInode: 1,
		Session:   session,
	}); err != nil {
		t.Fatalf("RegisterPortal() error = %v", err)
	}
}

func TestSessionMuxSyntheticRootLookupAndReadDir(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-2", 2)

	rootAttr, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: ShardRootNodeID})
	if err != nil {
		t.Fatalf("GetAttr(root) error = %v", err)
	}
	if rootAttr.Ino != ShardRootNodeID || rootAttr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) || rootAttr.Mode&0o222 != 0 {
		t.Fatalf("GetAttr(root) = %+v, want read-only synthetic directory", rootAttr)
	}

	lookup, err := mux.Lookup(context.Background(), &pb.LookupRequest{Parent: ShardRootNodeID, Name: "slot-2"})
	if err != nil {
		t.Fatalf("Lookup(root portal) error = %v", err)
	}
	wantRoot := mustNodeID(t, 2, 1)
	if lookup.Inode != wantRoot || lookup.Attr == nil || lookup.Attr.Ino != wantRoot {
		t.Fatalf("Lookup(root portal) = %+v, want encoded root %d", lookup, wantRoot)
	}
	if backend.lastGetAttr == nil || backend.lastGetAttr.VolumeId != "volume-slot-2" || backend.lastGetAttr.Inode != 1 {
		t.Fatalf("backend GetAttr request = %+v, want local root", backend.lastGetAttr)
	}

	dir, err := mux.ReadDir(context.Background(), &pb.ReadDirRequest{Inode: ShardRootNodeID, Plus: true})
	if err != nil {
		t.Fatalf("ReadDir(root) error = %v", err)
	}
	if len(dir.Entries) != 1 || dir.Entries[0].Name != "slot-2" || dir.Entries[0].Inode != wantRoot || dir.Entries[0].Attr.GetIno() != wantRoot {
		t.Fatalf("ReadDir(root) entries = %+v, want encoded slot-2 root", dir.Entries)
	}
	if _, err := mux.Create(context.Background(), &pb.CreateRequest{Parent: ShardRootNodeID, Name: "blocked"}); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("Create(root) error = %v, want %v", err, syscall.EROFS)
	}
}

func TestSessionMuxMapsNodeResponseAttrAndHandle(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-7", 7)

	resp, err := mux.Lookup(context.Background(), &pb.LookupRequest{
		VolumeId: "shard",
		Parent:   mustNodeID(t, 7, 10),
		Name:     "child",
	})
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if backend.lastLookup == nil || backend.lastLookup.VolumeId != "volume-slot-7" || backend.lastLookup.Parent != 10 {
		t.Fatalf("backend Lookup request = %+v, want volume-slot-7/local parent 10", backend.lastLookup)
	}
	if resp.Inode != mustNodeID(t, 7, 11) || resp.Attr.GetIno() != mustNodeID(t, 7, 11) || resp.HandleId != mustHandleID(t, 7, 21) {
		t.Fatalf("Lookup response = %+v, want all IDs encoded", resp)
	}
	if resp.Generation != 3 {
		t.Fatalf("Lookup generation = %d, want backend generation 3", resp.Generation)
	}
}

func TestSessionMuxUpdatesBackendWithoutChangingEncodedRoot(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	first := &muxTestSession{}
	second := &muxTestSession{}
	registerMuxTestPortal(t, mux, first, "slot-16", 16)
	root := mustNodeID(t, 16, 1)
	previous, err := mux.UpdatePortalSession("slot-16", "replacement-volume", second)
	if err != nil {
		t.Fatalf("UpdatePortalSession() error = %v", err)
	}
	if previous != first {
		t.Fatalf("UpdatePortalSession() previous = %T, want first backend", previous)
	}
	attr, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: root})
	if err != nil {
		t.Fatalf("GetAttr() error = %v", err)
	}
	if first.lastGetAttr != nil {
		t.Fatalf("old backend received request after update: %+v", first.lastGetAttr)
	}
	if second.lastGetAttr == nil || second.lastGetAttr.VolumeId != "replacement-volume" || second.lastGetAttr.Inode != 1 {
		t.Fatalf("replacement backend request = %+v, want local root", second.lastGetAttr)
	}
	if attr.Ino != root {
		t.Fatalf("GetAttr() inode = %d, want stable encoded root %d", attr.Ino, root)
	}
}

func TestSessionMuxUpdateWaitsForPreviousBackendCalls(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	previous := &blockingGetAttrSession{started: make(chan struct{}), release: make(chan struct{})}
	next := &muxTestSession{}
	registerMuxTestPortal(t, mux, previous, "slot-17", 17)
	requestDone := make(chan error, 1)
	go func() {
		_, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: mustNodeID(t, 17, 1)})
		requestDone <- err
	}()
	<-previous.started

	updateDone := make(chan struct{})
	var replaced volumefuse.Session
	var updateErr error
	go func() {
		replaced, updateErr = mux.UpdatePortalSession("slot-17", "next-volume", next)
		close(updateDone)
	}()
	select {
	case <-updateDone:
		t.Fatal("UpdatePortalSession() returned while the previous backend call was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(previous.release)
	if err := <-requestDone; err != nil {
		t.Fatalf("previous backend request error = %v", err)
	}
	select {
	case <-updateDone:
	case <-time.After(time.Second):
		t.Fatal("UpdatePortalSession() did not finish after the previous call drained")
	}
	if updateErr != nil || replaced != previous {
		t.Fatalf("UpdatePortalSession() = (%T, %v), want previous backend", replaced, updateErr)
	}
	if _, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: mustNodeID(t, 17, 1)}); err != nil {
		t.Fatalf("next backend GetAttr() error = %v", err)
	}
	if next.lastGetAttr == nil || next.lastGetAttr.VolumeId != "next-volume" {
		t.Fatalf("next backend request = %+v", next.lastGetAttr)
	}
}

func TestSessionMuxMapsAttrRequestsAndResponses(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-3", 3)
	nodeID := mustNodeID(t, 3, 30)
	handleID := mustHandleID(t, 3, 40)

	attr, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: nodeID})
	if err != nil {
		t.Fatalf("GetAttr() error = %v", err)
	}
	if backend.lastGetAttr.Inode != 30 || attr.Ino != nodeID {
		t.Fatalf("GetAttr mapping request=%+v response=%+v", backend.lastGetAttr, attr)
	}
	setResp, err := mux.SetAttr(context.Background(), &pb.SetAttrRequest{
		Inode:    nodeID,
		HandleId: handleID,
		Attr:     &pb.GetAttrResponse{Ino: nodeID},
	})
	if err != nil {
		t.Fatalf("SetAttr() error = %v", err)
	}
	if backend.lastSetAttr.Inode != 30 || backend.lastSetAttr.HandleId != 40 || backend.lastSetAttr.Attr.GetIno() != 30 {
		t.Fatalf("backend SetAttr request = %+v, want local IDs", backend.lastSetAttr)
	}
	if setResp.Attr.GetIno() != nodeID {
		t.Fatalf("SetAttr response inode = %d, want %d", setResp.Attr.GetIno(), nodeID)
	}
}

func TestSessionMuxMapsOpenCreateAndDirectoryHandles(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-4", 4)
	root := mustNodeID(t, 4, 1)

	created, err := mux.Create(context.Background(), &pb.CreateRequest{Parent: root, Name: "new"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if backend.lastCreate.Parent != 1 || created.Inode != mustNodeID(t, 4, 13) || created.Attr.GetIno() != mustNodeID(t, 4, 13) || created.HandleId != mustHandleID(t, 4, 23) {
		t.Fatalf("Create mapping request=%+v response=%+v", backend.lastCreate, created)
	}

	opened, err := mux.Open(context.Background(), &pb.OpenRequest{Inode: mustNodeID(t, 4, 13)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if backend.lastOpen.Inode != 13 || opened.HandleId != mustHandleID(t, 4, 24) {
		t.Fatalf("Open mapping request=%+v response=%+v", backend.lastOpen, opened)
	}
	flags, ok := mux.OpenFlagsForHandle(opened.HandleId)
	if !ok || flags != fuse.FOPEN_DIRECT_IO {
		t.Fatalf("OpenFlagsForHandle() = (%#x, %v), want DIRECT_IO", flags, ok)
	}

	openedDir, err := mux.OpenDir(context.Background(), &pb.OpenDirRequest{Inode: root})
	if err != nil {
		t.Fatalf("OpenDir() error = %v", err)
	}
	if backend.lastOpenDir.Inode != 1 || openedDir.HandleId != mustHandleID(t, 4, 25) {
		t.Fatalf("OpenDir mapping request=%+v response=%+v", backend.lastOpenDir, openedDir)
	}
	dir, err := mux.ReadDir(context.Background(), &pb.ReadDirRequest{Inode: root, HandleId: openedDir.HandleId, Plus: true})
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if backend.lastReadDir.Inode != 1 || backend.lastReadDir.HandleId != 25 {
		t.Fatalf("backend ReadDir request = %+v, want local IDs", backend.lastReadDir)
	}
	if len(dir.Entries) != 1 || dir.Entries[0].Inode != mustNodeID(t, 4, 14) || dir.Entries[0].Attr.GetIno() != mustNodeID(t, 4, 14) {
		t.Fatalf("ReadDir response entries = %+v, want encoded IDs", dir.Entries)
	}
}

func TestSessionMuxMapsHandleOnlyCopyAndStatFSRequests(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-5", 5)

	if _, err := mux.Flush(context.Background(), &pb.FlushRequest{HandleId: mustHandleID(t, 5, 51)}); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if backend.lastFlush.VolumeId != "volume-slot-5" || backend.lastFlush.HandleId != 51 {
		t.Fatalf("backend Flush request = %+v, want local handle", backend.lastFlush)
	}
	copyResp, err := mux.CopyFileRange(context.Background(), &pb.CopyFileRangeRequest{
		InodeIn:   mustNodeID(t, 5, 31),
		HandleIn:  mustHandleID(t, 5, 41),
		InodeOut:  mustNodeID(t, 5, 32),
		HandleOut: mustHandleID(t, 5, 42),
		Length:    123,
	})
	if err != nil {
		t.Fatalf("CopyFileRange() error = %v", err)
	}
	if copyResp.BytesCopied != 123 || backend.lastCopyFileRange.InodeIn != 31 || backend.lastCopyFileRange.HandleIn != 41 || backend.lastCopyFileRange.InodeOut != 32 || backend.lastCopyFileRange.HandleOut != 42 {
		t.Fatalf("backend CopyFileRange request = %+v, response = %+v", backend.lastCopyFileRange, copyResp)
	}
	stat, err := mux.StatFs(context.Background(), &pb.StatFsRequest{Inode: mustNodeID(t, 5, 33)})
	if err != nil {
		t.Fatalf("StatFs() error = %v", err)
	}
	if stat.Blocks != 99 || backend.lastStatFS.VolumeId != "volume-slot-5" || backend.lastStatFS.Inode != 33 {
		t.Fatalf("backend StatFs request = %+v, response = %+v", backend.lastStatFS, stat)
	}
}

func TestSessionMuxMapsTwoNodeRequestsAndLinkResponse(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-15", 15)
	if _, err := mux.Rename(context.Background(), &pb.RenameRequest{
		OldParent: mustNodeID(t, 15, 71),
		NewParent: mustNodeID(t, 15, 72),
		OldName:   "old",
		NewName:   "new",
	}); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	if backend.lastRename.VolumeId != "volume-slot-15" || backend.lastRename.OldParent != 71 || backend.lastRename.NewParent != 72 {
		t.Fatalf("backend Rename request = %+v, want local parents", backend.lastRename)
	}
	linked, err := mux.Link(context.Background(), &pb.LinkRequest{
		Inode:     mustNodeID(t, 15, 73),
		NewParent: mustNodeID(t, 15, 74),
		NewName:   "linked",
	})
	if err != nil {
		t.Fatalf("Link() error = %v", err)
	}
	if backend.lastLink.Inode != 73 || backend.lastLink.NewParent != 74 || linked.Inode != mustNodeID(t, 15, 15) || linked.Attr.GetIno() != mustNodeID(t, 15, 15) {
		t.Fatalf("Link mapping request=%+v response=%+v", backend.lastLink, linked)
	}
}

func TestSessionMuxPreservesReadIntoFastPath(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-6", 6)
	dest := make([]byte, 8)
	n, eof, err := mux.ReadInto(context.Background(), &pb.ReadRequest{
		Inode:    mustNodeID(t, 6, 61),
		HandleId: mustHandleID(t, 6, 62),
		Size:     int64(len(dest)),
	}, dest)
	if err != nil {
		t.Fatalf("ReadInto() error = %v", err)
	}
	if n != 3 || !eof || string(dest[:n]) != "mux" {
		t.Fatalf("ReadInto() = (%d, %v, %q), want (3, true, mux)", n, eof, dest[:n])
	}
	if backend.lastReadInto.VolumeId != "volume-slot-6" || backend.lastReadInto.Inode != 61 || backend.lastReadInto.HandleId != 62 {
		t.Fatalf("backend ReadInto request = %+v, want local IDs", backend.lastReadInto)
	}
}

func TestSessionMuxRejectsCrossPortalOperationsWithEXDEV(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	registerMuxTestPortal(t, mux, &muxTestSession{}, "slot-8", 8)
	registerMuxTestPortal(t, mux, &muxTestSession{}, "slot-9", 9)

	_, err := mux.Rename(context.Background(), &pb.RenameRequest{
		OldParent: mustNodeID(t, 8, 1),
		NewParent: mustNodeID(t, 9, 1),
	})
	assertRouteStatus(t, err, ErrCrossPortal, syscall.EXDEV)
	_, err = mux.Link(context.Background(), &pb.LinkRequest{
		Inode:     mustNodeID(t, 8, 2),
		NewParent: mustNodeID(t, 9, 1),
	})
	assertRouteStatus(t, err, ErrCrossPortal, syscall.EXDEV)
	_, err = mux.CopyFileRange(context.Background(), &pb.CopyFileRangeRequest{
		InodeIn:   mustNodeID(t, 8, 2),
		HandleIn:  mustHandleID(t, 8, 3),
		InodeOut:  mustNodeID(t, 9, 2),
		HandleOut: mustHandleID(t, 9, 3),
	})
	assertRouteStatus(t, err, ErrCrossPortal, syscall.EXDEV)

	fs := volumefuse.New("shard", time.Second, mux)
	status := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: mustNodeID(t, 8, 1)},
		Newdir:   mustNodeID(t, 9, 1),
	}, "old", "new")
	if status != fuse.Status(syscall.EXDEV) {
		t.Fatalf("volumefuse Rename status = %v, want EXDEV", status)
	}
}

func TestSessionMuxMapsMissingAndDrainingRoutes(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	if _, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: mustNodeID(t, 10, 1)}); err == nil {
		t.Fatal("GetAttr(missing route) error = nil")
	} else {
		assertRouteStatus(t, err, ErrSlotNotFound, syscall.ESTALE)
	}

	backend := &blockingGetAttrSession{started: make(chan struct{}), release: make(chan struct{})}
	registerMuxTestPortal(t, mux, backend, "slot-10", 10)
	done := make(chan error, 1)
	go func() {
		_, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: mustNodeID(t, 10, 1)})
		done <- err
	}()
	<-backend.started

	drainCtx, cancel := context.WithCancel(context.Background())
	drainDone := make(chan error, 1)
	go func() {
		_, err := mux.DrainPortal(drainCtx, "slot-10")
		drainDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		probe, _, err := mux.router.AcquireNode(mustNodeID(t, 10, 1))
		if errors.Is(err, ErrSlotDraining) {
			break
		}
		if err == nil {
			probe.Release()
		}
		if time.Now().After(deadline) {
			t.Fatalf("router did not enter draining state, last error = %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := mux.GetAttr(context.Background(), &pb.GetAttrRequest{Inode: mustNodeID(t, 10, 1)}); err == nil {
		t.Fatal("GetAttr(draining route) error = nil")
	} else {
		assertRouteStatus(t, err, ErrSlotDraining, syscall.EAGAIN)
	}
	cancel()
	if err := <-drainDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("DrainPortal(canceled) error = %v, want context canceled", err)
	}
	close(backend.release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight GetAttr() error = %v", err)
	}
	if _, err := mux.DrainPortal(context.Background(), "slot-10"); err != nil {
		t.Fatalf("DrainPortal(resume) error = %v", err)
	}
}

func TestSessionMuxRejectsUnsafeNamesAndSlotReuse(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	for _, name := range []string{"", ".", "..", "nested/name", "bad\x00name"} {
		err := mux.RegisterPortal(PortalSpec{Name: name, Slot: 1, VolumeID: "vol", Session: &muxTestSession{}})
		if !errors.Is(err, ErrInvalidPortalRoute) {
			t.Fatalf("RegisterPortal(%q) error = %v, want %v", name, err, ErrInvalidPortalRoute)
		}
	}
	registerMuxTestPortal(t, mux, &muxTestSession{}, "slot-11", 11)
	if _, err := mux.DrainPortal(context.Background(), "slot-11"); err != nil {
		t.Fatalf("DrainPortal() error = %v", err)
	}
	err := mux.RegisterPortal(PortalSpec{Name: "replacement", Slot: 11, VolumeID: "vol", Session: &muxTestSession{}})
	if !errors.Is(err, ErrSlotRetired) {
		t.Fatalf("RegisterPortal(reused slot) error = %v, want %v", err, ErrSlotRetired)
	}
}

func TestVolumefuseStatFsPassesNodeIDToSession(t *testing.T) {
	t.Parallel()
	mux := NewSessionMux()
	backend := &muxTestSession{}
	registerMuxTestPortal(t, mux, backend, "slot-12", 12)
	fs := volumefuse.New("shard", time.Second, mux)
	var out fuse.StatfsOut
	status := fs.StatFs(nil, &fuse.InHeader{NodeId: mustNodeID(t, 12, 71)}, &out)
	if status != fuse.OK {
		t.Fatalf("StatFs() status = %v, want OK", status)
	}
	if backend.lastStatFS == nil || backend.lastStatFS.Inode != 71 {
		t.Fatalf("backend StatFs request = %+v, want local inode 71", backend.lastStatFS)
	}
}

type blockingGetAttrSession struct {
	volumefuse.Session
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingGetAttrSession) GetAttr(context.Context, *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return &pb.GetAttrResponse{Ino: 1, Mode: uint32(syscall.S_IFDIR | 0o755)}, nil
}

func assertRouteStatus(t *testing.T, err, cause error, errno syscall.Errno) {
	t.Helper()
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want cause %v", err, cause)
	}
	var provider interface{ FuseErrno() syscall.Errno }
	if !errors.As(err, &provider) || provider.FuseErrno() != errno {
		t.Fatalf("error = %v, want FUSE errno %v", err, errno)
	}
}

type muxAccessBenchmarkSession struct {
	volumefuse.Session
}

var muxAccessBenchmarkResponse = &pb.Empty{}

func (muxAccessBenchmarkSession) Access(context.Context, *pb.AccessRequest) (*pb.Empty, error) {
	return muxAccessBenchmarkResponse, nil
}

func BenchmarkSessionMuxAccess(b *testing.B) {
	mux := NewSessionMux()
	if err := mux.RegisterPortal(PortalSpec{
		Name:      "slot-1",
		Slot:      1,
		VolumeID:  "volume-1",
		RootInode: 1,
		Session:   muxAccessBenchmarkSession{},
	}); err != nil {
		b.Fatal(err)
	}
	nodeID := mustNodeID(b, 1, 11)
	req := &pb.AccessRequest{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		req.Inode = nodeID
		if _, err := mux.Access(context.Background(), req); err != nil {
			b.Fatal(err)
		}
	}
}
