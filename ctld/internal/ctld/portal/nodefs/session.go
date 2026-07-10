package nodefs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

const (
	defaultPortalRootInode = uint64(1)
	maxPortalRouteNameLen  = 255
)

var (
	ErrInvalidPortalRoute     = errors.New("invalid nodefs portal route")
	ErrPortalRouteExists      = errors.New("nodefs portal route already exists")
	ErrPortalRouteMissing     = errors.New("nodefs portal route not found")
	ErrStaleBindingGeneration = errors.New("stale nodefs binding generation")
)

// PortalSpec describes one portal subtree below a nodefs shard root. Name is a
// shard-unique path component, normally derived from the durable slot rather
// than from a user-visible mount name.
type PortalSpec struct {
	Name       string
	Slot       Slot
	VolumeID   string
	RootInode  uint64
	Generation uint64
	Session    volumefuse.Session
}

type portalSessionRoute struct {
	name string
	slot Slot

	rootInode uint64
	binding   atomic.Pointer[portalSessionBinding]
	callMu    sync.RWMutex
	switching bool
	replies   *responseBarrier
}

type responseBarrier struct {
	pending atomic.Int64
	wake    chan struct{}
}

func newResponseBarrier() *responseBarrier {
	return &responseBarrier{wake: make(chan struct{}, 1)}
}

func (b *responseBarrier) track(ctx context.Context) {
	if b == nil {
		return
	}
	if _, ok := volumefuse.RequestIdentityFromContext(ctx); !ok {
		return
	}
	b.pending.Add(1)
	if volumefuse.AttachRequestCompletionToken(ctx, b) {
		return
	}
	b.RequestAcknowledged()
}

func (b *responseBarrier) RequestAcknowledged() {
	if b == nil {
		return
	}
	remaining := b.pending.Add(-1)
	if remaining < 0 {
		panic("nodefs response barrier acknowledged without a pending request")
	}
	if remaining == 0 {
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}
}

func (b *responseBarrier) wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for b.pending.Load() != 0 {
		select {
		case <-b.wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type portalSessionBinding struct {
	volumeID   string
	generation uint64
	session    volumefuse.Session
}

type portalSessionSnapshot struct {
	volumeID   string
	rootInode  uint64
	generation uint64
	session    volumefuse.Session
}

func (r *portalSessionRoute) snapshot() portalSessionSnapshot {
	binding := r.binding.Load()
	if binding == nil {
		return portalSessionSnapshot{rootInode: r.rootInode}
	}
	return portalSessionSnapshot{
		volumeID:   binding.volumeID,
		rootInode:  r.rootInode,
		generation: binding.generation,
		session:    binding.session,
	}
}

func (r *portalSessionRoute) acquireRouteLease() bool {
	r.callMu.RLock()
	if r.switching {
		r.callMu.RUnlock()
		return false
	}
	return true
}

func (r *portalSessionRoute) releaseRouteLease() {
	r.callMu.RUnlock()
}

// SessionMux implements one volumefuse session for a fixed nodefs shard. The
// synthetic root exposes portal route names, while every portal operation is
// decoded and forwarded to exactly one backend session.
type SessionMux struct {
	mu     sync.RWMutex
	byName map[string]*portalSessionRoute
	router *Router[*portalSessionRoute]
}

var _ volumefuse.Session = (*SessionMux)(nil)
var _ volumefuse.ReadIntoSession = (*SessionMux)(nil)
var _ volumefuse.OpenFlagsForHandleSession = (*SessionMux)(nil)

// NewSessionMux constructs an empty shard namespace.
func NewSessionMux() *SessionMux {
	return &SessionMux{
		byName: make(map[string]*portalSessionRoute),
		router: NewRouter[*portalSessionRoute](),
	}
}

// RegisterPortal publishes a route below the synthetic shard root.
func (m *SessionMux) RegisterPortal(spec PortalSpec) error {
	if m == nil {
		return fmt.Errorf("%w: session mux is nil", ErrInvalidPortalRoute)
	}
	name, err := validatePortalRouteName(spec.Name)
	if err != nil {
		return err
	}
	if _, err := NewSlot(uint64(spec.Slot)); err != nil {
		return err
	}
	if strings.TrimSpace(spec.VolumeID) == "" {
		return fmt.Errorf("%w: volume id is required", ErrInvalidPortalRoute)
	}
	if spec.Session == nil {
		return fmt.Errorf("%w: session is required", ErrInvalidPortalRoute)
	}
	if spec.RootInode == 0 {
		spec.RootInode = defaultPortalRootInode
	}
	if spec.RootInode > MaxBackendLocalID {
		return fmt.Errorf("%w: root inode %d exceeds %d", ErrInvalidPortalRoute, spec.RootInode, MaxBackendLocalID)
	}
	if err := validateBindingGeneration(spec.Generation); err != nil {
		return err
	}

	route := &portalSessionRoute{
		name:      name,
		slot:      spec.Slot,
		rootInode: spec.RootInode,
		replies:   newResponseBarrier(),
	}
	route.binding.Store(&portalSessionBinding{
		volumeID: spec.VolumeID, generation: spec.Generation, session: spec.Session,
	})
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.byName == nil {
		m.byName = make(map[string]*portalSessionRoute)
	}
	if m.router == nil {
		m.router = NewRouter[*portalSessionRoute]()
	}
	if _, exists := m.byName[name]; exists {
		return fmt.Errorf("%w: %s", ErrPortalRouteExists, name)
	}
	if err := m.router.Register(spec.Slot, route); err != nil {
		return err
	}
	m.byName[name] = route
	return nil
}

// UpdatePortalSession changes the backend session without a kernel cache
// transition. Runtime code should use SwitchPortalSession so late replies and
// cached pages cannot cross a backend generation.
func (m *SessionMux) UpdatePortalSession(
	name, volumeID string,
	generation uint64,
	session volumefuse.Session,
) (volumefuse.Session, error) {
	return m.SwitchPortalSession(context.Background(), name, volumeID, generation, session, nil)
}

// SwitchPortalSession rejects new route calls, waits until every reply from
// the previous backend has reached the kernel, invalidates that portal's cache
// domain, and then publishes the new binding. A failed invalidation leaves the
// old binding active after the short fail-fast switching window.
func (m *SessionMux) SwitchPortalSession(
	ctx context.Context,
	name, volumeID string,
	generation uint64,
	session volumefuse.Session,
	invalidate func(rootNodeID, nodeIDMask, generation uint64) error,
) (volumefuse.Session, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: session mux is nil", ErrPortalRouteMissing)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	name = strings.TrimSpace(name)
	if volumeID = strings.TrimSpace(volumeID); volumeID == "" {
		return nil, fmt.Errorf("%w: volume id is required", ErrInvalidPortalRoute)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: session is required", ErrInvalidPortalRoute)
	}
	if err := validateBindingGeneration(generation); err != nil {
		return nil, err
	}
	m.mu.RLock()
	route := m.byName[name]
	m.mu.RUnlock()
	if route == nil {
		return nil, fmt.Errorf("%w: %s", ErrPortalRouteMissing, name)
	}
	route.callMu.Lock()
	previous := route.binding.Load()
	if route.switching {
		route.callMu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrSlotSwitching, name)
	}
	if previous != nil && generation <= previous.generation {
		route.callMu.Unlock()
		return nil, fmt.Errorf(
			"%w: update generation %d must exceed current generation %d",
			ErrInvalidBindingGeneration, generation, previous.generation,
		)
	}
	route.switching = true
	route.callMu.Unlock()

	committed := false
	defer func() {
		if committed {
			return
		}
		route.callMu.Lock()
		route.switching = false
		route.callMu.Unlock()
	}()
	if err := route.replies.wait(ctx); err != nil {
		return nil, fmt.Errorf("wait for nodefs portal %s replies: %w", name, err)
	}
	rootNodeID, err := EncodeNodeID(route.slot, route.rootInode)
	if err != nil {
		return nil, err
	}
	if invalidate != nil {
		if err := invalidate(rootNodeID, PortalNodeIDMask, generation); err != nil {
			return nil, err
		}
	}

	route.callMu.Lock()
	route.binding.Store(&portalSessionBinding{
		volumeID: volumeID, generation: generation, session: session,
	})
	route.switching = false
	route.callMu.Unlock()
	committed = true
	if previous == nil {
		return nil, nil
	}
	return previous.session, nil
}

// DrainPortal removes the route name, rejects new requests for its slot, and
// waits for requests that already acquired the route.
func (m *SessionMux) DrainPortal(ctx context.Context, name string) (PortalSpec, error) {
	if m == nil {
		return PortalSpec{}, fmt.Errorf("%w: session mux is nil", ErrPortalRouteMissing)
	}
	name = strings.TrimSpace(name)
	m.mu.Lock()
	route := m.byName[name]
	if route == nil {
		m.mu.Unlock()
		return PortalSpec{}, fmt.Errorf("%w: %s", ErrPortalRouteMissing, name)
	}
	m.mu.Unlock()

	drained, err := m.router.Drain(ctx, route.slot)
	if err != nil {
		return PortalSpec{}, mapRouteError(err)
	}
	m.mu.Lock()
	if m.byName[name] == route {
		delete(m.byName, name)
	}
	m.mu.Unlock()
	drained.callMu.RLock()
	snapshot := drained.snapshot()
	drained.callMu.RUnlock()
	return PortalSpec{
		Name:       drained.name,
		Slot:       drained.slot,
		VolumeID:   snapshot.volumeID,
		RootInode:  snapshot.rootInode,
		Generation: snapshot.generation,
		Session:    snapshot.session,
	}, nil
}

// RetireSlot imports a durable deleted-slot high-water mark into the router.
func (m *SessionMux) RetireSlot(slot Slot) error {
	if m == nil {
		return fmt.Errorf("retire slot %d: session mux is nil", slot)
	}
	return m.router.Retire(slot)
}

// Close intentionally does not close backend sessions. They may be shared by
// multiple read-only portals and are owned by the portal manager.
func (m *SessionMux) Close() {}

func (m *SessionMux) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	if req.Parent == ShardRootNodeID {
		return m.lookupPortalRoot(ctx, req)
	}
	lease, route, localParent, err := m.acquireNode(ctx, req.Parent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Parent = localParent
	resp, err := route.session.Lookup(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeNodeResponse(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	if req.Inode == ShardRootNodeID {
		return syntheticRootAttr(), nil
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	resp, err := route.session.GetAttr(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeAttr(lease.Slot, route, localNode, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	if req.Attr != nil {
		req.Attr.Ino = localNode
	}
	resp, err := route.session.SetAttr(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		if err := encodeAttr(lease.Slot, route, localNode, resp.Attr); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (m *SessionMux) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	if req.Parent == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localParent, err := m.acquireNode(ctx, req.Parent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Parent = localParent
	resp, err := route.session.Mkdir(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeNodeResponse(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	if req.Parent == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localParent, err := m.acquireNode(ctx, req.Parent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Parent = localParent
	resp, err := route.session.Create(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeNodeResponse(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	if req.Parent == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localParent, err := m.acquireNode(ctx, req.Parent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Parent = localParent
	return route.session.Unlink(ctx, req)
}

func (m *SessionMux) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	if req.Parent == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localParent, err := m.acquireNode(ctx, req.Parent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Parent = localParent
	return route.session.Rmdir(ctx, req)
}

func (m *SessionMux) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	if req.OldParent == ShardRootNodeID || req.NewParent == ShardRootNodeID {
		if req.OldParent == req.NewParent {
			return nil, syscall.EROFS
		}
		return nil, newRouteStatusError(ErrCrossPortal, syscall.EXDEV)
	}
	lease, route, oldParent, newParent, err := m.acquireNodes(ctx, req.OldParent, req.NewParent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.OldParent = oldParent
	req.NewParent = newParent
	return route.session.Rename(ctx, req)
}

func (m *SessionMux) Link(ctx context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	if req.Inode == ShardRootNodeID || req.NewParent == ShardRootNodeID {
		return nil, newRouteStatusError(ErrCrossPortal, syscall.EXDEV)
	}
	lease, route, localNode, localParent, err := m.acquireNodes(ctx, req.Inode, req.NewParent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.NewParent = localParent
	resp, err := route.session.Link(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeNodeResponse(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	if req.Parent == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localParent, err := m.acquireNode(ctx, req.Parent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Parent = localParent
	resp, err := route.session.Symlink(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeNodeResponse(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EINVAL
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	return route.session.Readlink(ctx, req)
}

func (m *SessionMux) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		if req.Mask&2 != 0 {
			return nil, syscall.EROFS
		}
		return &pb.Empty{}, nil
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	return route.session.Access(ctx, req)
}

func (m *SessionMux) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EISDIR
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	resp, err := route.session.Open(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeOpenHandle(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EISDIR
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	return route.session.Read(ctx, req)
}

func (m *SessionMux) ReadInto(ctx context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	if req.Inode == ShardRootNodeID {
		return 0, false, syscall.EISDIR
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return 0, false, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	if reader, ok := route.session.(volumefuse.ReadIntoSession); ok {
		return reader.ReadInto(ctx, req, dest)
	}
	resp, err := route.session.Read(ctx, req)
	if err != nil {
		return 0, false, err
	}
	n := copy(dest, resp.GetData())
	return n, resp.GetEof() || n < len(dest), nil
}

func (m *SessionMux) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EISDIR
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	return route.session.Write(ctx, req)
}

func (m *SessionMux) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		return &pb.Empty{}, nil
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	return route.session.Release(ctx, req)
}

func (m *SessionMux) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	if req.HandleId == 0 {
		return &pb.Empty{}, nil
	}
	lease, route, localHandle, err := m.acquireHandle(ctx, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.HandleId = localHandle
	return route.session.Flush(ctx, req)
}

func (m *SessionMux) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	if req.HandleId == 0 {
		return &pb.Empty{}, nil
	}
	lease, route, localHandle, err := m.acquireHandle(ctx, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.HandleId = localHandle
	return route.session.Fsync(ctx, req)
}

func (m *SessionMux) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EISDIR
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	return route.session.Fallocate(ctx, req)
}

func (m *SessionMux) CopyFileRange(ctx context.Context, req *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	if req.InodeIn == ShardRootNodeID || req.InodeOut == ShardRootNodeID {
		return nil, newRouteStatusError(ErrCrossPortal, syscall.EXDEV)
	}
	lease, route, inodeIn, handleIn, inodeOut, handleOut, err := m.acquireCopy(ctx,
		req.InodeIn, req.HandleIn, req.InodeOut, req.HandleOut,
	)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.InodeIn = inodeIn
	req.HandleIn = handleIn
	req.InodeOut = inodeOut
	req.HandleOut = handleOut
	return route.session.CopyFileRange(ctx, req)
}

func (m *SessionMux) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	if req.Inode == ShardRootNodeID {
		return &pb.OpenDirResponse{}, nil
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	resp, err := route.session.OpenDir(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp != nil && resp.HandleId != 0 {
		resp.HandleId, err = EncodeBindingHandleID(lease.Slot, route.generation, resp.HandleId)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (m *SessionMux) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	if req.Inode == ShardRootNodeID {
		return m.readSyntheticRoot(ctx, req)
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	resp, err := route.session.ReadDir(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeDirEntries(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		return &pb.Empty{}, nil
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	return route.session.ReleaseDir(ctx, req)
}

func (m *SessionMux) StatFs(ctx context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	if req.Inode == ShardRootNodeID || req.Inode == 0 {
		return syntheticRootStatFS(), nil
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	return route.session.StatFs(ctx, req)
}

func (m *SessionMux) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.ENODATA
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	return route.session.GetXattr(ctx, req)
}

func (m *SessionMux) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	return route.session.SetXattr(ctx, req)
}

func (m *SessionMux) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	if req.Inode == ShardRootNodeID {
		return &pb.ListXattrResponse{}, nil
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	return route.session.ListXattr(ctx, req)
}

func (m *SessionMux) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localNode, err := m.acquireNode(ctx, req.Inode)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	return route.session.RemoveXattr(ctx, req)
}

func (m *SessionMux) Mknod(ctx context.Context, req *pb.MknodRequest) (*pb.NodeResponse, error) {
	if req.Parent == ShardRootNodeID {
		return nil, syscall.EROFS
	}
	lease, route, localParent, err := m.acquireNode(ctx, req.Parent)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Parent = localParent
	resp, err := route.session.Mknod(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := encodeNodeResponse(lease.Slot, route, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *SessionMux) GetLk(ctx context.Context, req *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EBADF
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	return route.session.GetLk(ctx, req)
}

func (m *SessionMux) SetLk(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	return m.setLk(ctx, req, false)
}

func (m *SessionMux) SetLkw(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	return m.setLk(ctx, req, true)
}

func (m *SessionMux) Flock(ctx context.Context, req *pb.FlockRequest) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EBADF
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	return route.session.Flock(ctx, req)
}

func (m *SessionMux) OpenFlagsForHandle(handleID uint64) (uint32, bool) {
	if m == nil || handleID == 0 {
		return 0, false
	}
	lease, route, localHandle, err := m.acquireHandle(nil, handleID)
	if err != nil {
		return 0, false
	}
	defer lease.Release()
	if provider, ok := route.session.(volumefuse.OpenFlagsForHandleSession); ok {
		return provider.OpenFlagsForHandle(localHandle)
	}
	if provider, ok := route.session.(volumefuse.OpenFlagsSession); ok {
		return provider.OpenFlags(), true
	}
	return 0, false
}

func (m *SessionMux) setLk(ctx context.Context, req *pb.SetLkRequest, wait bool) (*pb.Empty, error) {
	if req.Inode == ShardRootNodeID {
		return nil, syscall.EBADF
	}
	lease, route, localNode, localHandle, err := m.acquireNodeHandle(ctx, req.Inode, req.HandleId)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	req.VolumeId = route.volumeID
	req.Inode = localNode
	req.HandleId = localHandle
	if wait {
		return route.session.SetLkw(ctx, req)
	}
	return route.session.SetLk(ctx, req)
}

func (m *SessionMux) lookupPortalRoot(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	name := strings.TrimSpace(req.Name)
	m.mu.RLock()
	route := m.byName[name]
	m.mu.RUnlock()
	if route == nil {
		return nil, syscall.ENOENT
	}
	snapshot := route.snapshot()
	globalRoot, err := EncodeNodeID(route.slot, snapshot.rootInode)
	if err != nil {
		return nil, err
	}
	lease, _, err := m.router.AcquireNode(globalRoot)
	if err != nil {
		return nil, mapRouteError(err)
	}
	defer lease.Release()
	snapshot = lease.Target.snapshot()
	if snapshot.session == nil {
		return nil, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	lease.Target.replies.track(ctx)
	attr, err := snapshot.session.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: snapshot.volumeID,
		Inode:    snapshot.rootInode,
		Actor:    req.Actor,
	})
	if err != nil {
		return nil, err
	}
	if err := encodeAttr(lease.Slot, snapshot, snapshot.rootInode, attr); err != nil {
		return nil, err
	}
	return &pb.NodeResponse{Inode: globalRoot, Generation: 1, Attr: attr}, nil
}

func (m *SessionMux) readSyntheticRoot(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	m.mu.RLock()
	routes := make([]*portalSessionRoute, 0, len(m.byName))
	for _, route := range m.byName {
		routes = append(routes, route)
	}
	m.mu.RUnlock()
	sort.Slice(routes, func(i, j int) bool { return routes[i].name < routes[j].name })
	start := int(req.Offset)
	if start < 0 {
		start = 0
	}
	if start > len(routes) {
		start = len(routes)
	}
	entries := make([]*pb.DirEntry, 0, len(routes)-start)
	for index, route := range routes[start:] {
		snapshot := route.snapshot()
		globalRoot, err := EncodeNodeID(route.slot, snapshot.rootInode)
		if err != nil {
			return nil, err
		}
		lease, _, err := m.router.AcquireNode(globalRoot)
		if err != nil {
			if errors.Is(err, ErrSlotDraining) || errors.Is(err, ErrSlotSwitching) || errors.Is(err, ErrSlotNotFound) {
				continue
			}
			return nil, mapRouteError(err)
		}
		snapshot = lease.Target.snapshot()
		entry := &pb.DirEntry{
			Inode:  globalRoot,
			Offset: uint64(start + index + 1),
			Name:   route.name,
			Type:   uint32(syscall.S_IFDIR),
		}
		if req.Plus {
			lease.Target.replies.track(ctx)
			entry.Attr, err = snapshot.session.GetAttr(ctx, &pb.GetAttrRequest{
				VolumeId: snapshot.volumeID,
				Inode:    snapshot.rootInode,
				Actor:    req.Actor,
			})
			if err == nil {
				err = encodeAttr(lease.Slot, snapshot, snapshot.rootInode, entry.Attr)
			}
		}
		lease.Release()
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &pb.ReadDirResponse{Entries: entries, Eof: true}, nil
}

func (m *SessionMux) acquireNode(ctx context.Context, nodeID uint64) (Lease[*portalSessionRoute], portalSessionSnapshot, uint64, error) {
	var zero Lease[*portalSessionRoute]
	if m == nil || m.router == nil {
		return zero, portalSessionSnapshot{}, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	lease, encodedLocalNode, err := m.router.AcquireNode(nodeID)
	if err != nil {
		return zero, portalSessionSnapshot{}, 0, mapRouteError(err)
	}
	route := lease.Target.snapshot()
	if route.session == nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	localNode, err := decodePortalNode(route, encodedLocalNode)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, err
	}
	lease.Target.replies.track(ctx)
	return lease, route, localNode, nil
}

func (m *SessionMux) acquireHandle(ctx context.Context, handleID uint64) (Lease[*portalSessionRoute], portalSessionSnapshot, uint64, error) {
	var zero Lease[*portalSessionRoute]
	if m == nil || m.router == nil {
		return zero, portalSessionSnapshot{}, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	lease, encodedLocalHandle, err := m.router.AcquireHandle(handleID)
	if err != nil {
		return zero, portalSessionSnapshot{}, 0, mapRouteError(err)
	}
	route := lease.Target.snapshot()
	if route.session == nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	localHandle, err := decodePortalHandle(route, encodedLocalHandle)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, err
	}
	lease.Target.replies.track(ctx)
	return lease, route, localHandle, nil
}

func (m *SessionMux) acquireNodeHandle(ctx context.Context, nodeID, handleID uint64) (Lease[*portalSessionRoute], portalSessionSnapshot, uint64, uint64, error) {
	var zero Lease[*portalSessionRoute]
	if m == nil || m.router == nil {
		return zero, portalSessionSnapshot{}, 0, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	lease, encodedLocalNode, encodedLocalHandle, err := m.router.AcquireNodeHandle(nodeID, handleID)
	if err != nil {
		return zero, portalSessionSnapshot{}, 0, 0, mapRouteError(err)
	}
	route := lease.Target.snapshot()
	if route.session == nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	localNode, err := decodePortalNode(route, encodedLocalNode)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, err
	}
	localHandle := uint64(0)
	if encodedLocalHandle != 0 {
		localHandle, err = decodePortalHandle(route, encodedLocalHandle)
		if err != nil {
			lease.Release()
			return zero, portalSessionSnapshot{}, 0, 0, err
		}
	}
	lease.Target.replies.track(ctx)
	return lease, route, localNode, localHandle, nil
}

func (m *SessionMux) acquireNodes(
	ctx context.Context, firstNodeID, secondNodeID uint64,
) (Lease[*portalSessionRoute], portalSessionSnapshot, uint64, uint64, error) {
	var zero Lease[*portalSessionRoute]
	if m == nil || m.router == nil {
		return zero, portalSessionSnapshot{}, 0, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	lease, encodedFirst, encodedSecond, err := m.router.AcquireNodes(firstNodeID, secondNodeID)
	if err != nil {
		return zero, portalSessionSnapshot{}, 0, 0, mapRouteError(err)
	}
	route := lease.Target.snapshot()
	if route.session == nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	first, err := decodePortalNode(route, encodedFirst)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, err
	}
	second, err := decodePortalNode(route, encodedSecond)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, err
	}
	lease.Target.replies.track(ctx)
	return lease, route, first, second, nil
}

func (m *SessionMux) acquireCopy(
	ctx context.Context, inputNodeID, inputHandleID, outputNodeID, outputHandleID uint64,
) (Lease[*portalSessionRoute], portalSessionSnapshot, uint64, uint64, uint64, uint64, error) {
	var zero Lease[*portalSessionRoute]
	if m == nil || m.router == nil {
		return zero, portalSessionSnapshot{}, 0, 0, 0, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	lease, encodedInputNode, encodedInputHandle, encodedOutputNode, encodedOutputHandle, err := m.router.AcquireCopy(
		inputNodeID, inputHandleID, outputNodeID, outputHandleID,
	)
	if err != nil {
		return zero, portalSessionSnapshot{}, 0, 0, 0, 0, mapRouteError(err)
	}
	route := lease.Target.snapshot()
	if route.session == nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, 0, 0, newRouteStatusError(ErrSlotNotFound, syscall.ESTALE)
	}
	inputNode, err := decodePortalNode(route, encodedInputNode)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, 0, 0, err
	}
	outputNode, err := decodePortalNode(route, encodedOutputNode)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, 0, 0, err
	}
	inputHandle, err := decodeOptionalPortalHandle(route, encodedInputHandle)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, 0, 0, err
	}
	outputHandle, err := decodeOptionalPortalHandle(route, encodedOutputHandle)
	if err != nil {
		lease.Release()
		return zero, portalSessionSnapshot{}, 0, 0, 0, 0, err
	}
	lease.Target.replies.track(ctx)
	return lease, route, inputNode, inputHandle, outputNode, outputHandle, nil
}

func decodePortalNode(route portalSessionSnapshot, encodedLocalID uint64) (uint64, error) {
	if encodedLocalID == route.rootInode {
		return route.rootInode, nil
	}
	generation, backendLocalID, err := decodeBindingLocalID(encodedLocalID)
	if err != nil {
		return 0, mapRouteError(err)
	}
	if generation != route.generation {
		return 0, staleBindingGenerationError(generation, route.generation)
	}
	return backendLocalID, nil
}

func decodePortalHandle(route portalSessionSnapshot, encodedLocalID uint64) (uint64, error) {
	generation, backendLocalID, err := decodeBindingLocalID(encodedLocalID)
	if err != nil {
		return 0, mapRouteError(err)
	}
	if generation != route.generation {
		return 0, staleBindingGenerationError(generation, route.generation)
	}
	return backendLocalID, nil
}

func decodeOptionalPortalHandle(route portalSessionSnapshot, encodedLocalID uint64) (uint64, error) {
	if encodedLocalID == 0 {
		return 0, nil
	}
	return decodePortalHandle(route, encodedLocalID)
}

func encodeNodeResponse(slot Slot, route portalSessionSnapshot, resp *pb.NodeResponse) error {
	if resp == nil || resp.Inode == 0 {
		return syscall.EIO
	}
	localNode := resp.Inode
	globalNode, err := encodePortalNodeID(slot, route, localNode)
	if err != nil {
		return err
	}
	resp.Inode = globalNode
	if err := encodeAttr(slot, route, localNode, resp.Attr); err != nil {
		return err
	}
	if resp.HandleId != 0 {
		resp.HandleId, err = EncodeBindingHandleID(slot, route.generation, resp.HandleId)
		if err != nil {
			return err
		}
	}
	return nil
}

func encodeOpenHandle(slot Slot, route portalSessionSnapshot, resp *pb.OpenResponse) error {
	if resp == nil || resp.HandleId == 0 {
		return syscall.EIO
	}
	globalHandle, err := EncodeBindingHandleID(slot, route.generation, resp.HandleId)
	if err != nil {
		return err
	}
	resp.HandleId = globalHandle
	return nil
}

func encodeAttr(slot Slot, route portalSessionSnapshot, fallbackLocal uint64, attr *pb.GetAttrResponse) error {
	if attr == nil {
		return nil
	}
	localNode := attr.Ino
	if localNode == 0 {
		localNode = fallbackLocal
	}
	globalNode, err := encodePortalNodeID(slot, route, localNode)
	if err != nil {
		return err
	}
	attr.Ino = globalNode
	return nil
}

func encodeDirEntries(slot Slot, route portalSessionSnapshot, resp *pb.ReadDirResponse) error {
	if resp == nil {
		return syscall.EIO
	}
	for _, entry := range resp.Entries {
		if entry == nil || entry.Inode == 0 {
			return syscall.EIO
		}
		localNode := entry.Inode
		globalNode, err := encodePortalNodeID(slot, route, localNode)
		if err != nil {
			return err
		}
		entry.Inode = globalNode
		if err := encodeAttr(slot, route, localNode, entry.Attr); err != nil {
			return err
		}
	}
	return nil
}

func encodePortalNodeID(slot Slot, route portalSessionSnapshot, backendLocalID uint64) (uint64, error) {
	if backendLocalID == route.rootInode {
		return EncodeNodeID(slot, route.rootInode)
	}
	return EncodeBindingNodeID(slot, route.generation, backendLocalID)
}

func validateBindingGeneration(generation uint64) error {
	if generation == 0 || generation > MaxBindingGeneration {
		return fmt.Errorf(
			"%w: %d is outside [1,%d]",
			ErrInvalidBindingGeneration, generation, MaxBindingGeneration,
		)
	}
	return nil
}

func staleBindingGenerationError(received, current uint64) error {
	return newRouteStatusError(
		fmt.Errorf("%w: request generation %d, current generation %d", ErrStaleBindingGeneration, received, current),
		syscall.ESTALE,
	)
}

func syntheticRootAttr() *pb.GetAttrResponse {
	return &pb.GetAttrResponse{
		Ino:   ShardRootNodeID,
		Mode:  uint32(syscall.S_IFDIR | 0o555),
		Nlink: 2,
		Uid:   0,
		Gid:   0,
		Size:  4096,
	}
}

func syntheticRootStatFS() *pb.StatFsResponse {
	return &pb.StatFsResponse{Bsize: 4096, Frsize: 4096, Namelen: maxPortalRouteNameLen}
}

func validatePortalRouteName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || len(name) > maxPortalRouteNameLen || strings.ContainsAny(name, "/\x00") {
		return "", fmt.Errorf("%w: unsafe route name %q", ErrInvalidPortalRoute, name)
	}
	return name, nil
}

type routeStatusError struct {
	cause error
	errno syscall.Errno
}

func newRouteStatusError(cause error, errno syscall.Errno) error {
	return &routeStatusError{cause: cause, errno: errno}
}

func (e *routeStatusError) Error() string {
	return fmt.Sprintf("%v: %v", e.cause, e.errno)
}

func (e *routeStatusError) Unwrap() error {
	return e.cause
}

func (e *routeStatusError) FuseErrno() syscall.Errno {
	return e.errno
}

func mapRouteError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrCrossPortal):
		return newRouteStatusError(err, syscall.EXDEV)
	case errors.Is(err, ErrSlotDraining), errors.Is(err, ErrSlotSwitching):
		return newRouteStatusError(err, syscall.EAGAIN)
	case errors.Is(err, ErrSlotNotFound),
		errors.Is(err, ErrSyntheticNode),
		errors.Is(err, ErrInvalidSlot),
		errors.Is(err, ErrInvalidLocalID),
		errors.Is(err, ErrInvalidBackendLocalID),
		errors.Is(err, ErrInvalidBindingGeneration),
		errors.Is(err, ErrStaleBindingGeneration):
		return newRouteStatusError(err, syscall.ESTALE)
	default:
		return err
	}
}
