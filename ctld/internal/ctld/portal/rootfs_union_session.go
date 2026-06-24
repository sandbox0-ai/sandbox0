package portal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

const rootFSUnionLowerInodeBase uint64 = 1 << 62

type rootFSUnionBackend uint8

const (
	rootFSUnionBackendUpper rootFSUnionBackend = iota + 1
	rootFSUnionBackendLower
)

type rootFSUnionHandle struct {
	backend      rootFSUnionBackend
	handleID     uint64
	backendInode uint64
	visibleInode uint64
}

type rootFSUnionSession struct {
	engine *s0fs.Engine
	upper  volumefuse.Session
	lower  *rootFSBackedSession

	mu                   sync.Mutex
	nextHandle           uint64
	handles              map[uint64]rootFSUnionHandle
	nextLowerSynthetic   uint64
	lowerSyntheticByRaw  map[uint64]uint64
	lowerRawBySynthetic  map[uint64]uint64
	promotedLowerByInode map[uint64]uint64
	promotedLowerByRaw   map[uint64]uint64
	closed               bool
}

// NewS0FSRootFSSession returns a rootfs view that overlays mutable s0fs state
// on top of the still-mounted base image rootfs.
func NewS0FSRootFSSession(volumeID, teamID string, engine *s0fs.Engine, baseRoot string, logger *logrus.Logger) volumefuse.Session {
	return newRootFSUnionSession(volumeID, engine, NewS0FSSession(volumeID, teamID, engine, logger), newRootFSBackedSession(baseRoot))
}

func newRootFSUnionSession(volumeID string, engine *s0fs.Engine, upper volumefuse.Session, lower *rootFSBackedSession) *rootFSUnionSession {
	return &rootFSUnionSession{
		engine:               engine,
		upper:                upper,
		lower:                lower,
		nextHandle:           1,
		handles:              make(map[uint64]rootFSUnionHandle),
		nextLowerSynthetic:   rootFSUnionLowerInodeBase,
		lowerSyntheticByRaw:  make(map[uint64]uint64),
		lowerRawBySynthetic:  make(map[uint64]uint64),
		promotedLowerByInode: make(map[uint64]uint64),
		promotedLowerByRaw:   make(map[uint64]uint64),
	}
}

func (s *rootFSUnionSession) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	clear(s.handles)
	s.mu.Unlock()
	if s.upper != nil {
		s.upper.Close()
	}
	if s.lower != nil {
		s.lower.Close()
	}
}

func (s *rootFSUnionSession) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	if isRootFSWhiteoutName(req.GetName()) {
		return nil, rootFSUnionNotFound(req.GetName())
	}
	parentPath, err := s.pathForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	if s.upperHasWhiteout(ctx, parentPath, req.GetName()) {
		return nil, rootFSUnionNotFound(req.GetName())
	}
	if resp, ok, err := s.upperLookupChild(ctx, parentPath, req.GetName(), req.GetActor()); err != nil {
		return nil, err
	} else if ok {
		return resp, nil
	}
	if s.upperDirOpaque(ctx, parentPath) {
		return nil, rootFSUnionNotFound(req.GetName())
	}
	lowerParent, ok, err := s.lowerInodeForPath(ctx, parentPath)
	if err != nil || !ok {
		return nil, errOrNotFound(err, parentPath)
	}
	resp, err := s.lower.Lookup(ctx, &pb.LookupRequest{
		VolumeId: req.GetVolumeId(),
		Parent:   lowerParent,
		Name:     req.GetName(),
		Actor:    req.GetActor(),
	})
	if err != nil {
		return nil, err
	}
	return s.mapLowerNodeResponse(resp), nil
}

func (s *rootFSUnionSession) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	backend, backendInode, visibleInode, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	switch backend {
	case rootFSUnionBackendUpper:
		resp, err := s.upper.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: req.GetVolumeId(), Inode: backendInode, Actor: req.GetActor()})
		if err != nil {
			return nil, err
		}
		return cloneAttrWithInode(resp, visibleInode), nil
	case rootFSUnionBackendLower:
		resp, err := s.lower.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: req.GetVolumeId(), Inode: backendInode, Actor: req.GetActor()})
		if err != nil {
			return nil, err
		}
		return cloneAttrWithInode(resp, visibleInode), nil
	default:
		return nil, rootFSUnionNotFound(fmt.Sprintf("inode %d", req.GetInode()))
	}
}

func (s *rootFSUnionSession) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	backend, backendInode, handleID, visibleInode, err := s.routeSetAttrRequest(ctx, req.GetInode(), req.GetHandleId(), req.GetActor())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		backendInode, err = s.copyUpVisibleInode(ctx, visibleInode, req.GetActor())
		if err != nil {
			return nil, err
		}
	}
	resp, err := s.upper.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: req.GetVolumeId(),
		Inode:    backendInode,
		Valid:    req.GetValid(),
		Attr:     req.GetAttr(),
		HandleId: handleID,
		Actor:    req.GetActor(),
	})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Attr = cloneAttrWithInode(resp.GetAttr(), visibleInode)
	}
	return resp, nil
}

func (s *rootFSUnionSession) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	parent, err := s.ensureUpperParentForCreate(ctx, req.GetParent(), req.GetName(), req.GetActor())
	if err != nil {
		return nil, err
	}
	return s.upper.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: req.GetVolumeId(),
		Parent:   parent,
		Name:     req.GetName(),
		Mode:     req.GetMode(),
		Umask:    req.GetUmask(),
		Actor:    req.GetActor(),
	})
}

func (s *rootFSUnionSession) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	parent, err := s.ensureUpperParentForCreate(ctx, req.GetParent(), req.GetName(), req.GetActor())
	if err != nil {
		return nil, err
	}
	resp, err := s.upper.Create(ctx, &pb.CreateRequest{
		VolumeId: req.GetVolumeId(),
		Parent:   parent,
		Name:     req.GetName(),
		Mode:     req.GetMode(),
		Flags:    req.GetFlags(),
		Umask:    req.GetUmask(),
		Actor:    req.GetActor(),
	})
	if err != nil {
		return nil, err
	}
	resp.HandleId = s.trackHandle(rootFSUnionBackendUpper, resp.GetHandleId(), resp.GetInode(), resp.GetInode())
	return resp, nil
}

func (s *rootFSUnionSession) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	return s.unlinkPath(ctx, req.GetParent(), req.GetName(), false, req.GetActor())
}

func (s *rootFSUnionSession) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	lookup, err := s.Lookup(ctx, &pb.LookupRequest{VolumeId: req.GetVolumeId(), Parent: req.GetParent(), Name: req.GetName(), Actor: req.GetActor()})
	if err != nil {
		return nil, err
	}
	entries, err := s.ReadDir(ctx, &pb.ReadDirRequest{VolumeId: req.GetVolumeId(), Inode: lookup.GetInode(), Actor: req.GetActor()})
	if err != nil {
		return nil, err
	}
	if len(entries.GetEntries()) > 0 {
		return nil, fserror.New(fserror.FailedPrecondition, "directory is not empty")
	}
	return s.unlinkPath(ctx, req.GetParent(), req.GetName(), true, req.GetActor())
}

func (s *rootFSUnionSession) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	oldParentPath, err := s.pathForInode(req.GetOldParent())
	if err != nil {
		return nil, err
	}
	oldPath := rootFSUnionChildPath(oldParentPath, req.GetOldName())
	source, err := s.Lookup(ctx, &pb.LookupRequest{VolumeId: req.GetVolumeId(), Parent: req.GetOldParent(), Name: req.GetOldName(), Actor: req.GetActor()})
	if err != nil {
		return nil, err
	}
	oldWasLower := s.isLowerVisibleInode(source.GetInode())
	if oldWasLower {
		if err := s.copyUpTree(ctx, oldPath, req.GetActor()); err != nil {
			return nil, err
		}
		var ok bool
		_, ok, err = s.upperInodeForPath(ctx, oldPath, req.GetActor())
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, rootFSUnionNotFound(oldPath)
		}
	}
	newParent, err := s.ensureUpperDirForInode(ctx, req.GetNewParent(), req.GetActor())
	if err != nil {
		return nil, err
	}
	oldUpperParent, ok, err := s.upperInodeForPath(ctx, oldParentPath, req.GetActor())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, rootFSUnionNotFound(oldParentPath)
	}
	newParentPath, err := s.pathForInode(req.GetNewParent())
	if err != nil {
		return nil, err
	}
	if err := s.removeUpperWhiteout(ctx, newParentPath, req.GetNewName(), req.GetActor()); err != nil {
		return nil, err
	}
	if _, err := s.upper.Rename(ctx, &pb.RenameRequest{
		VolumeId:  req.GetVolumeId(),
		OldParent: oldUpperParent,
		OldName:   req.GetOldName(),
		NewParent: newParent,
		NewName:   req.GetNewName(),
		Flags:     req.GetFlags(),
		Actor:     req.GetActor(),
	}); err != nil {
		return nil, err
	}
	if s.lowerPathVisible(ctx, oldPath) || oldWasLower {
		if err := s.createUpperWhiteout(ctx, oldParentPath, req.GetOldName(), req.GetActor()); err != nil {
			return nil, err
		}
	}
	return &pb.Empty{}, nil
}

func (s *rootFSUnionSession) Link(ctx context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	backend, inode, _, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		inode, err = s.copyUpVisibleInode(ctx, req.GetInode(), req.GetActor())
		if err != nil {
			return nil, err
		}
	}
	parent, err := s.ensureUpperParentForCreate(ctx, req.GetNewParent(), req.GetNewName(), req.GetActor())
	if err != nil {
		return nil, err
	}
	return s.upper.Link(ctx, &pb.LinkRequest{
		VolumeId:  req.GetVolumeId(),
		Inode:     inode,
		NewParent: parent,
		NewName:   req.GetNewName(),
		Actor:     req.GetActor(),
	})
}

func (s *rootFSUnionSession) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	parent, err := s.ensureUpperParentForCreate(ctx, req.GetParent(), req.GetName(), req.GetActor())
	if err != nil {
		return nil, err
	}
	return s.upper.Symlink(ctx, &pb.SymlinkRequest{
		VolumeId: req.GetVolumeId(),
		Parent:   parent,
		Name:     req.GetName(),
		Target:   req.GetTarget(),
		Actor:    req.GetActor(),
	})
}

func (s *rootFSUnionSession) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	backend, inode, _, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		return s.lower.Readlink(ctx, &pb.ReadlinkRequest{VolumeId: req.GetVolumeId(), Inode: inode, Actor: req.GetActor()})
	}
	return s.upper.Readlink(ctx, &pb.ReadlinkRequest{VolumeId: req.GetVolumeId(), Inode: inode, Actor: req.GetActor()})
}

func (s *rootFSUnionSession) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	backend, inode, _, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		return s.lower.Access(ctx, &pb.AccessRequest{
			VolumeId: req.GetVolumeId(),
			Inode:    inode,
			Mask:     req.GetMask(),
			Uid:      req.GetUid(),
			Gids:     req.GetGids(),
			Actor:    req.GetActor(),
		})
	}
	return s.upper.Access(ctx, &pb.AccessRequest{
		VolumeId: req.GetVolumeId(),
		Inode:    inode,
		Mask:     req.GetMask(),
		Uid:      req.GetUid(),
		Gids:     req.GetGids(),
		Actor:    req.GetActor(),
	})
}

func (s *rootFSUnionSession) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	backend, inode, visibleInode, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower && rootFSUnionOpenNeedsUpper(req.GetFlags()) {
		inode, err = s.copyUpVisibleInode(ctx, visibleInode, req.GetActor())
		if err != nil {
			return nil, err
		}
		backend = rootFSUnionBackendUpper
	}
	if backend == rootFSUnionBackendLower {
		resp, err := s.lower.Open(ctx, &pb.OpenRequest{VolumeId: req.GetVolumeId(), Inode: inode, Flags: req.GetFlags(), Actor: req.GetActor()})
		if err != nil {
			return nil, err
		}
		resp.HandleId = s.trackHandle(rootFSUnionBackendLower, resp.GetHandleId(), inode, visibleInode)
		return resp, nil
	}
	resp, err := s.upper.Open(ctx, &pb.OpenRequest{VolumeId: req.GetVolumeId(), Inode: inode, Flags: req.GetFlags(), Actor: req.GetActor()})
	if err != nil {
		return nil, err
	}
	resp.HandleId = s.trackHandle(rootFSUnionBackendUpper, resp.GetHandleId(), inode, visibleInode)
	return resp, nil
}

func (s *rootFSUnionSession) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	backend, inode, handleID, err := s.routeFileRequest(req.GetInode(), req.GetHandleId())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		return s.lower.Read(ctx, &pb.ReadRequest{
			VolumeId: req.GetVolumeId(),
			Inode:    inode,
			Offset:   req.GetOffset(),
			Size:     req.GetSize(),
			HandleId: handleID,
			Actor:    req.GetActor(),
		})
	}
	return s.upper.Read(ctx, &pb.ReadRequest{
		VolumeId: req.GetVolumeId(),
		Inode:    inode,
		Offset:   req.GetOffset(),
		Size:     req.GetSize(),
		HandleId: handleID,
		Actor:    req.GetActor(),
	})
}

func (s *rootFSUnionSession) ReadInto(ctx context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	backend, inode, handleID, err := s.routeFileRequest(req.GetInode(), req.GetHandleId())
	if err != nil {
		return 0, false, err
	}
	next := &pb.ReadRequest{
		VolumeId: req.GetVolumeId(),
		Inode:    inode,
		Offset:   req.GetOffset(),
		Size:     req.GetSize(),
		HandleId: handleID,
		Actor:    req.GetActor(),
	}
	if backend == rootFSUnionBackendUpper {
		if reader, ok := s.upper.(volumefuse.ReadIntoSession); ok {
			return reader.ReadInto(ctx, next, dest)
		}
		resp, err := s.upper.Read(ctx, next)
		if err != nil {
			return 0, false, err
		}
		n := copy(dest, resp.GetData())
		return n, resp.GetEof() || n < len(dest), nil
	}
	resp, err := s.lower.Read(ctx, next)
	if err != nil {
		return 0, false, err
	}
	n := copy(dest, resp.GetData())
	return n, resp.GetEof() || n < len(dest), nil
}

func (s *rootFSUnionSession) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	backend, inode, handleID, visibleInode, err := s.routeWriteRequest(ctx, req.GetInode(), req.GetHandleId(), req.GetActor())
	if err != nil {
		return nil, err
	}
	resp, err := s.upper.Write(ctx, &pb.WriteRequest{
		VolumeId: req.GetVolumeId(),
		Inode:    inode,
		Offset:   req.GetOffset(),
		Data:     req.GetData(),
		HandleId: handleID,
		Actor:    req.GetActor(),
	})
	if err == nil && backend == rootFSUnionBackendLower {
		s.promoteVisibleInode(visibleInode, inode)
	}
	return resp, err
}

func (s *rootFSUnionSession) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	handle, ok := s.takeHandle(req.GetHandleId())
	if !ok {
		backend, inode, _, err := s.routeInode(req.GetInode())
		if err != nil {
			return &pb.Empty{}, nil
		}
		if backend == rootFSUnionBackendLower {
			return s.lower.Release(ctx, &pb.ReleaseRequest{VolumeId: req.GetVolumeId(), Inode: inode, HandleId: req.GetHandleId(), Actor: req.GetActor()})
		}
		return s.upper.Release(ctx, &pb.ReleaseRequest{VolumeId: req.GetVolumeId(), Inode: inode, HandleId: req.GetHandleId(), Actor: req.GetActor()})
	}
	if handle.backend == rootFSUnionBackendLower {
		return s.lower.Release(ctx, &pb.ReleaseRequest{VolumeId: req.GetVolumeId(), Inode: handle.backendInode, HandleId: handle.handleID, Actor: req.GetActor()})
	}
	return s.upper.Release(ctx, &pb.ReleaseRequest{VolumeId: req.GetVolumeId(), Inode: handle.backendInode, HandleId: handle.handleID, Actor: req.GetActor()})
}

func (s *rootFSUnionSession) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	handle, ok := s.lookupHandle(req.GetHandleId())
	if !ok {
		return &pb.Empty{}, nil
	}
	if handle.backend == rootFSUnionBackendLower {
		return s.lower.Flush(ctx, &pb.FlushRequest{VolumeId: req.GetVolumeId(), HandleId: handle.handleID, Actor: req.GetActor()})
	}
	return s.upper.Flush(ctx, &pb.FlushRequest{VolumeId: req.GetVolumeId(), HandleId: handle.handleID, Actor: req.GetActor()})
}

func (s *rootFSUnionSession) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	handle, ok := s.lookupHandle(req.GetHandleId())
	if !ok {
		return &pb.Empty{}, nil
	}
	if handle.backend == rootFSUnionBackendLower {
		return s.lower.Fsync(ctx, &pb.FsyncRequest{VolumeId: req.GetVolumeId(), HandleId: handle.handleID, Datasync: req.GetDatasync(), Actor: req.GetActor()})
	}
	return s.upper.Fsync(ctx, &pb.FsyncRequest{VolumeId: req.GetVolumeId(), HandleId: handle.handleID, Datasync: req.GetDatasync(), Actor: req.GetActor()})
}

func (s *rootFSUnionSession) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	backend, inode, handleID, visibleInode, err := s.routeWriteRequest(ctx, req.GetInode(), req.GetHandleId(), req.GetActor())
	if err != nil {
		return nil, err
	}
	resp, err := s.upper.Fallocate(ctx, &pb.FallocateRequest{
		VolumeId: req.GetVolumeId(),
		Inode:    inode,
		Mode:     req.GetMode(),
		Offset:   req.GetOffset(),
		Length:   req.GetLength(),
		HandleId: handleID,
		Actor:    req.GetActor(),
	})
	if err == nil && backend == rootFSUnionBackendLower {
		s.promoteVisibleInode(visibleInode, inode)
	}
	return resp, err
}

func (s *rootFSUnionSession) CopyFileRange(ctx context.Context, req *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	inBackend, inodeIn, handleIn, err := s.routeFileRequest(req.GetInodeIn(), req.GetHandleIn())
	if err != nil {
		return nil, err
	}
	if inBackend == rootFSUnionBackendLower {
		inodeIn, err = s.copyUpVisibleInode(ctx, req.GetInodeIn(), req.GetActor())
		if err != nil {
			return nil, err
		}
		handleIn = 0
	}
	_, inodeOut, handleOut, _, err := s.routeWriteRequest(ctx, req.GetInodeOut(), req.GetHandleOut(), req.GetActor())
	if err != nil {
		return nil, err
	}
	return s.upper.CopyFileRange(ctx, &pb.CopyFileRangeRequest{
		VolumeId:  req.GetVolumeId(),
		InodeIn:   inodeIn,
		HandleIn:  handleIn,
		OffsetIn:  req.GetOffsetIn(),
		InodeOut:  inodeOut,
		HandleOut: handleOut,
		OffsetOut: req.GetOffsetOut(),
		Length:    req.GetLength(),
		Flags:     req.GetFlags(),
		Actor:     req.GetActor(),
	})
}

func (s *rootFSUnionSession) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	backend, inode, visibleInode, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		resp, err := s.lower.OpenDir(ctx, &pb.OpenDirRequest{VolumeId: req.GetVolumeId(), Inode: inode, Flags: req.GetFlags(), Actor: req.GetActor()})
		if err != nil {
			return nil, err
		}
		resp.HandleId = s.trackHandle(rootFSUnionBackendLower, resp.GetHandleId(), inode, visibleInode)
		return resp, nil
	}
	resp, err := s.upper.OpenDir(ctx, &pb.OpenDirRequest{VolumeId: req.GetVolumeId(), Inode: inode, Flags: req.GetFlags(), Actor: req.GetActor()})
	if err != nil {
		return nil, err
	}
	resp.HandleId = s.trackHandle(rootFSUnionBackendUpper, resp.GetHandleId(), inode, visibleInode)
	return resp, nil
}

func (s *rootFSUnionSession) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	dirPath, err := s.pathForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	upperEntries := make(map[string]*pb.DirEntry)
	hidden := make(map[string]struct{})
	opaque := s.upperDirOpaque(ctx, dirPath)
	if upperInode, ok, err := s.upperInodeForPath(ctx, dirPath, req.GetActor()); err != nil {
		return nil, err
	} else if ok {
		resp, err := s.upper.ReadDir(ctx, &pb.ReadDirRequest{
			VolumeId: req.GetVolumeId(),
			Inode:    upperInode,
			HandleId: s.backendHandleID(req.GetHandleId(), rootFSUnionBackendUpper),
			Offset:   0,
			Size:     req.GetSize(),
			Plus:     true,
			Actor:    req.GetActor(),
		})
		if err != nil {
			return nil, err
		}
		for _, entry := range resp.GetEntries() {
			name := entry.GetName()
			switch {
			case name == ".wh..wh..opq":
				opaque = true
			case strings.HasPrefix(name, ".wh."):
				hidden[strings.TrimPrefix(name, ".wh.")] = struct{}{}
			default:
				upperEntries[name] = cloneDirEntry(entry)
			}
		}
	}
	merged := make(map[string]*pb.DirEntry, len(upperEntries))
	for name, entry := range upperEntries {
		merged[name] = entry
	}
	if !opaque {
		if lowerInode, ok, err := s.lowerInodeForPath(ctx, dirPath); err != nil {
			return nil, err
		} else if ok {
			resp, err := s.lower.ReadDir(ctx, &pb.ReadDirRequest{
				VolumeId: req.GetVolumeId(),
				Inode:    lowerInode,
				HandleId: s.backendHandleID(req.GetHandleId(), rootFSUnionBackendLower),
				Offset:   0,
				Size:     req.GetSize(),
				Plus:     true,
				Actor:    req.GetActor(),
			})
			if err != nil {
				return nil, err
			}
			for _, entry := range resp.GetEntries() {
				name := entry.GetName()
				if _, ok := hidden[name]; ok {
					continue
				}
				if _, ok := merged[name]; ok {
					continue
				}
				merged[name] = s.mapLowerDirEntry(entry)
			}
		}
	}
	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	slices.Sort(names)
	start := int(req.GetOffset())
	if start < 0 {
		start = 0
	}
	if start > len(names) {
		start = len(names)
	}
	out := make([]*pb.DirEntry, 0, len(names)-start)
	for i, name := range names[start:] {
		entry := cloneDirEntry(merged[name])
		entry.Offset = uint64(start + i + 1)
		out = append(out, entry)
	}
	return &pb.ReadDirResponse{Entries: out, Eof: true}, nil
}

func (s *rootFSUnionSession) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	handle, ok := s.takeHandle(req.GetHandleId())
	if !ok {
		return &pb.Empty{}, nil
	}
	if handle.backend == rootFSUnionBackendLower {
		return s.lower.ReleaseDir(ctx, &pb.ReleaseDirRequest{VolumeId: req.GetVolumeId(), Inode: handle.backendInode, HandleId: handle.handleID, Actor: req.GetActor()})
	}
	return s.upper.ReleaseDir(ctx, &pb.ReleaseDirRequest{VolumeId: req.GetVolumeId(), Inode: handle.backendInode, HandleId: handle.handleID, Actor: req.GetActor()})
}

func (s *rootFSUnionSession) StatFs(ctx context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	return s.upper.StatFs(ctx, req)
}

func (s *rootFSUnionSession) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	backend, inode, _, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		return s.lower.GetXattr(ctx, &pb.GetXattrRequest{VolumeId: req.GetVolumeId(), Inode: inode, Name: req.GetName(), Size: req.GetSize(), Actor: req.GetActor()})
	}
	return s.upper.GetXattr(ctx, &pb.GetXattrRequest{VolumeId: req.GetVolumeId(), Inode: inode, Name: req.GetName(), Size: req.GetSize(), Actor: req.GetActor()})
}

func (s *rootFSUnionSession) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	backend, inode, visibleInode, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		inode, err = s.copyUpVisibleInode(ctx, visibleInode, req.GetActor())
		if err != nil {
			return nil, err
		}
	}
	return s.upper.SetXattr(ctx, &pb.SetXattrRequest{VolumeId: req.GetVolumeId(), Inode: inode, Name: req.GetName(), Value: req.GetValue(), Flags: req.GetFlags(), Actor: req.GetActor()})
}

func (s *rootFSUnionSession) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	backend, inode, _, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		return s.lower.ListXattr(ctx, &pb.ListXattrRequest{VolumeId: req.GetVolumeId(), Inode: inode, Size: req.GetSize(), Actor: req.GetActor()})
	}
	return s.upper.ListXattr(ctx, &pb.ListXattrRequest{VolumeId: req.GetVolumeId(), Inode: inode, Size: req.GetSize(), Actor: req.GetActor()})
}

func (s *rootFSUnionSession) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	backend, inode, visibleInode, err := s.routeInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if backend == rootFSUnionBackendLower {
		inode, err = s.copyUpVisibleInode(ctx, visibleInode, req.GetActor())
		if err != nil {
			return nil, err
		}
	}
	return s.upper.RemoveXattr(ctx, &pb.RemoveXattrRequest{VolumeId: req.GetVolumeId(), Inode: inode, Name: req.GetName(), Actor: req.GetActor()})
}

func (s *rootFSUnionSession) Mknod(ctx context.Context, req *pb.MknodRequest) (*pb.NodeResponse, error) {
	parent, err := s.ensureUpperParentForCreate(ctx, req.GetParent(), req.GetName(), req.GetActor())
	if err != nil {
		return nil, err
	}
	return s.upper.Mknod(ctx, &pb.MknodRequest{
		VolumeId: req.GetVolumeId(),
		Parent:   parent,
		Name:     req.GetName(),
		Mode:     req.GetMode(),
		Rdev:     req.GetRdev(),
		Umask:    req.GetUmask(),
		Actor:    req.GetActor(),
	})
}

func (s *rootFSUnionSession) GetLk(ctx context.Context, req *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	backend, inode, handleID, err := s.routeFileRequest(req.GetInode(), req.GetHandleId())
	if err != nil {
		return nil, err
	}
	next := &pb.GetLkRequest{VolumeId: req.GetVolumeId(), Inode: inode, HandleId: handleID, Owner: req.GetOwner(), Lock: req.GetLock(), Actor: req.GetActor()}
	if backend == rootFSUnionBackendLower {
		return s.lower.GetLk(ctx, next)
	}
	return s.upper.GetLk(ctx, next)
}

func (s *rootFSUnionSession) SetLk(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	return s.setLock(ctx, req)
}

func (s *rootFSUnionSession) SetLkw(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	return s.setLock(ctx, req)
}

func (s *rootFSUnionSession) Flock(ctx context.Context, req *pb.FlockRequest) (*pb.Empty, error) {
	backend, inode, handleID, err := s.routeFileRequest(req.GetInode(), req.GetHandleId())
	if err != nil {
		return nil, err
	}
	next := &pb.FlockRequest{VolumeId: req.GetVolumeId(), Inode: inode, HandleId: handleID, Owner: req.GetOwner(), Typ: req.GetTyp(), Block: req.GetBlock(), Actor: req.GetActor()}
	if backend == rootFSUnionBackendLower {
		return s.lower.Flock(ctx, next)
	}
	return s.upper.Flock(ctx, next)
}

func (s *rootFSUnionSession) setLock(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	backend, inode, handleID, err := s.routeFileRequest(req.GetInode(), req.GetHandleId())
	if err != nil {
		return nil, err
	}
	next := &pb.SetLkRequest{VolumeId: req.GetVolumeId(), Inode: inode, HandleId: handleID, Owner: req.GetOwner(), Lock: req.GetLock(), Block: req.GetBlock(), Actor: req.GetActor()}
	if backend == rootFSUnionBackendLower {
		if req.GetBlock() {
			return s.lower.SetLkw(ctx, next)
		}
		return s.lower.SetLk(ctx, next)
	}
	if req.GetBlock() {
		return s.upper.SetLkw(ctx, next)
	}
	return s.upper.SetLk(ctx, next)
}

func (s *rootFSUnionSession) unlinkPath(ctx context.Context, parentInode uint64, name string, dir bool, actor *pb.PosixActor) (*pb.Empty, error) {
	parentPath, err := s.pathForInode(parentInode)
	if err != nil {
		return nil, err
	}
	childPath := rootFSUnionChildPath(parentPath, name)
	upperParent, upperParentOK, err := s.upperInodeForPath(ctx, parentPath, actor)
	if err != nil {
		return nil, err
	}
	upperChild, upperChildOK, err := s.upperLookupChild(ctx, parentPath, name, actor)
	if err != nil {
		return nil, err
	}
	lowerVisible := s.lowerPathVisible(ctx, childPath)
	if !upperChildOK && !lowerVisible {
		return nil, rootFSUnionNotFound(childPath)
	}
	if upperChildOK {
		if dir {
			_, err = s.upper.Rmdir(ctx, &pb.RmdirRequest{Parent: upperParent, Name: name, Actor: actor})
		} else {
			_, err = s.upper.Unlink(ctx, &pb.UnlinkRequest{Parent: upperParent, Name: name, Actor: actor})
		}
		if err != nil {
			return nil, err
		}
		s.dropPromotedVisible(upperChild.GetInode(), upperChild.GetInode())
	}
	if lowerVisible {
		if !upperParentOK {
			upperParent, err = s.ensureUpperDirPath(ctx, parentPath, actor)
			if err != nil {
				return nil, err
			}
		}
		if err := s.createUpperWhiteoutWithParent(ctx, upperParent, name, actor); err != nil {
			return nil, err
		}
	}
	return &pb.Empty{}, nil
}

func (s *rootFSUnionSession) ensureUpperParentForCreate(ctx context.Context, parentInode uint64, name string, actor *pb.PosixActor) (uint64, error) {
	parentPath, err := s.pathForInode(parentInode)
	if err != nil {
		return 0, err
	}
	childPath := rootFSUnionChildPath(parentPath, name)
	if s.lowerPathVisible(ctx, childPath) {
		return 0, fserror.New(fserror.AlreadyExists, childPath+" already exists")
	}
	parent, err := s.ensureUpperDirPath(ctx, parentPath, actor)
	if err != nil {
		return 0, err
	}
	if err := s.removeUpperWhiteout(ctx, parentPath, name, actor); err != nil {
		return 0, err
	}
	return parent, nil
}

func (s *rootFSUnionSession) ensureUpperDirForInode(ctx context.Context, inode uint64, actor *pb.PosixActor) (uint64, error) {
	p, err := s.pathForInode(inode)
	if err != nil {
		return 0, err
	}
	return s.ensureUpperDirPath(ctx, p, actor)
}

func (s *rootFSUnionSession) ensureUpperDirPath(ctx context.Context, target string, actor *pb.PosixActor) (uint64, error) {
	target = cleanRootFSUnionPath(target)
	if target == "/" {
		return s0fs.RootInode, nil
	}
	if inode, ok, err := s.upperInodeForPath(ctx, target, actor); err != nil || ok {
		return inode, err
	}
	parentPath := path.Dir(target)
	parent, err := s.ensureUpperDirPath(ctx, parentPath, actor)
	if err != nil {
		return 0, err
	}
	name := path.Base(target)
	if err := s.removeUpperWhiteout(ctx, parentPath, name, actor); err != nil {
		return 0, err
	}
	mode := uint32(0o755)
	if lowerInode, ok, err := s.lowerInodeForPath(ctx, target); err != nil {
		return 0, err
	} else if ok {
		attr, err := s.lower.GetAttr(ctx, &pb.GetAttrRequest{Inode: lowerInode, Actor: actor})
		if err != nil {
			return 0, err
		}
		mode = attr.GetMode() & 0o7777
	}
	node, err := s.engine.Mkdir(parent, name, mode)
	if err != nil && !errorsIsS0FSExists(err) {
		return 0, mapLocalS0FSError(err)
	}
	if err == nil {
		s.copyMetadataFromLower(ctx, target, node.Inode, actor)
		return node.Inode, nil
	}
	inode, ok, err := s.upperInodeForPath(ctx, target, actor)
	if err != nil || !ok {
		return 0, errOrNotFound(err, target)
	}
	return inode, nil
}

func (s *rootFSUnionSession) copyUpVisibleInode(ctx context.Context, visibleInode uint64, actor *pb.PosixActor) (uint64, error) {
	backend, backendInode, _, err := s.routeInode(visibleInode)
	if err != nil {
		return 0, err
	}
	if backend == rootFSUnionBackendUpper {
		return backendInode, nil
	}
	p, err := s.pathForInode(visibleInode)
	if err != nil {
		return 0, err
	}
	upperInode, err := s.copyUpPath(ctx, p, actor)
	if err != nil {
		return 0, err
	}
	s.promoteVisibleInode(visibleInode, upperInode)
	return upperInode, nil
}

func (s *rootFSUnionSession) copyUpTree(ctx context.Context, target string, actor *pb.PosixActor) error {
	target = cleanRootFSUnionPath(target)
	if _, err := s.copyUpPath(ctx, target, actor); err != nil {
		return err
	}
	lowerInode, ok, err := s.lowerInodeForPath(ctx, target)
	if err != nil || !ok {
		return err
	}
	attr, err := s.lower.GetAttr(ctx, &pb.GetAttrRequest{Inode: lowerInode, Actor: actor})
	if err != nil {
		return err
	}
	if attr.GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return nil
	}
	entries, err := s.lower.ReadDir(ctx, &pb.ReadDirRequest{Inode: lowerInode, Plus: true, Actor: actor})
	if err != nil {
		return err
	}
	for _, entry := range entries.GetEntries() {
		childPath := rootFSUnionChildPath(target, entry.GetName())
		if err := s.copyUpTree(ctx, childPath, actor); err != nil {
			return err
		}
	}
	return nil
}

func (s *rootFSUnionSession) copyUpPath(ctx context.Context, target string, actor *pb.PosixActor) (uint64, error) {
	target = cleanRootFSUnionPath(target)
	if inode, ok, err := s.upperInodeForPath(ctx, target, actor); err != nil || ok {
		return inode, err
	}
	lowerInode, ok, err := s.lowerInodeForPath(ctx, target)
	if err != nil || !ok {
		return 0, errOrNotFound(err, target)
	}
	attr, err := s.lower.GetAttr(ctx, &pb.GetAttrRequest{Inode: lowerInode, Actor: actor})
	if err != nil {
		return 0, err
	}
	parentPath := path.Dir(target)
	parent, err := s.ensureUpperDirPath(ctx, parentPath, actor)
	if err != nil {
		return 0, err
	}
	name := path.Base(target)
	if err := s.removeUpperWhiteout(ctx, parentPath, name, actor); err != nil {
		return 0, err
	}
	var node *s0fs.Node
	mode := attr.GetMode() & 0o7777
	switch attr.GetMode() & syscall.S_IFMT {
	case syscall.S_IFDIR:
		node, err = s.engine.Mkdir(parent, name, mode)
	case syscall.S_IFLNK:
		link, readlinkErr := s.lower.Readlink(ctx, &pb.ReadlinkRequest{Inode: lowerInode, Actor: actor})
		if readlinkErr != nil {
			return 0, readlinkErr
		}
		node, err = s.engine.Symlink(parent, name, link.GetTarget(), mode)
	case syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK:
		node, err = s.engine.Mknod(parent, name, attr.GetMode(), attr.GetRdev())
	default:
		node, err = s.engine.CreateFile(parent, name, mode)
	}
	if err != nil && !errorsIsS0FSExists(err) {
		return 0, mapLocalS0FSError(err)
	}
	if err != nil {
		inode, ok, lookupErr := s.upperInodeForPath(ctx, target, actor)
		if lookupErr != nil || !ok {
			return 0, errOrNotFound(lookupErr, target)
		}
		return inode, nil
	}
	_ = s.engine.SetOwner(node.Inode, attr.GetUid(), attr.GetGid())
	if node.Type == s0fs.TypeFile && attr.GetSize() > 0 {
		if err := s.copyLowerFileData(ctx, lowerInode, node.Inode, attr.GetSize(), actor); err != nil {
			return 0, err
		}
	}
	s.copyLowerXattrs(ctx, lowerInode, node.Inode, actor)
	s.promoteLowerRaw(lowerInode, node.Inode)
	return node.Inode, nil
}

func (s *rootFSUnionSession) copyLowerFileData(ctx context.Context, lowerInode, upperInode, size uint64, actor *pb.PosixActor) error {
	const chunkSize = 256 * 1024
	for offset := uint64(0); offset < size; {
		want := uint64(chunkSize)
		if remaining := size - offset; remaining < want {
			want = remaining
		}
		resp, err := s.lower.Read(ctx, &pb.ReadRequest{Inode: lowerInode, Offset: int64(offset), Size: int64(want), Actor: actor})
		if err != nil {
			return err
		}
		if len(resp.GetData()) == 0 {
			break
		}
		if _, err := s.engine.Write(upperInode, offset, resp.GetData()); err != nil {
			return mapLocalS0FSError(err)
		}
		offset += uint64(len(resp.GetData()))
	}
	return nil
}

func (s *rootFSUnionSession) copyMetadataFromLower(ctx context.Context, lowerPath string, upperInode uint64, actor *pb.PosixActor) {
	lowerInode, ok, err := s.lowerInodeForPath(ctx, lowerPath)
	if err != nil || !ok {
		return
	}
	attr, err := s.lower.GetAttr(ctx, &pb.GetAttrRequest{Inode: lowerInode, Actor: actor})
	if err == nil {
		_ = s.engine.SetOwner(upperInode, attr.GetUid(), attr.GetGid())
	}
	s.copyLowerXattrs(ctx, lowerInode, upperInode, actor)
}

func (s *rootFSUnionSession) copyLowerXattrs(ctx context.Context, lowerInode, upperInode uint64, actor *pb.PosixActor) {
	resp, err := s.lower.ListXattr(ctx, &pb.ListXattrRequest{Inode: lowerInode, Actor: actor})
	if err != nil {
		return
	}
	for _, name := range rootFSUnionXattrNames(resp.GetData()) {
		value, err := s.lower.GetXattr(ctx, &pb.GetXattrRequest{Inode: lowerInode, Name: name, Actor: actor})
		if err != nil {
			continue
		}
		_ = s.engine.SetXattr(upperInode, name, value.GetValue(), 0)
	}
}

func (s *rootFSUnionSession) upperLookupChild(ctx context.Context, parentPath, name string, actor *pb.PosixActor) (*pb.NodeResponse, bool, error) {
	parent, ok, err := s.upperInodeForPath(ctx, parentPath, actor)
	if err != nil || !ok {
		return nil, false, err
	}
	resp, err := s.upper.Lookup(ctx, &pb.LookupRequest{Parent: parent, Name: name, Actor: actor})
	if err != nil {
		if isRootFSUnionNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if isRootFSWhiteoutName(name) {
		return nil, false, nil
	}
	return resp, true, nil
}

func (s *rootFSUnionSession) upperInodeForPath(ctx context.Context, target string, actor *pb.PosixActor) (uint64, bool, error) {
	target = cleanRootFSUnionPath(target)
	if target == "/" {
		return s0fs.RootInode, true, nil
	}
	inode := s0fs.RootInode
	current := "/"
	for _, component := range rootFSUnionComponents(target) {
		if s.upperHasWhiteout(ctx, current, component) {
			return 0, false, nil
		}
		resp, err := s.upper.Lookup(ctx, &pb.LookupRequest{Parent: inode, Name: component, Actor: actor})
		if err != nil {
			if isRootFSUnionNotFound(err) {
				return 0, false, nil
			}
			return 0, false, err
		}
		inode = resp.GetInode()
		current = rootFSUnionChildPath(current, component)
	}
	return inode, true, nil
}

func (s *rootFSUnionSession) lowerInodeForPath(ctx context.Context, target string) (uint64, bool, error) {
	target = cleanRootFSUnionPath(target)
	if target == "/" {
		return s0fs.RootInode, true, nil
	}
	inode := s0fs.RootInode
	for _, component := range rootFSUnionComponents(target) {
		resp, err := s.lower.Lookup(ctx, &pb.LookupRequest{Parent: inode, Name: component})
		if err != nil {
			if isRootFSUnionNotFound(err) {
				return 0, false, nil
			}
			return 0, false, err
		}
		inode = resp.GetInode()
	}
	return inode, true, nil
}

func (s *rootFSUnionSession) lowerPathVisible(ctx context.Context, target string) bool {
	target = cleanRootFSUnionPath(target)
	parentPath := path.Dir(target)
	name := path.Base(target)
	if s.upperHasWhiteout(ctx, parentPath, name) || s.upperDirOpaque(ctx, parentPath) {
		return false
	}
	if _, ok, _ := s.upperInodeForPath(ctx, target, nil); ok {
		return false
	}
	_, ok, err := s.lowerInodeForPath(ctx, target)
	return err == nil && ok
}

func (s *rootFSUnionSession) upperHasWhiteout(ctx context.Context, parentPath, name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	parent, ok, err := s.upperInodeForPathNoWhiteout(ctx, parentPath)
	if err != nil || !ok {
		return false
	}
	_, err = s.upper.Lookup(ctx, &pb.LookupRequest{Parent: parent, Name: ".wh." + name})
	return err == nil
}

func (s *rootFSUnionSession) upperDirOpaque(ctx context.Context, dirPath string) bool {
	parent, ok, err := s.upperInodeForPathNoWhiteout(ctx, dirPath)
	if err != nil || !ok {
		return false
	}
	_, err = s.upper.Lookup(ctx, &pb.LookupRequest{Parent: parent, Name: ".wh..wh..opq"})
	return err == nil
}

func (s *rootFSUnionSession) upperInodeForPathNoWhiteout(ctx context.Context, target string) (uint64, bool, error) {
	target = cleanRootFSUnionPath(target)
	if target == "/" {
		return s0fs.RootInode, true, nil
	}
	inode := s0fs.RootInode
	for _, component := range rootFSUnionComponents(target) {
		resp, err := s.upper.Lookup(ctx, &pb.LookupRequest{Parent: inode, Name: component})
		if err != nil {
			if isRootFSUnionNotFound(err) {
				return 0, false, nil
			}
			return 0, false, err
		}
		inode = resp.GetInode()
	}
	return inode, true, nil
}

func (s *rootFSUnionSession) createUpperWhiteout(ctx context.Context, parentPath, name string, actor *pb.PosixActor) error {
	parent, err := s.ensureUpperDirPath(ctx, parentPath, actor)
	if err != nil {
		return err
	}
	return s.createUpperWhiteoutWithParent(ctx, parent, name, actor)
}

func (s *rootFSUnionSession) createUpperWhiteoutWithParent(ctx context.Context, parent uint64, name string, actor *pb.PosixActor) error {
	whiteoutName := ".wh." + name
	if _, err := s.upper.Lookup(ctx, &pb.LookupRequest{Parent: parent, Name: whiteoutName, Actor: actor}); err == nil {
		return nil
	}
	node, err := s.engine.CreateFile(parent, whiteoutName, 0)
	if err != nil && !errorsIsS0FSExists(err) {
		return mapLocalS0FSError(err)
	}
	if node != nil && actor != nil && len(actor.GetGids()) > 0 {
		_ = s.engine.SetOwner(node.Inode, actor.GetUid(), actor.GetGids()[0])
	}
	return nil
}

func (s *rootFSUnionSession) removeUpperWhiteout(ctx context.Context, parentPath, name string, actor *pb.PosixActor) error {
	parent, ok, err := s.upperInodeForPathNoWhiteout(ctx, parentPath)
	if err != nil || !ok {
		return err
	}
	whiteoutName := ".wh." + name
	if _, err := s.upper.Lookup(ctx, &pb.LookupRequest{Parent: parent, Name: whiteoutName, Actor: actor}); err != nil {
		if isRootFSUnionNotFound(err) {
			return nil
		}
		return err
	}
	if err := s.engine.Unlink(parent, whiteoutName); err != nil && !isS0FSNotFound(err) {
		return mapLocalS0FSError(err)
	}
	return nil
}

func (s *rootFSUnionSession) pathForInode(inode uint64) (string, error) {
	if inode == 0 || inode == s0fs.RootInode {
		return "/", nil
	}
	if upperInode, ok := s.promotedUpperInode(inode); ok {
		if p, ok := s.engine.Path(upperInode); ok {
			return cleanRootFSUnionPath(p), nil
		}
	}
	if raw, ok := s.lowerRawInode(inode); ok {
		rel, err := s.lower.relForInode(raw)
		if err != nil {
			return "", err
		}
		if rel == rootFSBackedSessionRoot {
			return "/", nil
		}
		return cleanRootFSUnionPath("/" + rel), nil
	}
	if p, ok := s.engine.Path(inode); ok {
		return cleanRootFSUnionPath(p), nil
	}
	return "", rootFSUnionNotFound(fmt.Sprintf("inode %d", inode))
}

func (s *rootFSUnionSession) routeInode(inode uint64) (rootFSUnionBackend, uint64, uint64, error) {
	if inode == 0 || inode == s0fs.RootInode {
		return rootFSUnionBackendUpper, s0fs.RootInode, s0fs.RootInode, nil
	}
	if upperInode, ok := s.promotedUpperInode(inode); ok {
		return rootFSUnionBackendUpper, upperInode, inode, nil
	}
	if raw, ok := s.lowerRawInode(inode); ok {
		return rootFSUnionBackendLower, raw, inode, nil
	}
	return rootFSUnionBackendUpper, inode, inode, nil
}

func (s *rootFSUnionSession) routeFileRequest(inode, handleID uint64) (rootFSUnionBackend, uint64, uint64, error) {
	if handle, ok := s.lookupHandle(handleID); ok {
		return handle.backend, handle.backendInode, handle.handleID, nil
	}
	backend, backendInode, _, err := s.routeInode(inode)
	return backend, backendInode, 0, err
}

func (s *rootFSUnionSession) routeWriteRequest(ctx context.Context, inode, handleID uint64, actor *pb.PosixActor) (rootFSUnionBackend, uint64, uint64, uint64, error) {
	if handle, ok := s.lookupHandle(handleID); ok {
		if handle.backend == rootFSUnionBackendLower {
			upperInode, err := s.copyUpVisibleInode(ctx, handle.visibleInode, actor)
			if err != nil {
				return rootFSUnionBackendLower, 0, 0, handle.visibleInode, err
			}
			return rootFSUnionBackendLower, upperInode, 0, handle.visibleInode, nil
		}
		return handle.backend, handle.backendInode, handle.handleID, handle.visibleInode, nil
	}
	backend, backendInode, visibleInode, err := s.routeInode(inode)
	if err != nil {
		return backend, 0, 0, visibleInode, err
	}
	if backend == rootFSUnionBackendLower {
		upperInode, err := s.copyUpVisibleInode(ctx, visibleInode, actor)
		if err != nil {
			return backend, 0, 0, visibleInode, err
		}
		return backend, upperInode, 0, visibleInode, nil
	}
	return backend, backendInode, 0, visibleInode, nil
}

func (s *rootFSUnionSession) routeSetAttrRequest(ctx context.Context, inode, handleID uint64, actor *pb.PosixActor) (rootFSUnionBackend, uint64, uint64, uint64, error) {
	if handle, ok := s.lookupHandle(handleID); ok {
		if handle.backend == rootFSUnionBackendLower {
			upperInode, err := s.copyUpVisibleInode(ctx, handle.visibleInode, actor)
			if err != nil {
				return rootFSUnionBackendLower, 0, 0, handle.visibleInode, err
			}
			return rootFSUnionBackendUpper, upperInode, 0, handle.visibleInode, nil
		}
		return rootFSUnionBackendUpper, handle.backendInode, handle.handleID, handle.visibleInode, nil
	}
	backend, backendInode, visibleInode, err := s.routeInode(inode)
	return backend, backendInode, 0, visibleInode, err
}

func (s *rootFSUnionSession) mapLowerNodeResponse(resp *pb.NodeResponse) *pb.NodeResponse {
	if resp == nil {
		return nil
	}
	inode := s.mapLowerInode(resp.GetInode())
	return &pb.NodeResponse{
		Inode:      inode,
		Generation: resp.GetGeneration(),
		Attr:       cloneAttrWithInode(resp.GetAttr(), inode),
		HandleId:   resp.GetHandleId(),
	}
}

func (s *rootFSUnionSession) mapLowerDirEntry(entry *pb.DirEntry) *pb.DirEntry {
	if entry == nil {
		return nil
	}
	mapped := cloneDirEntry(entry)
	mapped.Inode = s.mapLowerInode(entry.GetInode())
	mapped.Attr = cloneAttrWithInode(entry.GetAttr(), mapped.Inode)
	return mapped
}

func (s *rootFSUnionSession) mapLowerInode(raw uint64) uint64 {
	if raw == 0 || raw == s0fs.RootInode {
		return s0fs.RootInode
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if promoted := s.promotedLowerByRaw[raw]; promoted != 0 {
		return promoted
	}
	if inode, ok := s.lowerSyntheticByRaw[raw]; ok {
		return inode
	}
	inode := s.nextLowerSynthetic
	s.nextLowerSynthetic++
	s.lowerSyntheticByRaw[raw] = inode
	s.lowerRawBySynthetic[inode] = raw
	return inode
}

func (s *rootFSUnionSession) lowerRawInode(inode uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.lowerRawBySynthetic[inode]
	return raw, ok
}

func (s *rootFSUnionSession) isLowerVisibleInode(inode uint64) bool {
	_, ok := s.lowerRawInode(inode)
	return ok
}

func (s *rootFSUnionSession) promoteLowerRaw(raw, upperInode uint64) {
	if raw == 0 || upperInode == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promotedLowerByRaw[raw] = upperInode
	if visible := s.lowerSyntheticByRaw[raw]; visible != 0 {
		s.promotedLowerByInode[visible] = upperInode
	}
}

func (s *rootFSUnionSession) promoteVisibleInode(visibleInode, upperInode uint64) {
	if visibleInode == 0 || upperInode == 0 || visibleInode == upperInode {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promotedLowerByInode[visibleInode] = upperInode
	if raw := s.lowerRawBySynthetic[visibleInode]; raw != 0 {
		s.promotedLowerByRaw[raw] = upperInode
	}
}

func (s *rootFSUnionSession) promotedUpperInode(visibleInode uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	upper := s.promotedLowerByInode[visibleInode]
	return upper, upper != 0
}

func (s *rootFSUnionSession) dropPromotedVisible(visibleInode, upperInode uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.promotedLowerByInode, visibleInode)
	for raw, promoted := range s.promotedLowerByRaw {
		if promoted == upperInode {
			delete(s.promotedLowerByRaw, raw)
		}
	}
}

func (s *rootFSUnionSession) trackHandle(backend rootFSUnionBackend, handleID, backendInode, visibleInode uint64) uint64 {
	if handleID == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextHandle
	s.nextHandle++
	s.handles[id] = rootFSUnionHandle{
		backend:      backend,
		handleID:     handleID,
		backendInode: backendInode,
		visibleInode: visibleInode,
	}
	return id
}

func (s *rootFSUnionSession) lookupHandle(handleID uint64) (rootFSUnionHandle, bool) {
	if handleID == 0 {
		return rootFSUnionHandle{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, ok := s.handles[handleID]
	return handle, ok
}

func (s *rootFSUnionSession) takeHandle(handleID uint64) (rootFSUnionHandle, bool) {
	if handleID == 0 {
		return rootFSUnionHandle{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, ok := s.handles[handleID]
	delete(s.handles, handleID)
	return handle, ok
}

func (s *rootFSUnionSession) backendHandleID(handleID uint64, backend rootFSUnionBackend) uint64 {
	handle, ok := s.lookupHandle(handleID)
	if !ok || handle.backend != backend {
		return 0
	}
	return handle.handleID
}

func rootFSUnionOpenNeedsUpper(flags uint32) bool {
	switch flags & uint32(syscall.O_ACCMODE) {
	case uint32(syscall.O_WRONLY), uint32(syscall.O_RDWR):
		return true
	}
	return flags&(uint32(syscall.O_TRUNC)|uint32(syscall.O_APPEND)) != 0
}

func isRootFSWhiteoutName(name string) bool {
	return name == ".wh..wh..opq" || strings.HasPrefix(name, ".wh.")
}

func rootFSUnionXattrNames(raw []byte) []string {
	var names []string
	for _, part := range strings.Split(string(raw), "\x00") {
		if name := strings.TrimSpace(part); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func cleanRootFSUnionPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "." || value == "/" {
		return "/"
	}
	return path.Clean("/" + strings.TrimPrefix(value, "/"))
}

func rootFSUnionComponents(value string) []string {
	value = cleanRootFSUnionPath(value)
	if value == "/" {
		return nil
	}
	return strings.Split(strings.TrimPrefix(value, "/"), "/")
}

func rootFSUnionChildPath(parent, name string) string {
	parent = cleanRootFSUnionPath(parent)
	if parent == "/" {
		return cleanRootFSUnionPath("/" + name)
	}
	return cleanRootFSUnionPath(parent + "/" + name)
}

func cloneAttrWithInode(attr *pb.GetAttrResponse, inode uint64) *pb.GetAttrResponse {
	if attr == nil {
		return nil
	}
	return &pb.GetAttrResponse{
		Ino:       inode,
		Mode:      attr.GetMode(),
		Nlink:     attr.GetNlink(),
		Uid:       attr.GetUid(),
		Gid:       attr.GetGid(),
		Rdev:      attr.GetRdev(),
		Size:      attr.GetSize(),
		Blocks:    attr.GetBlocks(),
		AtimeSec:  attr.GetAtimeSec(),
		AtimeNsec: attr.GetAtimeNsec(),
		MtimeSec:  attr.GetMtimeSec(),
		MtimeNsec: attr.GetMtimeNsec(),
		CtimeSec:  attr.GetCtimeSec(),
		CtimeNsec: attr.GetCtimeNsec(),
	}
}

func cloneDirEntry(entry *pb.DirEntry) *pb.DirEntry {
	if entry == nil {
		return nil
	}
	return &pb.DirEntry{
		Inode:  entry.GetInode(),
		Offset: entry.GetOffset(),
		Name:   entry.GetName(),
		Type:   entry.GetType(),
		Attr:   cloneAttrWithInode(entry.GetAttr(), entry.GetInode()),
	}
}

func rootFSUnionNotFound(value string) error {
	return fserror.New(fserror.NotFound, value+" not found")
}

func errOrNotFound(err error, value string) error {
	if err != nil {
		return err
	}
	return rootFSUnionNotFound(value)
}

func isRootFSUnionNotFound(err error) bool {
	if err == nil {
		return false
	}
	return fserror.CodeOf(err) == fserror.NotFound || os.IsNotExist(err) || isS0FSNotFound(err)
}

func isS0FSNotFound(err error) bool {
	return errors.Is(err, s0fs.ErrNotFound)
}

func errorsIsS0FSExists(err error) bool {
	return errors.Is(err, s0fs.ErrExists)
}

var _ volumefuse.Session = (*rootFSUnionSession)(nil)
var _ volumefuse.ReadIntoSession = (*rootFSUnionSession)(nil)
