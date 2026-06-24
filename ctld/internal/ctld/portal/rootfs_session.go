package portal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"golang.org/x/sys/unix"
)

const rootFSBackedSessionRoot = ""

type rootFSBackedSession struct {
	root string

	mu          sync.Mutex
	nextInode   uint64
	inodeByPath map[string]uint64
	pathByInode map[uint64]string
	nextHandle  uint64
	handles     map[uint64]*os.File
	closed      bool
}

func newRootFSBackedSession(root string) *rootFSBackedSession {
	return &rootFSBackedSession{
		root:        filepath.Clean(root),
		nextInode:   s0fs.RootInode + 1,
		inodeByPath: map[string]uint64{rootFSBackedSessionRoot: s0fs.RootInode},
		pathByInode: map[uint64]string{s0fs.RootInode: rootFSBackedSessionRoot},
		nextHandle:  1,
		handles:     make(map[uint64]*os.File),
	}
}

func (s *rootFSBackedSession) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for id, handle := range s.handles {
		_ = handle.Close()
		delete(s.handles, id)
	}
}

func (s *rootFSBackedSession) Lookup(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	parent, err := s.relForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	inode := s.inodeForPath(rel)
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) GetAttr(_ context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		s.dropPathIfMissing(rel, err)
		return nil, mapRootFSBackedError(err)
	}
	return attrFromFileInfo(req.GetInode(), info), nil
}

func (s *rootFSBackedSession) SetAttr(_ context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	hostPath := s.hostPath(rel)
	attr := req.GetAttr()
	valid := req.GetValid()
	if valid&fuse.FATTR_MODE != 0 {
		if err := os.Chmod(hostPath, os.FileMode(attr.GetMode()&0o7777)); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	if valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		uid := -1
		gid := -1
		if valid&fuse.FATTR_UID != 0 {
			uid = int(attr.GetUid())
		}
		if valid&fuse.FATTR_GID != 0 {
			gid = int(attr.GetGid())
		}
		if err := os.Lchown(hostPath, uid, gid); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	if valid&fuse.FATTR_SIZE != 0 {
		if err := os.Truncate(hostPath, int64(attr.GetSize())); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	if valid&(fuse.FATTR_ATIME|fuse.FATTR_MTIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME_NOW) != 0 {
		now := time.Now()
		atime := now
		mtime := now
		if valid&fuse.FATTR_ATIME != 0 {
			atime = time.Unix(attr.GetAtimeSec(), attr.GetAtimeNsec())
		}
		if valid&fuse.FATTR_MTIME != 0 {
			mtime = time.Unix(attr.GetMtimeSec(), attr.GetMtimeNsec())
		}
		if err := os.Chtimes(hostPath, atime, mtime); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	info, err := os.Lstat(hostPath)
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.SetAttrResponse{Attr: attrFromFileInfo(req.GetInode(), info)}, nil
}

func (s *rootFSBackedSession) Mkdir(_ context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	parent, err := s.relForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	mode := os.FileMode(req.GetMode() & 0o7777)
	if mode == 0 {
		mode = 0o755
	}
	if err := os.Mkdir(s.hostPath(rel), mode); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	if actor := req.GetActor(); actor != nil {
		_ = os.Lchown(s.hostPath(rel), int(actor.GetUid()), actorPrimaryGID(actor))
	}
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	inode := s.inodeForPath(rel)
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) Create(_ context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	parent, err := s.relForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	flags := int(req.GetFlags()) | os.O_CREATE
	mode := os.FileMode(req.GetMode() & 0o7777)
	if mode == 0 {
		mode = 0o666
	}
	handle, err := os.OpenFile(s.hostPath(rel), flags, mode)
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	if actor := req.GetActor(); actor != nil {
		_ = handle.Chown(int(actor.GetUid()), actorPrimaryGID(actor))
	}
	info, err := handle.Stat()
	if err != nil {
		_ = handle.Close()
		return nil, mapRootFSBackedError(err)
	}
	inode := s.inodeForPath(rel)
	handleID := s.trackHandle(handle)
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info), HandleId: handleID}, nil
}

func (s *rootFSBackedSession) Unlink(_ context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	parent, err := s.relForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	if err := os.Remove(s.hostPath(rel)); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	s.dropPath(rel)
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Rmdir(_ context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	parent, err := s.relForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	if err := os.Remove(s.hostPath(rel)); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	s.dropPathTree(rel)
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Rename(_ context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	oldParent, err := s.relForInode(req.GetOldParent())
	if err != nil {
		return nil, err
	}
	newParent, err := s.relForInode(req.GetNewParent())
	if err != nil {
		return nil, err
	}
	oldRel, err := childRel(oldParent, req.GetOldName())
	if err != nil {
		return nil, err
	}
	newRel, err := childRel(newParent, req.GetNewName())
	if err != nil {
		return nil, err
	}
	if err := os.Rename(s.hostPath(oldRel), s.hostPath(newRel)); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	s.renamePathTree(oldRel, newRel)
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Link(_ context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	sourceRel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	parentRel, err := s.relForInode(req.GetNewParent())
	if err != nil {
		return nil, err
	}
	newRel, err := childRel(parentRel, req.GetNewName())
	if err != nil {
		return nil, err
	}
	if err := os.Link(s.hostPath(sourceRel), s.hostPath(newRel)); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	info, err := os.Lstat(s.hostPath(newRel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	inode := s.inodeForPath(newRel)
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) Symlink(_ context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	parentRel, err := s.relForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parentRel, req.GetName())
	if err != nil {
		return nil, err
	}
	if err := os.Symlink(req.GetTarget(), s.hostPath(rel)); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	inode := s.inodeForPath(rel)
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) Readlink(_ context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	target, err := os.Readlink(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.ReadlinkResponse{Target: target}, nil
}

func (s *rootFSBackedSession) Access(_ context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if err := unix.Access(s.hostPath(rel), req.GetMask()); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Open(_ context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	handle, err := os.OpenFile(s.hostPath(rel), int(req.GetFlags()), 0)
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.OpenResponse{HandleId: s.trackHandle(handle)}, nil
}

func (s *rootFSBackedSession) Read(_ context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	handle, release, err := s.handleForRead(req.GetInode(), req.GetHandleId())
	if err != nil {
		return nil, err
	}
	defer release()
	buf := make([]byte, req.GetSize())
	n, err := handle.ReadAt(buf, req.GetOffset())
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.ReadResponse{Data: buf[:n], Eof: errors.Is(err, io.EOF)}, nil
}

func (s *rootFSBackedSession) Write(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	handle, release, err := s.handleForWrite(req.GetInode(), req.GetHandleId())
	if err != nil {
		return nil, err
	}
	defer release()
	n, err := handle.WriteAt(req.GetData(), req.GetOffset())
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.WriteResponse{BytesWritten: int64(n)}, nil
}

func (s *rootFSBackedSession) Release(_ context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	if handle := s.takeHandle(req.GetHandleId()); handle != nil {
		if err := handle.Close(); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Flush(_ context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	if handle := s.lookupHandle(req.GetHandleId()); handle != nil {
		if err := handle.Sync(); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Fsync(_ context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	return s.Flush(context.Background(), &pb.FlushRequest{HandleId: req.GetHandleId()})
}

func (s *rootFSBackedSession) Fallocate(_ context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	handle, release, err := s.handleForWrite(req.GetInode(), req.GetHandleId())
	if err != nil {
		return nil, err
	}
	defer release()
	if err := unix.Fallocate(int(handle.Fd()), req.GetMode(), req.GetOffset(), req.GetLength()); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) CopyFileRange(context.Context, *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	return nil, fserror.New(fserror.Unimplemented, "copy_file_range is not implemented for rootfs-backed portals")
}

func (s *rootFSBackedSession) OpenDir(_ context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	if !info.IsDir() {
		return nil, syscall.ENOTDIR
	}
	return &pb.OpenDirResponse{HandleId: s.nextSyntheticHandle()}, nil
}

func (s *rootFSBackedSession) ReadDir(_ context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	out := make([]*pb.DirEntry, 0, len(entries))
	for i, entry := range entries {
		offset := uint64(i + 1)
		if int64(offset) <= req.GetOffset() {
			continue
		}
		child, err := childRel(rel, entry.Name())
		if err != nil {
			return nil, err
		}
		info, err := os.Lstat(s.hostPath(child))
		if err != nil {
			return nil, mapRootFSBackedError(err)
		}
		inode := s.inodeForPath(child)
		attr := attrFromFileInfo(inode, info)
		out = append(out, &pb.DirEntry{
			Inode:  inode,
			Offset: offset,
			Name:   entry.Name(),
			Type:   attr.GetMode(),
			Attr:   attr,
		})
	}
	return &pb.ReadDirResponse{Entries: out, Eof: true}, nil
}

func (s *rootFSBackedSession) ReleaseDir(context.Context, *pb.ReleaseDirRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) StatFs(_ context.Context, _ *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(s.root, &stat); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.StatFsResponse{
		Blocks:  stat.Blocks,
		Bfree:   stat.Bfree,
		Bavail:  stat.Bavail,
		Files:   stat.Files,
		Ffree:   stat.Ffree,
		Bsize:   uint32(stat.Bsize),
		Frsize:  uint32(stat.Frsize),
		Namelen: uint32(stat.Namelen),
	}, nil
}

func (s *rootFSBackedSession) GetXattr(_ context.Context, req *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	size, err := unix.Lgetxattr(s.hostPath(rel), req.GetName(), nil)
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	buf := make([]byte, size)
	if size > 0 {
		if _, err := unix.Lgetxattr(s.hostPath(rel), req.GetName(), buf); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	return &pb.GetXattrResponse{Value: buf}, nil
}

func (s *rootFSBackedSession) SetXattr(_ context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if err := unix.Lsetxattr(s.hostPath(rel), req.GetName(), req.GetValue(), int(req.GetFlags())); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) ListXattr(_ context.Context, req *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	size, err := unix.Llistxattr(s.hostPath(rel), nil)
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	buf := make([]byte, size)
	if size > 0 {
		if _, err := unix.Llistxattr(s.hostPath(rel), buf); err != nil {
			return nil, mapRootFSBackedError(err)
		}
	}
	return &pb.ListXattrResponse{Data: buf}, nil
}

func (s *rootFSBackedSession) RemoveXattr(_ context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	rel, err := s.relForInode(req.GetInode())
	if err != nil {
		return nil, err
	}
	if err := unix.Lremovexattr(s.hostPath(rel), req.GetName()); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Mknod(_ context.Context, req *pb.MknodRequest) (*pb.NodeResponse, error) {
	parent, err := s.relForInode(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	if err := unix.Mknod(s.hostPath(rel), req.GetMode(), int(req.GetRdev())); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	if actor := req.GetActor(); actor != nil {
		_ = os.Lchown(s.hostPath(rel), int(actor.GetUid()), actorPrimaryGID(actor))
	}
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	inode := s.inodeForPath(rel)
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) GetLk(context.Context, *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	return &pb.GetLkResponse{}, nil
}

func (s *rootFSBackedSession) SetLk(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) SetLkw(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Flock(context.Context, *pb.FlockRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) relForInode(inode uint64) (string, error) {
	if inode == 0 {
		inode = s0fs.RootInode
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", fserror.New(fserror.FailedPrecondition, "rootfs-backed portal is closed")
	}
	rel, ok := s.pathByInode[inode]
	if !ok {
		return "", fserror.New(fserror.NotFound, fmt.Sprintf("inode %d not found", inode))
	}
	return rel, nil
}

func (s *rootFSBackedSession) inodeForPath(rel string) uint64 {
	rel = cleanRootFSBackedRel(rel)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inodeForPathLocked(rel)
}

func (s *rootFSBackedSession) inodeForPathLocked(rel string) uint64 {
	if inode, ok := s.inodeByPath[rel]; ok {
		return inode
	}
	inode := s.nextInode
	s.nextInode++
	if inode <= s0fs.RootInode {
		inode = s.nextInode
		s.nextInode++
	}
	s.inodeByPath[rel] = inode
	s.pathByInode[inode] = rel
	return inode
}

func (s *rootFSBackedSession) trackHandle(handle *os.File) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = handle.Close()
		return 0
	}
	id := s.nextHandle
	s.nextHandle++
	s.handles[id] = handle
	return id
}

func (s *rootFSBackedSession) nextSyntheticHandle() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextHandle
	s.nextHandle++
	return id
}

func (s *rootFSBackedSession) lookupHandle(id uint64) *os.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handles[id]
}

func (s *rootFSBackedSession) takeHandle(id uint64) *os.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle := s.handles[id]
	delete(s.handles, id)
	return handle
}

func (s *rootFSBackedSession) handleForRead(inode, handleID uint64) (*os.File, func(), error) {
	if handle := s.lookupHandle(handleID); handle != nil {
		return handle, func() {}, nil
	}
	rel, err := s.relForInode(inode)
	if err != nil {
		return nil, nil, err
	}
	handle, err := os.OpenFile(s.hostPath(rel), os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, mapRootFSBackedError(err)
	}
	return handle, func() { _ = handle.Close() }, nil
}

func (s *rootFSBackedSession) handleForWrite(inode, handleID uint64) (*os.File, func(), error) {
	if handle := s.lookupHandle(handleID); handle != nil {
		return handle, func() {}, nil
	}
	rel, err := s.relForInode(inode)
	if err != nil {
		return nil, nil, err
	}
	handle, err := os.OpenFile(s.hostPath(rel), os.O_WRONLY, 0)
	if err != nil {
		return nil, nil, mapRootFSBackedError(err)
	}
	return handle, func() { _ = handle.Close() }, nil
}

func (s *rootFSBackedSession) hostPath(rel string) string {
	rel = cleanRootFSBackedRel(rel)
	if rel == rootFSBackedSessionRoot {
		return s.root
	}
	return filepath.Join(s.root, filepath.FromSlash(rel))
}

func (s *rootFSBackedSession) dropPathIfMissing(rel string, err error) {
	if !errors.Is(err, os.ErrNotExist) {
		return
	}
	s.dropPath(rel)
}

func (s *rootFSBackedSession) dropPath(rel string) {
	rel = cleanRootFSBackedRel(rel)
	s.mu.Lock()
	defer s.mu.Unlock()
	if inode, ok := s.inodeByPath[rel]; ok {
		delete(s.inodeByPath, rel)
		delete(s.pathByInode, inode)
	}
}

func (s *rootFSBackedSession) dropPathTree(rel string) {
	rel = cleanRootFSBackedRel(rel)
	s.mu.Lock()
	defer s.mu.Unlock()
	for path, inode := range s.inodeByPath {
		if path == rel || strings.HasPrefix(path, rel+"/") {
			delete(s.inodeByPath, path)
			delete(s.pathByInode, inode)
		}
	}
}

func (s *rootFSBackedSession) renamePathTree(oldRel, newRel string) {
	oldRel = cleanRootFSBackedRel(oldRel)
	newRel = cleanRootFSBackedRel(newRel)
	s.mu.Lock()
	defer s.mu.Unlock()
	updates := make(map[string]uint64)
	for path, inode := range s.inodeByPath {
		if path != oldRel && !strings.HasPrefix(path, oldRel+"/") {
			continue
		}
		nextPath := newRel + strings.TrimPrefix(path, oldRel)
		updates[nextPath] = inode
		delete(s.inodeByPath, path)
	}
	for path, inode := range updates {
		s.inodeByPath[path] = inode
		s.pathByInode[inode] = path
	}
}

func childRel(parent, name string) (string, error) {
	parent = cleanRootFSBackedRel(parent)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.ContainsRune(name, filepath.Separator) {
		return "", fserror.New(fserror.InvalidArgument, "invalid path name")
	}
	if parent == rootFSBackedSessionRoot {
		return name, nil
	}
	return parent + "/" + name, nil
}

func cleanRootFSBackedRel(rel string) string {
	rel = strings.TrimSpace(rel)
	if rel == "" || rel == "." || rel == "/" {
		return rootFSBackedSessionRoot
	}
	clean := filepath.ToSlash(filepath.Clean("/" + rel))
	return strings.TrimPrefix(clean, "/")
}

func attrFromFileInfo(inode uint64, info os.FileInfo) *pb.GetAttrResponse {
	now := time.Now()
	attr := &pb.GetAttrResponse{
		Ino:       inode,
		Mode:      0o100000 | 0o644,
		Nlink:     1,
		AtimeSec:  now.Unix(),
		AtimeNsec: int64(now.Nanosecond()),
		MtimeSec:  now.Unix(),
		MtimeNsec: int64(now.Nanosecond()),
		CtimeSec:  now.Unix(),
		CtimeNsec: int64(now.Nanosecond()),
	}
	if info == nil {
		return attr
	}
	attr.Size = uint64(info.Size())
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		attr.Mode = stat.Mode
		attr.Nlink = uint32(stat.Nlink)
		attr.Uid = stat.Uid
		attr.Gid = stat.Gid
		attr.Rdev = uint64(stat.Rdev)
		attr.Blocks = uint64(stat.Blocks)
		attr.AtimeSec = stat.Atim.Sec
		attr.AtimeNsec = stat.Atim.Nsec
		attr.MtimeSec = stat.Mtim.Sec
		attr.MtimeNsec = stat.Mtim.Nsec
		attr.CtimeSec = stat.Ctim.Sec
		attr.CtimeNsec = stat.Ctim.Nsec
		return attr
	}
	mode := uint32(info.Mode().Perm())
	switch {
	case info.IsDir():
		mode |= 0o040000
	case info.Mode()&os.ModeSymlink != 0:
		mode |= 0o120000
	default:
		mode |= 0o100000
	}
	attr.Mode = mode
	attr.MtimeSec = info.ModTime().Unix()
	attr.MtimeNsec = int64(info.ModTime().Nanosecond())
	return attr
}

func actorPrimaryGID(actor *pb.PosixActor) int {
	if actor == nil || len(actor.GetGids()) == 0 {
		return -1
	}
	return int(actor.GetGids()[0])
}

func mapRootFSBackedError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, os.ErrNotExist):
		return fserror.New(fserror.NotFound, err.Error())
	case errors.Is(err, os.ErrExist):
		return fserror.New(fserror.AlreadyExists, err.Error())
	case errors.Is(err, os.ErrPermission):
		return fserror.New(fserror.PermissionDenied, err.Error())
	case errors.Is(err, syscall.ENOTEMPTY), errors.Is(err, syscall.EISDIR):
		return fserror.New(fserror.FailedPrecondition, err.Error())
	case errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.EINVAL):
		return fserror.New(fserror.InvalidArgument, err.Error())
	case errors.Is(err, syscall.ENOSPC):
		return fserror.New(fserror.ResourceExhausted, err.Error())
	default:
		return err
	}
}

var _ volumefuse.Session = (*rootFSBackedSession)(nil)
