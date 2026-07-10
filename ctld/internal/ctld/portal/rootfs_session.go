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
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"golang.org/x/sys/unix"
)

const rootFSBackedSessionRoot = ""

type rootFSBackedSession struct {
	root string

	mu              sync.Mutex
	inodeByPath     map[string]uint64
	pathByInode     map[uint64]string
	identityByInode map[uint64]rootFSHostIdentity
	handles         map[uint64]*rootFSOpenHandle
	orphans         map[uint64]rootFSOrphanRecord
	recovered       bool
	initErr         error
	closed          bool
}

func newRootFSBackedSession(root string) *rootFSBackedSession {
	session := &rootFSBackedSession{root: filepath.Clean(root)}
	session.initErr = session.initializeState()
	return session
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
		closeRootFSHandleFiles(handle)
		delete(s.handles, id)
	}
}

// InitError reports recovery failures before the session is mounted or routed.
func (s *rootFSBackedSession) InitError() error {
	if s == nil {
		return fserror.New(fserror.FailedPrecondition, "rootfs-backed portal session is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initErr
}

func (s *rootFSBackedSession) Lookup(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, err := s.relForInodeLocked(req.GetParent())
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
	inode, err := s.inodeForExistingPathLocked(rel, info)
	if err != nil {
		return nil, err
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, err := s.relForInodeLocked(req.GetParent())
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
	inode, err := s.inodeForExistingPathLocked(rel, info)
	if err != nil {
		_ = os.Remove(s.hostPath(rel))
		return nil, err
	}
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) Create(_ context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, err := s.relForInodeLocked(req.GetParent())
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
	inode, err := s.inodeForExistingPathLocked(rel, info)
	if err != nil {
		_ = handle.Close()
		return nil, err
	}
	handleID := s.openHandleLocked(inode, false, handle, flags)
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info), HandleId: handleID}, nil
}

func (s *rootFSBackedSession) Unlink(_ context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, err := s.relForInodeLocked(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	return s.removePathLocked(rel, false)
}

func (s *rootFSBackedSession) Rmdir(_ context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, err := s.relForInodeLocked(req.GetParent())
	if err != nil {
		return nil, err
	}
	rel, err := childRel(parent, req.GetName())
	if err != nil {
		return nil, err
	}
	return s.removePathLocked(rel, true)
}

func (s *rootFSBackedSession) Rename(_ context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldParent, err := s.relForInodeLocked(req.GetOldParent())
	if err != nil {
		return nil, err
	}
	newParent, err := s.relForInodeLocked(req.GetNewParent())
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
	oldInfo, err := os.Lstat(s.hostPath(oldRel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	oldIdentity, err := rootFSIdentityFromInfo(oldInfo)
	if err != nil {
		return nil, err
	}
	newInfo, newInfoErr := os.Lstat(s.hostPath(newRel))
	if newInfoErr != nil && !errors.Is(newInfoErr, os.ErrNotExist) {
		return nil, mapRootFSBackedError(newInfoErr)
	}
	if newInfoErr == nil {
		newIdentity, identityErr := rootFSIdentityFromInfo(newInfo)
		if identityErr != nil {
			return nil, identityErr
		}
		if newIdentity == oldIdentity {
			return &pb.Empty{}, nil
		}
	}

	targetInode := uint64(0)
	targetOrphan := ""
	targetMoved := false
	targetLinked := false
	if newInfoErr == nil {
		targetInode, err = s.inodeForExistingPathLocked(newRel, newInfo)
		if err != nil {
			return nil, err
		}
		if s.otherVisiblePathLocked(targetInode, newRel) == "" && (s.hasHandlesForInodeLocked(targetInode) || s.recovered) {
			targetOrphan = rootFSOrphanPath(targetInode)
			if s.inodeByPath[targetOrphan] == targetInode {
				// An earlier crash already left a private recovery anchor.
			} else if newInfo.IsDir() {
				err = renameRootFSNoReplace(s.hostPath(newRel), s.hostPath(targetOrphan))
				targetMoved = err == nil
			} else {
				err = os.Link(s.hostPath(newRel), s.hostPath(targetOrphan))
				targetLinked = err == nil
			}
			if err != nil {
				return nil, mapRootFSBackedError(err)
			}
		}
	}
	if err := os.Rename(s.hostPath(oldRel), s.hostPath(newRel)); err != nil {
		if targetMoved {
			_ = os.Rename(s.hostPath(targetOrphan), s.hostPath(newRel))
		} else if targetLinked {
			_ = os.Remove(s.hostPath(targetOrphan))
		}
		return nil, mapRootFSBackedError(err)
	}

	if newInfoErr == nil {
		if targetOrphan != "" {
			delete(s.inodeByPath, newRel)
			s.inodeByPath[targetOrphan] = targetInode
			s.pathByInode[targetInode] = targetOrphan
			identity, _ := rootFSIdentityFromInfo(newInfo)
			openCount := uint64(0)
			if handle := s.handles[targetInode]; handle != nil {
				openCount = handle.openCount
			}
			s.orphans[targetInode] = rootFSOrphanRecord{
				Inode: targetInode, Identity: identity, OpenCount: openCount, Directory: newInfo.IsDir(), Retain: s.recovered,
			}
		} else if newInfo.IsDir() {
			s.dropPathTreeLocked(newRel)
		} else {
			s.dropPathLocked(newRel)
		}
	}
	s.renamePathTreeLocked(oldRel, newRel)
	if targetOrphan != "" {
		if err := s.commitOrphansLocked("rename over open target"); err != nil {
			return nil, err
		}
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Link(_ context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sourceRel, err := s.relForInodeLocked(req.GetInode())
	if err != nil {
		return nil, err
	}
	parentRel, err := s.relForInodeLocked(req.GetNewParent())
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
	inode, err := s.inodeForExistingPathLocked(newRel, info)
	if err != nil {
		_ = os.Remove(s.hostPath(newRel))
		return nil, err
	}
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) Symlink(_ context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parentRel, err := s.relForInodeLocked(req.GetParent())
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
	inode, err := s.inodeForExistingPathLocked(rel, info)
	if err != nil {
		_ = os.Remove(s.hostPath(rel))
		return nil, err
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	rel, err := s.relForInodeLocked(req.GetInode())
	if err != nil {
		return nil, err
	}
	handle, err := os.OpenFile(s.hostPath(rel), int(req.GetFlags()), 0)
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	info, err := handle.Stat()
	if err != nil {
		_ = handle.Close()
		return nil, mapRootFSBackedError(err)
	}
	identity, err := rootFSIdentityFromInfo(info)
	if err != nil || identity != s.identityByInode[normalizeRootFSInode(req.GetInode())] {
		_ = handle.Close()
		return nil, fserror.New(fserror.FailedPrecondition, "rootfs inode host identity changed before open")
	}
	handleID := s.openHandleLocked(normalizeRootFSInode(req.GetInode()), false, handle, int(req.GetFlags()))
	return &pb.OpenResponse{HandleId: handleID}, nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseHandleLocked(req.GetHandleId(), false)
}

func (s *rootFSBackedSession) Flush(_ context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	rel, err := s.relForInode(req.GetHandleId())
	if err != nil {
		return nil, err
	}
	file, err := os.Open(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return nil, mapRootFSBackedError(err)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	rel, err := s.relForInodeLocked(req.GetInode())
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
	handle, err := os.Open(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	openedInfo, err := handle.Stat()
	if err != nil {
		_ = handle.Close()
		return nil, mapRootFSBackedError(err)
	}
	identity, err := rootFSIdentityFromInfo(openedInfo)
	if err != nil || identity != s.identityByInode[normalizeRootFSInode(req.GetInode())] {
		_ = handle.Close()
		return nil, fserror.New(fserror.FailedPrecondition, "rootfs directory host identity changed before open")
	}
	handleID := s.openHandleLocked(normalizeRootFSInode(req.GetInode()), true, handle, int(req.GetFlags()))
	return &pb.OpenDirResponse{HandleId: handleID}, nil
}

func (s *rootFSBackedSession) ReadDir(_ context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rel, err := s.relForInodeLocked(req.GetInode())
	if err != nil {
		return nil, err
	}
	if req.GetHandleId() != 0 && req.GetHandleId() != normalizeRootFSInode(req.GetInode()) {
		return nil, fserror.New(fserror.NotFound, fmt.Sprintf("directory handle %d not found", req.GetHandleId()))
	}
	entries, err := os.ReadDir(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	visibleEntries := entries[:0]
	for _, entry := range entries {
		if rel == rootFSBackedSessionRoot && entry.Name() == rootFSStateDirectoryName {
			continue
		}
		visibleEntries = append(visibleEntries, entry)
	}
	out := make([]*pb.DirEntry, 0, len(visibleEntries))
	for i, entry := range visibleEntries {
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
		inode, err := s.inodeForExistingPathLocked(child, info)
		if err != nil {
			return nil, err
		}
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

func (s *rootFSBackedSession) ReleaseDir(_ context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseHandleLocked(req.GetHandleId(), true)
}

func (s *rootFSBackedSession) StatFs(_ context.Context, _ *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	s.mu.Lock()
	if err := s.ensureOpenLocked(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, err := s.relForInodeLocked(req.GetParent())
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
	inode, err := s.inodeForExistingPathLocked(rel, info)
	if err != nil {
		_ = os.Remove(s.hostPath(rel))
		return nil, err
	}
	return &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}, nil
}

func (s *rootFSBackedSession) GetLk(context.Context, *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	if err := s.sessionStateError(); err != nil {
		return nil, err
	}
	return &pb.GetLkResponse{}, nil
}

func (s *rootFSBackedSession) SetLk(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	if err := s.sessionStateError(); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) SetLkw(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	if err := s.sessionStateError(); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) Flock(context.Context, *pb.FlockRequest) (*pb.Empty, error) {
	if err := s.sessionStateError(); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) sessionStateError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureOpenLocked()
}

func (s *rootFSBackedSession) relForInode(inode uint64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.relForInodeLocked(inode)
}

func (s *rootFSBackedSession) relForInodeLocked(inode uint64) (string, error) {
	if err := s.ensureOpenLocked(); err != nil {
		return "", err
	}
	inode = normalizeRootFSInode(inode)
	rel, ok := s.pathByInode[inode]
	if !ok {
		return "", fserror.New(fserror.NotFound, fmt.Sprintf("inode %d not found", inode))
	}
	return rel, nil
}

func (s *rootFSBackedSession) handleForRead(inode, handleID uint64) (*os.File, func(), error) {
	inode = normalizeRootFSInode(inode)
	if handleID != 0 && handleID != inode {
		return nil, nil, fserror.New(fserror.NotFound, fmt.Sprintf("file handle %d not found", handleID))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if handle := s.handles[inode]; handle != nil && handle.readFile != nil {
		return handle.readFile, func() {}, nil
	}
	rel, err := s.relForInodeLocked(inode)
	if err != nil {
		return nil, nil, err
	}
	file, err := os.OpenFile(s.hostPath(rel), os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, mapRootFSBackedError(err)
	}
	handle := s.handles[inode]
	if handle == nil {
		handle = &rootFSOpenHandle{}
		s.handles[inode] = handle
	}
	cacheRootFSFile(handle, file, os.O_RDONLY)
	return handle.readFile, func() {}, nil
}

func (s *rootFSBackedSession) handleForWrite(inode, handleID uint64) (*os.File, func(), error) {
	inode = normalizeRootFSInode(inode)
	if handleID != 0 && handleID != inode {
		return nil, nil, fserror.New(fserror.NotFound, fmt.Sprintf("file handle %d not found", handleID))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if handle := s.handles[inode]; handle != nil && handle.writeFile != nil {
		return handle.writeFile, func() {}, nil
	}
	rel, err := s.relForInodeLocked(inode)
	if err != nil {
		return nil, nil, err
	}
	file, err := os.OpenFile(s.hostPath(rel), os.O_WRONLY, 0)
	if err != nil {
		return nil, nil, mapRootFSBackedError(err)
	}
	handle := s.handles[inode]
	if handle == nil {
		handle = &rootFSOpenHandle{}
		s.handles[inode] = handle
	}
	cacheRootFSFile(handle, file, os.O_WRONLY)
	return handle.writeFile, func() {}, nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initErr != nil || s.closed {
		return
	}
	rel = cleanRootFSBackedRel(rel)
	inode, ok := s.inodeByPath[rel]
	if !ok {
		return
	}
	if s.hasHandlesForInodeLocked(inode) && s.countPathsForInodeLocked(inode) == 1 {
		s.initErr = fmt.Errorf("rootfs inode %d lost its only recoverable path %q", inode, rel)
		return
	}
	s.dropPathLocked(rel)
}

func normalizeRootFSInode(inode uint64) uint64 {
	if inode == 0 {
		return s0fs.RootInode
	}
	return inode
}

func childRel(parent, name string) (string, error) {
	parent = cleanRootFSBackedRel(parent)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.ContainsRune(name, filepath.Separator) {
		return "", fserror.New(fserror.InvalidArgument, "invalid path name")
	}
	if parent == rootFSBackedSessionRoot && name == rootFSStateDirectoryName {
		return "", fserror.New(fserror.PermissionDenied, "reserved rootfs path")
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
