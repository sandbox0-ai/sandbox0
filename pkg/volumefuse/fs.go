package volumefuse

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type FileSystem struct {
	fuse.RawFileSystem
	mu                    sync.RWMutex
	volumeID              string
	session               Session
	cacheTTL              time.Duration
	kernelCache           kernelCacheNotifier
	storeCacheUnsupported atomic.Bool
}

const fileOpenFlags = fuse.FOPEN_KEEP_CACHE | fuse.FOPEN_NOFLUSH

type kernelCacheNotifier interface {
	InodeNotify(node uint64, off int64, length int64) fuse.Status
	InodeNotifyStoreCache(node uint64, offset int64, data []byte) fuse.Status
	EntryNotify(parent uint64, name string) fuse.Status
}

func New(volumeID string, cacheTTL time.Duration, session Session) *FileSystem {
	if cacheTTL < 0 {
		cacheTTL = time.Second
	}
	return &FileSystem{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		volumeID:      volumeID,
		session:       session,
		cacheTTL:      cacheTTL,
	}
}

func (fs *FileSystem) String() string {
	return "sandbox0-volume"
}

func (fs *FileSystem) Init(server *fuse.Server) {
	fs.setKernelCache(server)
}

func (fs *FileSystem) OnUnmount() {
	fs.setKernelCache(nil)
}

func (fs *FileSystem) SetSession(session Session) {
	if fs == nil {
		return
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.session = session
}

func (fs *FileSystem) requireSession() (Session, fuse.Status) {
	if fs == nil {
		return nil, fuse.EIO
	}
	fs.mu.RLock()
	session := fs.session
	fs.mu.RUnlock()
	if session == nil {
		return nil, fuse.EIO
	}
	return session, fuse.OK
}

func (fs *FileSystem) setKernelCache(cache kernelCacheNotifier) {
	if fs == nil {
		return
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.kernelCache = cache
	fs.storeCacheUnsupported.Store(false)
}

func (fs *FileSystem) kernelCacheNotifyTarget() kernelCacheNotifier {
	if fs == nil {
		return nil
	}
	fs.mu.RLock()
	cache := fs.kernelCache
	fs.mu.RUnlock()
	return cache
}

func (fs *FileSystem) invalidateKernelData(inode uint64, off int64, length int64) {
	cache := fs.kernelCacheNotifyTarget()
	if cache == nil {
		return
	}
	_ = cache.InodeNotify(inode, off, length)
}

func (fs *FileSystem) invalidateKernelInode(inode uint64) {
	fs.invalidateKernelData(inode, 0, 0)
}

func (fs *FileSystem) invalidateKernelAttr(inode uint64) {
	fs.invalidateKernelData(inode, -1, 0)
}

func (fs *FileSystem) invalidateKernelEntry(parent uint64, name string) {
	cache := fs.kernelCacheNotifyTarget()
	if cache == nil || name == "" {
		return
	}
	_ = cache.EntryNotify(parent, name)
}

func (fs *FileSystem) storeKernelCache(inode uint64, off int64, data []byte) {
	if len(data) == 0 || fs.storeCacheUnsupported.Load() {
		return
	}
	cache := fs.kernelCacheNotifyTarget()
	if cache == nil {
		return
	}
	if st := cache.InodeNotifyStoreCache(inode, off, data); st == fuse.ENOSYS {
		fs.storeCacheUnsupported.Store(true)
	}
}

func actorFromCaller(caller fuse.Caller) *pb.PosixActor {
	return &pb.PosixActor{
		Pid:  caller.Pid,
		Uid:  caller.Uid,
		Gids: []uint32{caller.Gid},
	}
}

func actorFromHeader(header *fuse.InHeader) *pb.PosixActor {
	if header == nil {
		return nil
	}
	return actorFromCaller(header.Caller)
}

func (fs *FileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.LookupRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
		Actor:    actorFromHeader(header),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.Lookup(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}

	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *FileSystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.GetAttr(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	setAttrOut(out, resp, fs.cacheTTL)
	return fuse.OK
}

func (fs *FileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	attr := &pb.GetAttrResponse{
		Ino:       input.NodeId,
		Mode:      input.Mode,
		Uid:       input.Uid,
		Gid:       input.Gid,
		Size:      input.Size,
		AtimeSec:  int64(input.Atime),
		AtimeNsec: int64(input.Atimensec),
		MtimeSec:  int64(input.Mtime),
		MtimeNsec: int64(input.Mtimensec),
		CtimeSec:  int64(input.Ctime),
		CtimeNsec: int64(input.Ctimensec),
	}

	handleID := uint64(0)
	if fh, ok := input.GetFh(); ok {
		handleID = fh
	}
	req := &pb.SetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Valid:    input.Valid,
		Attr:     attr,
		HandleId: handleID,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	sizeChanged := input.Valid&fuse.FATTR_SIZE != 0
	if sizeChanged {
		fs.invalidateKernelInode(input.NodeId)
	}
	resp, err := session.SetAttr(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	if sizeChanged {
		fs.invalidateKernelInode(input.NodeId)
	} else {
		fs.invalidateKernelAttr(input.NodeId)
	}
	setAttrOut(out, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *FileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.MkdirRequest{
		VolumeId: fs.volumeID,
		Parent:   input.NodeId,
		Name:     name,
		Mode:     input.Mode,
		Umask:    input.Umask,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.Mkdir(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(input.NodeId, name)
	fs.invalidateKernelAttr(input.NodeId)
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *FileSystem) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.UnlinkRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
		Actor:    actorFromHeader(header),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	fs.invalidateKernelEntry(header.NodeId, name)
	_, err := session.Unlink(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(header.NodeId, name)
	fs.invalidateKernelAttr(header.NodeId)
	return fuse.OK
}

func (fs *FileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.RmdirRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
		Actor:    actorFromHeader(header),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	fs.invalidateKernelEntry(header.NodeId, name)
	_, err := session.Rmdir(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(header.NodeId, name)
	fs.invalidateKernelAttr(header.NodeId)
	return fuse.OK
}

func (fs *FileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.RenameRequest{
		VolumeId:  fs.volumeID,
		OldParent: input.NodeId,
		OldName:   oldName,
		NewParent: input.Newdir,
		NewName:   newName,
		Flags:     input.Flags,
		Actor:     actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	fs.invalidateKernelEntry(input.NodeId, oldName)
	fs.invalidateKernelEntry(input.Newdir, newName)
	_, err := session.Rename(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(input.NodeId, oldName)
	fs.invalidateKernelEntry(input.Newdir, newName)
	fs.invalidateKernelAttr(input.NodeId)
	if input.Newdir != input.NodeId {
		fs.invalidateKernelAttr(input.Newdir)
	}
	return fuse.OK
}

func (fs *FileSystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.Link(context.Background(), &pb.LinkRequest{
		VolumeId:  fs.volumeID,
		Inode:     input.Oldnodeid,
		NewParent: input.NodeId,
		NewName:   filename,
		Actor:     actorFromCaller(input.Caller),
	})
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(input.NodeId, filename)
	fs.invalidateKernelAttr(input.NodeId)
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *FileSystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.SymlinkRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     linkName,
		Target:   pointedTo,
		Actor:    actorFromHeader(header),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.Symlink(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(header.NodeId, linkName)
	fs.invalidateKernelAttr(header.NodeId)
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *FileSystem) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	if isCanceled(cancel) {
		return nil, fuse.EINTR
	}
	req := &pb.ReadlinkRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Actor:    actorFromHeader(header),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return nil, st
	}
	resp, err := session.Readlink(context.Background(), req)
	if err != nil {
		return nil, statusToFuse(err)
	}
	return []byte(resp.Target), fuse.OK
}

func (fs *FileSystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.AccessRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Mask:     input.Mask,
		Uid:      input.Uid,
		Gids:     []uint32{input.Gid},
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	_, err := session.Access(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	return fuse.OK
}

func (fs *FileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.CreateRequest{
		VolumeId: fs.volumeID,
		Parent:   input.NodeId,
		Name:     name,
		Mode:     input.Mode,
		Flags:    input.Flags,
		Umask:    input.Umask,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.Create(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(input.NodeId, name)
	fs.invalidateKernelAttr(input.NodeId)
	setEntryOut(&out.EntryOut, resp.Inode, resp.Attr, fs.cacheTTL)
	out.Fh = resp.HandleId
	out.OpenFlags = fileOpenFlags
	return fuse.OK
}

func (fs *FileSystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.OpenRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Flags:    input.Flags,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.Open(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	out.Fh = resp.HandleId
	out.OpenFlags = fileOpenFlags
	return fuse.OK
}

func (fs *FileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	if isCanceled(cancel) {
		return nil, fuse.EINTR
	}
	req := &pb.ReadRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Offset:   int64(input.Offset),
		Size:     int64(input.Size),
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return nil, st
	}
	readBuf := buf
	if input.Size < uint32(len(readBuf)) {
		readBuf = readBuf[:input.Size]
	}
	if len(readBuf) == 0 {
		return fuse.ReadResultData(nil), fuse.OK
	}
	if reader, ok := session.(ReadIntoSession); ok && len(readBuf) > 0 {
		n, _, err := reader.ReadInto(context.Background(), req, readBuf)
		if err != nil {
			return nil, statusToFuse(err)
		}
		return fuse.ReadResultData(readBuf[:n]), fuse.OK
	}
	resp, err := session.Read(context.Background(), req)
	if err != nil {
		return nil, statusToFuse(err)
	}
	return fuse.ReadResultData(resp.Data), fuse.OK
}

func (fs *FileSystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	if isCanceled(cancel) {
		return 0, fuse.EINTR
	}
	req := &pb.WriteRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Offset:   int64(input.Offset),
		Data:     data,
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return 0, st
	}
	fs.invalidateKernelData(input.NodeId, int64(input.Offset), int64(len(data)))
	resp, err := session.Write(context.Background(), req)
	if err != nil {
		return 0, statusToFuse(err)
	}
	written := int(resp.BytesWritten)
	if written > len(data) {
		written = len(data)
	}
	if written > 0 {
		fs.invalidateKernelAttr(input.NodeId)
		fs.storeKernelCache(input.NodeId, int64(input.Offset), data[:written])
	}
	return uint32(resp.BytesWritten), fuse.OK
}

func (fs *FileSystem) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	req := &pb.ReleaseRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return
	}
	_, _ = session.Release(context.Background(), req)
}

func (fs *FileSystem) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.FlushRequest{
		VolumeId: fs.volumeID,
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	_, err := session.Flush(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	return fuse.OK
}

func (fs *FileSystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.FsyncRequest{
		VolumeId: fs.volumeID,
		HandleId: input.Fh,
		Datasync: input.FsyncFlags != 0,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	_, err := session.Fsync(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	return fuse.OK
}

func (fs *FileSystem) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	fs.invalidateKernelInode(input.NodeId)
	_, err := session.Fallocate(context.Background(), &pb.FallocateRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Mode:     input.Mode,
		Offset:   int64(input.Offset),
		Length:   int64(input.Length),
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelInode(input.NodeId)
	return fuse.OK
}

func (fs *FileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.OpenDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Flags:    input.Flags,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.OpenDir(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	out.Fh = resp.HandleId
	return fuse.OK
}

func (fs *FileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Offset:   int64(input.Offset),
		Size:     input.Size,
		Plus:     false,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.ReadDir(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	for _, entry := range resp.Entries {
		mode := entry.Type
		if entry.Attr != nil {
			mode = entry.Attr.Mode
		}
		if !out.AddDirEntry(fuse.DirEntry{
			Ino:  entry.Inode,
			Name: entry.Name,
			Mode: mode,
			Off:  entry.Offset,
		}) {
			break
		}
	}
	return fuse.OK
}

func (fs *FileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Offset:   int64(input.Offset),
		Size:     input.Size,
		Plus:     true,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.ReadDir(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	for _, entry := range resp.Entries {
		mode := entry.Type
		if entry.Attr != nil {
			mode = entry.Attr.Mode
		}
		entryOut := out.AddDirLookupEntry(fuse.DirEntry{
			Ino:  entry.Inode,
			Name: entry.Name,
			Mode: mode,
			Off:  entry.Offset,
		})
		if entryOut == nil {
			break
		}
		setEntryOut(entryOut, entry.Inode, entry.Attr, fs.cacheTTL)
	}
	return fuse.OK
}

func (fs *FileSystem) ReleaseDir(input *fuse.ReleaseIn) {
	req := &pb.ReleaseDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return
	}
	_, _ = session.ReleaseDir(context.Background(), req)
}

func (fs *FileSystem) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.StatFsRequest{
		VolumeId: fs.volumeID,
		Actor:    actorFromHeader(input),
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.StatFs(context.Background(), req)
	if err != nil {
		return statusToFuse(err)
	}
	out.Blocks = resp.Blocks
	out.Bfree = resp.Bfree
	out.Bavail = resp.Bavail
	out.Files = resp.Files
	out.Ffree = resp.Ffree
	out.Bsize = resp.Bsize
	out.Frsize = resp.Frsize
	out.NameLen = resp.Namelen
	return fuse.OK
}

func (fs *FileSystem) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	if isCanceled(cancel) {
		return 0, fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return 0, st
	}
	resp, err := session.GetXattr(context.Background(), &pb.GetXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Name:     attr,
		Size:     uint32(len(dest)),
		Actor:    actorFromHeader(header),
	})
	if err != nil {
		return 0, statusToFuse(err)
	}
	if len(dest) == 0 {
		return uint32(len(resp.Value)), fuse.OK
	}
	if len(resp.Value) > len(dest) {
		return uint32(len(resp.Value)), fuse.ERANGE
	}
	copy(dest, resp.Value)
	return uint32(len(resp.Value)), fuse.OK
}

func (fs *FileSystem) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	if isCanceled(cancel) {
		return 0, fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return 0, st
	}
	resp, err := session.ListXattr(context.Background(), &pb.ListXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Size:     int32(len(dest)),
		Actor:    actorFromHeader(header),
	})
	if err != nil {
		return 0, statusToFuse(err)
	}
	if len(dest) == 0 {
		return uint32(len(resp.Data)), fuse.OK
	}
	if len(resp.Data) > len(dest) {
		return uint32(len(resp.Data)), fuse.ERANGE
	}
	copy(dest, resp.Data)
	return uint32(len(resp.Data)), fuse.OK
}

func (fs *FileSystem) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	_, err := session.SetXattr(context.Background(), &pb.SetXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Name:     attr,
		Value:    data,
		Flags:    input.Flags,
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return statusToFuse(err)
	}
	return fuse.OK
}

func (fs *FileSystem) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	_, err := session.RemoveXattr(context.Background(), &pb.RemoveXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Name:     attr,
		Actor:    actorFromHeader(header),
	})
	if err != nil {
		return statusToFuse(err)
	}
	return fuse.OK
}

func (fs *FileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}
	resp, err := session.Mknod(context.Background(), &pb.MknodRequest{
		VolumeId: fs.volumeID,
		Parent:   input.NodeId,
		Name:     name,
		Mode:     input.Mode,
		Rdev:     input.Rdev,
		Umask:    input.Umask,
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return statusToFuse(err)
	}
	fs.invalidateKernelEntry(input.NodeId, name)
	fs.invalidateKernelAttr(input.NodeId)
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *FileSystem) Lseek(cancel <-chan struct{}, input *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FileSystem) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}

	resp, err := session.GetLk(context.Background(), &pb.GetLkRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Owner:    input.Owner,
		Lock:     toPBLock(input.Lk),
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return statusToFuse(err)
	}
	if resp != nil && resp.Lock != nil {
		out.Lk = fromPBLock(resp.Lock)
	}
	return fuse.OK
}

func (fs *FileSystem) SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	return fs.setLk(cancel, input, false)
}

func (fs *FileSystem) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	return fs.setLk(cancel, input, true)
}

func (fs *FileSystem) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	if isCanceled(cancel) {
		return 0, fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return 0, st
	}
	fs.invalidateKernelInode(input.NodeIdOut)
	resp, err := session.CopyFileRange(context.Background(), &pb.CopyFileRangeRequest{
		VolumeId:  fs.volumeID,
		InodeIn:   input.NodeId,
		HandleIn:  input.FhIn,
		OffsetIn:  input.OffIn,
		InodeOut:  input.NodeIdOut,
		HandleOut: input.FhOut,
		OffsetOut: input.OffOut,
		Length:    input.Len,
		Flags:     uint32(input.Flags),
		Actor:     actorFromCaller(input.Caller),
	})
	if err != nil {
		return 0, statusToFuse(err)
	}
	if resp == nil {
		return 0, fuse.EIO
	}
	if resp.BytesCopied > 0 {
		fs.invalidateKernelInode(input.NodeIdOut)
	}
	return uint32(resp.BytesCopied), fuse.OK
}

func (fs *FileSystem) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FileSystem) Ioctl(cancel <-chan struct{}, in *fuse.IoctlIn, bufIn []byte, out *fuse.IoctlOut, bufOut []byte) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	return fuse.ENOSYS
}

func (fs *FileSystem) setLk(cancel <-chan struct{}, input *fuse.LkIn, block bool) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	session, st := fs.requireSession()
	if st != fuse.OK {
		return st
	}

	if input.LkFlags&fuse.FUSE_LK_FLOCK != 0 {
		_, err := session.Flock(context.Background(), &pb.FlockRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			HandleId: input.Fh,
			Owner:    input.Owner,
			Typ:      input.Lk.Typ,
			Block:    block,
			Actor:    actorFromCaller(input.Caller),
		})
		if err != nil {
			return statusToFuse(err)
		}
		return fuse.OK
	}

	req := &pb.SetLkRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Owner:    input.Owner,
		Lock:     toPBLock(input.Lk),
		Block:    block,
		Actor:    actorFromCaller(input.Caller),
	}
	var err error
	if block {
		_, err = session.SetLkw(context.Background(), req)
	} else {
		_, err = session.SetLk(context.Background(), req)
	}
	if err != nil {
		return statusToFuse(err)
	}
	return fuse.OK
}

func toPBLock(lock fuse.FileLock) *pb.FileLock {
	return &pb.FileLock{
		Start: lock.Start,
		End:   lock.End,
		Typ:   lock.Typ,
		Pid:   lock.Pid,
	}
}

func fromPBLock(lock *pb.FileLock) fuse.FileLock {
	if lock == nil {
		return fuse.FileLock{}
	}
	return fuse.FileLock{
		Start: lock.Start,
		End:   lock.End,
		Typ:   lock.Typ,
		Pid:   lock.Pid,
	}
}

func isCanceled(cancel <-chan struct{}) bool {
	if cancel == nil {
		return false
	}
	select {
	case <-cancel:
		return true
	default:
		return false
	}
}

func setEntryOut(out *fuse.EntryOut, inode uint64, attr *pb.GetAttrResponse, ttl time.Duration) {
	out.NodeId = inode
	out.Generation = 1
	out.SetEntryTimeout(ttl)
	setAttr(&out.Attr, attr)
	out.SetAttrTimeout(ttl)
}

func setAttrOut(out *fuse.AttrOut, attr *pb.GetAttrResponse, ttl time.Duration) {
	setAttr(&out.Attr, attr)
	out.SetTimeout(ttl)
}

func setAttr(out *fuse.Attr, attr *pb.GetAttrResponse) {
	if attr == nil {
		return
	}
	out.Ino = attr.Ino
	out.Mode = attr.Mode
	out.Nlink = attr.Nlink
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Rdev = uint32(attr.Rdev)
	out.Size = attr.Size
	out.Blocks = attr.Blocks
	out.Atime = uint64(attr.AtimeSec)
	out.Atimensec = uint32(attr.AtimeNsec)
	out.Mtime = uint64(attr.MtimeSec)
	out.Mtimensec = uint32(attr.MtimeNsec)
	out.Ctime = uint64(attr.CtimeSec)
	out.Ctimensec = uint32(attr.CtimeNsec)
	out.Blksize = 4096
}

func statusToFuse(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	if err == context.Canceled {
		return fuse.EINTR
	}
	if err == context.DeadlineExceeded {
		return fuse.EIO
	}
	var fsErr *fserror.Error
	if errors.As(err, &fsErr) {
		switch fsErr.Code() {
		case fserror.NotFound:
			return fuse.ENOENT
		case fserror.AlreadyExists:
			return fuse.Status(syscall.EEXIST)
		case fserror.PermissionDenied, fserror.Unauthenticated:
			return fuse.EPERM
		case fserror.InvalidArgument:
			return fuse.EINVAL
		case fserror.FailedPrecondition:
			return fuse.EIO
		case fserror.Unimplemented:
			return fuse.ENOSYS
		default:
			return fuse.EIO
		}
	}
	if errno, ok := err.(syscall.Errno); ok {
		return fuse.Status(errno)
	}
	return fuse.EIO
}
