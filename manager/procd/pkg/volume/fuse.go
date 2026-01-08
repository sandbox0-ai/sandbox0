package volume

import (
	"context"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/sandbox0-ai/infra/storage-proxy/proto/fs"
)

// RemoteFS implements fuse.RawFileSystem, proxying all operations to storage-proxy.
type RemoteFS struct {
	fuse.RawFileSystem

	client   pb.FileSystemClient
	volumeID string
	logger   *zap.Logger

	// File handle management
	handleMu    sync.RWMutex
	nextHandle  uint64
	openHandles map[uint64]*fileHandle
}

type fileHandle struct {
	inode    uint64
	handleID uint64
	flags    uint32
}

// NewRemoteFS creates a new remote filesystem.
func NewRemoteFS(client pb.FileSystemClient, volumeID string, logger *zap.Logger) *RemoteFS {
	return &RemoteFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		client:        client,
		volumeID:      volumeID,
		logger:        logger,
		openHandles:   make(map[uint64]*fileHandle),
	}
}

func (fs *RemoteFS) newContext(cancel <-chan struct{}) context.Context {
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		select {
		case <-cancel:
			cancelFunc()
		case <-ctx.Done():
		}
	}()
	return ctx
}

// String returns the filesystem name.
func (fs *RemoteFS) String() string {
	return "sandbox0-remote"
}

// Lookup looks up a child entry.
func (fs *RemoteFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.Lookup(ctx, &pb.LookupRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
	})
	if err != nil {
		fs.logger.Debug("Lookup failed", zap.Error(err), zap.String("name", name))
		return grpcErrToFuseStatus(err)
	}

	out.NodeId = resp.Inode
	out.Generation = resp.Generation
	if resp.Attr != nil {
		fillEntryAttr(resp.Attr, &out.Attr)
	}
	out.SetEntryTimeout(time.Second)
	out.SetAttrTimeout(time.Second)

	return fuse.OK
}

// GetAttr gets file attributes.
func (fs *RemoteFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		fs.logger.Error("GetAttr failed", zap.Error(err), zap.Uint64("inode", input.NodeId))
		return grpcErrToFuseStatus(err)
	}

	fillEntryAttr(resp, &out.Attr)
	out.SetTimeout(time.Second)

	return fuse.OK
}

// SetAttr sets file attributes.
func (fs *RemoteFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx := fs.newContext(cancel)

	req := &pb.SetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Valid:    input.Valid,
		Attr: &pb.GetAttrResponse{
			Mode: input.Mode,
			Uid:  input.Uid,
			Gid:  input.Gid,
			Size: input.Size,
		},
	}

	resp, err := fs.client.SetAttr(ctx, req)
	if err != nil {
		fs.logger.Error("SetAttr failed", zap.Error(err), zap.Uint64("inode", input.NodeId))
		return grpcErrToFuseStatus(err)
	}

	if resp.Attr != nil {
		fillEntryAttr(resp.Attr, &out.Attr)
	}
	out.SetTimeout(time.Second)

	return fuse.OK
}

// Open opens a file.
func (fs *RemoteFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.Open(ctx, &pb.OpenRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Flags:    input.Flags,
	})
	if err != nil {
		fs.logger.Error("Open failed", zap.Error(err), zap.Uint64("inode", input.NodeId))
		return grpcErrToFuseStatus(err)
	}

	fs.handleMu.Lock()
	fs.nextHandle++
	localHandle := fs.nextHandle
	fs.openHandles[localHandle] = &fileHandle{
		inode:    input.NodeId,
		handleID: resp.HandleId,
		flags:    input.Flags,
	}
	fs.handleMu.Unlock()

	out.Fh = localHandle
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

// Read reads data from a file.
func (fs *RemoteFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	ctx := fs.newContext(cancel)

	fs.handleMu.RLock()
	handle, exists := fs.openHandles[input.Fh]
	fs.handleMu.RUnlock()

	if !exists {
		return nil, fuse.EBADF
	}

	resp, err := fs.client.Read(ctx, &pb.ReadRequest{
		VolumeId: fs.volumeID,
		Inode:    handle.inode,
		Offset:   int64(input.Offset),
		Size:     int64(input.Size),
		HandleId: handle.handleID,
	})
	if err != nil {
		fs.logger.Error("Read failed", zap.Error(err), zap.Uint64("inode", handle.inode))
		return nil, grpcErrToFuseStatus(err)
	}

	return fuse.ReadResultData(resp.Data), fuse.OK
}

// Write writes data to a file.
func (fs *RemoteFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	ctx := fs.newContext(cancel)

	fs.handleMu.RLock()
	handle, exists := fs.openHandles[input.Fh]
	fs.handleMu.RUnlock()

	if !exists {
		return 0, fuse.EBADF
	}

	resp, err := fs.client.Write(ctx, &pb.WriteRequest{
		VolumeId: fs.volumeID,
		Inode:    handle.inode,
		Offset:   int64(input.Offset),
		Data:     data,
		HandleId: handle.handleID,
	})
	if err != nil {
		fs.logger.Error("Write failed", zap.Error(err), zap.Uint64("inode", handle.inode))
		return 0, grpcErrToFuseStatus(err)
	}

	return uint32(resp.BytesWritten), fuse.OK
}

// Release closes a file handle.
func (fs *RemoteFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	ctx := fs.newContext(cancel)

	fs.handleMu.Lock()
	handle, exists := fs.openHandles[input.Fh]
	if exists {
		delete(fs.openHandles, input.Fh)
	}
	fs.handleMu.Unlock()

	if !exists {
		return
	}

	_, err := fs.client.Release(ctx, &pb.ReleaseRequest{
		VolumeId: fs.volumeID,
		Inode:    handle.inode,
		HandleId: handle.handleID,
	})
	if err != nil {
		fs.logger.Warn("Release failed", zap.Error(err))
	}
}

// Create creates a new file.
func (fs *RemoteFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.Create(ctx, &pb.CreateRequest{
		VolumeId: fs.volumeID,
		Parent:   input.NodeId,
		Name:     name,
		Mode:     input.Mode,
		Flags:    input.Flags,
	})
	if err != nil {
		fs.logger.Error("Create failed", zap.Error(err), zap.String("name", name))
		return grpcErrToFuseStatus(err)
	}

	out.NodeId = resp.Inode
	out.Generation = resp.Generation
	if resp.Attr != nil {
		fillEntryAttr(resp.Attr, &out.Attr)
	}
	out.SetEntryTimeout(time.Second)
	out.SetAttrTimeout(time.Second)

	fs.handleMu.Lock()
	fs.nextHandle++
	localHandle := fs.nextHandle
	fs.openHandles[localHandle] = &fileHandle{
		inode:    resp.Inode,
		handleID: resp.HandleId,
		flags:    input.Flags,
	}
	fs.handleMu.Unlock()

	out.Fh = localHandle
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

// Mkdir creates a directory.
func (fs *RemoteFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: fs.volumeID,
		Parent:   input.NodeId,
		Name:     name,
		Mode:     input.Mode,
	})
	if err != nil {
		fs.logger.Error("Mkdir failed", zap.Error(err), zap.String("name", name))
		return grpcErrToFuseStatus(err)
	}

	out.NodeId = resp.Inode
	out.Generation = resp.Generation
	if resp.Attr != nil {
		fillEntryAttr(resp.Attr, &out.Attr)
	}
	out.SetEntryTimeout(time.Second)
	out.SetAttrTimeout(time.Second)

	return fuse.OK
}

// Unlink removes a file.
func (fs *RemoteFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx := fs.newContext(cancel)

	_, err := fs.client.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
	})
	if err != nil {
		fs.logger.Error("Unlink failed", zap.Error(err), zap.String("name", name))
		return grpcErrToFuseStatus(err)
	}

	return fuse.OK
}

// Rmdir removes a directory.
func (fs *RemoteFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx := fs.newContext(cancel)

	_, err := fs.client.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
	})
	if err != nil {
		fs.logger.Error("Rmdir failed", zap.Error(err), zap.String("name", name))
		return grpcErrToFuseStatus(err)
	}

	return fuse.OK
}

// Rename renames a file or directory.
func (fs *RemoteFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	ctx := fs.newContext(cancel)

	_, err := fs.client.Rename(ctx, &pb.RenameRequest{
		VolumeId:  fs.volumeID,
		OldParent: input.NodeId,
		OldName:   oldName,
		NewParent: input.Newdir,
		NewName:   newName,
		Flags:     input.Flags, // Pass rename flags (RENAME_NOREPLACE, RENAME_EXCHANGE, etc.)
	})
	if err != nil {
		fs.logger.Error("Rename failed", zap.Error(err), zap.String("old", oldName), zap.String("new", newName))
		return grpcErrToFuseStatus(err)
	}

	return fuse.OK
}

// OpenDir opens a directory.
func (fs *RemoteFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	// For directories, we don't need to maintain handles
	out.Fh = input.NodeId
	return fuse.OK
}

// ReadDir reads directory entries.
func (fs *RemoteFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Offset:   int64(input.Offset),
	})
	if err != nil {
		fs.logger.Error("ReadDir failed", zap.Error(err), zap.Uint64("inode", input.NodeId))
		return grpcErrToFuseStatus(err)
	}

	for _, entry := range resp.Entries {
		if !out.AddDirEntry(fuse.DirEntry{
			Ino:  entry.Inode,
			Mode: entry.Type,
			Name: entry.Name,
		}) {
			break
		}
	}

	return fuse.OK
}

// ReadDirPlus reads directory entries with attributes.
func (fs *RemoteFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Offset:   int64(input.Offset),
	})
	if err != nil {
		fs.logger.Error("ReadDirPlus failed", zap.Error(err), zap.Uint64("inode", input.NodeId))
		return grpcErrToFuseStatus(err)
	}

	for _, entry := range resp.Entries {
		entryOut := out.AddDirLookupEntry(fuse.DirEntry{
			Ino:  entry.Inode,
			Mode: entry.Type,
			Name: entry.Name,
		})
		if entryOut == nil {
			break
		}
		entryOut.NodeId = entry.Inode
		if entry.Attr != nil {
			fillEntryAttr(entry.Attr, &entryOut.Attr)
		}
		entryOut.SetEntryTimeout(time.Second)
		entryOut.SetAttrTimeout(time.Second)
	}

	return fuse.OK
}

// ReleaseDir releases a directory handle.
func (fs *RemoteFS) ReleaseDir(input *fuse.ReleaseIn) {
	// Nothing to do for directories
}

// Flush flushes a file.
func (fs *RemoteFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ctx := fs.newContext(cancel)

	fs.handleMu.RLock()
	handle, exists := fs.openHandles[input.Fh]
	fs.handleMu.RUnlock()

	if !exists {
		return fuse.OK
	}

	_, err := fs.client.Flush(ctx, &pb.FlushRequest{
		VolumeId: fs.volumeID,
		HandleId: handle.handleID,
	})
	if err != nil {
		fs.logger.Warn("Flush failed", zap.Error(err))
	}

	return fuse.OK
}

// Fsync syncs a file.
func (fs *RemoteFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ctx := fs.newContext(cancel)

	fs.handleMu.RLock()
	handle, exists := fs.openHandles[input.Fh]
	fs.handleMu.RUnlock()

	if !exists {
		return fuse.OK
	}

	_, err := fs.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: fs.volumeID,
		HandleId: handle.handleID,
		Datasync: input.FsyncFlags&1 != 0,
	})
	if err != nil {
		fs.logger.Warn("Fsync failed", zap.Error(err))
	}

	return fuse.OK
}

// StatFs gets filesystem statistics.
func (fs *RemoteFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.StatFs(ctx, &pb.StatFsRequest{
		VolumeId: fs.volumeID,
	})
	if err != nil {
		fs.logger.Error("StatFs failed", zap.Error(err))
		return grpcErrToFuseStatus(err)
	}

	out.Blocks = resp.Blocks
	out.Bfree = resp.Bfree
	out.Bavail = resp.Bavail
	out.Files = resp.Files
	out.Ffree = resp.Ffree
	out.Bsize = resp.Bsize
	out.NameLen = resp.Namelen
	out.Frsize = resp.Frsize

	return fuse.OK
}

// Symlink creates a symbolic link.
func (fs *RemoteFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.Symlink(ctx, &pb.SymlinkRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     linkName,
		Target:   pointedTo,
	})
	if err != nil {
		fs.logger.Error("Symlink failed", zap.Error(err), zap.String("name", linkName), zap.String("target", pointedTo))
		return grpcErrToFuseStatus(err)
	}

	out.NodeId = resp.Inode
	out.Generation = resp.Generation
	if resp.Attr != nil {
		fillEntryAttr(resp.Attr, &out.Attr)
	}
	out.SetEntryTimeout(time.Second)
	out.SetAttrTimeout(time.Second)

	return fuse.OK
}

// Readlink reads the target of a symbolic link.
func (fs *RemoteFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.Readlink(ctx, &pb.ReadlinkRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
	})
	if err != nil {
		fs.logger.Error("Readlink failed", zap.Error(err), zap.Uint64("inode", header.NodeId))
		return nil, grpcErrToFuseStatus(err)
	}

	return []byte(resp.Target), fuse.OK
}

// Link creates a hard link.
func (fs *RemoteFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.Link(ctx, &pb.LinkRequest{
		VolumeId:  fs.volumeID,
		Inode:     input.Oldnodeid,
		NewParent: input.NodeId,
		NewName:   name,
	})
	if err != nil {
		fs.logger.Error("Link failed", zap.Error(err), zap.String("name", name))
		return grpcErrToFuseStatus(err)
	}

	out.NodeId = resp.Inode
	out.Generation = resp.Generation
	if resp.Attr != nil {
		fillEntryAttr(resp.Attr, &out.Attr)
	}
	out.SetEntryTimeout(time.Second)
	out.SetAttrTimeout(time.Second)

	return fuse.OK
}

// Access checks file access permissions.
func (fs *RemoteFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	ctx := fs.newContext(cancel)

	// Pass caller's uid/gid for proper POSIX permission checking
	// Note: FUSE provides only the primary gid, not supplementary groups
	_, err := fs.client.Access(ctx, &pb.AccessRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Mask:     input.Mask,
		Uid:      input.Uid,
		Gids:     []uint32{input.Gid}, // FUSE only provides primary gid
	})
	if err != nil {
		fs.logger.Debug("Access denied",
			zap.Error(err),
			zap.Uint64("inode", input.NodeId),
			zap.Uint32("mask", input.Mask),
			zap.Uint32("uid", input.Uid),
			zap.Uint32("gid", input.Gid))
		return grpcErrToFuseStatus(err)
	}

	return fuse.OK
}

// Fallocate preallocates or deallocates space for a file.
func (fs *RemoteFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	ctx := fs.newContext(cancel)

	fs.handleMu.RLock()
	handle, exists := fs.openHandles[input.Fh]
	fs.handleMu.RUnlock()

	if !exists {
		return fuse.EBADF
	}

	_, err := fs.client.Fallocate(ctx, &pb.FallocateRequest{
		VolumeId: fs.volumeID,
		Inode:    handle.inode,
		Mode:     uint32(input.Mode),
		Offset:   int64(input.Offset),
		Length:   int64(input.Length),
		HandleId: handle.handleID,
	})
	if err != nil {
		fs.logger.Error("Fallocate failed",
			zap.Error(err),
			zap.Uint64("inode", handle.inode),
			zap.Uint32("mode", uint32(input.Mode)),
			zap.Uint64("offset", input.Offset),
			zap.Uint64("length", input.Length))
		return grpcErrToFuseStatus(err)
	}

	return fuse.OK
}

// GetXAttr gets an extended attribute.
func (fs *RemoteFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.GetXattr(ctx, &pb.GetXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Name:     attr,
		Size:     uint32(len(dest)),
	})
	if err != nil {
		fs.logger.Debug("GetXAttr failed",
			zap.Error(err),
			zap.Uint64("inode", header.NodeId),
			zap.String("attr", attr))
		return 0, grpcErrToFuseStatus(err)
	}

	if len(dest) == 0 {
		// Query size
		return uint32(len(resp.Value)), fuse.OK
	}

	if len(resp.Value) > len(dest) {
		return 0, fuse.Status(syscall.ERANGE)
	}

	copy(dest, resp.Value)
	return uint32(len(resp.Value)), fuse.OK
}

// SetXAttr sets an extended attribute.
func (fs *RemoteFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	ctx := fs.newContext(cancel)

	_, err := fs.client.SetXattr(ctx, &pb.SetXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Name:     attr,
		Value:    data,
		Flags:    input.Flags,
	})
	if err != nil {
		fs.logger.Error("SetXAttr failed",
			zap.Error(err),
			zap.Uint64("inode", input.NodeId),
			zap.String("attr", attr),
			zap.Uint32("flags", input.Flags))
		return grpcErrToFuseStatus(err)
	}

	return fuse.OK
}

// ListXAttr lists all extended attributes.
func (fs *RemoteFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	ctx := fs.newContext(cancel)

	resp, err := fs.client.ListXattr(ctx, &pb.ListXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Size:     int32(len(dest)),
	})
	if err != nil {
		fs.logger.Error("ListXAttr failed",
			zap.Error(err),
			zap.Uint64("inode", header.NodeId))
		return 0, grpcErrToFuseStatus(err)
	}

	if len(dest) == 0 {
		// Query size
		return uint32(len(resp.Data)), fuse.OK
	}

	if len(resp.Data) > len(dest) {
		return 0, fuse.Status(syscall.ERANGE)
	}

	copy(dest, resp.Data)
	return uint32(len(resp.Data)), fuse.OK
}

// RemoveXAttr removes an extended attribute.
func (fs *RemoteFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	ctx := fs.newContext(cancel)

	_, err := fs.client.RemoveXattr(ctx, &pb.RemoveXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Name:     attr,
	})
	if err != nil {
		fs.logger.Error("RemoveXAttr failed",
			zap.Error(err),
			zap.Uint64("inode", header.NodeId),
			zap.String("attr", attr))
		return grpcErrToFuseStatus(err)
	}

	return fuse.OK
}

// grpcErrToFuseStatus converts gRPC error to FUSE status with proper error codes.
func grpcErrToFuseStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}

	st, ok := status.FromError(err)
	if !ok {
		return fuse.EIO
	}

	switch st.Code() {
	case codes.OK:
		return fuse.OK
	case codes.NotFound:
		return fuse.ENOENT
	case codes.PermissionDenied:
		return fuse.EACCES
	case codes.AlreadyExists:
		return fuse.Status(syscall.EEXIST)
	case codes.InvalidArgument:
		return fuse.EINVAL
	case codes.ResourceExhausted:
		return fuse.Status(syscall.ENOSPC)
	case codes.FailedPrecondition:
		return fuse.EPERM
	case codes.Aborted:
		return fuse.Status(syscall.EINTR)
	case codes.OutOfRange:
		return fuse.Status(syscall.ERANGE)
	case codes.Unimplemented:
		return fuse.ENOSYS
	case codes.Unavailable:
		return fuse.Status(syscall.EAGAIN)
	default:
		return fuse.EIO
	}
}

// fillEntryAttr fills fuse.Attr from protobuf response.
func fillEntryAttr(resp *pb.GetAttrResponse, attr *fuse.Attr) {
	attr.Ino = resp.Ino
	attr.Mode = resp.Mode
	attr.Nlink = resp.Nlink
	attr.Owner.Uid = resp.Uid
	attr.Owner.Gid = resp.Gid
	attr.Rdev = uint32(resp.Rdev)
	attr.Size = resp.Size
	attr.Blocks = resp.Blocks
	attr.Atime = uint64(resp.AtimeSec)
	attr.Atimensec = uint32(resp.AtimeNsec)
	attr.Mtime = uint64(resp.MtimeSec)
	attr.Mtimensec = uint32(resp.MtimeNsec)
	attr.Ctime = uint64(resp.CtimeSec)
	attr.Ctimensec = uint32(resp.CtimeNsec)
}

// MountFUSE mounts a FUSE filesystem at the given path.
func MountFUSE(mountPoint string, remoteFS *RemoteFS, logger *zap.Logger) (*fuse.Server, error) {
	opts := fuse.MountOptions{
		Name:        "sandbox0",
		FsName:      "sandbox0:remote",
		AllowOther:  true,
		Debug:       false,
		DirectMount: true,
		MaxWrite:    128 * 1024,
	}

	server, err := fuse.NewServer(remoteFS, mountPoint, &opts)
	if err != nil {
		return nil, err
	}

	// Start serving in background
	go server.Serve()

	return server, nil
}

// UnmountFUSE unmounts a FUSE filesystem.
func UnmountFUSE(server *fuse.Server, mountPoint string) error {
	if server != nil {
		if err := server.Unmount(); err != nil {
			// Fallback to syscall unmount
			return syscall.Unmount(mountPoint, 0)
		}
	}
	return nil
}

// IsFUSEMountPoint checks if a path is a FUSE mount point.
func IsFUSEMountPoint(path string) bool {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false
	}
	// FUSE magic number is 0x65735546
	return stat.Type == 0x65735546
}

// CleanupStaleMounts attempts to cleanup stale FUSE mounts.
func CleanupStaleMounts(mountPoint string, logger *zap.Logger) {
	if IsFUSEMountPoint(mountPoint) {
		logger.Info("Cleaning up stale FUSE mount", zap.String("mount_point", mountPoint))
		// Use 0 as flags; MNT_FORCE is not portable across platforms
		_ = syscall.Unmount(mountPoint, 0)
	}
	// Ensure directory exists for new mount
	_ = os.MkdirAll(mountPoint, 0755)
}
