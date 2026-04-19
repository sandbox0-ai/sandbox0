package volume

import (
	"context"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type grpcFS struct {
	fuse.RawFileSystem
	volumeID      string
	sessionID     string
	sessionSecret string
	client        pb.FileSystemClient
	session       *sessionFS
	tokenProvider TokenProvider
	cacheTTL      time.Duration
	logger        *zap.Logger
}

func newGrpcFS(volumeID, sessionID, sessionSecret string, client pb.FileSystemClient, tokenProvider TokenProvider, cacheTTL time.Duration, logger *zap.Logger) *grpcFS {
	if cacheTTL < 0 {
		cacheTTL = time.Second
	}
	return &grpcFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		volumeID:      volumeID,
		sessionID:     sessionID,
		sessionSecret: sessionSecret,
		client:        client,
		tokenProvider: tokenProvider,
		cacheTTL:      cacheTTL,
		logger:        logger,
	}
}

func (fs *grpcFS) String() string {
	return "sandbox0-volume"
}

func (fs *grpcFS) setSession(session *sessionFS) {
	if fs == nil {
		return
	}
	fs.session = session
}

func (fs *grpcFS) withToken(ctx context.Context) (context.Context, error) {
	if fs.sessionID != "" && fs.sessionSecret != "" {
		pairs := []string{
			strings.ToLower(internalauth.VolumeSessionIDHeader), fs.sessionID,
			strings.ToLower(internalauth.VolumeSessionSecretHeader), fs.sessionSecret,
		}
		if fs.volumeID != "" {
			pairs = append(pairs, strings.ToLower(internalauth.VolumeIDHeader), fs.volumeID)
		}
		return metadata.AppendToOutgoingContext(ctx, pairs...), nil
	}
	if fs.tokenProvider == nil {
		return nil, ErrMissingInternalToken
	}
	token := fs.tokenProvider.GetInternalToken()
	if token == "" {
		return nil, ErrMissingInternalToken
	}
	return metadata.AppendToOutgoingContext(ctx, "x-internal-token", token), nil
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

func (fs *grpcFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.LookupRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
		Actor:    actorFromHeader(header),
	}
	var (
		resp *pb.NodeResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Lookup(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.Lookup(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}

	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *grpcFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Actor:    actorFromCaller(input.Caller),
	}
	var (
		resp *pb.GetAttrResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.GetAttr(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.GetAttr(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	setAttrOut(out, resp, fs.cacheTTL)
	return fuse.OK
}

func (fs *grpcFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
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
	var (
		resp *pb.SetAttrResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.SetAttr(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.SetAttr(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	setAttrOut(out, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *grpcFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
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
	var (
		resp *pb.NodeResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Mkdir(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.Mkdir(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *grpcFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.UnlinkRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
		Actor:    actorFromHeader(header),
	}
	var err error
	if fs.session != nil {
		_, err = fs.session.Unlink(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		_, err = fs.client.Unlink(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.RmdirRequest{
		VolumeId: fs.volumeID,
		Parent:   header.NodeId,
		Name:     name,
		Actor:    actorFromHeader(header),
	}
	var err error
	if fs.session != nil {
		_, err = fs.session.Rmdir(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		_, err = fs.client.Rmdir(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) fuse.Status {
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
	var err error
	if fs.session != nil {
		_, err = fs.session.Rename(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		_, err = fs.client.Rename(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}
	resp, err := fs.client.Link(ctx, &pb.LinkRequest{
		VolumeId:  fs.volumeID,
		Inode:     input.Oldnodeid,
		NewParent: input.NodeId,
		NewName:   filename,
		Actor:     actorFromCaller(input.Caller),
	})
	if err != nil {
		return grpcToFuse(err)
	}
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *grpcFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status {
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
	var (
		resp *pb.NodeResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Symlink(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.Symlink(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *grpcFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	if isCanceled(cancel) {
		return nil, fuse.EINTR
	}
	req := &pb.ReadlinkRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Actor:    actorFromHeader(header),
	}
	var (
		resp *pb.ReadlinkResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Readlink(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return nil, fuse.EPERM
		}
		resp, err = fs.client.Readlink(ctx, req)
	}
	if err != nil {
		return nil, grpcToFuse(err)
	}
	return []byte(resp.Target), fuse.OK
}

func (fs *grpcFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
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
	var err error
	if fs.session != nil {
		_, err = fs.session.Access(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		_, err = fs.client.Access(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
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
	var (
		resp *pb.NodeResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Create(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.Create(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	setEntryOut(&out.EntryOut, resp.Inode, resp.Attr, fs.cacheTTL)
	out.Fh = resp.HandleId
	return fuse.OK
}

func (fs *grpcFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.OpenRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Flags:    input.Flags,
		Actor:    actorFromCaller(input.Caller),
	}
	var (
		resp *pb.OpenResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Open(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.Open(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	out.Fh = resp.HandleId
	return fuse.OK
}

func (fs *grpcFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
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
	var (
		resp *pb.ReadResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Read(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return nil, fuse.EPERM
		}
		resp, err = fs.client.Read(ctx, req)
	}
	if err != nil {
		return nil, grpcToFuse(err)
	}
	return fuse.ReadResultData(resp.Data), fuse.OK
}

func (fs *grpcFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
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
	var (
		resp *pb.WriteResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.Write(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return 0, fuse.EPERM
		}
		resp, err = fs.client.Write(ctx, req)
	}
	if err != nil {
		return 0, grpcToFuse(err)
	}
	return uint32(resp.BytesWritten), fuse.OK
}

func (fs *grpcFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	req := &pb.ReleaseRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	if fs.session != nil {
		_, _ = fs.session.Release(context.Background(), req)
		return
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return
	}
	_, _ = fs.client.Release(ctx, req)
}

func (fs *grpcFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.FlushRequest{
		VolumeId: fs.volumeID,
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	var err error
	if fs.session != nil {
		_, err = fs.session.Flush(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		_, err = fs.client.Flush(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.FsyncRequest{
		VolumeId: fs.volumeID,
		HandleId: input.Fh,
		Datasync: input.FsyncFlags != 0,
		Actor:    actorFromCaller(input.Caller),
	}
	var err error
	if fs.session != nil {
		_, err = fs.session.Fsync(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		_, err = fs.client.Fsync(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}
	_, err = fs.client.Fallocate(ctx, &pb.FallocateRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Mode:     input.Mode,
		Offset:   int64(input.Offset),
		Length:   int64(input.Length),
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.OpenDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Flags:    input.Flags,
		Actor:    actorFromCaller(input.Caller),
	}
	var (
		resp *pb.OpenDirResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.OpenDir(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.OpenDir(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
	}
	out.Fh = resp.HandleId
	return fuse.OK
}

func (fs *grpcFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
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
	var (
		resp *pb.ReadDirResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.ReadDir(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.ReadDir(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
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
		}) {
			break
		}
	}
	return fuse.OK
}

func (fs *grpcFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
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
	var (
		resp *pb.ReadDirResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.ReadDir(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.ReadDir(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
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
		})
		if entryOut == nil {
			break
		}
		setEntryOut(entryOut, entry.Inode, entry.Attr, fs.cacheTTL)
	}
	return fuse.OK
}

func (fs *grpcFS) ReleaseDir(input *fuse.ReleaseIn) {
	req := &pb.ReleaseDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Actor:    actorFromCaller(input.Caller),
	}
	if fs.session != nil {
		_, _ = fs.session.ReleaseDir(context.Background(), req)
		return
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return
	}
	_, _ = fs.client.ReleaseDir(ctx, req)
}

func (fs *grpcFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	req := &pb.StatFsRequest{
		VolumeId: fs.volumeID,
		Actor:    actorFromHeader(input),
	}
	var (
		resp *pb.StatFsResponse
		err  error
	)
	if fs.session != nil {
		resp, err = fs.session.StatFs(context.Background(), req)
	} else {
		ctx, tokenErr := fs.withToken(context.Background())
		if tokenErr != nil {
			return fuse.EPERM
		}
		resp, err = fs.client.StatFs(ctx, req)
	}
	if err != nil {
		return grpcToFuse(err)
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

func (fs *grpcFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	if isCanceled(cancel) {
		return 0, fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return 0, fuse.EPERM
	}
	resp, err := fs.client.GetXattr(ctx, &pb.GetXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Name:     attr,
		Size:     uint32(len(dest)),
		Actor:    actorFromHeader(header),
	})
	if err != nil {
		return 0, grpcToFuse(err)
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

func (fs *grpcFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	if isCanceled(cancel) {
		return 0, fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return 0, fuse.EPERM
	}
	resp, err := fs.client.ListXattr(ctx, &pb.ListXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Size:     int32(len(dest)),
		Actor:    actorFromHeader(header),
	})
	if err != nil {
		return 0, grpcToFuse(err)
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

func (fs *grpcFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}
	_, err = fs.client.SetXattr(ctx, &pb.SetXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Name:     attr,
		Value:    data,
		Flags:    input.Flags,
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}
	_, err = fs.client.RemoveXattr(ctx, &pb.RemoveXattrRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
		Name:     attr,
		Actor:    actorFromHeader(header),
	})
	if err != nil {
		return grpcToFuse(err)
	}
	return fuse.OK
}

func (fs *grpcFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}
	resp, err := fs.client.Mknod(ctx, &pb.MknodRequest{
		VolumeId: fs.volumeID,
		Parent:   input.NodeId,
		Name:     name,
		Mode:     input.Mode,
		Rdev:     input.Rdev,
		Umask:    input.Umask,
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return grpcToFuse(err)
	}
	setEntryOut(out, resp.Inode, resp.Attr, fs.cacheTTL)
	return fuse.OK
}

func (fs *grpcFS) Lseek(cancel <-chan struct{}, input *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *grpcFS) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}

	resp, err := fs.client.GetLk(ctx, &pb.GetLkRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Owner:    input.Owner,
		Lock:     toPBLock(input.Lk),
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return grpcToFuse(err)
	}
	if resp != nil && resp.Lock != nil {
		out.Lk = fromPBLock(resp.Lock)
	}
	return fuse.OK
}

func (fs *grpcFS) SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	return fs.setLk(cancel, input, false)
}

func (fs *grpcFS) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	return fs.setLk(cancel, input, true)
}

func (fs *grpcFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	if isCanceled(cancel) {
		return 0, fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return 0, fuse.EPERM
	}
	resp, err := fs.client.CopyFileRange(ctx, &pb.CopyFileRangeRequest{
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
		return 0, grpcToFuse(err)
	}
	if resp == nil {
		return 0, fuse.EIO
	}
	return uint32(resp.BytesCopied), fuse.OK
}

func (fs *grpcFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *grpcFS) Ioctl(cancel <-chan struct{}, in *fuse.IoctlIn, out *fuse.IoctlOut, bufIn, bufOut []byte) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}
	resp, err := fs.client.Ioctl(ctx, &pb.IoctlRequest{
		VolumeId:    fs.volumeID,
		Inode:       in.NodeId,
		Cmd:         in.Cmd,
		Arg:         in.Arg,
		DataIn:      bufIn,
		DataOutSize: uint32(len(bufOut)),
		Actor:       actorFromCaller(in.Caller),
	})
	if err != nil {
		return grpcToFuse(err)
	}
	if resp == nil {
		return fuse.EIO
	}
	if len(resp.DataOut) > len(bufOut) {
		copy(bufOut, resp.DataOut[:len(bufOut)])
		return fuse.ERANGE
	}
	copy(bufOut, resp.DataOut)
	return fuse.OK
}

func (fs *grpcFS) setLk(cancel <-chan struct{}, input *fuse.LkIn, block bool) fuse.Status {
	if isCanceled(cancel) {
		return fuse.EINTR
	}
	ctx, err := fs.withToken(context.Background())
	if err != nil {
		return fuse.EPERM
	}

	if input.LkFlags&fuse.FUSE_LK_FLOCK != 0 {
		_, err := fs.client.Flock(ctx, &pb.FlockRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			HandleId: input.Fh,
			Owner:    input.Owner,
			Typ:      input.Lk.Typ,
			Block:    block,
			Actor:    actorFromCaller(input.Caller),
		})
		if err != nil {
			return grpcToFuse(err)
		}
		return fuse.OK
	}

	_, err = fs.client.SetLk(ctx, &pb.SetLkRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		HandleId: input.Fh,
		Owner:    input.Owner,
		Lock:     toPBLock(input.Lk),
		Block:    block,
		Actor:    actorFromCaller(input.Caller),
	})
	if err != nil {
		return grpcToFuse(err)
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

func grpcToFuse(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	if err == context.Canceled {
		return fuse.EINTR
	}
	if err == context.DeadlineExceeded {
		return fuse.EIO
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			return fuse.ENOENT
		case codes.PermissionDenied, codes.Unauthenticated:
			return fuse.EPERM
		case codes.InvalidArgument:
			return fuse.EINVAL
		case codes.FailedPrecondition:
			return fuse.EIO
		case codes.Unimplemented:
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
