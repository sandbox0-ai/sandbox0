package grpc

import (
	"context"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/infra/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FileSystemServer implements the gRPC FileSystem service
type FileSystemServer struct {
	pb.UnimplementedFileSystemServer

	volMgr           *volume.Manager
	eventHub         *notify.Hub
	eventBroadcaster notify.Broadcaster
	logger           *logrus.Logger
}

// NewFileSystemServer creates a new file system server
func NewFileSystemServer(volMgr *volume.Manager, eventHub *notify.Hub, eventBroadcaster notify.Broadcaster, logger *logrus.Logger) *FileSystemServer {
	if eventBroadcaster == nil && eventHub != nil {
		eventBroadcaster = notify.NewLocalBroadcaster(eventHub)
	}
	return &FileSystemServer{
		volMgr:           volMgr,
		eventHub:         eventHub,
		eventBroadcaster: eventBroadcaster,
		logger:           logger,
	}
}

// MountVolume mounts a volume
func (s *FileSystemServer) MountVolume(ctx context.Context, req *pb.MountVolumeRequest) (*pb.MountVolumeResponse, error) {
	// Extract team ID from context for multi-tenant isolation
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.TeamID == "" {
		s.logger.WithField("volume_id", req.VolumeId).Error("TeamID not found in context")
		return nil, status.Error(codes.Unauthenticated, "team id not found in context")
	}

	config := &volume.VolumeConfig{
		CacheSize:  req.Config.CacheSize,
		Prefetch:   int(req.Config.Prefetch),
		BufferSize: req.Config.BufferSize,
		Writeback:  req.Config.Writeback,
		ReadOnly:   req.Config.ReadOnly,
	}

	// Build S3 prefix with team ID for multi-tenant isolation (object-store namespace).
	prefix, err := naming.S3VolumePrefix(claims.TeamID, req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err = s.volMgr.MountVolume(ctx, prefix, req.VolumeId, config)
	if err != nil {
		s.logger.WithError(err).WithField("volume_id", req.VolumeId).Error("Failed to mount volume")
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"team_id":   claims.TeamID,
		"prefix":    prefix,
	}).Info("Volume mounted with team prefix")

	return &pb.MountVolumeResponse{
		VolumeId:  req.VolumeId,
		MountedAt: time.Now().Unix(),
	}, nil
}

// UnmountVolume unmounts a volume
func (s *FileSystemServer) UnmountVolume(ctx context.Context, req *pb.UnmountVolumeRequest) (*pb.Empty, error) {
	err := s.volMgr.UnmountVolume(ctx, req.VolumeId)
	if err != nil {
		s.logger.WithError(err).WithField("volume_id", req.VolumeId).Error("Failed to unmount volume")
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.Empty{}, nil
}

// WatchVolumeEvents streams volume change events to clients.
func (s *FileSystemServer) WatchVolumeEvents(req *pb.WatchRequest, stream pb.FileSystem_WatchVolumeEventsServer) error {
	if s.eventHub == nil {
		return status.Error(codes.FailedPrecondition, "watch events disabled")
	}
	if req == nil || req.VolumeId == "" {
		return status.Error(codes.InvalidArgument, "volume_id is required")
	}

	_, ch, cancel := s.eventHub.Subscribe(req)
	defer cancel()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

func (s *FileSystemServer) publishEvent(ctx context.Context, event *pb.WatchEvent) {
	if s.eventBroadcaster == nil || event == nil {
		return
	}
	s.eventBroadcaster.Publish(ctx, event)
}

// GetAttr implements FUSE getattr
func (s *FileSystemServer) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {

	// Get volume context
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Get attributes from JuiceFS
	var attr meta.Attr
	inode := meta.Ino(req.Inode)
	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.GetAttr(vfsCtx, inode, &attr)
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"error":     st,
		}).Error("GetAttr failed")
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	return convertAttr(&attr), nil
}

// Lookup implements FUSE lookup
func (s *FileSystemServer) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Lookup entry in JuiceFS
	var inode meta.Ino
	var attr meta.Attr
	parent := meta.Ino(req.Parent)
	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Lookup(vfsCtx, parent, req.Name, &inode, &attr, true)
	if st != 0 {
		if st == syscall.ENOENT {
			return nil, status.Error(codes.NotFound, "entry not found")
		}
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	return &pb.NodeResponse{
		Inode:      uint64(inode),
		Generation: 0,
		Attr:       convertAttr(&attr),
	}, nil
}

// Open implements FUSE open using JuiceFS VFS layer
func (s *FileSystemServer) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	inode := meta.Ino(req.Inode)

	// Open file using VFS (which creates proper handle with reader/writer)
	vfsCtx := vfs.NewLogContext(meta.Background())

	// VFS.Open returns (Entry, handleID, errno)
	_, handleID, errno := volCtx.VFS.Open(vfsCtx, inode, req.Flags)
	if errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"flags":     req.Flags,
			"error":     errno,
		}).Error("Open failed")
		return nil, status.Error(codes.Internal, syscall.Errno(errno).Error())
	}

	return &pb.OpenResponse{
		HandleId: handleID,
	}, nil
}

// Read implements FUSE read using JuiceFS VFS layer
func (s *FileSystemServer) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Allocate buffer for read
	buf := make([]byte, req.Size)

	// Create VFS context
	vfsCtx := vfs.NewLogContext(meta.Background())

	// Read from JuiceFS VFS (convert offset to uint64)
	n, errno := volCtx.VFS.Read(vfsCtx, meta.Ino(req.Inode), buf, uint64(req.Offset), req.HandleId)
	if errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"offset":    req.Offset,
			"size":      req.Size,
			"handle_id": req.HandleId,
			"error":     errno,
		}).Error("Read failed")

		return nil, status.Error(codes.Internal, syscall.Errno(errno).Error())
	}

	// Check if EOF
	eof := false
	if n < len(buf) {
		eof = true
		buf = buf[:n]
	}

	return &pb.ReadResponse{
		Data: buf,
		Eof:  eof,
	}, nil
}

// Write implements FUSE write using JuiceFS VFS layer
func (s *FileSystemServer) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Create VFS context
	vfsCtx := vfs.NewLogContext(meta.Background())

	// Write to JuiceFS VFS (convert offset to uint64)
	errno := volCtx.VFS.Write(vfsCtx, meta.Ino(req.Inode), req.Data, uint64(req.Offset), req.HandleId)
	if errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"offset":    req.Offset,
			"size":      len(req.Data),
			"handle_id": req.HandleId,
			"error":     errno,
		}).Error("Write failed")

		return nil, status.Error(codes.Internal, syscall.Errno(errno).Error())
	}

	path := resolveInodePath(volCtx, req.Inode)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     req.Inode,
	})

	return &pb.WriteResponse{
		BytesWritten: int64(len(req.Data)),
	}, nil
}

// Create implements FUSE create using JuiceFS VFS layer
func (s *FileSystemServer) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Create file using VFS (which creates proper handle with reader/writer)
	parent := meta.Ino(req.Parent)
	vfsCtx := vfs.NewLogContext(meta.Background())

	// VFS.Create returns (Entry, handleID, errno)
	entry, handleID, errno := volCtx.VFS.Create(vfsCtx, parent, req.Name, uint16(req.Mode), 0, req.Flags)
	if errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"parent":    req.Parent,
			"name":      req.Name,
			"mode":      req.Mode,
			"error":     errno,
		}).Error("Create failed")
		return nil, status.Error(mapErrnoToCode(syscall.Errno(errno)), syscall.Errno(errno).Error())
	}

	path := resolveChildPath(volCtx, req.Parent, req.Name)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     uint64(entry.Inode),
	})

	return &pb.NodeResponse{
		Inode:      uint64(entry.Inode),
		Generation: 0,
		Attr:       convertAttr(entry.Attr),
		HandleId:   handleID,
	}, nil
}

// Mkdir implements FUSE mkdir
func (s *FileSystemServer) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Create directory in JuiceFS
	parent := meta.Ino(req.Parent)
	var inode meta.Ino
	var attr meta.Attr

	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Mkdir(vfsCtx, parent, req.Name, uint16(req.Mode), 0, 0, &inode, &attr)
	if st != 0 {
		return nil, status.Error(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	path := resolveChildPath(volCtx, req.Parent, req.Name)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     uint64(inode),
	})

	return &pb.NodeResponse{
		Inode:      uint64(inode),
		Generation: 0,
		Attr:       convertAttr(&attr),
	}, nil
}

func mapErrnoToCode(errno syscall.Errno) codes.Code {
	switch errno {
	case syscall.EEXIST:
		return codes.AlreadyExists
	case syscall.ENOENT:
		return codes.NotFound
	case syscall.EACCES, syscall.EPERM:
		return codes.PermissionDenied
	case syscall.ENOSPC:
		return codes.ResourceExhausted
	case syscall.EINVAL, syscall.ENOTDIR:
		return codes.InvalidArgument
	default:
		return codes.Internal
	}
}

// Unlink implements FUSE unlink
func (s *FileSystemServer) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Unlink file in JuiceFS
	parent := meta.Ino(req.Parent)
	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Unlink(vfsCtx, parent, req.Name)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	path := resolveChildPath(volCtx, req.Parent, req.Name)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
	})

	return &pb.Empty{}, nil
}

// ReadDir implements FUSE readdir
func (s *FileSystemServer) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Read directory from JuiceFS
	inode := meta.Ino(req.Inode)
	var entries []*meta.Entry
	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Readdir(vfsCtx, inode, 1, &entries)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	// Convert entries
	var result []*pb.DirEntry
	for _, e := range entries {
		result = append(result, &pb.DirEntry{
			Inode:  uint64(e.Inode),
			Offset: 0,
			Name:   string(e.Name),
			Type:   uint32(e.Attr.Typ),
			Attr:   convertAttr(e.Attr),
		})
	}

	return &pb.ReadDirResponse{
		Entries: result,
		Eof:     false,
	}, nil
}

// Rename implements FUSE rename
func (s *FileSystemServer) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Rename in JuiceFS
	oldParent := meta.Ino(req.OldParent)
	newParent := meta.Ino(req.NewParent)
	var inode meta.Ino
	var attr meta.Attr

	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Rename(vfsCtx, oldParent, req.OldName, newParent, req.NewName, req.Flags, &inode, &attr)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	oldPath := resolveChildPath(volCtx, req.OldParent, req.OldName)
	newPath := resolveChildPath(volCtx, req.NewParent, req.NewName)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_RENAME
	if oldPath == "" && newPath == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      newPath,
		OldPath:   oldPath,
		Inode:     uint64(inode),
	})

	return &pb.Empty{}, nil
}

// SetAttr implements FUSE setattr
func (s *FileSystemServer) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {

	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	inode := meta.Ino(req.Inode)
	var attr meta.Attr

	// Set attributes in JuiceFS
	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.SetAttr(vfsCtx, inode, uint16(req.Valid), 0, &attr)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	path := resolveInodePath(volCtx, req.Inode)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	if req.Valid&uint32(meta.SetAttrMode) != 0 {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD
	} else if path != "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
	}
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     req.Inode,
	})

	return &pb.SetAttrResponse{
		Attr: convertAttr(&attr),
	}, nil
}

// Flush implements FUSE flush
func (s *FileSystemServer) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	// Flush is mostly a no-op in JuiceFS (writes are buffered)
	return &pb.Empty{}, nil
}

// Fsync implements FUSE fsync
func (s *FileSystemServer) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	// Fsync - data is synced by chunk store's writeback cache
	return &pb.Empty{}, nil
}

// Release implements FUSE release (close) using JuiceFS VFS layer
func (s *FileSystemServer) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Release the file handle in VFS
	vfsCtx := vfs.NewLogContext(meta.Background())
	volCtx.VFS.Release(vfsCtx, meta.Ino(req.Inode), req.HandleId)

	s.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"inode":     req.Inode,
		"handle_id": req.HandleId,
	}).Debug("Released file handle")

	return &pb.Empty{}, nil
}

// Rmdir implements FUSE rmdir (remove directory)
func (s *FileSystemServer) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Remove directory in JuiceFS
	parent := meta.Ino(req.Parent)
	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Rmdir(vfsCtx, parent, req.Name)
	if st != 0 {
		if st == syscall.ENOTEMPTY {
			return nil, status.Error(codes.FailedPrecondition, "directory not empty")
		}
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	path := resolveChildPath(volCtx, req.Parent, req.Name)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
	})

	return &pb.Empty{}, nil
}

// StatFs implements FUSE statfs (filesystem statistics)
func (s *FileSystemServer) StatFs(ctx context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Get filesystem statistics from JuiceFS
	vfsCtx := vfs.NewLogContext(meta.Background())
	var totalSpace, availSpace, iused, iavail uint64
	st := volCtx.Meta.StatFS(vfsCtx, meta.RootInode, &totalSpace, &availSpace, &iused, &iavail)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	// Use configured block size if available, otherwise default to 4096
	blockSize := uint64(volCtx.VFS.Conf.Format.BlockSize) * 1024
	if blockSize == 0 {
		blockSize = 4096
	}
	blocks := totalSpace / blockSize
	bavail := availSpace / blockSize

	return &pb.StatFsResponse{
		Blocks:  blocks,
		Bfree:   bavail,
		Bavail:  bavail,
		Files:   iused + iavail,
		Ffree:   iavail,
		Bsize:   uint32(blockSize),
		Namelen: 255,
		Frsize:  uint32(blockSize),
	}, nil
}

// Symlink implements FUSE symlink (create symbolic link)
func (s *FileSystemServer) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Create symbolic link in JuiceFS
	parent := meta.Ino(req.Parent)
	var inode meta.Ino
	var attr meta.Attr

	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Symlink(vfsCtx, parent, req.Name, req.Target, &inode, &attr)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	path := resolveChildPath(volCtx, req.Parent, req.Name)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     uint64(inode),
	})

	return &pb.NodeResponse{
		Inode:      uint64(inode),
		Generation: 0,
		Attr:       convertAttr(&attr),
	}, nil
}

// Readlink implements FUSE readlink (read symbolic link target)
func (s *FileSystemServer) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Read symbolic link from JuiceFS
	inode := meta.Ino(req.Inode)
	var target []byte

	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.ReadLink(vfsCtx, inode, &target)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	return &pb.ReadlinkResponse{
		Target: string(target),
	}, nil
}

// Link implements FUSE link (create hard link)
func (s *FileSystemServer) Link(ctx context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Create hard link in JuiceFS
	inode := meta.Ino(req.Inode)
	newParent := meta.Ino(req.NewParent)
	var attr meta.Attr

	vfsCtx := vfs.NewLogContext(meta.Background())
	st := volCtx.Meta.Link(vfsCtx, inode, newParent, req.NewName, &attr)
	if st != 0 {
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	path := resolveChildPath(volCtx, req.NewParent, req.NewName)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     req.Inode,
	})

	return &pb.NodeResponse{
		Inode:      uint64(inode),
		Generation: 0,
		Attr:       convertAttr(&attr),
	}, nil
}

// Access implements FUSE access (check file access permissions)
func (s *FileSystemServer) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Create JuiceFS context with caller's uid/gid for permission checking
	inode := meta.Ino(req.Inode)

	// Default to root user if no uid/gid provided (backward compatibility)
	uid := req.Uid
	gids := req.Gids
	if len(gids) == 0 {
		gids = []uint32{0}
	}

	// Create context with user credentials
	vfsCtx := vfs.NewLogContext(meta.NewContext(0, uid, gids))

	// Use JuiceFS's Access method which implements full POSIX permission checking
	// This checks:
	// - Root user (uid=0) always has access
	// - Owner/group/other permission bits
	// - ACL rules if present
	// - Sticky bit for directories
	st := volCtx.Meta.Access(vfsCtx, inode, uint8(req.Mask), nil)
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"mask":      req.Mask,
			"uid":       uid,
			"gids":      gids,
			"error":     st,
		}).Debug("Access denied")
		return nil, status.Error(codes.PermissionDenied, syscall.Errno(st).Error())
	}

	return &pb.Empty{}, nil
}

// Fallocate preallocates or deallocates space for a file
func (s *FileSystemServer) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Call JuiceFS VFS Fallocate
	vfsCtx := vfs.NewLogContext(meta.Background())
	inode := meta.Ino(req.Inode)
	st := volCtx.VFS.Fallocate(vfsCtx, inode, uint8(req.Mode), req.Offset, req.Length, req.HandleId)
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"mode":      req.Mode,
			"offset":    req.Offset,
			"length":    req.Length,
			"error":     st,
		}).Error("Fallocate failed")
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	s.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"inode":     req.Inode,
		"mode":      req.Mode,
		"offset":    req.Offset,
		"length":    req.Length,
	}).Debug("Fallocate succeeded")

	path := resolveInodePath(volCtx, req.Inode)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     req.Inode,
	})

	return &pb.Empty{}, nil
}

// GetXattr gets an extended attribute
func (s *FileSystemServer) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Call JuiceFS VFS GetXattr
	vfsCtx := vfs.NewLogContext(meta.Background())
	inode := meta.Ino(req.Inode)
	value, st := volCtx.VFS.GetXattr(vfsCtx, inode, req.Name, req.Size)
	if st != 0 {
		// ENODATA/ENOATTR is not an error, just means attribute doesn't exist
		if st == syscall.ENODATA || st == meta.ENOATTR {
			return nil, status.Error(codes.NotFound, "attribute not found")
		}
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"name":      req.Name,
			"error":     st,
		}).Error("GetXattr failed")
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	return &pb.GetXattrResponse{
		Value: value,
	}, nil
}

// SetXattr sets an extended attribute
func (s *FileSystemServer) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Call JuiceFS VFS SetXattr
	vfsCtx := vfs.NewLogContext(meta.Background())
	inode := meta.Ino(req.Inode)
	st := volCtx.VFS.SetXattr(vfsCtx, inode, req.Name, req.Value, req.Flags)
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"name":      req.Name,
			"flags":     req.Flags,
			"error":     st,
		}).Error("SetXattr failed")
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	path := resolveInodePath(volCtx, req.Inode)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     req.Inode,
	})

	return &pb.Empty{}, nil
}

// ListXattr lists all extended attributes
func (s *FileSystemServer) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Call JuiceFS VFS ListXattr
	vfsCtx := vfs.NewLogContext(meta.Background())
	inode := meta.Ino(req.Inode)
	data, st := volCtx.VFS.ListXattr(vfsCtx, inode, int(req.Size))
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"error":     st,
		}).Error("ListXattr failed")
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	return &pb.ListXattrResponse{
		Data: data,
	}, nil
}

// RemoveXattr removes an extended attribute
func (s *FileSystemServer) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	volCtx, err := s.volMgr.GetVolume(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Call JuiceFS VFS RemoveXattr
	vfsCtx := vfs.NewLogContext(meta.Background())
	inode := meta.Ino(req.Inode)
	st := volCtx.VFS.RemoveXattr(vfsCtx, inode, req.Name)
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"name":      req.Name,
			"error":     st,
		}).Error("RemoveXattr failed")
		return nil, status.Error(codes.Internal, syscall.Errno(st).Error())
	}

	path := resolveInodePath(volCtx, req.Inode)
	eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD
	if path == "" {
		eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
	}
	s.publishEvent(ctx, &pb.WatchEvent{
		VolumeId:  req.VolumeId,
		EventType: eventType,
		Path:      path,
		Inode:     req.Inode,
	})

	return &pb.Empty{}, nil
}

func resolveInodePath(volCtx *volume.VolumeContext, inode uint64) string {
	if volCtx == nil {
		return ""
	}
	paths := volCtx.Meta.GetPaths(meta.Background(), meta.Ino(inode))
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func resolveChildPath(volCtx *volume.VolumeContext, parent uint64, name string) string {
	if volCtx == nil {
		return ""
	}
	parentPaths := volCtx.Meta.GetPaths(meta.Background(), meta.Ino(parent))
	if len(parentPaths) == 0 {
		return ""
	}
	parentPath := parentPaths[0]
	if parentPath == "/" {
		return "/" + name
	}
	return parentPath + "/" + name
}

// Helper: convert meta.Attr to protobuf GetAttrResponse
func convertAttr(attr *meta.Attr) *pb.GetAttrResponse {
	return &pb.GetAttrResponse{
		Ino:       uint64(meta.RootInode), // Would need proper inode tracking
		Mode:      uint32(attr.Mode),
		Nlink:     attr.Nlink,
		Uid:       attr.Uid,
		Gid:       attr.Gid,
		Rdev:      uint64(attr.Rdev),
		Size:      attr.Length,
		Blocks:    0,
		AtimeSec:  attr.Atime,
		AtimeNsec: int64(attr.Atimensec),
		MtimeSec:  attr.Mtime,
		MtimeNsec: int64(attr.Mtimensec),
		CtimeSec:  attr.Ctime,
		CtimeNsec: int64(attr.Ctimensec),
	}
}
