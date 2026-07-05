package portal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

const (
	s3RootInode = uint64(fsmeta.RootInode)
	s3DirMode   = uint32(syscall.S_IFDIR | 0o755)
	s3FileMode  = uint32(syscall.S_IFREG | 0o644)

	fuseFattrSize = uint32(1 << 3)
)

type s3NodeKind uint8

const (
	s3NodeFile s3NodeKind = iota + 1
	s3NodeDir
)

type s3Node struct {
	inode    uint64
	path     string
	kind     s3NodeKind
	size     int64
	modified time.Time
}

type s3Handle struct {
	inode     uint64
	path      string
	writable  bool
	buffer    bytes.Buffer
	committed bool
	closed    bool
}

type s3Session struct {
	volumeID string
	store    objectstore.Store
	access   volume.AccessMode
	logger   *logrus.Logger

	mu           sync.Mutex
	nextInode    uint64
	nextHandleID uint64
	nodesByInode map[uint64]*s3Node
	inodeByPath  map[string]uint64
	handles      map[uint64]*s3Handle
	implicit     map[uint64]*s3Handle
}

func newS3Session(volumeID string, store objectstore.Store, access volume.AccessMode, logger *logrus.Logger) *s3Session {
	if logger == nil {
		logger = logrus.New()
	}
	now := time.Now().UTC()
	root := &s3Node{inode: s3RootInode, kind: s3NodeDir, modified: now}
	return &s3Session{
		volumeID:     volumeID,
		store:        store,
		access:       volume.NormalizeAccessMode(string(access)),
		logger:       logger,
		nextInode:    s3RootInode + 1,
		nodesByInode: map[uint64]*s3Node{s3RootInode: root},
		inodeByPath:  map[string]uint64{"": s3RootInode},
		handles:      make(map[uint64]*s3Handle),
		implicit:     make(map[uint64]*s3Handle),
	}
}

func (s *s3Session) OpenFlags() uint32 {
	return fuse.FOPEN_DIRECT_IO
}

func (s *s3Session) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	handles := make([]*s3Handle, 0, len(s.handles))
	for id, handle := range s.handles {
		if handle != nil && handle.writable && !handle.closed {
			handles = append(handles, handle)
		}
		delete(s.handles, id)
	}
	for inode, handle := range s.implicit {
		if handle != nil && handle.writable && !handle.closed {
			handles = append(handles, handle)
		}
		delete(s.implicit, inode)
	}
	s.mu.Unlock()
	for _, handle := range handles {
		if err := s.commitHandle(context.Background(), handle); err != nil && s.logger != nil {
			s.logger.WithError(err).WithField("key", handle.path).Warn("Failed to commit s3 handle during close")
		}
	}
}

func (s *s3Session) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	parent, err := s.nodeForInode(req.Parent)
	if err != nil {
		return nil, err
	}
	if parent.kind != s3NodeDir {
		return nil, syscall.ENOTDIR
	}
	name, err := cleanS3Name(req.Name)
	if err != nil {
		return nil, err
	}
	childPath := joinS3Path(parent.path, name)
	node, err := s.resolvePath(ctx, childPath)
	if err != nil {
		return nil, err
	}
	return s.nodeResponse(node, 0), nil
}

func (s *s3Session) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	node, err := s.nodeForInode(req.Inode)
	if err != nil {
		return nil, err
	}
	if node.inode != s3RootInode {
		if refreshed, refreshErr := s.resolvePath(ctx, node.path); refreshErr == nil {
			node = refreshed
		} else {
			return nil, refreshErr
		}
	}
	return s.attr(node), nil
}

func (s *s3Session) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	node, err := s.nodeForInode(req.Inode)
	if err != nil {
		return nil, err
	}
	if req.Valid == 0 {
		return &pb.SetAttrResponse{Attr: s.attr(node)}, nil
	}
	if req.Valid&^uint32(fuse.FATTR_SIZE) != 0 {
		return nil, syscall.EOPNOTSUPP
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	if node.kind != s3NodeFile {
		return nil, syscall.EISDIR
	}
	if req.Attr == nil || req.Attr.Size != 0 {
		return nil, syscall.EOPNOTSUPP
	}
	if _, ok := s.nodeForPath(node.path); !ok {
		if _, err := s.resolvePath(ctx, node.path); err != nil {
			return nil, err
		}
	}
	handle := s.handle(req.HandleId)
	if handle != nil && handle.writable && !handle.closed {
		handle.buffer.Reset()
		handle.committed = false
		s.updateNodeSize(handle.inode, 0)
		node = s.rememberPath(handle.path, s3NodeFile, 0, time.Now().UTC())
		return &pb.SetAttrResponse{Attr: s.attr(node)}, nil
	}
	if writer := s.findWritableHandle(node.inode); writer != nil {
		writer.buffer.Reset()
		writer.committed = false
		s.updateNodeSize(writer.inode, 0)
		node = s.rememberPath(writer.path, s3NodeFile, 0, time.Now().UTC())
		return &pb.SetAttrResponse{Attr: s.attr(node)}, nil
	}
	if err := s.store.Put(node.path, bytes.NewReader(nil)); err != nil {
		return nil, err
	}
	node = s.rememberPath(node.path, s3NodeFile, 0, time.Now().UTC())
	return &pb.SetAttrResponse{Attr: s.attr(node)}, nil
}

func (s *s3Session) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	parent, err := s.nodeForInode(req.Parent)
	if err != nil {
		return nil, err
	}
	if parent.kind != s3NodeDir {
		return nil, syscall.ENOTDIR
	}
	name, err := cleanS3Name(req.Name)
	if err != nil {
		return nil, err
	}
	dirPath := joinS3Path(parent.path, name)
	if _, err := s.resolvePath(ctx, dirPath); err == nil {
		return nil, fserror.New(fserror.AlreadyExists, "entry already exists")
	} else if fserror.CodeOf(err) != fserror.NotFound {
		return nil, err
	}
	if err := s.store.Put(dirPath+"/", bytes.NewReader(nil)); err != nil {
		return nil, err
	}
	node := s.rememberPath(dirPath, s3NodeDir, 0, time.Now().UTC())
	return s.nodeResponse(node, 0), nil
}

func (s *s3Session) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	parent, err := s.nodeForInode(req.Parent)
	if err != nil {
		return nil, err
	}
	if parent.kind != s3NodeDir {
		return nil, syscall.ENOTDIR
	}
	name, err := cleanS3Name(req.Name)
	if err != nil {
		return nil, err
	}
	filePath := joinS3Path(parent.path, name)
	if _, err := s.resolvePath(ctx, filePath); err == nil {
		return nil, fserror.New(fserror.AlreadyExists, "entry already exists")
	} else if fserror.CodeOf(err) != fserror.NotFound {
		return nil, err
	}
	node := s.rememberPath(filePath, s3NodeFile, 0, time.Now().UTC())
	handleID := s.newHandle(&s3Handle{inode: node.inode, path: filePath, writable: true})
	return s.nodeResponse(node, handleID), nil
}

func (s *s3Session) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	parent, err := s.nodeForInode(req.Parent)
	if err != nil {
		return nil, err
	}
	name, err := cleanS3Name(req.Name)
	if err != nil {
		return nil, err
	}
	filePath := joinS3Path(parent.path, name)
	node, err := s.resolvePath(ctx, filePath)
	if err != nil {
		return nil, err
	}
	if node.kind != s3NodeFile {
		return nil, syscall.EISDIR
	}
	if err := s.store.Delete(filePath); err != nil && !objectstore.IsNotFound(err) {
		return nil, err
	}
	s.forgetPath(filePath)
	return &pb.Empty{}, nil
}

func (s *s3Session) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	parent, err := s.nodeForInode(req.Parent)
	if err != nil {
		return nil, err
	}
	name, err := cleanS3Name(req.Name)
	if err != nil {
		return nil, err
	}
	dirPath := joinS3Path(parent.path, name)
	if _, err := s.resolvePath(ctx, dirPath); err != nil {
		return nil, err
	}
	prefix := dirPath + "/"
	entries, _, _, err := s.store.List(prefix, "", "", "", 2)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Key != prefix {
			return nil, syscall.ENOTEMPTY
		}
	}
	if err := s.store.Delete(prefix); err != nil && !objectstore.IsNotFound(err) {
		return nil, err
	}
	s.forgetPath(dirPath)
	return &pb.Empty{}, nil
}

func (s *s3Session) Rename(context.Context, *pb.RenameRequest) (*pb.Empty, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) Link(context.Context, *pb.LinkRequest) (*pb.NodeResponse, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) Symlink(context.Context, *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) Readlink(context.Context, *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	return nil, syscall.EINVAL
}

func (s *s3Session) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	if _, err := s.GetAttr(ctx, &pb.GetAttrRequest{Inode: req.Inode}); err != nil {
		return nil, err
	}
	if req.Mask&2 != 0 {
		return &pb.Empty{}, s.ensureWritable()
	}
	return &pb.Empty{}, nil
}

func (s *s3Session) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	node, err := s.nodeForInode(req.Inode)
	if err != nil {
		return nil, err
	}
	writable := req.Flags&uint32(syscall.O_ACCMODE) != uint32(syscall.O_RDONLY)
	if writable {
		if err := s.ensureWritable(); err != nil {
			return nil, err
		}
		if s.hasOpenReader(node.inode) || s.hasOpenWriter(node.inode) {
			return nil, syscall.EPERM
		}
	} else if s.hasOpenWriter(node.inode) {
		return nil, syscall.EPERM
	}
	node, err = s.resolvePath(ctx, node.path)
	if err != nil {
		return nil, err
	}
	if node.kind == s3NodeDir {
		return nil, syscall.EISDIR
	}
	if writable {
		if req.Flags&uint32(syscall.O_APPEND) != 0 {
			return nil, syscall.EOPNOTSUPP
		}
		handleID := s.newHandle(&s3Handle{inode: node.inode, path: node.path, writable: true})
		s.updateNodeSize(node.inode, 0)
		return &pb.OpenResponse{HandleId: handleID}, nil
	}
	handleID := s.newHandle(&s3Handle{inode: node.inode, path: node.path})
	return &pb.OpenResponse{HandleId: handleID}, nil
}

func (s *s3Session) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	size := req.Size
	if size < 0 {
		return nil, fserror.New(fserror.InvalidArgument, "negative read size")
	}
	buf := make([]byte, size)
	n, eof, err := s.ReadInto(ctx, req, buf)
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Data: buf[:n], Eof: eof}, nil
}

func (s *s3Session) ReadInto(ctx context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	if req.Offset < 0 {
		return 0, false, fserror.New(fserror.InvalidArgument, "negative read offset")
	}
	node, err := s.nodeForInode(req.Inode)
	if err != nil {
		return 0, false, err
	}
	if s.hasOpenWriter(node.inode) {
		return 0, false, syscall.EPERM
	}
	node, err = s.resolvePath(ctx, node.path)
	if err != nil {
		return 0, false, err
	}
	if node.kind != s3NodeFile {
		return 0, false, syscall.EISDIR
	}
	if req.Offset >= node.size || len(dest) == 0 {
		return 0, true, nil
	}
	limit := int64(len(dest))
	if remaining := node.size - req.Offset; remaining < limit {
		limit = remaining
		dest = dest[:remaining]
	}
	reader, err := s.store.Get(node.path, req.Offset, limit)
	if err != nil {
		if objectstore.IsNotFound(err) {
			return 0, false, fserror.New(fserror.NotFound, "entry not found")
		}
		return 0, false, err
	}
	defer reader.Close()
	n, err := io.ReadFull(reader, dest)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, false, err
	}
	return n, req.Offset+int64(n) >= node.size, nil
}

func (s *s3Session) Write(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	handle := s.handle(req.HandleId)
	implicit := false
	if handle == nil && req.HandleId == 0 {
		var err error
		handle, err = s.handlelessWritableHandle(req.Inode)
		if err != nil {
			return nil, err
		}
		implicit = true
	}
	if handle == nil || !handle.writable || handle.closed {
		return nil, syscall.EBADF
	}
	if req.Offset != int64(handle.buffer.Len()) {
		return nil, fserror.New(fserror.InvalidArgument, "s3 backend only supports sequential writes for new files")
	}
	n, err := handle.buffer.Write(req.Data)
	if err != nil {
		return nil, err
	}
	handle.committed = false
	s.updateNodeSize(handle.inode, int64(handle.buffer.Len()))
	if implicit {
		if err := s.commitHandle(context.Background(), handle); err != nil {
			return nil, err
		}
	}
	return &pb.WriteResponse{BytesWritten: int64(n)}, nil
}

func (s *s3Session) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	handle := s.takeHandle(req.HandleId)
	if handle == nil && req.HandleId == 0 {
		handle = s.takeHandlelessWritableHandle(req.Inode)
	}
	if handle == nil {
		return &pb.Empty{}, nil
	}
	handle.closed = true
	if handle.writable {
		return &pb.Empty{}, s.commitHandle(ctx, handle)
	}
	return &pb.Empty{}, nil
}

func (s *s3Session) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	handle := s.handle(req.HandleId)
	if handle == nil || !handle.writable {
		return &pb.Empty{}, nil
	}
	return &pb.Empty{}, s.commitHandle(ctx, handle)
}

func (s *s3Session) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	return s.Flush(ctx, &pb.FlushRequest{HandleId: req.HandleId})
}

func (s *s3Session) Fallocate(context.Context, *pb.FallocateRequest) (*pb.Empty, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) CopyFileRange(context.Context, *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	node, err := s.nodeForInode(req.Inode)
	if err != nil {
		return nil, err
	}
	node, err = s.resolvePath(ctx, node.path)
	if err != nil {
		return nil, err
	}
	if node.kind != s3NodeDir {
		return nil, syscall.ENOTDIR
	}
	handleID := s.newHandle(&s3Handle{inode: node.inode, path: node.path})
	return &pb.OpenDirResponse{HandleId: handleID}, nil
}

func (s *s3Session) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	node, err := s.nodeForInode(req.Inode)
	if err != nil {
		return nil, err
	}
	if node.kind != s3NodeDir {
		return nil, syscall.ENOTDIR
	}
	prefix := ""
	if node.path != "" {
		prefix = node.path + "/"
	}
	infos, err := s.listAll(ctx, prefix, "/")
	if err != nil {
		return nil, err
	}
	entriesByName := make(map[string]*pb.DirEntry)
	for _, info := range infos {
		if info.Key == prefix {
			continue
		}
		name, kind, ok := directS3Entry(prefix, info)
		if !ok {
			continue
		}
		entryPath := joinS3Path(node.path, name)
		entryNode := s.rememberPath(entryPath, kind, info.Size, info.Modified)
		entry := &pb.DirEntry{
			Inode: entryNode.inode,
			Name:  name,
			Type:  s3TypeNumber(kind),
		}
		if req.Plus {
			entry.Attr = s.attr(entryNode)
		}
		if existing := entriesByName[name]; existing != nil && existing.Type&uint32(syscall.S_IFMT) == uint32(syscall.S_IFDIR) {
			continue
		}
		entriesByName[name] = entry
	}
	names := make([]string, 0, len(entriesByName))
	for name := range entriesByName {
		names = append(names, name)
	}
	sort.Strings(names)
	start := int(req.Offset)
	if start < 0 {
		start = 0
	}
	if start > len(names) {
		start = len(names)
	}
	out := make([]*pb.DirEntry, 0, len(names)-start)
	for i, name := range names[start:] {
		entry := entriesByName[name]
		entry.Offset = uint64(start + i + 1)
		out = append(out, entry)
	}
	return &pb.ReadDirResponse{Entries: out, Eof: true}, nil
}

func (s *s3Session) ReleaseDir(_ context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	s.takeHandle(req.HandleId)
	return &pb.Empty{}, nil
}

func (s *s3Session) StatFs(context.Context, *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	return &pb.StatFsResponse{
		Blocks:  1 << 30,
		Bfree:   1 << 30,
		Bavail:  1 << 30,
		Files:   1 << 30,
		Ffree:   1 << 30,
		Bsize:   4096,
		Frsize:  4096,
		Namelen: 255,
	}, nil
}

func (s *s3Session) GetXattr(context.Context, *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	return nil, syscall.ENODATA
}

func (s *s3Session) SetXattr(context.Context, *pb.SetXattrRequest) (*pb.Empty, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) ListXattr(context.Context, *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) RemoveXattr(context.Context, *pb.RemoveXattrRequest) (*pb.Empty, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) Mknod(context.Context, *pb.MknodRequest) (*pb.NodeResponse, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) GetLk(context.Context, *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) SetLk(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) SetLkw(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) Flock(context.Context, *pb.FlockRequest) (*pb.Empty, error) {
	return nil, syscall.EOPNOTSUPP
}

func (s *s3Session) resolvePath(ctx context.Context, key string) (*s3Node, error) {
	key = cleanS3Path(key)
	if key == "" {
		return s.rememberPath("", s3NodeDir, 0, time.Now().UTC()), nil
	}
	if info, ok, err := s.dirInfo(ctx, key); err != nil {
		return nil, err
	} else if ok {
		return s.rememberPath(key, s3NodeDir, 0, info.Modified), nil
	}
	info, err := s.store.Head(key)
	if err != nil {
		if objectstore.IsNotFound(err) {
			return nil, fserror.New(fserror.NotFound, "entry not found")
		}
		return nil, err
	}
	return s.rememberPath(key, s3NodeFile, info.Size, info.Modified), nil
}

func (s *s3Session) dirInfo(ctx context.Context, key string) (objectstore.Info, bool, error) {
	prefix := strings.TrimRight(key, "/") + "/"
	if info, err := s.store.Head(prefix); err == nil {
		return info, true, nil
	}
	infos, err := s.listAllLimited(ctx, prefix, "/", 1)
	if err != nil {
		return objectstore.Info{}, false, err
	}
	if len(infos) == 0 {
		return objectstore.Info{}, false, nil
	}
	return infos[0], true, nil
}

func (s *s3Session) listAll(ctx context.Context, prefix, delimiter string) ([]objectstore.Info, error) {
	return s.listAllLimited(ctx, prefix, delimiter, 0)
}

func (s *s3Session) listAllLimited(ctx context.Context, prefix, delimiter string, max int) ([]objectstore.Info, error) {
	out := make([]objectstore.Info, 0)
	token := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		limit := int64(1000)
		if max > 0 && max-len(out) < 1000 {
			limit = int64(max - len(out))
		}
		if limit <= 0 {
			return out, nil
		}
		items, more, next, err := s.store.List(prefix, "", token, delimiter, limit)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		if max > 0 && len(out) >= max {
			return out[:max], nil
		}
		if !more || next == "" {
			return out, nil
		}
		token = next
	}
}

func (s *s3Session) nodeForInode(inode uint64) (*s3Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodesByInode[inode]
	if node == nil {
		return nil, fserror.New(fserror.NotFound, "inode not found")
	}
	copyNode := *node
	return &copyNode, nil
}

func (s *s3Session) nodeForPath(key string) (*s3Node, bool) {
	key = cleanS3Path(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	inode := s.inodeByPath[key]
	if inode == 0 {
		return nil, false
	}
	node := s.nodesByInode[inode]
	if node == nil {
		return nil, false
	}
	copyNode := *node
	return &copyNode, true
}

func (s *s3Session) rememberPath(key string, kind s3NodeKind, size int64, modified time.Time) *s3Node {
	key = cleanS3Path(key)
	if modified.IsZero() {
		modified = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	inode := s.inodeByPath[key]
	if inode == 0 {
		inode = s.nextInode
		s.nextInode++
		s.inodeByPath[key] = inode
	}
	node := &s3Node{inode: inode, path: key, kind: kind, size: size, modified: modified}
	s.nodesByInode[inode] = node
	copyNode := *node
	return &copyNode
}

func (s *s3Session) forgetPath(key string) {
	key = cleanS3Path(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	inode := s.inodeByPath[key]
	if inode != 0 {
		delete(s.nodesByInode, inode)
	}
	delete(s.inodeByPath, key)
}

func (s *s3Session) newHandle(handle *s3Handle) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextHandleID++
	s.handles[s.nextHandleID] = handle
	return s.nextHandleID
}

func (s *s3Session) handle(handleID uint64) *s3Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle := s.handles[handleID]
	return handle
}

func (s *s3Session) findWritableHandle(inode uint64) *s3Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, handle := range s.handles {
		if handle != nil && handle.inode == inode && handle.writable && !handle.closed {
			return handle
		}
	}
	if handle := s.implicit[inode]; handle != nil && handle.writable && !handle.closed {
		return handle
	}
	return nil
}

func (s *s3Session) hasOpenWriter(inode uint64) bool {
	return s.findWritableHandle(inode) != nil
}

func (s *s3Session) hasOpenReader(inode uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, handle := range s.handles {
		if handle != nil && handle.inode == inode && !handle.writable && !handle.closed {
			return true
		}
	}
	return false
}

func (s *s3Session) takeHandle(handleID uint64) *s3Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle := s.handles[handleID]
	delete(s.handles, handleID)
	return handle
}

func (s *s3Session) handlelessWritableHandle(inode uint64) (*s3Handle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if handle := s.implicit[inode]; handle != nil {
		return handle, nil
	}
	var existing *s3Handle
	for _, handle := range s.handles {
		if handle == nil || handle.inode != inode || !handle.writable || handle.closed {
			continue
		}
		if existing != nil {
			return nil, fserror.New(fserror.InvalidArgument, "ambiguous handle-less write")
		}
		existing = handle
	}
	if existing != nil {
		return existing, nil
	}
	node := s.nodesByInode[inode]
	if node == nil {
		return nil, fserror.New(fserror.NotFound, "inode not found")
	}
	if node.kind != s3NodeFile {
		return nil, syscall.EISDIR
	}
	handle := &s3Handle{inode: inode, path: node.path, writable: true}
	s.implicit[inode] = handle
	return handle, nil
}

func (s *s3Session) takeHandlelessWritableHandle(inode uint64) *s3Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	if handle := s.implicit[inode]; handle != nil {
		delete(s.implicit, inode)
		return handle
	}
	for id, handle := range s.handles {
		if handle != nil && handle.inode == inode && handle.writable && !handle.closed {
			delete(s.handles, id)
			return handle
		}
	}
	return nil
}

func (s *s3Session) updateNodeSize(inode uint64, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node := s.nodesByInode[inode]; node != nil {
		node.size = size
		node.modified = time.Now().UTC()
	}
}

func (s *s3Session) commitHandle(_ context.Context, handle *s3Handle) error {
	if handle == nil || !handle.writable || handle.committed {
		return nil
	}
	if err := s.store.Put(handle.path, bytes.NewReader(handle.buffer.Bytes())); err != nil {
		return err
	}
	handle.committed = true
	s.rememberPath(handle.path, s3NodeFile, int64(handle.buffer.Len()), time.Now().UTC())
	return nil
}

func (s *s3Session) ensureWritable() error {
	if volume.NormalizeAccessMode(string(s.access)) == volume.AccessModeROX {
		return syscall.EROFS
	}
	return nil
}

func (s *s3Session) attr(node *s3Node) *pb.GetAttrResponse {
	if node == nil {
		return &pb.GetAttrResponse{}
	}
	mode := s3FileMode
	nlink := uint32(1)
	size := uint64(0)
	if node.kind == s3NodeDir {
		mode = s3DirMode
		nlink = 2
	} else if node.size > 0 {
		size = uint64(node.size)
	}
	modified := node.modified
	if modified.IsZero() {
		modified = time.Now().UTC()
	}
	return &pb.GetAttrResponse{
		Ino:       node.inode,
		Mode:      mode,
		Nlink:     nlink,
		Size:      size,
		Blocks:    (size + 511) / 512,
		AtimeSec:  modified.Unix(),
		AtimeNsec: int64(modified.Nanosecond()),
		MtimeSec:  modified.Unix(),
		MtimeNsec: int64(modified.Nanosecond()),
		CtimeSec:  modified.Unix(),
		CtimeNsec: int64(modified.Nanosecond()),
	}
}

func (s *s3Session) nodeResponse(node *s3Node, handleID uint64) *pb.NodeResponse {
	return &pb.NodeResponse{
		Inode:      node.inode,
		Generation: 1,
		Attr:       s.attr(node),
		HandleId:   handleID,
	}
}

func directS3Entry(prefix string, info objectstore.Info) (string, s3NodeKind, bool) {
	key := strings.TrimPrefix(info.Key, prefix)
	if key == "" {
		return "", 0, false
	}
	kind := s3NodeFile
	if info.IsPrefix || strings.HasSuffix(key, "/") {
		kind = s3NodeDir
		key = strings.TrimRight(key, "/")
	}
	if strings.Contains(key, "/") {
		parts := strings.SplitN(key, "/", 2)
		key = parts[0]
		kind = s3NodeDir
	}
	if key == "" || key == "." || key == ".." || strings.ContainsRune(key, 0) {
		return "", 0, false
	}
	return key, kind, true
}

func cleanS3Name(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.ContainsRune(name, 0) {
		return "", fserror.New(fserror.InvalidArgument, "invalid path segment")
	}
	return name, nil
}

func joinS3Path(parent, name string) string {
	if parent == "" {
		return name
	}
	return cleanS3Path(parent + "/" + name)
}

func cleanS3Path(value string) string {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if value == "" {
		return ""
	}
	cleaned := path.Clean("/" + value)
	return strings.Trim(cleaned, "/")
}

func s3TypeNumber(kind s3NodeKind) uint32 {
	if kind == s3NodeDir {
		return s3DirMode
	}
	return s3FileMode
}

var _ volumefuse.Session = (*s3Session)(nil)
var _ volumefuse.ReadIntoSession = (*s3Session)(nil)
var _ volumefuse.OpenFlagsSession = (*s3Session)(nil)
