// Package rootfsfuse exposes one immutable v3 Head as a read-only filesystem.
package rootfsfuse

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsreader"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

const immutableTimeout = time.Hour

const overlayOpaqueXAttr = "trusted.overlay.opaque"

type Node struct {
	gofs.Inode
	reader *rootfsreader.Reader
	entry  rootfshead.Entry

	manifestMu  sync.Mutex
	manifest    *rootfshead.FileManifest
	directoryMu sync.Mutex
	directory   *rootfsreader.Directory
}

func NewRoot(reader *rootfsreader.Reader, head rootfshead.Head) (*Node, error) {
	if reader == nil {
		return nil, fmt.Errorf("rootfs FUSE reader is required")
	}
	if err := head.Validate(); err != nil {
		return nil, err
	}
	return &Node{reader: reader, entry: head.Root}, nil
}

func Mount(mountPoint string, reader *rootfsreader.Reader, head rootfshead.Head) (*fuse.Server, error) {
	root, err := NewRoot(reader, head)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(mountPoint, 0o700); err != nil {
		return nil, fmt.Errorf("create rootfs FUSE mount point: %w", err)
	}
	options := &gofs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:    os.Getuid() == 0,
			DirectMount:   true,
			FsName:        "sandbox0-rootfs-" + head.HeadID,
			Name:          "sandbox0-rootfs",
			MaxBackground: 128,
			MaxWrite:      1 << 20,
			Options:       []string{"default_permissions", "ro"},
			// Stacked overlay mounts reject idmapped FUSE on affected kernels.
			DisabledCapabilities: fuse.CAP_ALLOW_IDMAP,
		},
		EntryTimeout:    durationPointer(immutableTimeout),
		AttrTimeout:     durationPointer(immutableTimeout),
		NegativeTimeout: durationPointer(immutableTimeout),
		NullPermissions: true,
		RootStableAttr:  &gofs.StableAttr{Ino: inodeNumber(head.Root.Inode)},
	}
	server, err := gofs.Mount(mountPoint, root, options)
	if err != nil {
		return nil, fmt.Errorf("mount rootfs Head %s: %w", head.HeadID, err)
	}
	return server, nil
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		return nil, syscall.EINVAL
	}
	if existing := n.GetChild(name); existing != nil {
		if child, ok := existing.Operations().(*Node); ok {
			fillEntryOut(out, child.entry)
		}
		return existing, 0
	}
	directory, err := n.directoryView(ctx)
	if err != nil {
		return nil, errno(err)
	}
	entry, err := directory.Lookup(ctx, name)
	if err != nil {
		return nil, errno(err)
	}
	fillEntryOut(out, entry)
	if existing := n.GetChild(name); existing != nil {
		return existing, 0
	}
	child := &Node{reader: n.reader, entry: entry}
	inode := n.NewInode(ctx, child, gofs.StableAttr{Mode: entry.Mode & syscall.S_IFMT, Ino: inodeNumber(entry.Inode)})
	return inode, 0
}

func (n *Node) Readdir(ctx context.Context) (gofs.DirStream, syscall.Errno) {
	directory, err := n.directoryView(ctx)
	if err != nil {
		return nil, errno(err)
	}
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &directoryStream{ctx: streamCtx, cancel: cancel, directory: directory, iterator: directory.Iterator()}, 0
}

func (n *Node) directoryView(ctx context.Context) (*rootfsreader.Directory, error) {
	n.directoryMu.Lock()
	defer n.directoryMu.Unlock()
	if n.directory != nil {
		return n.directory, nil
	}
	directory, err := n.reader.OpenDirectory(ctx, n.entry)
	if err != nil {
		return nil, err
	}
	n.directory = directory
	return n.directory, nil
}

type directoryStream struct {
	ctx       context.Context
	cancel    context.CancelFunc
	directory *rootfsreader.Directory
	iterator  *rootfsreader.DirectoryIterator
	next      fuse.DirEntry
	offset    uint64
	prepared  bool
	pending   syscall.Errno
	done      bool
	closed    bool
}

var _ gofs.FileSeekdirer = (*directoryStream)(nil)

func (s *directoryStream) HasNext() bool {
	if s == nil || s.closed || s.done {
		return false
	}
	if s.prepared || s.pending != 0 {
		return true
	}
	entry, ok, err := s.iterator.Next(s.ctx)
	if err != nil {
		s.pending = errno(err)
		return true
	}
	if !ok {
		s.done = true
		return false
	}
	s.next = fuse.DirEntry{
		Name: entry.Name,
		Mode: entry.Mode & syscall.S_IFMT,
		Ino:  inodeNumber(entry.Inode),
	}
	s.prepared = true
	return true
}

func (s *directoryStream) Next() (fuse.DirEntry, syscall.Errno) {
	if !s.HasNext() {
		return fuse.DirEntry{}, 0
	}
	if s.pending != 0 {
		result := s.pending
		s.pending = 0
		s.done = true
		return fuse.DirEntry{}, result
	}
	result := s.next
	s.next = fuse.DirEntry{}
	s.prepared = false
	s.offset++
	result.Off = s.offset
	return result, 0
}

func (s *directoryStream) Seekdir(ctx context.Context, offset uint64) syscall.Errno {
	if s == nil || s.closed || s.directory == nil {
		return syscall.EBADF
	}
	if offset == s.offset && !s.prepared && s.pending == 0 {
		return 0
	}
	iterator := s.directory.Iterator()
	if err := iterator.Seek(ctx, offset); err != nil {
		return errno(err)
	}
	s.iterator = iterator
	s.next = fuse.DirEntry{}
	s.offset = offset
	s.prepared = false
	s.pending = 0
	s.done = false
	return 0
}

func (s *directoryStream) Close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
}

func (n *Node) Getattr(_ context.Context, _ gofs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fillAttr(&out.Attr, n.entry)
	out.SetTimeout(immutableTimeout)
	return 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (gofs.FileHandle, uint32, syscall.Errno) {
	if n.entry.Kind != rootfshead.EntryFile {
		return nil, 0, syscall.EINVAL
	}
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}
	if _, err := n.fileManifest(ctx); err != nil {
		return nil, 0, errno(err)
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *Node) Read(ctx context.Context, _ gofs.FileHandle, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	if n.entry.Kind != rootfshead.EntryFile {
		return nil, syscall.EINVAL
	}
	manifest, err := n.fileManifest(ctx)
	if err != nil {
		return nil, errno(err)
	}
	read, err := n.reader.ReadFileManifest(ctx, *manifest, destination, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errno(err)
	}
	return fuse.ReadResultData(destination[:read]), 0
}

func (n *Node) fileManifest(ctx context.Context) (*rootfshead.FileManifest, error) {
	n.manifestMu.Lock()
	defer n.manifestMu.Unlock()
	if n.manifest != nil {
		return n.manifest, nil
	}
	manifest, err := n.reader.LoadFileManifest(ctx, n.entry)
	if err != nil {
		return nil, err
	}
	n.manifest = &manifest
	return n.manifest, nil
}

func (n *Node) Readlink(_ context.Context) ([]byte, syscall.Errno) {
	if n.entry.Kind != rootfshead.EntrySymlink {
		return nil, syscall.EINVAL
	}
	return []byte(n.entry.Target), 0
}

func (n *Node) Getxattr(_ context.Context, name string, destination []byte) (uint32, syscall.Errno) {
	if n.entry.Kind == rootfshead.EntryDirectory && n.entry.Opaque && name == overlayOpaqueXAttr {
		return copyXAttr(destination, []byte("y"))
	}
	position := sort.Search(len(n.entry.XAttrs), func(i int) bool { return n.entry.XAttrs[i].Name >= name })
	if position == len(n.entry.XAttrs) || n.entry.XAttrs[position].Name != name {
		return 0, syscall.ENODATA
	}
	return copyXAttr(destination, n.entry.XAttrs[position].Value)
}

func copyXAttr(destination, value []byte) (uint32, syscall.Errno) {
	if len(destination) == 0 {
		return uint32(len(value)), 0
	}
	if len(destination) < len(value) {
		return uint32(len(value)), syscall.ERANGE
	}
	return uint32(copy(destination, value)), 0
}

func (n *Node) Listxattr(_ context.Context, destination []byte) (uint32, syscall.Errno) {
	size := 0
	if n.entry.Kind == rootfshead.EntryDirectory && n.entry.Opaque {
		size += len(overlayOpaqueXAttr) + 1
	}
	for _, attr := range n.entry.XAttrs {
		size += len(attr.Name) + 1
	}
	if len(destination) == 0 {
		return uint32(size), 0
	}
	if len(destination) < size {
		return uint32(size), syscall.ERANGE
	}
	offset := 0
	if n.entry.Kind == rootfshead.EntryDirectory && n.entry.Opaque {
		offset += copy(destination[offset:], overlayOpaqueXAttr)
		destination[offset] = 0
		offset++
	}
	for _, attr := range n.entry.XAttrs {
		offset += copy(destination[offset:], attr.Name)
		destination[offset] = 0
		offset++
	}
	return uint32(size), 0
}

func fillAttr(attr *fuse.Attr, entry rootfshead.Entry) {
	attr.Ino = inodeNumber(entry.Inode)
	attr.Size = entry.Size
	attr.Blocks = entry.Blocks
	attr.Atime = uint64(max(entry.AccessTime.Seconds, 0))
	attr.Mtime = uint64(max(entry.ModTime.Seconds, 0))
	attr.Ctime = uint64(max(entry.ChangeTime.Seconds, 0))
	attr.Atimensec = entry.AccessTime.Nanoseconds
	attr.Mtimensec = entry.ModTime.Nanoseconds
	attr.Ctimensec = entry.ChangeTime.Nanoseconds
	attr.Mode = entry.Mode
	attr.Nlink = max(entry.Nlink, 1)
	attr.Owner = fuse.Owner{Uid: entry.UID, Gid: entry.GID}
	attr.Rdev = entry.Rdev
	attr.Blksize = 4096
}

func fillEntryOut(out *fuse.EntryOut, entry rootfshead.Entry) {
	if out == nil {
		return
	}
	fillAttr(&out.Attr, entry)
	out.SetEntryTimeout(immutableTimeout)
	out.SetAttrTimeout(immutableTimeout)
}

func inodeNumber(identity string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(identity))
	value := hash.Sum64() &^ (uint64(1) << 63)
	if value < 2 {
		value += 2
	}
	return value
}

func errno(err error) syscall.Errno {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, rootfsreader.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, rootfsreader.ErrInvalidDirectoryOffset):
		return syscall.EINVAL
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return syscall.EINTR
	default:
		return syscall.EIO
	}
}

func durationPointer(value time.Duration) *time.Duration {
	return &value
}
