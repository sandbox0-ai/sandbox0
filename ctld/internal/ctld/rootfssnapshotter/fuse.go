package rootfssnapshotter

import (
	"context"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func (t *LayerTree) rawFileSystem() fuse.RawFileSystem {
	root := &layerNode{tree: t, entry: t.root}
	ttl := time.Hour
	return fs.NewNodeFS(root, &fs.Options{
		EntryTimeout:    &ttl,
		AttrTimeout:     &ttl,
		NegativeTimeout: &ttl,
		NullPermissions: true,
		RootStableAttr: &fs.StableAttr{
			Mode: uint32(syscall.S_IFDIR),
			Ino:  1,
			Gen:  1,
		},
	})
}

type layerNode struct {
	fs.Inode
	tree  *LayerTree
	entry *treeEntry
}

func (n *layerNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !n.isDirectory() {
		return nil, syscall.ENOTDIR
	}
	child, err := n.tree.lookup(ctx, n.entry.entry.Directory, name)
	if err != nil {
		return nil, errno(err)
	}
	setFuseAttr(&out.Attr, child)
	stable := fs.StableAttr{Mode: child.mode & uint32(syscall.S_IFMT), Ino: child.inode, Gen: 1}
	return n.NewInode(ctx, &layerNode{tree: n.tree, entry: child}, stable), 0
}

func (n *layerNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if !n.isDirectory() {
		return nil, syscall.ENOTDIR
	}
	children, err := n.tree.readDir(ctx, n.entry.entry.Directory)
	if err != nil {
		return nil, errno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(children))
	for _, child := range children {
		entries = append(entries, fuse.DirEntry{Name: child.entry.Name, Ino: child.inode, Mode: child.mode})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *layerNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n == nil || n.entry == nil {
		return syscall.ENOENT
	}
	setFuseAttr(&out.Attr, n.entry)
	return 0
}

func (n *layerNode) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if n == nil || n.entry == nil {
		return nil, 0, syscall.ENOENT
	}
	if flags&uint32(syscall.O_ACCMODE) != uint32(syscall.O_RDONLY) {
		return nil, 0, syscall.EROFS
	}
	if n.entry.mode&syscall.S_IFMT != syscall.S_IFREG || n.entry.entry.File == nil {
		return nil, 0, syscall.EINVAL
	}
	return &layerFile{tree: n.tree, entry: n.entry}, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *layerNode) Readlink(context.Context) ([]byte, syscall.Errno) {
	if n == nil || n.entry == nil {
		return nil, syscall.ENOENT
	}
	if n.entry.mode&syscall.S_IFMT != syscall.S_IFLNK {
		return nil, syscall.EINVAL
	}
	return []byte(n.entry.entry.Target), 0
}

func (n *layerNode) Getxattr(_ context.Context, name string, destination []byte) (uint32, syscall.Errno) {
	if n == nil || n.entry == nil {
		return 0, syscall.ENOENT
	}
	value, ok := n.entry.xattrs[name]
	if !ok {
		return 0, syscall.ENODATA
	}
	if len(destination) == 0 {
		return uint32(len(value)), 0
	}
	if len(destination) < len(value) {
		return uint32(len(value)), syscall.ERANGE
	}
	copy(destination, value)
	return uint32(len(value)), 0
}

func (n *layerNode) Listxattr(_ context.Context, destination []byte) (uint32, syscall.Errno) {
	if n == nil || n.entry == nil {
		return 0, syscall.ENOENT
	}
	names := make([]string, 0, len(n.entry.xattrs))
	for name := range n.entry.xattrs {
		names = append(names, name)
	}
	sort.Strings(names)
	payload := []byte(strings.Join(names, "\x00"))
	if len(payload) > 0 {
		payload = append(payload, 0)
	}
	if len(destination) == 0 {
		return uint32(len(payload)), 0
	}
	if len(destination) < len(payload) {
		return uint32(len(payload)), syscall.ERANGE
	}
	copy(destination, payload)
	return uint32(len(payload)), 0
}

func (n *layerNode) isDirectory() bool {
	return n != nil && n.entry != nil && n.entry.mode&syscall.S_IFMT == syscall.S_IFDIR
}

type layerFile struct {
	tree  *LayerTree
	entry *treeEntry
}

func (f *layerFile) Read(ctx context.Context, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	if f == nil || f.tree == nil || f.entry == nil {
		return nil, syscall.EBADF
	}
	n, err := f.tree.readEntryRange(ctx, f.entry, destination, offset)
	if err != nil {
		return nil, errno(err)
	}
	return fuse.ReadResultData(destination[:n]), 0
}

func setFuseAttr(out *fuse.Attr, entry *treeEntry) {
	if out == nil || entry == nil {
		return
	}
	out.Ino = entry.inode
	out.Mode = entry.mode
	out.Uid = entry.entry.UID
	out.Gid = entry.entry.GID
	out.Rdev = entry.entry.Rdev
	out.Size = entry.entry.Size
	if entry.mode&syscall.S_IFMT == syscall.S_IFLNK {
		out.Size = uint64(len(entry.entry.Target))
	}
	out.Blocks = (out.Size + 511) / 512
	out.Blksize = 4096
	out.Nlink = max(entry.entry.Nlink, 1)
	if entry.mode&syscall.S_IFMT == syscall.S_IFDIR {
		out.Nlink = max(out.Nlink, 2)
	}
	setFuseTime(&out.Mtime, &out.Mtimensec, entry.entry.ModTime.Time())
	setFuseTime(&out.Atime, &out.Atimensec, entry.entry.AccessTime.Time())
	setFuseTime(&out.Ctime, &out.Ctimensec, entry.entry.ChangeTime.Time())
}

func setFuseTime(seconds *uint64, nanoseconds *uint32, value time.Time) {
	if value.IsZero() {
		return
	}
	*seconds = uint64(value.Unix())
	*nanoseconds = uint32(value.Nanosecond())
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if value, ok := err.(syscall.Errno); ok {
		return value
	}
	return syscall.EIO
}
