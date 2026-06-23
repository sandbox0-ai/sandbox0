package s0fs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type HostImportOptions struct {
	Base          *SnapshotState
	ExcludedPaths []string
}

func ImportHostTree(ctx context.Context, root string, opts HostImportOptions) (*SnapshotState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "" || root == "." {
		return nil, fmt.Errorf("%w: root path is required", ErrInvalidInput)
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("stat import root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: import root must be a directory", ErrInvalidInput)
	}

	base := cloneState(opts.Base)
	normalizeState(base)
	basePaths := statePaths(base)
	excluded := normalizeImportExcludes(opts.ExcludedPaths)
	now := time.Now().UTC()
	nextSeq := uint64(2)
	if base != nil && base.NextSeq >= nextSeq {
		nextSeq = base.NextSeq + 1
	}
	state := &SnapshotState{
		NextSeq:   nextSeq,
		NextInode: RootInode + 1,
		Nodes: map[uint64]*Node{
			RootInode: nodeFromFileInfo(RootInode, TypeDirectory, info, "", 0, now),
		},
		Children:  map[uint64]map[string]uint64{RootInode: {}},
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent),
		Segments:  make(map[string]*Segment),
	}
	if xattrs := readHostXattrs(root); len(xattrs) > 0 {
		state.Nodes[RootInode].Xattrs = xattrs
	}

	pathInodes := map[string]uint64{"/": RootInode}
	hardlinks := make(map[hostFileKey]uint64)
	err = walkHostImportTree(root, func(hostPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if hostPath == root {
			return nil
		}
		rel, err := filepath.Rel(root, hostPath)
		if err != nil {
			return err
		}
		importPath := "/" + filepath.ToSlash(rel)
		if importPathExcluded(importPath, excluded) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := os.Lstat(hostPath)
		if err != nil {
			return err
		}
		parentPath := path.Dir(importPath)
		parentInode := pathInodes[parentPath]
		if parentInode == 0 {
			return fmt.Errorf("%w: missing parent %s", ErrInvalidInput, parentPath)
		}
		name := path.Base(importPath)
		if existing := hardlinkInode(info, hardlinks); existing != 0 {
			state.Children[parentInode][name] = existing
			if node := state.Nodes[existing]; node != nil {
				node.Nlink++
			}
			pathInodes[importPath] = existing
			return nil
		}

		inode := state.NextInode
		state.NextInode++
		node, err := importHostNode(hostPath, info, inode, now)
		if err != nil {
			return err
		}
		if xattrs := readHostXattrs(hostPath); len(xattrs) > 0 {
			node.Xattrs = xattrs
		}
		state.Nodes[inode] = node
		state.Children[parentInode][name] = inode
		pathInodes[importPath] = inode
		if node.Type == TypeDirectory {
			state.Children[inode] = make(map[string]uint64)
			return nil
		}
		if key, ok := hostHardlinkKey(info); ok {
			hardlinks[key] = inode
		}
		if node.Type != TypeFile {
			return nil
		}
		if preserveImportedFileData(state, base, basePaths[importPath], inode, node) {
			return nil
		}
		payload, err := os.ReadFile(hostPath)
		if err != nil {
			return err
		}
		if len(payload) > 0 {
			state.Data[inode] = payload
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("import host tree: %w", err)
	}
	return state, nil
}

func walkHostImportTree(root string, fn fs.WalkDirFunc) error {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return fn(root, nil, err)
	}
	return walkHostImportDir(root, root, fs.FileInfoToDirEntry(rootInfo), fn)
}

func walkHostImportDir(root, hostPath string, entry fs.DirEntry, fn fs.WalkDirFunc) error {
	if err := fn(hostPath, entry, nil); err != nil {
		if errors.Is(err, filepath.SkipDir) && hostPath != root {
			return nil
		}
		return err
	}
	if !entry.IsDir() {
		return nil
	}
	entries, err := os.ReadDir(hostPath)
	if err != nil {
		if err := fn(hostPath, entry, err); err != nil && !errors.Is(err, filepath.SkipDir) {
			return err
		}
		return nil
	}
	for _, child := range entries {
		childPath := filepath.Join(hostPath, child.Name())
		if err := walkHostImportDir(root, childPath, child, fn); err != nil {
			if errors.Is(err, filepath.SkipDir) {
				continue
			}
			return err
		}
	}
	return nil
}

func importHostNode(hostPath string, info os.FileInfo, inode uint64, now time.Time) (*Node, error) {
	mode := info.Mode()
	switch {
	case mode.IsDir():
		return nodeFromFileInfo(inode, TypeDirectory, info, "", 0, now), nil
	case mode.Type()&os.ModeSymlink != 0:
		target, err := os.Readlink(hostPath)
		if err != nil {
			return nil, err
		}
		return nodeFromFileInfo(inode, TypeSymlink, info, target, 0, now), nil
	case mode.IsRegular():
		return nodeFromFileInfo(inode, TypeFile, info, "", 0, now), nil
	case mode.Type()&os.ModeNamedPipe != 0:
		return nodeFromFileInfo(inode, TypeFIFO, info, "", rdevFromFileInfo(info), now), nil
	case mode.Type()&os.ModeDevice != 0:
		typ := TypeBlock
		if mode.Type()&os.ModeCharDevice != 0 {
			typ = TypeChar
		}
		return nodeFromFileInfo(inode, typ, info, "", rdevFromFileInfo(info), now), nil
	case mode.Type()&os.ModeSocket != 0:
		return nodeFromFileInfo(inode, TypeSocket, info, "", 0, now), nil
	default:
		return nil, fmt.Errorf("%w: unsupported file type at %s", ErrInvalidInput, hostPath)
	}
}

func nodeFromFileInfo(inode uint64, typ FileType, info os.FileInfo, target string, rdev uint64, fallbackTime time.Time) *Node {
	stat, _ := info.Sys().(*syscall.Stat_t)
	mtime := info.ModTime().UTC()
	if mtime.IsZero() {
		mtime = fallbackTime
	}
	atime := mtime
	ctime := mtime
	uid := uint32(0)
	gid := uint32(0)
	if stat != nil {
		uid = stat.Uid
		gid = stat.Gid
		atime = time.Unix(stat.Atim.Sec, stat.Atim.Nsec).UTC()
		ctime = time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec).UTC()
	}
	size := uint64(0)
	if typ == TypeFile {
		size = uint64(info.Size())
	}
	return &Node{
		Inode:  inode,
		Type:   typ,
		Mode:   uint32(info.Mode().Perm()),
		UID:    uid,
		GID:    gid,
		Nlink:  1,
		Size:   size,
		Target: target,
		Rdev:   rdev,
		Atime:  atime,
		Mtime:  mtime,
		Ctime:  ctime,
	}
}

func preserveImportedFileData(state, base *SnapshotState, baseInode uint64, inode uint64, node *Node) bool {
	if state == nil || base == nil || baseInode == 0 || node == nil {
		return false
	}
	baseNode := base.Nodes[baseInode]
	if baseNode == nil || baseNode.Type != TypeFile || !sameImportFileMetadata(baseNode, node) {
		return false
	}
	if payload, ok := base.Data[baseInode]; ok {
		state.Data[inode] = slices.Clone(payload)
		return true
	}
	if extents := base.ColdFiles[baseInode]; len(extents) > 0 {
		state.ColdFiles[inode] = slices.Clone(extents)
		for _, extent := range extents {
			if segment := base.Segments[extent.SegmentID]; segment != nil {
				state.Segments[extent.SegmentID] = cloneSegment(segment)
			}
		}
		return true
	}
	return node.Size == 0
}

func sameImportFileMetadata(a, b *Node) bool {
	return a != nil && b != nil &&
		a.Type == b.Type &&
		a.Mode == b.Mode &&
		a.UID == b.UID &&
		a.GID == b.GID &&
		a.Size == b.Size &&
		a.Mtime.Equal(b.Mtime)
}

func statePaths(state *SnapshotState) map[string]uint64 {
	out := make(map[string]uint64)
	if state == nil {
		return out
	}
	var walk func(uint64, string)
	walk = func(inode uint64, current string) {
		out[current] = inode
		children := state.Children[inode]
		names := make([]string, 0, len(children))
		for name := range children {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			child := children[name]
			childPath := path.Join(current, name)
			if current == "/" {
				childPath = "/" + name
			}
			walk(child, childPath)
		}
	}
	walk(RootInode, "/")
	return out
}

func normalizeImportExcludes(paths []string) map[string]struct{} {
	out := make(map[string]struct{}, len(paths))
	for _, raw := range paths {
		value := strings.TrimSpace(raw)
		if value == "" || !strings.HasPrefix(value, "/") {
			continue
		}
		clean := path.Clean(value)
		if clean == "/" {
			continue
		}
		out[clean] = struct{}{}
	}
	return out
}

func importPathExcluded(importPath string, excluded map[string]struct{}) bool {
	if len(excluded) == 0 {
		return false
	}
	for candidate := range excluded {
		if importPath == candidate || strings.HasPrefix(importPath, candidate+"/") {
			return true
		}
	}
	return false
}

func readHostXattrs(hostPath string) map[string][]byte {
	size, err := unix.Llistxattr(hostPath, nil)
	if err != nil || size <= 0 {
		return nil
	}
	buf := make([]byte, size)
	n, err := unix.Llistxattr(hostPath, buf)
	if err != nil || n <= 0 {
		return nil
	}
	xattrs := make(map[string][]byte)
	for _, raw := range strings.Split(string(buf[:n]), "\x00") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		valueSize, err := unix.Lgetxattr(hostPath, name, nil)
		if err != nil || valueSize < 0 {
			continue
		}
		value := make([]byte, valueSize)
		if valueSize > 0 {
			if _, err := unix.Lgetxattr(hostPath, name, value); err != nil {
				continue
			}
		}
		xattrs[name] = value
	}
	if len(xattrs) == 0 {
		return nil
	}
	return xattrs
}

type hostFileKey struct {
	dev uint64
	ino uint64
}

func hostHardlinkKey(info os.FileInfo) (hostFileKey, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil || stat.Nlink <= 1 || !info.Mode().IsRegular() {
		return hostFileKey{}, false
	}
	return hostFileKey{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}, true
}

func hardlinkInode(info os.FileInfo, hardlinks map[hostFileKey]uint64) uint64 {
	key, ok := hostHardlinkKey(info)
	if !ok {
		return 0
	}
	return hardlinks[key]
}

func rdevFromFileInfo(info os.FileInfo) uint64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return 0
	}
	return uint64(stat.Rdev)
}
