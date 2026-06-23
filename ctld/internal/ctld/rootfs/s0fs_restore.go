package rootfs

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"golang.org/x/sys/unix"
)

func restoreS0FSStateToHostTree(ctx context.Context, state *s0fs.SnapshotState, materializer *s0fs.Materializer, targetRoot string, excludedPaths []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	targetRoot = filepath.Clean(strings.TrimSpace(targetRoot))
	if targetRoot == "" || targetRoot == "." {
		return fmt.Errorf("%w: target root is required", ErrBadRequest)
	}
	if state == nil {
		return nil
	}
	filter := newRootFSPathFilter(excludedPaths)
	reader := s0fs.NewSnapshotReader(state, materializer)
	restored := make(map[uint64]string)
	return restoreS0FSChildren(ctx, state, reader, targetRoot, s0fs.RootInode, "/", filter, restored)
}

func restoreS0FSChildren(ctx context.Context, state *s0fs.SnapshotState, reader *s0fs.SnapshotReader, targetRoot string, parent uint64, parentPath string, filter rootFSPathFilter, restored map[uint64]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	children := state.Children[parent]
	names := make([]string, 0, len(children))
	for name := range children {
		names = append(names, name)
	}
	slices.Sort(names)
	if err := applyS0FSOpaqueWhiteout(targetRoot, parentPath, children, state); err != nil {
		return err
	}
	for _, name := range names {
		child := children[name]
		node := state.Nodes[child]
		if node == nil {
			continue
		}
		childPath := rootFSChildPath(parentPath, name)
		if filter.Excludes(childPath) {
			continue
		}
		handled, err := applyS0FSWhiteout(targetRoot, parentPath, name)
		if err != nil {
			return err
		}
		if handled {
			continue
		}
		if err := restoreS0FSNode(ctx, state, reader, targetRoot, childPath, node, filter, restored); err != nil {
			return err
		}
	}
	return nil
}

func restoreS0FSNode(ctx context.Context, state *s0fs.SnapshotState, reader *s0fs.SnapshotReader, targetRoot, nodePath string, node *s0fs.Node, filter rootFSPathFilter, restored map[uint64]string) error {
	target, err := rootFSRestoreTargetPath(targetRoot, nodePath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("create rootfs restore parent: %w", err)
	}
	switch node.Type {
	case s0fs.TypeDirectory:
		if err := os.MkdirAll(target, os.FileMode(node.Mode)); err != nil {
			return fmt.Errorf("create rootfs directory %s: %w", nodePath, err)
		}
		if err := applyS0FSMetadata(target, node); err != nil {
			return err
		}
		return restoreS0FSChildren(ctx, state, reader, targetRoot, node.Inode, nodePath, filter, restored)
	case s0fs.TypeFile:
		if first := restored[node.Inode]; first != "" {
			_ = os.Remove(target)
			if err := os.Link(first, target); err == nil {
				return applyS0FSMetadata(target, node)
			}
		}
		payload, err := reader.Read(node.Inode, 0, node.Size)
		if err != nil {
			return fmt.Errorf("read s0fs file %s: %w", nodePath, err)
		}
		if err := os.WriteFile(target, payload, os.FileMode(node.Mode)); err != nil {
			return fmt.Errorf("restore s0fs file %s: %w", nodePath, err)
		}
		restored[node.Inode] = target
		return applyS0FSMetadata(target, node)
	case s0fs.TypeSymlink:
		_ = os.Remove(target)
		if err := os.Symlink(node.Target, target); err != nil {
			return fmt.Errorf("restore s0fs symlink %s: %w", nodePath, err)
		}
		return applyS0FSXattrs(target, node)
	case s0fs.TypeFIFO:
		_ = os.Remove(target)
		if err := unix.Mkfifo(target, uint32(node.Mode)); err != nil {
			return fmt.Errorf("restore s0fs fifo %s: %w", nodePath, err)
		}
		return applyS0FSMetadata(target, node)
	case s0fs.TypeChar, s0fs.TypeBlock:
		_ = os.Remove(target)
		mode := uint32(node.Mode)
		if node.Type == s0fs.TypeChar {
			mode |= syscall.S_IFCHR
		} else {
			mode |= syscall.S_IFBLK
		}
		if err := unix.Mknod(target, mode, int(node.Rdev)); err != nil {
			return fmt.Errorf("restore s0fs device %s: %w", nodePath, err)
		}
		return applyS0FSMetadata(target, node)
	case s0fs.TypeSocket:
		return nil
	default:
		return fmt.Errorf("%w: unsupported s0fs node type %q at %s", ErrBadRequest, node.Type, nodePath)
	}
}

func applyS0FSWhiteout(targetRoot, parentPath, name string) (bool, error) {
	if name == ".wh..wh..opq" {
		return true, nil
	}
	if !strings.HasPrefix(name, ".wh.") {
		return false, nil
	}
	targetName := strings.TrimPrefix(name, ".wh.")
	if targetName == "" {
		return true, nil
	}
	target, err := rootFSRestoreTargetPath(targetRoot, rootFSChildPath(parentPath, targetName))
	if err != nil {
		return true, err
	}
	if err := os.RemoveAll(target); err != nil {
		return true, fmt.Errorf("apply s0fs whiteout %s: %w", targetName, err)
	}
	return true, nil
}

func applyS0FSOpaqueWhiteout(targetRoot, parentPath string, children map[string]uint64, state *s0fs.SnapshotState) error {
	opaqueInode := children[".wh..wh..opq"]
	if opaqueInode == 0 {
		return nil
	}
	target, err := rootFSRestoreTargetPath(targetRoot, parentPath)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read opaque rootfs directory %s: %w", parentPath, err)
	}
	keep := make(map[string]struct{}, len(children))
	for name, inode := range children {
		if name == ".wh..wh..opq" || strings.HasPrefix(name, ".wh.") || state.Nodes[inode] == nil {
			continue
		}
		keep[name] = struct{}{}
	}
	for _, entry := range entries {
		if _, ok := keep[entry.Name()]; ok {
			continue
		}
		if err := os.RemoveAll(filepath.Join(target, entry.Name())); err != nil {
			return fmt.Errorf("apply opaque rootfs whiteout %s: %w", parentPath, err)
		}
	}
	return nil
}

func applyS0FSMetadata(target string, node *s0fs.Node) error {
	if node == nil {
		return nil
	}
	_ = os.Chmod(target, os.FileMode(node.Mode))
	_ = os.Chown(target, int(node.UID), int(node.GID))
	if !node.Mtime.IsZero() {
		atime := node.Atime
		if atime.IsZero() {
			atime = node.Mtime
		}
		_ = os.Chtimes(target, atime, node.Mtime)
	}
	return applyS0FSXattrs(target, node)
}

func applyS0FSXattrs(target string, node *s0fs.Node) error {
	for name, value := range node.Xattrs {
		_ = unix.Lsetxattr(target, name, value, 0)
	}
	return nil
}

func rootFSChildPath(parentPath, name string) string {
	if parentPath == "/" {
		return "/" + name
	}
	return path.Join(parentPath, name)
}

func rootFSRestoreTargetPath(root, nodePath string) (string, error) {
	clean := cleanRootFSPath(nodePath)
	target := filepath.Join(root, strings.TrimPrefix(clean, "/"))
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, "../") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: rootfs restore path escapes root", ErrBadRequest)
	}
	return target, nil
}
