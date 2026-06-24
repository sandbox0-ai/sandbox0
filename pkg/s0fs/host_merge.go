package s0fs

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"
)

// MergeHostTree imports hostRoot and replaces targetPath in state with the
// imported subtree. The returned state is detached from the input state.
func MergeHostTree(ctx context.Context, state *SnapshotState, hostRoot, targetPath string, opts HostImportOptions) (*SnapshotState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	targetPath = cleanStatePath(targetPath)
	if targetPath == "" || targetPath == "/" {
		return nil, fmt.Errorf("%w: target path must be absolute and non-root", ErrInvalidInput)
	}
	imported, err := ImportHostTree(ctx, hostRoot, opts)
	if err != nil {
		return nil, err
	}
	out := cloneState(state)
	if out == nil {
		out = emptySnapshotState()
	}
	normalizeState(out)
	ensureStateNextInode(out)

	parent, name, err := ensureStateParentDirs(out, targetPath)
	if err != nil {
		return nil, err
	}
	if existing := out.Children[parent][name]; existing != 0 {
		removeStateSubtree(out, existing)
	}
	copied := make(map[uint64]uint64)
	targetInode, err := copyStateSubtree(out, imported, RootInode, copied)
	if err != nil {
		return nil, err
	}
	out.Children[parent][name] = targetInode
	out.NextSeq++
	pruneUnreferencedSegments(out)
	return out, nil
}

func emptySnapshotState() *SnapshotState {
	now := time.Now().UTC()
	return &SnapshotState{
		NextSeq:   1,
		NextInode: RootInode + 1,
		Nodes: map[uint64]*Node{
			RootInode: {
				Inode: RootInode,
				Type:  TypeDirectory,
				Mode:  0o755,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
		},
		Children:  map[uint64]map[string]uint64{RootInode: {}},
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent),
		Segments:  make(map[string]*Segment),
	}
}

func cleanStatePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || !strings.HasPrefix(value, "/") {
		return ""
	}
	return path.Clean(value)
}

func ensureStateParentDirs(state *SnapshotState, targetPath string) (uint64, string, error) {
	targetPath = cleanStatePath(targetPath)
	if targetPath == "" || targetPath == "/" {
		return 0, "", fmt.Errorf("%w: target path must be absolute and non-root", ErrInvalidInput)
	}
	parts := strings.Split(strings.TrimPrefix(targetPath, "/"), "/")
	parent := RootInode
	now := time.Now().UTC()
	for _, part := range parts[:len(parts)-1] {
		if part == "" {
			continue
		}
		children := state.Children[parent]
		if children == nil {
			return 0, "", fmt.Errorf("%w: parent inode %d is not a directory", ErrInvalidInput, parent)
		}
		child := children[part]
		if child != 0 {
			node := state.Nodes[child]
			if node != nil && node.Type == TypeDirectory {
				parent = child
				continue
			}
			removeStateSubtree(state, child)
		}
		inode := allocateStateInode(state)
		state.Nodes[inode] = &Node{
			Inode: inode,
			Type:  TypeDirectory,
			Mode:  0o755,
			Nlink: 1,
			Atime: now,
			Mtime: now,
			Ctime: now,
		}
		state.Children[inode] = make(map[string]uint64)
		children[part] = inode
		parent = inode
	}
	return parent, parts[len(parts)-1], nil
}

func copyStateSubtree(dst, src *SnapshotState, srcInode uint64, copied map[uint64]uint64) (uint64, error) {
	if dst == nil || src == nil {
		return 0, fmt.Errorf("%w: snapshot state is required", ErrInvalidInput)
	}
	if dstInode := copied[srcInode]; dstInode != 0 {
		return dstInode, nil
	}
	srcNode := src.Nodes[srcInode]
	if srcNode == nil {
		return 0, fmt.Errorf("%w: missing source inode %d", ErrInvalidInput, srcInode)
	}
	dstInode := allocateStateInode(dst)
	node := cloneNode(srcNode)
	node.Inode = dstInode
	dst.Nodes[dstInode] = node
	copied[srcInode] = dstInode

	if payload, ok := src.Data[srcInode]; ok {
		dst.Data[dstInode] = slices.Clone(payload)
	}
	if extents := src.ColdFiles[srcInode]; len(extents) > 0 {
		dst.ColdFiles[dstInode] = slices.Clone(extents)
		for _, extent := range extents {
			if segment := src.Segments[extent.SegmentID]; segment != nil {
				dst.Segments[extent.SegmentID] = cloneSegment(segment)
			}
		}
	}
	if node.Type != TypeDirectory {
		return dstInode, nil
	}
	dst.Children[dstInode] = make(map[string]uint64)
	names := make([]string, 0, len(src.Children[srcInode]))
	for name := range src.Children[srcInode] {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		childInode, err := copyStateSubtree(dst, src, src.Children[srcInode][name], copied)
		if err != nil {
			return 0, err
		}
		dst.Children[dstInode][name] = childInode
	}
	return dstInode, nil
}

func removeStateSubtree(state *SnapshotState, inode uint64) {
	if state == nil || inode == 0 || inode == RootInode {
		return
	}
	for _, child := range state.Children[inode] {
		removeStateSubtree(state, child)
	}
	delete(state.Children, inode)
	delete(state.Nodes, inode)
	delete(state.Data, inode)
	delete(state.ColdFiles, inode)
}

func allocateStateInode(state *SnapshotState) uint64 {
	ensureStateNextInode(state)
	inode := state.NextInode
	state.NextInode++
	return inode
}

func ensureStateNextInode(state *SnapshotState) {
	if state == nil {
		return
	}
	next := state.NextInode
	if next <= RootInode {
		next = RootInode + 1
	}
	for inode := range state.Nodes {
		if inode >= next {
			next = inode + 1
		}
	}
	state.NextInode = next
}
