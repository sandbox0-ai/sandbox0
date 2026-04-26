package s0fs

import (
	"fmt"
	pathpkg "path"
	"slices"
	"strings"
	"time"
)

type FileCloneMode string

const (
	FileCloneModeCOW  FileCloneMode = "cow"
	FileCloneModeCopy FileCloneMode = "copy"
)

type FileCloneSource struct {
	VolumeID string
	State    *SnapshotState
}

type FileCloneEntry struct {
	SourceVolumeID string
	SourcePath     string
	TargetPath     string
	Overwrite      bool
	CreateParents  bool
	Mode           FileCloneMode
	Data           []byte
}

type FileCloneResult struct {
	SourceVolumeID string
	SourcePath     string
	TargetPath     string
	Mode           FileCloneMode
	SizeBytes      uint64
}

type PathLookup struct {
	Clean  string
	Parent uint64
	Inode  uint64
	Base   string
	Node   *Node
	Exists bool
}

// LookupPath resolves a logical absolute path against a detached snapshot state.
func LookupPath(state *SnapshotState, raw string, allowRoot bool) (*PathLookup, error) {
	if state == nil {
		return nil, fmt.Errorf("%w: state is required", ErrInvalidInput)
	}
	normalizeState(state)
	cleaned, err := cleanClonePath(raw, allowRoot)
	if err != nil {
		return nil, err
	}
	if cleaned == "/" {
		node := state.Nodes[RootInode]
		if node == nil {
			return nil, ErrNotFound
		}
		return &PathLookup{
			Clean:  cleaned,
			Inode:  RootInode,
			Node:   cloneNode(node),
			Exists: true,
		}, nil
	}

	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	current := RootInode
	for i, part := range parts {
		children := state.Children[current]
		if children == nil {
			return nil, ErrNotDir
		}
		inode, ok := children[part]
		if !ok {
			return &PathLookup{
				Clean:  cleaned,
				Parent: current,
				Base:   part,
				Exists: false,
			}, nil
		}
		node := state.Nodes[inode]
		if node == nil {
			return nil, ErrNotFound
		}
		if i == len(parts)-1 {
			return &PathLookup{
				Clean:  cleaned,
				Parent: current,
				Inode:  inode,
				Base:   part,
				Node:   cloneNode(node),
				Exists: true,
			}, nil
		}
		if node.Type != TypeDirectory {
			return nil, ErrNotDir
		}
		current = inode
	}

	return nil, ErrInvalidInput
}

func CloneFilesIntoState(target *SnapshotState, sources map[string]FileCloneSource, entries []FileCloneEntry) (*SnapshotState, []FileCloneResult, error) {
	if target == nil {
		return nil, nil, fmt.Errorf("%w: target state is required", ErrInvalidInput)
	}
	if len(entries) == 0 {
		return nil, nil, fmt.Errorf("%w: clone entries are required", ErrInvalidInput)
	}

	next := cloneState(target)
	normalizeState(next)

	now := time.Now().UTC()
	results := make([]FileCloneResult, 0, len(entries))
	for _, entry := range entries {
		result, err := cloneFileIntoState(next, sources, entry, now)
		if err != nil {
			return nil, nil, err
		}
		results = append(results, result)
	}
	next.NextSeq++
	return next, results, nil
}

func cloneFileIntoState(target *SnapshotState, sources map[string]FileCloneSource, entry FileCloneEntry, now time.Time) (FileCloneResult, error) {
	source, ok := sources[strings.TrimSpace(entry.SourceVolumeID)]
	if !ok || source.State == nil {
		return FileCloneResult{}, fmt.Errorf("%w: source volume %q is unavailable", ErrInvalidInput, entry.SourceVolumeID)
	}
	normalizeState(source.State)

	sourcePath, err := LookupPath(source.State, entry.SourcePath, false)
	if err != nil {
		return FileCloneResult{}, err
	}
	if !sourcePath.Exists {
		return FileCloneResult{}, ErrNotFound
	}
	if sourcePath.Node == nil || sourcePath.Node.Type != TypeFile {
		return FileCloneResult{}, ErrIsDir
	}

	targetParent, targetBase, err := ensureCloneParent(target, entry.TargetPath, entry.CreateParents, now)
	if err != nil {
		return FileCloneResult{}, err
	}
	if err := removeCloneDestinationIfNeeded(target, targetParent, targetBase, entry.Overwrite, now); err != nil {
		return FileCloneResult{}, err
	}

	targetInode := allocateCloneInode(target)
	targetNode := cloneNode(sourcePath.Node)
	targetNode.Inode = targetInode
	targetNode.Nlink = 1
	targetNode.Ctime = now
	target.Nodes[targetInode] = targetNode
	target.Children[targetParent][targetBase] = targetInode

	resultMode := entry.Mode
	switch entry.Mode {
	case FileCloneModeCopy:
		target.Data[targetInode] = slices.Clone(entry.Data)
		targetNode.Size = uint64(len(entry.Data))
	case FileCloneModeCOW, "":
		if err := attachColdClone(target, source, sourcePath.Inode, targetInode); err != nil {
			return FileCloneResult{}, err
		}
		resultMode = FileCloneModeCOW
	default:
		return FileCloneResult{}, fmt.Errorf("%w: unsupported clone mode %q", ErrInvalidInput, entry.Mode)
	}

	return FileCloneResult{
		SourceVolumeID: source.VolumeID,
		SourcePath:     sourcePath.Clean,
		TargetPath:     joinClonePath(parentPath(target, targetParent), targetBase),
		Mode:           resultMode,
		SizeBytes:      targetNode.Size,
	}, nil
}

func attachColdClone(target *SnapshotState, source FileCloneSource, sourceInode, targetInode uint64) error {
	sourceNode := source.State.Nodes[sourceInode]
	if sourceNode == nil {
		return ErrNotFound
	}
	extents := source.State.ColdFiles[sourceInode]
	if sourceNode.Size > 0 && len(extents) == 0 {
		return fmt.Errorf("%w: source file is not materialized", ErrInvalidInput)
	}
	if len(extents) == 0 {
		return nil
	}

	remapped := make([]FileExtent, 0, len(extents))
	idMap := make(map[string]string)
	for _, extent := range extents {
		sourceSegment := source.State.Segments[extent.SegmentID]
		if sourceSegment == nil {
			return fmt.Errorf("%w: missing source segment %s", ErrInvalidInput, extent.SegmentID)
		}
		targetSegmentID, ok := idMap[extent.SegmentID]
		if !ok {
			segment := cloneSegment(sourceSegment)
			if strings.TrimSpace(segment.VolumeID) == "" {
				segment.VolumeID = source.VolumeID
			}
			targetSegmentID = importCloneSegment(target, segment)
			idMap[extent.SegmentID] = targetSegmentID
		}
		remapped = append(remapped, FileExtent{
			SegmentID: targetSegmentID,
			Offset:    extent.Offset,
			Length:    extent.Length,
		})
	}
	target.ColdFiles[targetInode] = remapped
	return nil
}

func importCloneSegment(target *SnapshotState, segment *Segment) string {
	if segment == nil {
		return ""
	}
	if existing := target.Segments[segment.ID]; existing == nil || sameSegment(existing, segment) {
		target.Segments[segment.ID] = segment
		return segment.ID
	}
	base := strings.TrimSpace(segment.VolumeID) + "/" + segment.ID
	candidate := base
	for i := 1; ; i++ {
		existing := target.Segments[candidate]
		if existing == nil || sameSegment(existing, segment) {
			clone := cloneSegment(segment)
			clone.ID = candidate
			target.Segments[candidate] = clone
			return candidate
		}
		candidate = fmt.Sprintf("%s-%d", base, i)
	}
}

func sameSegment(a, b *Segment) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.VolumeID == b.VolumeID &&
		a.Key == b.Key &&
		a.Length == b.Length &&
		a.SHA256 == b.SHA256
}

func ensureCloneParent(state *SnapshotState, raw string, createParents bool, now time.Time) (uint64, string, error) {
	cleaned, err := cleanClonePath(raw, false)
	if err != nil {
		return 0, "", err
	}
	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	base := parts[len(parts)-1]
	current := RootInode

	for _, part := range parts[:len(parts)-1] {
		children := state.Children[current]
		if children == nil {
			return 0, "", ErrNotDir
		}
		inode, ok := children[part]
		if !ok {
			if !createParents {
				return 0, "", ErrNotFound
			}
			inode = allocateCloneInode(state)
			state.Nodes[inode] = &Node{
				Inode: inode,
				Type:  TypeDirectory,
				Mode:  0o755,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: now,
			}
			state.Children[inode] = map[string]uint64{}
			children[part] = inode
		}
		node := state.Nodes[inode]
		if node == nil {
			return 0, "", ErrNotFound
		}
		if node.Type != TypeDirectory {
			return 0, "", ErrNotDir
		}
		current = inode
	}
	return current, base, nil
}

func removeCloneDestinationIfNeeded(state *SnapshotState, parent uint64, name string, overwrite bool, now time.Time) error {
	inode, exists := state.Children[parent][name]
	if !exists {
		return nil
	}
	node := state.Nodes[inode]
	if node == nil {
		delete(state.Children[parent], name)
		return nil
	}
	if node.Type == TypeDirectory {
		return ErrIsDir
	}
	if !overwrite {
		return ErrExists
	}
	delete(state.Children[parent], name)
	if node.Nlink > 0 {
		node.Nlink--
		node.Ctime = now
	}
	if node.Nlink == 0 {
		delete(state.Nodes, inode)
		delete(state.Data, inode)
		delete(state.ColdFiles, inode)
		delete(state.Children, inode)
	}
	return nil
}

func allocateCloneInode(state *SnapshotState) uint64 {
	if state.NextInode <= RootInode {
		state.NextInode = RootInode + 1
	}
	inode := state.NextInode
	state.NextInode++
	return inode
}

func parentPath(state *SnapshotState, inode uint64) string {
	if inode == RootInode {
		return "/"
	}
	var walk func(current uint64, prefix string) (string, bool)
	walk = func(current uint64, prefix string) (string, bool) {
		for name, child := range state.Children[current] {
			childPath := joinClonePath(prefix, name)
			if child == inode {
				return childPath, true
			}
			if node := state.Nodes[child]; node != nil && node.Type == TypeDirectory {
				if found, ok := walk(child, childPath); ok {
					return found, true
				}
			}
		}
		return "", false
	}
	if found, ok := walk(RootInode, "/"); ok {
		return found
	}
	return "/"
}

func cleanClonePath(raw string, allowRoot bool) (string, error) {
	cleaned := pathpkg.Clean("/" + strings.TrimSpace(raw))
	if cleaned == "/" && allowRoot {
		return "/", nil
	}
	if cleaned == "/" {
		return "", ErrInvalidInput
	}
	return cleaned, nil
}

func joinClonePath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}
