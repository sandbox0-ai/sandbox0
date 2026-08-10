package s0fs

import (
	"cmp"
	"context"
	"slices"
)

type metadataDirEntry struct {
	Name  string
	Inode uint64
}

// metadataStore is the engine-facing namespace boundary. Implementations may
// keep metadata eagerly in memory or resolve it from an indexed backing store.
// Returned values are owned by the caller and must be written back explicitly.
type metadataStore interface {
	Node(uint64) (*Node, bool)
	PutNode(uint64, *Node)
	DeleteNode(uint64)
	RangeNodes(func(uint64, *Node) bool)
	NodeCount() int

	Child(uint64, string) (uint64, bool)
	PutChild(uint64, string, uint64)
	DeleteChild(uint64, string)
	EnsureDirectory(uint64)
	DeleteDirectory(uint64)
	DirectoryEntries(uint64) (map[string]uint64, bool)
	DirectoryPage(uint64, uint64, uint32) ([]metadataDirEntry, bool, bool)
	RangeDirectoryRecords(func(parent uint64, name string, inode uint64, first bool) bool)
	RangeDirectories(func(uint64, map[string]uint64) bool)
	DirectoryEntryCount() int
	Path(uint64) (string, bool)

	Data(uint64) ([]byte, bool)
	PutData(uint64, []byte)
	DeleteData(uint64)
	RangeData(func(uint64, []byte) bool)

	ColdFile(uint64) ([]FileExtent, bool)
	PutColdFile(uint64, []FileExtent)
	DeleteColdFile(uint64)
	RangeColdFiles(func(uint64, []FileExtent) bool)

	Segment(string) (*Segment, bool)
	PutSegment(string, *Segment)
	DeleteSegment(string)
	RangeSegments(func(string, *Segment) bool)
	SegmentCount() int
	NeedsMaterialization() bool
	PruneUnlinked(context.Context, map[uint64]struct{}) error

	EstimatedMemoryBytes() int64
	EstimatedPersistentBytes() int64
	Snapshot(uint64, uint64) *SnapshotState
	ReferenceSnapshot(uint64, uint64) *SnapshotState
	Err() error
	Close() error
}

type eagerMetadataStore struct {
	state *SnapshotState
}

func newEagerMetadataStore(state *SnapshotState) *eagerMetadataStore {
	normalizeState(state)
	return &eagerMetadataStore{state: state}
}

func (s *eagerMetadataStore) Node(inode uint64) (*Node, bool) {
	node, ok := s.state.Nodes[inode]
	return cloneNode(node), ok && node != nil
}

func (s *eagerMetadataStore) PutNode(inode uint64, node *Node) {
	s.state.Nodes[inode] = cloneNode(node)
}
func (s *eagerMetadataStore) DeleteNode(inode uint64) { delete(s.state.Nodes, inode) }
func (s *eagerMetadataStore) RangeNodes(yield func(uint64, *Node) bool) {
	for _, inode := range sortedUint64Keys(s.state.Nodes) {
		node := s.state.Nodes[inode]
		if !yield(inode, cloneNode(node)) {
			return
		}
	}
}
func (s *eagerMetadataStore) NodeCount() int { return len(s.state.Nodes) }

func (s *eagerMetadataStore) Child(parent uint64, name string) (uint64, bool) {
	inode, ok := s.state.Children[parent][name]
	return inode, ok
}
func (s *eagerMetadataStore) PutChild(parent uint64, name string, inode uint64) {
	s.EnsureDirectory(parent)
	s.state.Children[parent][name] = inode
}
func (s *eagerMetadataStore) DeleteChild(parent uint64, name string) {
	delete(s.state.Children[parent], name)
}
func (s *eagerMetadataStore) EnsureDirectory(inode uint64) {
	if s.state.Children[inode] == nil {
		s.state.Children[inode] = make(map[string]uint64)
	}
}
func (s *eagerMetadataStore) DeleteDirectory(inode uint64) { delete(s.state.Children, inode) }
func (s *eagerMetadataStore) DirectoryEntries(inode uint64) (map[string]uint64, bool) {
	entries, ok := s.state.Children[inode]
	if !ok {
		return nil, false
	}
	clone := make(map[string]uint64, len(entries))
	for name, child := range entries {
		clone[name] = child
	}
	return clone, true
}
func (s *eagerMetadataStore) DirectoryPage(inode, offset uint64, limit uint32) ([]metadataDirEntry, bool, bool) {
	children, ok := s.DirectoryEntries(inode)
	if !ok {
		return nil, false, false
	}
	entries := make([]metadataDirEntry, 0, len(children))
	for name, child := range children {
		entries = append(entries, metadataDirEntry{Name: name, Inode: child})
	}
	slices.SortFunc(entries, func(a, b metadataDirEntry) int { return cmp.Compare(a.Name, b.Name) })
	if offset >= uint64(len(entries)) {
		return nil, true, true
	}
	entries = entries[offset:]
	if limit == 0 || uint64(limit) >= uint64(len(entries)) {
		return entries, true, true
	}
	return entries[:limit], false, true
}
func (s *eagerMetadataStore) RangeDirectories(yield func(uint64, map[string]uint64) bool) {
	for _, inode := range sortedUint64Keys(s.state.Children) {
		entries, _ := s.DirectoryEntries(inode)
		if !yield(inode, entries) {
			return
		}
	}
}
func (s *eagerMetadataStore) RangeDirectoryRecords(yield func(parent uint64, name string, inode uint64, first bool) bool) {
	for _, parent := range sortedUint64Keys(s.state.Children) {
		names := sortedStringKeys(s.state.Children[parent])
		if len(names) == 0 {
			if !yield(parent, "", 0, true) {
				return
			}
			continue
		}
		for index, name := range names {
			if !yield(parent, name, s.state.Children[parent][name], index == 0) {
				return
			}
		}
	}
}
func (s *eagerMetadataStore) DirectoryEntryCount() int { return directoryEntryCount(s.state.Children) }
func (s *eagerMetadataStore) Path(target uint64) (string, bool) {
	if target == RootInode {
		return "/", true
	}
	if s.state.Nodes[target] == nil {
		return "", false
	}
	type frame struct {
		inode uint64
		path  string
	}
	queue := []frame{{inode: RootInode, path: "/"}}
	seen := map[uint64]struct{}{RootInode: {}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		names := sortedStringKeys(s.state.Children[current.inode])
		for _, name := range names {
			child := s.state.Children[current.inode][name]
			childPath := current.path + name
			if current.path != "/" {
				childPath = current.path + "/" + name
			}
			if child == target {
				return childPath, true
			}
			if _, ok := seen[child]; ok {
				continue
			}
			seen[child] = struct{}{}
			if node := s.state.Nodes[child]; node != nil && node.Type == TypeDirectory {
				queue = append(queue, frame{inode: child, path: childPath})
			}
		}
	}
	return "", false
}

func (s *eagerMetadataStore) Data(inode uint64) ([]byte, bool) {
	payload, ok := s.state.Data[inode]
	return slices.Clone(payload), ok
}
func (s *eagerMetadataStore) PutData(inode uint64, payload []byte) {
	s.state.Data[inode] = slices.Clone(payload)
}
func (s *eagerMetadataStore) DeleteData(inode uint64) { delete(s.state.Data, inode) }
func (s *eagerMetadataStore) RangeData(yield func(uint64, []byte) bool) {
	for _, inode := range sortedUint64Keys(s.state.Data) {
		payload := s.state.Data[inode]
		if !yield(inode, slices.Clone(payload)) {
			return
		}
	}
}

func (s *eagerMetadataStore) ColdFile(inode uint64) ([]FileExtent, bool) {
	extents, ok := s.state.ColdFiles[inode]
	return slices.Clone(extents), ok
}
func (s *eagerMetadataStore) PutColdFile(inode uint64, extents []FileExtent) {
	s.state.ColdFiles[inode] = slices.Clone(extents)
}
func (s *eagerMetadataStore) DeleteColdFile(inode uint64) { delete(s.state.ColdFiles, inode) }
func (s *eagerMetadataStore) RangeColdFiles(yield func(uint64, []FileExtent) bool) {
	for _, inode := range sortedUint64Keys(s.state.ColdFiles) {
		extents := s.state.ColdFiles[inode]
		if !yield(inode, slices.Clone(extents)) {
			return
		}
	}
}

func (s *eagerMetadataStore) Segment(id string) (*Segment, bool) {
	segment, ok := s.state.Segments[id]
	return cloneSegment(segment), ok && segment != nil
}
func (s *eagerMetadataStore) PutSegment(id string, segment *Segment) {
	s.state.Segments[id] = cloneSegment(segment)
}
func (s *eagerMetadataStore) DeleteSegment(id string) { delete(s.state.Segments, id) }
func (s *eagerMetadataStore) RangeSegments(yield func(string, *Segment) bool) {
	for _, id := range sortedStringKeys(s.state.Segments) {
		segment := s.state.Segments[id]
		if !yield(id, cloneSegment(segment)) {
			return
		}
	}
}
func (s *eagerMetadataStore) SegmentCount() int { return len(s.state.Segments) }
func (s *eagerMetadataStore) NeedsMaterialization() bool {
	for _, payload := range s.state.Data {
		if len(payload) != 0 {
			return true
		}
	}
	for _, extents := range s.state.ColdFiles {
		for _, extent := range extents {
			if extent.SegmentID != "" && isInlineSegment(s.state.Segments[extent.SegmentID]) {
				return true
			}
		}
	}
	return false
}
func (s *eagerMetadataStore) PruneUnlinked(ctx context.Context, retain map[uint64]struct{}) error {
	ctx = nonNilContext(ctx)
	for inode, node := range s.state.Nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if inode == RootInode || node == nil || node.Nlink != 0 {
			continue
		}
		if _, ok := retain[inode]; ok {
			continue
		}
		delete(s.state.Children, inode)
		delete(s.state.Nodes, inode)
		delete(s.state.Data, inode)
		delete(s.state.ColdFiles, inode)
	}
	return nil
}

func (s *eagerMetadataStore) EstimatedMemoryBytes() int64 { return estimatedStateMemoryBytes(s.state) }
func (s *eagerMetadataStore) EstimatedPersistentBytes() int64 {
	return estimatedStateBytes(s.state)
}
func (s *eagerMetadataStore) Snapshot(nextSeq, nextInode uint64) *SnapshotState {
	state := cloneState(s.state)
	state.NextSeq = nextSeq
	state.NextInode = nextInode
	return state
}
func (s *eagerMetadataStore) ReferenceSnapshot(nextSeq, nextInode uint64) *SnapshotState {
	state := cloneStateForMaterialization(s.state)
	state.NextSeq = nextSeq
	state.NextInode = nextInode
	return state
}
func (*eagerMetadataStore) Err() error   { return nil }
func (*eagerMetadataStore) Close() error { return nil }
