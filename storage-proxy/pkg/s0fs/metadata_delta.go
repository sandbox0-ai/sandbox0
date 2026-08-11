package s0fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

const (
	// Normal commits remain small deltas. Background compaction starts a
	// metadata-only checkpoint well before the hard reader bound.
	metadataCompactionDeltaDepth = uint32(16)
	maxManifestDeltaDepth        = uint32(127)
)

type metadataChanges struct {
	inodes     map[uint64]struct{}
	dirs       map[uint64]struct{}
	dirents    map[uint64]map[string]struct{}
	dirMarkers map[uint64]struct{}
	full       bool
}

func newMetadataChanges() *metadataChanges {
	return &metadataChanges{
		inodes: make(map[uint64]struct{}), dirs: make(map[uint64]struct{}),
		dirents: make(map[uint64]map[string]struct{}), dirMarkers: make(map[uint64]struct{}),
	}
}

func collectMetadataChanges(ctx context.Context, path, volumeID string, encryption *EncryptionConfig, throughSeq uint64) (*metadataChanges, error) {
	changes := newMetadataChanges()
	replay, err := openWALReplay(path, volumeID, encryption)
	if err != nil {
		return nil, err
	}
	defer replay.Close()
	for {
		record, ok, err := replay.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok || record.Seq > throughSeq {
			break
		}
		switch record.Op {
		case "create":
			changes.addInode(record.Inode)
			changes.addDirent(record.Parent, record.Name)
			if record.Type == TypeDirectory {
				changes.addDirMarker(record.Inode)
			}
		case "link":
			changes.addInode(record.Inode)
			changes.addDirent(record.NewParent, record.NewName)
		case "rename":
			if record.Inode == 0 {
				changes.full = true
			}
			changes.addInode(record.Inode)
			changes.addInode(record.SourceInode)
			changes.addDirent(record.Parent, record.Name)
			changes.addDirent(record.NewParent, record.NewName)
			changes.addDirMarker(record.Inode)
			changes.addDirMarker(record.SourceInode)
		case "unlink", "rmdir":
			if record.Inode == 0 {
				changes.full = true
			}
			changes.addInode(record.Inode)
			changes.addDirent(record.Parent, record.Name)
			if record.Op == "rmdir" {
				changes.addDirMarker(record.Inode)
			}
		case "write", "chmod", "chown", "truncate", "set_xattr", "remove_xattr", "set_times", "fallocate":
			changes.addInode(record.Inode)
		case "copy_file_range":
			changes.addInode(record.Inode)
		default:
			// Unknown records must never produce an incomplete remote commit.
			changes.full = true
		}
	}
	return changes, nil
}

func (c *metadataChanges) addInode(inode uint64) {
	if c != nil && inode != 0 {
		c.inodes[inode] = struct{}{}
	}
}

func (c *metadataChanges) addDir(inode uint64) {
	if c != nil && inode != 0 {
		c.dirs[inode] = struct{}{}
	}
}

func (c *metadataChanges) addDirent(parent uint64, name string) {
	if c == nil || parent == 0 || name == "" {
		return
	}
	if c.dirents[parent] == nil {
		c.dirents[parent] = make(map[string]struct{})
	}
	c.dirents[parent][name] = struct{}{}
}

func (c *metadataChanges) addDirMarker(inode uint64) {
	if c != nil && inode != 0 {
		c.dirMarkers[inode] = struct{}{}
	}
}

// buildMetadataDelta reads only records named by the WAL change set. It is
// the production incremental publication path: it avoids constructing a
// SnapshotState proportional to the complete namespace and avoids cloning a
// complete high-fanout directory for a one-name mutation.
func buildMetadataDelta(metadata metadataStore, nextSeq, nextInode uint64, changes *metadataChanges) (*SnapshotDelta, error) {
	if metadata == nil || changes == nil || changes.full {
		return nil, fmt.Errorf("%w: incremental metadata changes are unavailable", ErrInvalidInput)
	}
	delta := &SnapshotDelta{
		NextSeq: nextSeq, NextInode: nextInode,
		Nodes: make(map[uint64]*Node), Data: make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent), Segments: make(map[string]*Segment),
	}
	for inode := range changes.inodes {
		if node, ok := metadata.Node(inode); ok {
			delta.Nodes[inode] = node
		} else {
			delta.DeletedNodes = append(delta.DeletedNodes, inode)
		}
		if payload, ok := metadata.Data(inode); ok {
			delta.Data[inode] = payload
		} else {
			delta.DeletedData = append(delta.DeletedData, inode)
		}
		if extents, ok := metadata.ColdFile(inode); ok {
			delta.ColdFiles[inode] = extents
			for _, extent := range extents {
				if extent.SegmentID == "" {
					continue
				}
				segment, exists := metadata.Segment(extent.SegmentID)
				if !exists || segment == nil {
					return nil, fmt.Errorf("%w: delta inode %d references missing segment %s", ErrInvalidInput, inode, extent.SegmentID)
				}
				delta.Segments[extent.SegmentID] = segment
			}
		} else {
			delta.DeletedColdFiles = append(delta.DeletedColdFiles, inode)
		}
	}
	for inode := range changes.dirMarkers {
		if _, _, ok := metadata.DirectoryPage(inode, 0, 1); ok {
			delta.CreatedDirectories = append(delta.CreatedDirectories, inode)
		} else {
			delta.DeletedDirectories = append(delta.DeletedDirectories, inode)
		}
	}
	for parent, names := range changes.dirents {
		for name := range names {
			inode, _ := metadata.Child(parent, name)
			delta.Dirents = append(delta.Dirents, SnapshotDirentDelta{Parent: parent, Name: name, Inode: inode})
		}
	}
	if err := metadata.Err(); err != nil {
		return nil, err
	}
	sortSnapshotDelta(delta)
	return delta, nil
}

func sortSnapshotDelta(delta *SnapshotDelta) {
	if delta == nil {
		return
	}
	sort.Slice(delta.Dirents, func(i, j int) bool {
		if delta.Dirents[i].Parent != delta.Dirents[j].Parent {
			return delta.Dirents[i].Parent < delta.Dirents[j].Parent
		}
		return delta.Dirents[i].Name < delta.Dirents[j].Name
	})
	sort.Slice(delta.DeletedNodes, func(i, j int) bool { return delta.DeletedNodes[i] < delta.DeletedNodes[j] })
	sort.Slice(delta.CreatedDirectories, func(i, j int) bool { return delta.CreatedDirectories[i] < delta.CreatedDirectories[j] })
	sort.Slice(delta.DeletedDirectories, func(i, j int) bool { return delta.DeletedDirectories[i] < delta.DeletedDirectories[j] })
	sort.Slice(delta.DeletedData, func(i, j int) bool { return delta.DeletedData[i] < delta.DeletedData[j] })
	sort.Slice(delta.DeletedColdFiles, func(i, j int) bool { return delta.DeletedColdFiles[i] < delta.DeletedColdFiles[j] })
}

type materializedSnapshotDelta struct {
	delta *SnapshotDelta
	state *SnapshotState
}

func materializeSnapshotDelta(manifestSeq uint64, commitID, volumeID string, delta *SnapshotDelta, targetSize uint64) (*materializedSnapshotDelta, []*materializedSegment, error) {
	if delta == nil {
		return nil, nil, fmt.Errorf("%w: snapshot delta is required", ErrInvalidInput)
	}
	partial := &SnapshotState{
		NextSeq: delta.NextSeq, NextInode: delta.NextInode,
		Nodes: cloneNodeMap(delta.Nodes), Children: make(map[uint64]map[string]uint64),
		Data: cloneDataMap(delta.Data), ColdFiles: cloneColdFileMap(delta.ColdFiles),
		Segments: cloneSegmentMap(delta.Segments),
	}
	defaultSegmentVolumeIDs(partial, volumeID)
	state, segments, err := buildMaterializedState(manifestSeq, commitID, volumeID, partial, targetSize)
	if err != nil {
		return nil, nil, err
	}
	result := *delta
	result.Nodes = cloneNodeMap(state.Nodes)
	result.Data = cloneDataMap(state.Data)
	result.ColdFiles = cloneColdFileMap(state.ColdFiles)
	result.Segments = cloneSegmentMap(state.Segments)
	sortSnapshotDelta(&result)
	return &materializedSnapshotDelta{delta: &result, state: state}, segments, nil
}

func snapshotDeltaStateDigest(parentStateDigest string, delta *SnapshotDelta) (string, error) {
	if parentStateDigest == "" || delta == nil {
		return "", fmt.Errorf("%w: parent state digest and delta are required", ErrInvalidInput)
	}
	clone := *delta
	sortSnapshotDelta(&clone)
	payload, err := json.Marshal(&clone)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("s0fs-delta-state-v1\x00"))
	_, _ = hash.Write([]byte(parentStateDigest))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(payload)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func cloneDataMap(source map[uint64][]byte) map[uint64][]byte {
	result := make(map[uint64][]byte, len(source))
	for inode, payload := range source {
		result[inode] = append([]byte(nil), payload...)
	}
	return result
}

func cloneColdFileMap(source map[uint64][]FileExtent) map[uint64][]FileExtent {
	result := make(map[uint64][]FileExtent, len(source))
	for inode, extents := range source {
		result[inode] = cloneExtents(extents)
	}
	return result
}

func cloneSegmentMap(source map[string]*Segment) map[string]*Segment {
	result := make(map[string]*Segment, len(source))
	for id, segment := range source {
		result[id] = cloneSegment(segment)
	}
	return result
}

func buildSnapshotDelta(state *SnapshotState, changes *metadataChanges) (*SnapshotDelta, error) {
	if state == nil || changes == nil || changes.full {
		return nil, fmt.Errorf("%w: incremental metadata changes are unavailable", ErrInvalidInput)
	}
	delta := &SnapshotDelta{
		NextSeq:   state.NextSeq,
		NextInode: state.NextInode,
		Nodes:     make(map[uint64]*Node),
		Children:  make(map[uint64]map[string]uint64),
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent),
		Segments:  make(map[string]*Segment),
	}
	for inode := range changes.inodes {
		if node := state.Nodes[inode]; node != nil {
			delta.Nodes[inode] = cloneNode(node)
		} else {
			delta.DeletedNodes = append(delta.DeletedNodes, inode)
		}
		if payload, ok := state.Data[inode]; ok {
			delta.Data[inode] = append([]byte(nil), payload...)
		} else {
			delta.DeletedData = append(delta.DeletedData, inode)
		}
		if extents, ok := state.ColdFiles[inode]; ok {
			delta.ColdFiles[inode] = cloneExtents(extents)
			for _, extent := range extents {
				if extent.SegmentID == "" {
					continue
				}
				segment := state.Segments[extent.SegmentID]
				if segment == nil {
					return nil, fmt.Errorf("%w: delta inode %d references missing segment %s", ErrInvalidInput, inode, extent.SegmentID)
				}
				delta.Segments[extent.SegmentID] = cloneSegment(segment)
			}
		} else {
			delta.DeletedColdFiles = append(delta.DeletedColdFiles, inode)
		}
	}
	for inode := range changes.dirs {
		if children, ok := state.Children[inode]; ok {
			delta.Children[inode] = cloneChildren(children)
		} else {
			delta.DeletedDirectories = append(delta.DeletedDirectories, inode)
		}
	}
	return delta, nil
}

func applySnapshotDelta(base *SnapshotState, delta *SnapshotDelta) (*SnapshotState, error) {
	if base == nil || delta == nil || delta.NextSeq == 0 || delta.NextInode == 0 {
		return nil, fmt.Errorf("%w: invalid metadata delta", ErrCommittedStateIntegrity)
	}
	state := cloneState(base)
	state.NextSeq, state.NextInode = delta.NextSeq, delta.NextInode
	for _, inode := range delta.DeletedNodes {
		delete(state.Nodes, inode)
		delete(state.Data, inode)
		delete(state.ColdFiles, inode)
		delete(state.Children, inode)
	}
	for inode, node := range delta.Nodes {
		state.Nodes[inode] = cloneNode(node)
	}
	for _, inode := range delta.DeletedDirectories {
		delete(state.Children, inode)
	}
	for _, inode := range delta.CreatedDirectories {
		if state.Children[inode] == nil {
			state.Children[inode] = make(map[string]uint64)
		}
	}
	for inode, children := range delta.Children {
		state.Children[inode] = cloneChildren(children)
	}
	for _, entry := range delta.Dirents {
		if entry.Parent == 0 || entry.Name == "" {
			return nil, fmt.Errorf("%w: invalid directory-entry delta", ErrCommittedStateIntegrity)
		}
		if entry.Inode == 0 {
			delete(state.Children[entry.Parent], entry.Name)
			continue
		}
		if state.Children[entry.Parent] == nil {
			state.Children[entry.Parent] = make(map[string]uint64)
		}
		state.Children[entry.Parent][entry.Name] = entry.Inode
	}
	for _, inode := range delta.DeletedData {
		delete(state.Data, inode)
	}
	for inode, payload := range delta.Data {
		state.Data[inode] = append([]byte(nil), payload...)
	}
	for _, inode := range delta.DeletedColdFiles {
		delete(state.ColdFiles, inode)
	}
	for inode, extents := range delta.ColdFiles {
		state.ColdFiles[inode] = cloneExtents(extents)
	}
	for id, segment := range delta.Segments {
		state.Segments[id] = cloneSegment(segment)
	}
	pruneUnreferencedSegments(state)
	return state, nil
}

func applySnapshotDeltaToMetadata(metadata metadataStore, delta *SnapshotDelta) error {
	if metadata == nil || delta == nil || delta.NextSeq == 0 || delta.NextInode == 0 {
		return fmt.Errorf("%w: invalid metadata delta", ErrCommittedStateIntegrity)
	}
	segmentCandidates := make(map[string]struct{})
	for inode := range delta.ColdFiles {
		if extents, ok := metadata.ColdFile(inode); ok {
			for _, extent := range extents {
				if extent.SegmentID != "" {
					segmentCandidates[extent.SegmentID] = struct{}{}
				}
			}
		}
	}
	for _, inode := range delta.DeletedColdFiles {
		if extents, ok := metadata.ColdFile(inode); ok {
			for _, extent := range extents {
				if extent.SegmentID != "" {
					segmentCandidates[extent.SegmentID] = struct{}{}
				}
			}
		}
	}
	if err := metadata.ApplyMutation(func() error {
		for _, inode := range delta.DeletedNodes {
			metadata.DeleteNode(inode)
			metadata.DeleteData(inode)
			metadata.DeleteColdFile(inode)
			metadata.DeleteDirectory(inode)
		}
		for inode, node := range delta.Nodes {
			metadata.PutNode(inode, node)
		}
		for _, inode := range delta.DeletedDirectories {
			metadata.DeleteDirectory(inode)
		}
		for _, inode := range delta.CreatedDirectories {
			metadata.EnsureDirectory(inode)
		}
		for inode, children := range delta.Children {
			metadata.DeleteDirectory(inode)
			metadata.EnsureDirectory(inode)
			for name, child := range children {
				metadata.PutChild(inode, name, child)
			}
		}
		for _, entry := range delta.Dirents {
			if entry.Parent == 0 || entry.Name == "" {
				return fmt.Errorf("%w: invalid directory-entry delta", ErrCommittedStateIntegrity)
			}
			if entry.Inode == 0 {
				metadata.DeleteChild(entry.Parent, entry.Name)
			} else {
				metadata.PutChild(entry.Parent, entry.Name, entry.Inode)
			}
		}
		for _, inode := range delta.DeletedData {
			metadata.DeleteData(inode)
		}
		for inode, payload := range delta.Data {
			metadata.PutData(inode, payload)
		}
		for id, segment := range delta.Segments {
			metadata.PutSegment(id, segment)
		}
		for _, inode := range delta.DeletedColdFiles {
			metadata.DeleteColdFile(inode)
		}
		for inode, extents := range delta.ColdFiles {
			metadata.PutColdFile(inode, extents)
		}
		candidates := make([]string, 0, len(segmentCandidates))
		for id := range segmentCandidates {
			candidates = append(candidates, id)
		}
		metadata.PruneSegments(candidates)
		return metadata.Err()
	}); err != nil {
		return err
	}
	root, ok := metadata.Node(RootInode)
	if !ok || root == nil || root.Type != TypeDirectory {
		return fmt.Errorf("%w: delta removed filesystem root", ErrCommittedStateIntegrity)
	}
	return metadata.Err()
}

func cloneChildren(children map[string]uint64) map[string]uint64 {
	result := make(map[string]uint64, len(children))
	for name, inode := range children {
		result[name] = inode
	}
	return result
}
