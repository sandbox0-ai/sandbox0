package s0fs

import (
	"context"
	"fmt"
)

const maxManifestDeltaDepth = uint32(31)

type metadataChanges struct {
	inodes map[uint64]struct{}
	dirs   map[uint64]struct{}
	full   bool
}

func newMetadataChanges() *metadataChanges {
	return &metadataChanges{inodes: make(map[uint64]struct{}), dirs: make(map[uint64]struct{})}
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
			changes.addDir(record.Parent)
			if record.Type == TypeDirectory {
				changes.addDir(record.Inode)
			}
		case "link":
			changes.addInode(record.Inode)
			changes.addDir(record.NewParent)
		case "rename":
			if record.Inode == 0 {
				changes.full = true
			}
			changes.addInode(record.Inode)
			changes.addInode(record.SourceInode)
			changes.addDir(record.Parent)
			changes.addDir(record.NewParent)
		case "unlink", "rmdir":
			if record.Inode == 0 {
				changes.full = true
			}
			changes.addInode(record.Inode)
			changes.addDir(record.Parent)
			if record.Op == "rmdir" {
				changes.addDir(record.Inode)
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
	for inode, children := range delta.Children {
		state.Children[inode] = cloneChildren(children)
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

func cloneChildren(children map[string]uint64) map[string]uint64 {
	result := make(map[string]uint64, len(children))
	for name, inode := range children {
		result[name] = inode
	}
	return result
}
