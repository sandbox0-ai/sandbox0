package s0fs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var ErrSnapshotNotFound = fmt.Errorf("%w: snapshot not found", ErrNotFound)

func LoadSnapshot(_ context.Context, cfg Config, snapshotID string) (*SnapshotState, error) {
	if cfg.WALPath == "" {
		return nil, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	return loadSnapshotState(snapshotFilePath(cfg.WALPath, snapshotID), cfg.VolumeID, "snapshot:"+snapshotID, cfg.Encryption)
}

func snapshotFilePath(walPath, snapshotID string) string {
	return filepath.Join(filepath.Dir(walPath), "snapshots", snapshotID+".json")
}

func headStatePath(walPath string) string {
	return filepath.Join(filepath.Dir(walPath), "head.json")
}

func loadSnapshotState(path, volumeID, role string, encryption *EncryptionConfig) (*SnapshotState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrSnapshotNotFound
		}
		return nil, fmt.Errorf("read snapshot state: %w", err)
	}
	if plaintext, encrypted, err := encryption.decryptBlobIfEncrypted(data, stateBlobAAD(volumeID, role)); encrypted || err != nil {
		if err != nil {
			return nil, err
		}
		data = plaintext
	}

	var state SnapshotState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode snapshot state: %w", err)
	}
	normalizeState(&state)
	return &state, nil
}

func saveSnapshotState(path, volumeID, role string, state *SnapshotState, encryption *EncryptionConfig) error {
	if state == nil {
		return fmt.Errorf("%w: snapshot state is required", ErrInvalidInput)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode snapshot state: %w", err)
	}
	data, err = encryption.encryptBlob(data, stateBlobAAD(volumeID, role))
	if err != nil {
		return fmt.Errorf("encrypt snapshot state: %w", err)
	}

	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		return fmt.Errorf("write snapshot state: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace snapshot state: %w", err)
	}
	return nil
}

func normalizeState(state *SnapshotState) {
	if state == nil {
		return
	}
	if state.Nodes == nil {
		state.Nodes = make(map[uint64]*Node)
	}
	if state.Children == nil {
		state.Children = make(map[uint64]map[string]uint64)
	}
	if state.Data == nil {
		state.Data = make(map[uint64][]byte)
	}
	if state.ColdFiles == nil {
		state.ColdFiles = make(map[uint64][]FileExtent)
	}
	if state.Segments == nil {
		state.Segments = make(map[string]*Segment)
	}
	for inode, children := range state.Children {
		if children == nil {
			state.Children[inode] = make(map[string]uint64)
		}
	}
}

func cloneState(state *SnapshotState) *SnapshotState {
	if state == nil {
		return nil
	}

	clone := &SnapshotState{
		NextSeq:   state.NextSeq,
		NextInode: state.NextInode,
		Nodes:     make(map[uint64]*Node, len(state.Nodes)),
		Children:  make(map[uint64]map[string]uint64, len(state.Children)),
		Data:      make(map[uint64][]byte, len(state.Data)),
		ColdFiles: make(map[uint64][]FileExtent, len(state.ColdFiles)),
		Segments:  make(map[string]*Segment, len(state.Segments)),
	}
	for inode, node := range state.Nodes {
		clone.Nodes[inode] = cloneNode(node)
	}
	for inode, children := range state.Children {
		childClone := make(map[string]uint64, len(children))
		for name, childInode := range children {
			childClone[name] = childInode
		}
		clone.Children[inode] = childClone
	}
	for inode, payload := range state.Data {
		clone.Data[inode] = slices.Clone(payload)
	}
	for inode, extents := range state.ColdFiles {
		clone.ColdFiles[inode] = slices.Clone(extents)
	}
	for segmentID, segment := range state.Segments {
		clone.Segments[segmentID] = cloneSegment(segment)
	}
	return clone
}

// PrepareForkState returns a child-ready metadata snapshot that keeps cold file
// segments addressed to the source volume instead of inlining file contents.
func PrepareForkState(state *SnapshotState, sourceVolumeID string) (*SnapshotState, error) {
	sourceVolumeID = strings.TrimSpace(sourceVolumeID)
	if sourceVolumeID == "" {
		return nil, fmt.Errorf("%w: source volume id is required", ErrInvalidInput)
	}
	if state == nil {
		return nil, fmt.Errorf("%w: source state is required", ErrInvalidInput)
	}
	clone := cloneState(state)
	normalizeState(clone)
	for inode, payload := range clone.Data {
		if len(payload) > 0 {
			return nil, fmt.Errorf("%w: source state has inline data for inode %d", ErrInvalidInput, inode)
		}
	}
	for inode, extents := range clone.ColdFiles {
		if clone.Nodes[inode] == nil {
			delete(clone.ColdFiles, inode)
			continue
		}
		for _, extent := range extents {
			segment := clone.Segments[extent.SegmentID]
			if segment == nil {
				return nil, fmt.Errorf("%w: missing source segment %s", ErrInvalidInput, extent.SegmentID)
			}
			if strings.TrimSpace(segment.VolumeID) == "" {
				segment.VolumeID = sourceVolumeID
			}
		}
	}
	clone.Data = make(map[uint64][]byte)
	return clone, nil
}

func (s *SnapshotState) Lookup(parent uint64, name string) (*Node, error) {
	if s == nil {
		return nil, ErrNotFound
	}
	children := s.Children[parent]
	if children == nil {
		return nil, ErrNotDir
	}
	inode, ok := children[name]
	if !ok {
		return nil, ErrNotFound
	}
	node := s.Nodes[inode]
	if node == nil {
		return nil, ErrNotFound
	}
	return cloneNode(node), nil
}

func (s *SnapshotState) GetAttr(inode uint64) (*Node, error) {
	if s == nil {
		return nil, ErrNotFound
	}
	node := s.Nodes[inode]
	if node == nil {
		return nil, ErrNotFound
	}
	return cloneNode(node), nil
}

func (s *SnapshotState) ReadDir(inode uint64) ([]DirEntry, error) {
	if s == nil {
		return nil, ErrNotFound
	}
	node := s.Nodes[inode]
	if node == nil {
		return nil, ErrNotFound
	}
	if node.Type != TypeDirectory {
		return nil, ErrNotDir
	}
	entries := make([]DirEntry, 0, len(s.Children[inode]))
	for name, childInode := range s.Children[inode] {
		child := s.Nodes[childInode]
		if child == nil {
			continue
		}
		entries = append(entries, DirEntry{Name: name, Inode: childInode, Type: child.Type})
	}
	slices.SortFunc(entries, func(a, b DirEntry) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return entries, nil
}

func (s *SnapshotState) Read(inode uint64, offset uint64, size uint64) ([]byte, error) {
	if s == nil {
		return nil, ErrNotFound
	}
	node := s.Nodes[inode]
	if node == nil {
		return nil, ErrNotFound
	}
	if node.Type == TypeDirectory {
		return nil, ErrIsDir
	}
	payload := s.Data[inode]
	if len(payload) == 0 && len(s.ColdFiles[inode]) > 0 {
		return nil, fmt.Errorf("%w: detached snapshot state does not have inline data for inode %d", ErrInvalidInput, inode)
	}
	if offset >= uint64(len(payload)) {
		return nil, nil
	}
	end := offset + size
	if end > uint64(len(payload)) {
		end = uint64(len(payload))
	}
	return slices.Clone(payload[offset:end]), nil
}
