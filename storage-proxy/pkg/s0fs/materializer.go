package s0fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

const (
	manifestLatestKey = "manifests/latest.json"
	manifestDir       = "manifests"
	segmentDir        = "segments"
)

var ErrMaterializedManifestNotFound = errors.New("materialized manifest not found")

type Manifest struct {
	Version       int            `json:"version"`
	VolumeID      string         `json:"volume_id"`
	ManifestSeq   uint64         `json:"manifest_seq"`
	CheckpointSeq uint64         `json:"checkpoint_seq"`
	CreatedAt     time.Time      `json:"created_at"`
	State         *SnapshotState `json:"state"`
}

type Materializer struct {
	store object.ObjectStorage
}

func NewMaterializer(store object.ObjectStorage) *Materializer {
	if store == nil {
		return nil
	}
	return &Materializer{store: store}
}

func (m *Materializer) Enabled() bool {
	return m != nil && m.store != nil
}

func (m *Materializer) Materialize(ctx context.Context, volumeID string, state *SnapshotState) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, nil
	}
	if volumeID == "" {
		return nil, fmt.Errorf("%w: volume id is required", ErrInvalidInput)
	}
	if state == nil {
		return nil, fmt.Errorf("%w: snapshot state is required", ErrInvalidInput)
	}

	inline := cloneState(state)
	if len(inline.ColdFiles) > 0 {
		return nil, fmt.Errorf("%w: materializer requires inline file data", ErrInvalidInput)
	}

	nextSeq, err := m.nextManifestSequence(ctx)
	if err != nil {
		return nil, err
	}

	segment, fileExtents, err := buildSegment(nextSeq, inline)
	if err != nil {
		return nil, err
	}

	manifestState := &SnapshotState{
		NextSeq:   inline.NextSeq,
		NextInode: inline.NextInode,
		Nodes:     cloneNodeMap(inline.Nodes),
		Children:  cloneChildrenMap(inline.Children),
		Data:      nil,
		ColdFiles: fileExtents,
		Segments: map[string]*Segment{
			segment.ID: &Segment{
				ID:     segment.ID,
				Key:    segment.Key,
				Length: uint64(len(segment.Payload)),
				SHA256: segment.SHA256,
			},
		},
	}
	manifest := &Manifest{
		Version:       1,
		VolumeID:      volumeID,
		ManifestSeq:   nextSeq,
		CheckpointSeq: checkpointSequence(inline),
		CreatedAt:     time.Now().UTC(),
		State:         manifestState,
	}

	if len(segment.Payload) > 0 {
		if err := m.putBytes(ctx, segment.Key, segment.Payload); err != nil {
			return nil, err
		}
	}
	if err := m.putJSON(ctx, manifestKey(nextSeq), manifest); err != nil {
		return nil, err
	}
	if err := m.putJSON(ctx, manifestLatestKey, manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (m *Materializer) LoadLatestManifest(ctx context.Context) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, ErrMaterializedManifestNotFound
	}
	if _, err := m.store.Head(manifestLatestKey); err != nil {
		return nil, ErrMaterializedManifestNotFound
	}
	var manifest Manifest
	if err := m.getJSON(ctx, manifestLatestKey, &manifest); err != nil {
		return nil, err
	}
	if manifest.State == nil {
		return nil, fmt.Errorf("materialized manifest %s has no state", manifestLatestKey)
	}
	normalizeState(manifest.State)
	return &manifest, nil
}

func (m *Materializer) LoadLatestState(ctx context.Context) (*SnapshotState, *Manifest, error) {
	manifest, err := m.LoadLatestManifest(ctx)
	if err != nil {
		return nil, nil, err
	}
	state := cloneState(manifest.State)
	return state, manifest, nil
}

type materializedSegment struct {
	ID      string
	Key     string
	Payload []byte
	SHA256  string
}

func buildSegment(manifestSeq uint64, state *SnapshotState) (*materializedSegment, map[uint64][]FileExtent, error) {
	segmentID := fmt.Sprintf("%020d-0", manifestSeq)
	segmentKey := fmt.Sprintf("%s/%s.bin", segmentDir, segmentID)

	inodes := make([]uint64, 0, len(state.Data))
	for inode, payload := range state.Data {
		if len(payload) == 0 {
			continue
		}
		if node := state.Nodes[inode]; node == nil || node.Type == TypeDirectory {
			continue
		}
		inodes = append(inodes, inode)
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })

	var buf bytes.Buffer
	files := make(map[uint64][]FileExtent, len(inodes))
	for _, inode := range inodes {
		payload := state.Data[inode]
		offset := uint64(buf.Len())
		if _, err := buf.Write(payload); err != nil {
			return nil, nil, fmt.Errorf("write segment buffer: %w", err)
		}
		files[inode] = []FileExtent{{
			SegmentID: segmentID,
			Offset:    offset,
			Length:    uint64(len(payload)),
		}}
	}

	sum := sha256.Sum256(buf.Bytes())
	segment := &materializedSegment{
		ID:      segmentID,
		Key:     segmentKey,
		Payload: buf.Bytes(),
		SHA256:  hex.EncodeToString(sum[:]),
	}
	return segment, files, nil
}

func manifestKey(seq uint64) string {
	return fmt.Sprintf("%s/%020d.json", manifestDir, seq)
}

func checkpointSequence(state *SnapshotState) uint64 {
	if state == nil || state.NextSeq == 0 {
		return 0
	}
	return state.NextSeq - 1
}

func (m *Materializer) nextManifestSequence(ctx context.Context) (uint64, error) {
	manifest, err := m.LoadLatestManifest(ctx)
	switch {
	case err == nil:
		return manifest.ManifestSeq + 1, nil
	case errors.Is(err, ErrMaterializedManifestNotFound):
		return 1, nil
	default:
		return 0, err
	}
}

func (m *Materializer) putJSON(ctx context.Context, key string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	return m.putBytes(ctx, key, payload)
}

func (m *Materializer) getJSON(ctx context.Context, key string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reader, err := m.store.Get(key, 0, -1)
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	defer reader.Close()

	payload, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read %s: %w", key, err)
	}
	if err := json.Unmarshal(payload, value); err != nil {
		return fmt.Errorf("decode %s: %w", key, err)
	}
	return nil
}

func (m *Materializer) putBytes(ctx context.Context, key string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.store.Put(key, bytes.NewReader(payload)); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func cloneNodeMap(nodes map[uint64]*Node) map[uint64]*Node {
	cloned := make(map[uint64]*Node, len(nodes))
	for inode, node := range nodes {
		cloned[inode] = cloneNode(node)
	}
	return cloned
}

func cloneChildrenMap(children map[uint64]map[string]uint64) map[uint64]map[string]uint64 {
	cloned := make(map[uint64]map[string]uint64, len(children))
	for inode, entries := range children {
		entryClone := make(map[string]uint64, len(entries))
		for name, childInode := range entries {
			entryClone[name] = childInode
		}
		cloned[inode] = entryClone
	}
	return cloned
}

func cloneSegment(segment *Segment) *Segment {
	if segment == nil {
		return nil
	}
	copy := *segment
	return &copy
}

func cloneFileExtents(extents map[uint64][]FileExtent) map[uint64][]FileExtent {
	if extents == nil {
		return nil
	}
	cloned := make(map[uint64][]FileExtent, len(extents))
	for inode, ranges := range extents {
		cloned[inode] = slices.Clone(ranges)
	}
	return cloned
}
