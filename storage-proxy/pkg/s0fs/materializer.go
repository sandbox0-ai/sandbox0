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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

const (
	manifestLatestKey = "manifests/latest.json"
	manifestDir       = "manifests"
	segmentDir        = "segments"
)

const defaultSegmentCacheMaxBytes int64 = 64 << 20

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
	volumeID  string
	store     objectstore.Store
	headStore HeadStore
	cache     *segmentCache
}

func NewMaterializer(volumeID string, store objectstore.Store, headStore HeadStore) *Materializer {
	if store == nil {
		return nil
	}
	return &Materializer{
		volumeID:  volumeID,
		store:     store,
		headStore: headStore,
		cache:     newSegmentCache(defaultSegmentCacheMaxBytes),
	}
}

func (m *Materializer) Enabled() bool {
	return m != nil && m.store != nil
}

func (m *Materializer) Materialize(ctx context.Context, state *SnapshotState, expectedManifestSeq uint64) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, nil
	}
	if m.volumeID == "" {
		return nil, fmt.Errorf("%w: volume id is required", ErrInvalidInput)
	}
	if state == nil {
		return nil, fmt.Errorf("%w: snapshot state is required", ErrInvalidInput)
	}

	inline := cloneState(state)
	normalizeState(inline)

	nextSeq := checkpointSequence(inline)
	if nextSeq == 0 {
		return nil, fmt.Errorf("%w: manifest sequence must be non-zero", ErrInvalidInput)
	}
	if nextSeq <= expectedManifestSeq {
		return nil, fmt.Errorf("%w: manifest seq %d must advance beyond %d", ErrCommittedHeadConflict, nextSeq, expectedManifestSeq)
	}

	segment, fileExtents, err := buildSegment(nextSeq, inline)
	if err != nil {
		return nil, err
	}

	manifestState, err := buildManifestState(inline, segment, fileExtents)
	if err != nil {
		return nil, err
	}
	manifest := &Manifest{
		Version:       1,
		VolumeID:      m.volumeID,
		ManifestSeq:   nextSeq,
		CheckpointSeq: checkpointSequence(inline),
		CreatedAt:     time.Now().UTC(),
		State:         manifestState,
	}

	if len(segment.Payload) > 0 {
		if err := m.putBytes(ctx, segment.Key, segment.Payload); err != nil {
			return nil, err
		}
		m.cache.put(segment.Key, segment.Payload)
	}
	if err := m.putJSON(ctx, manifestKey(nextSeq), manifest); err != nil {
		return nil, err
	}
	if m.headStore != nil {
		head := &CommittedHead{
			VolumeID:      m.volumeID,
			ManifestSeq:   manifest.ManifestSeq,
			CheckpointSeq: manifest.CheckpointSeq,
			ManifestKey:   manifestKey(manifest.ManifestSeq),
			UpdatedAt:     manifest.CreatedAt,
		}
		if err := m.headStore.CompareAndSwapCommittedHead(ctx, m.volumeID, expectedManifestSeq, head); err != nil {
			return nil, err
		}
	} else if err := m.putJSON(ctx, manifestLatestKey, manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (m *Materializer) ReadSegmentRange(segment *Segment, off, limit int64) ([]byte, error) {
	if !m.Enabled() {
		return nil, fmt.Errorf("%w: materializer is not configured", ErrInvalidInput)
	}
	if segment == nil || segment.Key == "" {
		return nil, fmt.Errorf("%w: segment is required", ErrInvalidInput)
	}
	if limit == 0 {
		return nil, nil
	}
	if off < 0 {
		return nil, fmt.Errorf("%w: negative segment offset", ErrInvalidInput)
	}

	if segment.Length > 0 && int64(segment.Length) <= defaultSegmentCacheMaxBytes {
		if payload, ok := m.cache.get(segment.Key); ok {
			return cloneByteRange(payload, off, limit), nil
		}
		reader, err := m.store.Get(segment.Key, 0, int64(segment.Length))
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		m.cache.put(segment.Key, payload)
		return cloneByteRange(payload, off, limit), nil
	}

	reader, err := m.store.Get(segment.Key, off, limit)
	if err != nil {
		return nil, err
	}
	payload, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return payload, nil
}

func (m *Materializer) LoadLatestManifest(ctx context.Context) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, ErrMaterializedManifestNotFound
	}
	if m.headStore != nil {
		head, err := m.headStore.LoadCommittedHead(ctx, m.volumeID)
		switch {
		case err == nil:
			return m.loadManifestByKey(ctx, head.ManifestKey)
		case !errors.Is(err, ErrCommittedHeadNotFound):
			return nil, err
		}

		manifest, err := m.loadLegacyLatestManifest(ctx)
		if err != nil {
			return nil, err
		}
		head = &CommittedHead{
			VolumeID:      m.volumeID,
			ManifestSeq:   manifest.ManifestSeq,
			CheckpointSeq: manifest.CheckpointSeq,
			ManifestKey:   manifestKey(manifest.ManifestSeq),
			UpdatedAt:     manifest.CreatedAt,
		}
		if err := m.headStore.CompareAndSwapCommittedHead(ctx, m.volumeID, 0, head); err != nil {
			if errors.Is(err, ErrCommittedHeadConflict) {
				return m.LoadLatestManifest(ctx)
			}
			return nil, err
		}
		return manifest, nil
	}
	return m.loadLegacyLatestManifest(ctx)
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

func buildManifestState(state *SnapshotState, segment *materializedSegment, hotFiles map[uint64][]FileExtent) (*SnapshotState, error) {
	manifestState := &SnapshotState{
		NextSeq:   state.NextSeq,
		NextInode: state.NextInode,
		Nodes:     cloneNodeMap(state.Nodes),
		Children:  cloneChildrenMap(state.Children),
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent),
		Segments:  make(map[string]*Segment),
	}

	hotInodes := make(map[uint64]struct{}, len(state.Data))
	for inode := range state.Data {
		hotInodes[inode] = struct{}{}
	}

	for inode, extents := range state.ColdFiles {
		if _, hot := hotInodes[inode]; hot {
			continue
		}
		if state.Nodes[inode] == nil {
			continue
		}
		manifestState.ColdFiles[inode] = append([]FileExtent(nil), extents...)
		for _, extent := range extents {
			existing := state.Segments[extent.SegmentID]
			if existing == nil {
				return nil, fmt.Errorf("%w: missing retained segment %s", ErrInvalidInput, extent.SegmentID)
			}
			manifestState.Segments[extent.SegmentID] = cloneSegment(existing)
		}
	}

	for inode, extents := range hotFiles {
		if state.Nodes[inode] == nil {
			continue
		}
		manifestState.ColdFiles[inode] = append([]FileExtent(nil), extents...)
	}
	if segment != nil && len(segment.Payload) > 0 {
		manifestState.Segments[segment.ID] = &Segment{
			ID:     segment.ID,
			Key:    segment.Key,
			Length: uint64(len(segment.Payload)),
			SHA256: segment.SHA256,
		}
	}
	return manifestState, nil
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

func (m *Materializer) loadManifestByKey(ctx context.Context, key string) (*Manifest, error) {
	if strings.TrimSpace(key) == "" {
		return nil, ErrMaterializedManifestNotFound
	}
	var manifest Manifest
	if err := m.getJSON(ctx, key, &manifest); err != nil {
		return nil, err
	}
	if manifest.State == nil {
		return nil, fmt.Errorf("materialized manifest %s has no state", key)
	}
	normalizeState(manifest.State)
	return &manifest, nil
}

func (m *Materializer) loadLegacyLatestManifest(ctx context.Context) (*Manifest, error) {
	if _, err := m.store.Head(manifestLatestKey); err != nil {
		return nil, ErrMaterializedManifestNotFound
	}
	return m.loadManifestByKey(ctx, manifestLatestKey)
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

func cloneByteRange(payload []byte, off, limit int64) []byte {
	if off > int64(len(payload)) {
		off = int64(len(payload))
	}
	end := int64(len(payload))
	if limit >= 0 && off+limit < end {
		end = off + limit
	}
	return append([]byte(nil), payload[off:end]...)
}

type segmentCache struct {
	mu       sync.Mutex
	maxBytes int64
	size     int64
	entries  map[string][]byte
	order    []string
}

func newSegmentCache(maxBytes int64) *segmentCache {
	if maxBytes <= 0 {
		maxBytes = defaultSegmentCacheMaxBytes
	}
	return &segmentCache{
		maxBytes: maxBytes,
		entries:  make(map[string][]byte),
	}
}

func (c *segmentCache) get(key string) ([]byte, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	payload, ok := c.entries[key]
	return payload, ok
}

func (c *segmentCache) put(key string, payload []byte) {
	if c == nil || key == "" || int64(len(payload)) > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.entries[key]; ok {
		c.size -= int64(len(existing))
	} else {
		c.order = append(c.order, key)
	}
	c.entries[key] = append([]byte(nil), payload...)
	c.size += int64(len(payload))

	for c.size > c.maxBytes && len(c.order) > 0 {
		evict := c.order[0]
		c.order = c.order[1:]
		if evicted, ok := c.entries[evict]; ok {
			delete(c.entries, evict)
			c.size -= int64(len(evicted))
		}
	}
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
