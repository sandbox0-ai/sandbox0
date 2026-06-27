package s0fs

import (
	"context"
	"fmt"
	"sort"
	"time"
)

type CompactionOptions struct {
	SegmentTargetSize uint64
	MinDeadRatio      float64
	MinReclaimBytes   uint64
	Force             bool
}

type CompactionResult struct {
	CompactedSegments []string
	RewrittenBytes    uint64
	ReclaimableBytes  uint64
}

func (e *Engine) Compact(ctx context.Context, opts CompactionOptions) (*Manifest, *CompactionResult, error) {
	if _, err := e.SyncMaterialize(ctx); err != nil {
		return nil, nil, err
	}

	e.materializeMu.Lock()
	defer e.materializeMu.Unlock()

	e.mu.RLock()
	if err := e.checkOpen(); err != nil {
		e.mu.RUnlock()
		return nil, nil, err
	}
	if e.materializer == nil || !e.materializer.Enabled() {
		e.mu.RUnlock()
		return nil, nil, nil
	}
	version := e.mutationVersion
	state := cloneState(e.currentStateLocked())
	expectedManifestSeq := e.lastCommittedManifest
	if state.NextSeq <= expectedManifestSeq+1 {
		state.NextSeq = expectedManifestSeq + 2
	}
	e.mu.RUnlock()

	manifest, result, err := e.materializer.Compact(ctx, state, expectedManifestSeq, opts)
	if err != nil || manifest == nil {
		return manifest, result, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mutationVersion == version && e.lastCommittedManifest == expectedManifestSeq {
		e.replaceStateLocked(cloneState(manifest.State))
		if err := e.persistCurrentStateLocked(); err != nil {
			return nil, nil, err
		}
		if err := e.wal.reset(); err != nil {
			return nil, nil, err
		}
		e.refreshLocalDiskGuardLocked()
		e.lastCommittedManifest = manifest.ManifestSeq
		e.lastMaterializedVersion = e.mutationVersion
		e.dirty = false
	} else if manifest.ManifestSeq > e.lastCommittedManifest {
		e.lastCommittedManifest = manifest.ManifestSeq
	}
	return manifest, result, nil
}

func (m *Materializer) Compact(ctx context.Context, state *SnapshotState, expectedManifestSeq uint64, opts CompactionOptions) (*Manifest, *CompactionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if !m.Enabled() {
		return nil, nil, nil
	}
	inline := cloneState(state)
	normalizeState(inline)
	defaultSegmentVolumeIDs(inline, m.volumeID)
	if inline.NextSeq <= expectedManifestSeq+1 {
		inline.NextSeq = expectedManifestSeq + 2
	}
	nextSeq := checkpointSequence(inline)
	if nextSeq <= expectedManifestSeq {
		return nil, nil, fmt.Errorf("%w: compact manifest seq %d must advance beyond %d", ErrCommittedHeadConflict, nextSeq, expectedManifestSeq)
	}

	selected, result := planCompactionSegments(inline, opts)
	if len(selected) == 0 && !hasInlineSegments(inline) {
		return nil, result, nil
	}
	manifestState, segments, err := buildCompactedState(ctx, m, nextSeq, m.volumeID, inline, selected, opts.SegmentTargetSize)
	if err != nil {
		return nil, nil, err
	}
	manifest := &Manifest{
		Version:       1,
		VolumeID:      m.volumeID,
		ManifestSeq:   nextSeq,
		CheckpointSeq: checkpointSequence(inline),
		CreatedAt:     time.Now().UTC(),
		State:         manifestState,
	}
	for _, segment := range segments {
		storedSegmentPayload, segmentEncryption, err := m.encryption.encryptSegment(m.volumeID, segment)
		if err != nil {
			return nil, nil, err
		}
		segment.Encryption = segmentEncryption
		if meta := manifest.State.Segments[segment.ID]; meta != nil {
			meta.Encryption = segmentEncryption
		}
		if err := m.putBytes(ctx, segment.Key, storedSegmentPayload); err != nil {
			return nil, nil, err
		}
		m.cache.put(segmentCacheKey(segment.VolumeID, segment.Key), segment.Payload)
	}
	if err := m.putJSON(ctx, manifestKey(nextSeq), manifest); err != nil {
		return nil, nil, err
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
			return nil, nil, err
		}
	} else if err := m.putJSON(ctx, manifestLatestKey, manifest); err != nil {
		return nil, nil, err
	}
	return manifest, result, nil
}

func planCompactionSegments(state *SnapshotState, opts CompactionOptions) (map[string]struct{}, *CompactionResult) {
	live := make(map[string]uint64)
	for _, extents := range state.ColdFiles {
		for _, extent := range extents {
			if extent.SegmentID != "" {
				live[extent.SegmentID] += extent.Length
			}
		}
	}
	selected := make(map[string]struct{})
	result := &CompactionResult{}
	for segmentID, liveBytes := range live {
		segment := state.Segments[segmentID]
		if segment == nil || isInlineSegment(segment) || segment.Key == "" || segment.Length == 0 {
			continue
		}
		if liveBytes >= segment.Length && !opts.Force {
			continue
		}
		deadBytes := segment.Length - minUint64(liveBytes, segment.Length)
		deadRatio := float64(deadBytes) / float64(segment.Length)
		if !opts.Force {
			if opts.MinReclaimBytes > 0 && deadBytes < opts.MinReclaimBytes {
				continue
			}
			if opts.MinDeadRatio > 0 && deadRatio < opts.MinDeadRatio {
				continue
			}
			if deadBytes == 0 {
				continue
			}
		}
		selected[segmentID] = struct{}{}
		result.CompactedSegments = append(result.CompactedSegments, segmentID)
		result.RewrittenBytes += liveBytes
		result.ReclaimableBytes += deadBytes
	}
	sort.Strings(result.CompactedSegments)
	return selected, result
}

func buildCompactedState(ctx context.Context, materializer *Materializer, manifestSeq uint64, volumeID string, state *SnapshotState, selected map[string]struct{}, targetSize uint64) (*SnapshotState, []*materializedSegment, error) {
	manifestState := &SnapshotState{
		NextSeq:   state.NextSeq,
		NextInode: state.NextInode,
		Nodes:     cloneNodeMap(state.Nodes),
		Children:  cloneChildrenMap(state.Children),
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent),
		Segments:  make(map[string]*Segment),
	}
	builder := newSegmentBuilder(manifestSeq, volumeID, targetSize)
	inodes := make([]uint64, 0, len(state.ColdFiles)+len(state.Data))
	seen := make(map[uint64]struct{}, len(state.ColdFiles)+len(state.Data))
	for inode := range state.ColdFiles {
		seen[inode] = struct{}{}
		inodes = append(inodes, inode)
	}
	for inode := range state.Data {
		if _, ok := seen[inode]; !ok {
			seen[inode] = struct{}{}
			inodes = append(inodes, inode)
		}
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })

	for _, inode := range inodes {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		node := state.Nodes[inode]
		if node == nil || node.Type == TypeDirectory {
			continue
		}
		var fileExtents []FileExtent
		if payload := state.Data[inode]; len(payload) > 0 {
			extents, err := builder.append(payload)
			if err != nil {
				return nil, nil, err
			}
			fileExtents = append(fileExtents, extents...)
		}
		for _, extent := range state.ColdFiles[inode] {
			if extent.Length == 0 {
				continue
			}
			if extent.SegmentID == "" {
				fileExtents = append(fileExtents, extent)
				continue
			}
			segment := state.Segments[extent.SegmentID]
			if segment == nil {
				return nil, nil, fmt.Errorf("%w: missing compact segment %s", ErrInvalidInput, extent.SegmentID)
			}
			if isInlineSegment(segment) {
				payload, err := inlineSegmentRange(segment, extent.Offset, extent.Length)
				if err != nil {
					return nil, nil, err
				}
				extents, err := builder.append(payload)
				if err != nil {
					return nil, nil, err
				}
				fileExtents = append(fileExtents, extents...)
				continue
			}
			if _, ok := selected[extent.SegmentID]; !ok {
				fileExtents = append(fileExtents, extent)
				manifestState.Segments[extent.SegmentID] = cloneSegment(segment)
				continue
			}
			payload, err := materializer.ReadSegmentRange(segment, int64(extent.Offset), int64(extent.Length))
			if err != nil {
				return nil, nil, fmt.Errorf("read compact segment %s: %w", segment.Key, err)
			}
			extents, err := builder.append(payload)
			if err != nil {
				return nil, nil, err
			}
			fileExtents = append(fileExtents, extents...)
		}
		fileExtents = coalesceExtents(fileExtents)
		if len(fileExtents) > 0 {
			manifestState.ColdFiles[inode] = fileExtents
		}
	}
	segments := builder.finish()
	for _, segment := range segments {
		manifestState.Segments[segment.ID] = &Segment{
			ID:       segment.ID,
			VolumeID: segment.VolumeID,
			Key:      segment.Key,
			Length:   uint64(len(segment.Payload)),
			SHA256:   segment.SHA256,
		}
	}
	return manifestState, segments, nil
}

func hasInlineSegments(state *SnapshotState) bool {
	for _, extents := range state.ColdFiles {
		for _, extent := range extents {
			if isInlineSegment(state.Segments[extent.SegmentID]) {
				return true
			}
		}
	}
	return false
}
