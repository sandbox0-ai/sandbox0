package s0fs

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
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
	state := e.currentStateLocked()
	expectedManifestSeq := e.lastCommittedManifest
	expected := cloneCommittedHead(e.lastCommittedHead)
	if state.NextSeq <= expectedManifestSeq+1 {
		state.NextSeq = expectedManifestSeq + 2
	}
	e.mu.RUnlock()

	manifest, result, err := e.materializer.prepareCompaction(ctx, state, expected, opts, true)
	if err != nil || manifest == nil {
		e.failClosed(err)
		return manifest, result, err
	}
	defer func() { _ = e.materializer.abortCommit(context.Background(), manifest.CommitID) }()

	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, nil, err
	}
	if e.mutationVersion != version || e.lastCommittedManifest != expectedManifestSeq || !sameCommittedHeadIdentity(e.lastCommittedHead, expected) {
		// Mutations may proceed while immutable compaction objects are being
		// prepared. If they do, leave the committed head and live state untouched;
		// the unique candidate objects are safe for a later fenced GC pass.
		return nil, nil, nil
	}
	if err := e.materializer.publishManifest(ctx, manifest, expected); err != nil {
		e.failClosed(err)
		return nil, nil, err
	}
	committedHead := committedHeadForManifest(manifest, manifestKey(manifest.ManifestSeq, manifest.CommitID), expected)
	e.lastCommittedManifest = manifest.ManifestSeq
	e.lastCommittedHead = committedHead
	installFailed := func(phase string, installErr error) (*Manifest, *CompactionResult, error) {
		terminalErr := fmt.Errorf("%w: %s after committed compaction: %w", ErrCommittedHeadConflict, phase, installErr)
		e.failClosed(terminalErr)
		return nil, nil, terminalErr
	}
	if err := e.persistStateLocked(manifest.State, true); err != nil {
		return installFailed("persist local state", err)
	}
	if err := saveRecoveryBinding(localHeadBindingPath(e.wal.path), e.volumeID, committedHead, manifest.StateDigest); err != nil {
		return installFailed("persist local binding", err)
	}
	if err := saveRecoveryBinding(walBaseBindingPath(e.wal.path), e.volumeID, committedHead, ""); err != nil {
		return installFailed("persist wal base", err)
	}
	if err := e.wal.reset(); err != nil {
		return installFailed("reset wal", err)
	}
	if err := e.replaceStateLocked(cloneState(manifest.State)); err != nil {
		return installFailed("install local state", err)
	}
	e.refreshLocalDiskGuardLocked()
	e.lastMaterializedVersion = e.mutationVersion
	e.dirty = false
	return manifest, result, nil
}

func (m *Materializer) Compact(ctx context.Context, state *SnapshotState, expectedManifestSeq uint64, opts CompactionOptions) (*Manifest, *CompactionResult, error) {
	expected, err := m.expectedHeadForSequence(ctx, expectedManifestSeq)
	if err != nil {
		return nil, nil, err
	}
	return m.compact(ctx, state, expected, opts, false)
}

func (m *Materializer) compact(ctx context.Context, state *SnapshotState, expected *CommittedHead, opts CompactionOptions, owned bool) (*Manifest, *CompactionResult, error) {
	manifest, result, err := m.prepareCompaction(ctx, state, expected, opts, owned)
	if err != nil || manifest == nil {
		return manifest, result, err
	}
	if err := m.publishManifest(ctx, manifest, expected); err != nil {
		return nil, nil, err
	}
	return manifest, result, nil
}

// prepareCompaction uploads the immutable objects for a candidate compacted
// state without making that state authoritative. The engine must revalidate
// its mutation version before publishing the prepared manifest.
func (m *Materializer) prepareCompaction(ctx context.Context, state *SnapshotState, expected *CommittedHead, opts CompactionOptions, owned bool) (*Manifest, *CompactionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if !m.Enabled() {
		return nil, nil, nil
	}
	inline := state
	if !owned {
		inline = cloneState(state)
	}
	normalizeState(inline)
	defaultSegmentVolumeIDs(inline, m.volumeID)
	expectedManifestSeq := uint64(0)
	if expected != nil {
		expectedManifestSeq = expected.ManifestSeq
	}
	if inline.NextSeq <= expectedManifestSeq+1 {
		inline.NextSeq = expectedManifestSeq + 2
	}
	nextSeq := checkpointSequence(inline)
	if nextSeq <= expectedManifestSeq {
		return nil, nil, fmt.Errorf("%w: compact manifest seq %d must advance beyond %d", ErrCommittedHeadConflict, nextSeq, expectedManifestSeq)
	}

	selected, result := planCompactionSegments(inline, opts)
	if len(selected) == 0 && !hasInlineSegments(inline) && !opts.Force {
		return nil, result, nil
	}
	commitID := uuid.NewString()
	manifestState, segments, err := buildCompactedState(ctx, m, nextSeq, commitID, m.volumeID, inline, selected, opts.SegmentTargetSize)
	if err != nil {
		return nil, nil, err
	}
	manifest := &Manifest{
		Version:       m.stateFormatVersion,
		VolumeID:      m.volumeID,
		ManifestSeq:   nextSeq,
		CheckpointSeq: checkpointSequence(inline),
		CommitID:      commitID,
		CreatedAt:     time.Now().UTC(),
		State:         manifestState,
	}
	if err := m.beginCommit(ctx, commitID, expected); err != nil {
		return nil, nil, err
	}
	keepIntent := false
	defer func() {
		if !keepIntent {
			_ = m.abortCommit(context.Background(), commitID)
		}
	}()
	for _, segment := range segments {
		if err := m.renewCommit(ctx, commitID); err != nil {
			return nil, nil, err
		}
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
	if err := finalizeManifest(manifest, expected); err != nil {
		return nil, nil, err
	}
	if err := m.renewCommit(ctx, commitID); err != nil {
		return nil, nil, err
	}
	if err := m.putManifest(ctx, manifestKey(nextSeq, commitID), manifest); err != nil {
		return nil, nil, err
	}
	keepIntent = true
	return manifest, result, nil
}

func (m *Materializer) publishManifest(ctx context.Context, manifest *Manifest, expected *CommittedHead) error {
	if manifest == nil {
		return fmt.Errorf("%w: manifest is required", ErrInvalidInput)
	}
	if m.headStore != nil {
		key := manifestKey(manifest.ManifestSeq, manifest.CommitID)
		head := committedHeadForManifest(manifest, key, expected)
		if err := m.renewCommit(ctx, manifest.CommitID); err != nil {
			return err
		}
		if err := m.headStore.CompareAndSwapCommittedHead(ctx, m.volumeID, expected, head); err != nil {
			_ = m.abortCommit(context.Background(), manifest.CommitID)
			return err
		}
	} else if err := m.putManifest(ctx, manifestLatestKey, manifest); err != nil {
		return err
	}
	return nil
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

func buildCompactedState(ctx context.Context, materializer *Materializer, manifestSeq uint64, commitID, volumeID string, state *SnapshotState, selected map[string]struct{}, targetSize uint64) (*SnapshotState, []*materializedSegment, error) {
	manifestState := &SnapshotState{
		NextSeq:   state.NextSeq,
		NextInode: state.NextInode,
		Nodes:     cloneNodeMap(state.Nodes),
		Children:  cloneChildrenMap(state.Children),
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent),
		Segments:  make(map[string]*Segment),
	}
	builder := newSegmentBuilder(manifestSeq, commitID, volumeID, targetSize)
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
