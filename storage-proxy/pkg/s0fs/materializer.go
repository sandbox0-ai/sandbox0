package s0fs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

const (
	manifestLatestKey = "manifests/latest.json"
	manifestDir       = "manifests"
	segmentDir        = "segments"
)

const (
	DefaultSegmentTargetSizeBytes uint64 = 4 << 20
	defaultSegmentCacheMaxBytes   int64  = 64 << 20
	defaultCommitIntentTTL               = 15 * time.Minute
)

var segmentCacheMaxBytes int64 = defaultSegmentCacheMaxBytes

var ErrMaterializedManifestNotFound = errors.New("materialized manifest not found")

type Manifest struct {
	Version              int            `json:"version"`
	VolumeID             string         `json:"volume_id"`
	ManifestSeq          uint64         `json:"manifest_seq"`
	CheckpointSeq        uint64         `json:"checkpoint_seq"`
	CommitID             string         `json:"commit_id,omitempty"`
	StateDigest          string         `json:"state_digest,omitempty"`
	ManifestDigest       string         `json:"manifest_digest,omitempty"`
	ParentManifestKey    string         `json:"parent_manifest_key,omitempty"`
	ParentManifestDigest string         `json:"parent_manifest_digest,omitempty"`
	ParentCommitID       string         `json:"parent_commit_id,omitempty"`
	ParentGeneration     uint64         `json:"parent_generation,omitempty"`
	CreatedAt            time.Time      `json:"created_at"`
	State                *SnapshotState `json:"state"`
}

type manifestCommitment struct {
	Version              int    `json:"version"`
	VolumeID             string `json:"volume_id"`
	ManifestSeq          uint64 `json:"manifest_seq"`
	CheckpointSeq        uint64 `json:"checkpoint_seq"`
	CommitID             string `json:"commit_id"`
	StateDigest          string `json:"state_digest"`
	ParentManifestKey    string `json:"parent_manifest_key"`
	ParentManifestDigest string `json:"parent_manifest_digest"`
	ParentCommitID       string `json:"parent_commit_id"`
	ParentGeneration     uint64 `json:"parent_generation"`
	CreatedAtUnixNano    int64  `json:"created_at_unix_nano"`
}

func finalizeManifest(manifest *Manifest, parent *CommittedHead) error {
	if manifest == nil || manifest.State == nil {
		return fmt.Errorf("%w: manifest state is required", ErrInvalidInput)
	}
	if strings.TrimSpace(manifest.CommitID) == "" {
		manifest.CommitID = uuid.NewString()
	}
	stateDigest, err := snapshotStateDigest(manifest.State)
	if err != nil {
		return err
	}
	if manifest.StateDigest != "" && manifest.StateDigest != stateDigest {
		return fmt.Errorf("%w: manifest state digest mismatch", ErrCommittedStateIntegrity)
	}
	manifest.StateDigest = stateDigest
	if parent != nil {
		manifest.ParentManifestKey = parent.ManifestKey
		manifest.ParentManifestDigest = parent.ManifestDigest
		manifest.ParentCommitID = parent.CommitID
		manifest.ParentGeneration = parent.Generation
	}
	manifest.ManifestDigest = calculateManifestDigest(manifest)
	return nil
}

func calculateManifestDigest(manifest *Manifest) string {
	if manifest == nil {
		return ""
	}
	payload, _ := json.Marshal(manifestCommitment{
		Version: manifest.Version, VolumeID: manifest.VolumeID, ManifestSeq: manifest.ManifestSeq,
		CheckpointSeq: manifest.CheckpointSeq, CommitID: manifest.CommitID, StateDigest: manifest.StateDigest,
		ParentManifestKey: manifest.ParentManifestKey, ParentManifestDigest: manifest.ParentManifestDigest,
		ParentCommitID: manifest.ParentCommitID, ParentGeneration: manifest.ParentGeneration,
		CreatedAtUnixNano: manifest.CreatedAt.UTC().UnixNano(),
	})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func committedHeadForManifest(manifest *Manifest, key string, parent *CommittedHead) *CommittedHead {
	generation := uint64(1)
	if parent != nil {
		generation = parent.Generation + 1
	}
	return &CommittedHead{
		VolumeID: manifest.VolumeID, ManifestSeq: manifest.ManifestSeq, CheckpointSeq: manifest.CheckpointSeq,
		ManifestKey: key, ManifestDigest: manifest.ManifestDigest, CommitID: manifest.CommitID,
		Generation: generation, UpdatedAt: manifest.CreatedAt,
	}
}

type Materializer struct {
	volumeID             string
	store                objectstore.Store
	objectStoreForVolume ObjectStoreResolver
	headStore            HeadStore
	encryption           *EncryptionConfig
	cache                *segmentCache
	segmentTargetSize    uint64
	openObserver         OpenObserver
	stateFormatVersion   int
}

func NewMaterializer(volumeID string, store objectstore.Store, headStore HeadStore, resolvers ...ObjectStoreResolver) *Materializer {
	if store == nil {
		return nil
	}
	var resolver ObjectStoreResolver
	if len(resolvers) > 0 {
		resolver = resolvers[0]
	}
	return &Materializer{
		volumeID:             volumeID,
		store:                store,
		objectStoreForVolume: resolver,
		headStore:            headStore,
		cache:                newSegmentCache(segmentCacheMaxBytes),
		segmentTargetSize:    DefaultSegmentTargetSizeBytes,
		stateFormatVersion:   StateFormatV1,
	}
}

func (m *Materializer) SetEncryption(encryption *EncryptionConfig) {
	if m != nil {
		m.encryption = encryption
	}
}

func (m *Materializer) SetSegmentTargetSize(size uint64) {
	if m == nil {
		return
	}
	if size == 0 {
		size = DefaultSegmentTargetSizeBytes
	}
	m.segmentTargetSize = size
}

func (m *Materializer) SetOpenObserver(observer OpenObserver) {
	if m != nil {
		m.openObserver = observer
	}
}

func (m *Materializer) SetStateFormatVersion(version int) {
	if m != nil {
		m.stateFormatVersion = normalizedStateFormatVersion(version)
	}
}

func (m *Materializer) Enabled() bool {
	return m != nil && m.store != nil
}

func (m *Materializer) estimatedCacheMemoryBytes() int64 {
	if m == nil {
		return 0
	}
	return m.cache.estimatedMemoryBytes()
}

func (m *Materializer) loadCommittedHead(ctx context.Context) (*CommittedHead, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m == nil || m.headStore == nil {
		return nil, ErrCommittedHeadNotFound
	}
	started := time.Now()
	head, err := m.headStore.LoadCommittedHead(ctx, m.volumeID)
	m.observeOpenPhase("committed_head", "remote", 0, started, -1, nil, err)
	return head, err
}

func (m *Materializer) Materialize(ctx context.Context, state *SnapshotState, expectedManifestSeq uint64) (*Manifest, error) {
	expected, err := m.expectedHeadForSequence(ctx, expectedManifestSeq)
	if err != nil {
		return nil, err
	}
	return m.materialize(ctx, state, expected, false)
}

func (m *Materializer) materializeOwned(ctx context.Context, state *SnapshotState, expected *CommittedHead) (*Manifest, error) {
	return m.materialize(ctx, state, expected, true)
}

func (m *Materializer) materialize(ctx context.Context, state *SnapshotState, expected *CommittedHead, owned bool) (*Manifest, error) {
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

	inline := state
	if !owned {
		inline = cloneStateForMaterialization(state)
	}
	normalizeState(inline)
	defaultSegmentVolumeIDs(inline, m.volumeID)
	ensureMaterializableSequence(inline)

	nextSeq := checkpointSequence(inline)
	expectedManifestSeq := uint64(0)
	if expected != nil {
		expectedManifestSeq = expected.ManifestSeq
	}
	if nextSeq == 0 {
		return nil, fmt.Errorf("%w: manifest sequence must be non-zero", ErrInvalidInput)
	}
	if nextSeq <= expectedManifestSeq {
		return nil, fmt.Errorf("%w: manifest seq %d must advance beyond %d", ErrCommittedHeadConflict, nextSeq, expectedManifestSeq)
	}

	commitID := uuid.NewString()
	manifestState, segments, err := buildMaterializedState(nextSeq, commitID, m.volumeID, inline, m.segmentTargetSize)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer func() { _ = m.abortCommit(context.Background(), commitID) }()

	for _, segment := range segments {
		if err := m.renewCommit(ctx, commitID); err != nil {
			return nil, err
		}
		storedSegmentPayload, segmentEncryption, err := m.encryption.encryptSegment(m.volumeID, segment)
		if err != nil {
			return nil, err
		}
		segment.Encryption = segmentEncryption
		if meta := manifest.State.Segments[segment.ID]; meta != nil {
			meta.Encryption = segmentEncryption
		}
		if err := m.putBytes(ctx, segment.Key, storedSegmentPayload); err != nil {
			return nil, err
		}
		m.cache.put(segmentCacheKey(segment.VolumeID, segment.Key), segment.Payload)
	}
	if err := finalizeManifest(manifest, expected); err != nil {
		return nil, err
	}
	key := manifestKey(nextSeq, commitID)
	if err := m.renewCommit(ctx, commitID); err != nil {
		return nil, err
	}
	if err := m.putManifest(ctx, key, manifest); err != nil {
		return nil, err
	}
	if m.headStore != nil {
		head := committedHeadForManifest(manifest, key, expected)
		if err := m.renewCommit(ctx, commitID); err != nil {
			return nil, err
		}
		if err := m.headStore.CompareAndSwapCommittedHead(ctx, m.volumeID, expected, head); err != nil {
			return nil, err
		}
	} else if err := m.putManifest(ctx, manifestLatestKey, manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (m *Materializer) beginCommit(ctx context.Context, commitID string, expected *CommittedHead) error {
	coordinator, ok := m.headStore.(CommitCoordinator)
	if !ok || coordinator == nil {
		return nil
	}
	return coordinator.BeginCommit(ctx, m.volumeID, commitID, expected, time.Now().UTC().Add(defaultCommitIntentTTL))
}

func (m *Materializer) renewCommit(ctx context.Context, commitID string) error {
	coordinator, ok := m.headStore.(CommitCoordinator)
	if !ok || coordinator == nil {
		return nil
	}
	return coordinator.RenewCommit(ctx, m.volumeID, commitID, time.Now().UTC().Add(defaultCommitIntentTTL))
}

func (m *Materializer) abortCommit(ctx context.Context, commitID string) error {
	coordinator, ok := m.headStore.(CommitCoordinator)
	if !ok || coordinator == nil || strings.TrimSpace(commitID) == "" {
		return nil
	}
	return coordinator.AbortCommit(ctx, m.volumeID, commitID)
}

func (m *Materializer) expectedHeadForSequence(ctx context.Context, expectedManifestSeq uint64) (*CommittedHead, error) {
	if m == nil || m.headStore == nil {
		if expectedManifestSeq == 0 {
			return nil, nil
		}
		return &CommittedHead{VolumeID: m.volumeID, ManifestSeq: expectedManifestSeq, CheckpointSeq: expectedManifestSeq}, nil
	}
	head, err := m.loadCommittedHead(ctx)
	if errors.Is(err, ErrCommittedHeadNotFound) {
		if expectedManifestSeq == 0 {
			return nil, nil
		}
		return nil, ErrCommittedHeadConflict
	}
	if err != nil {
		return nil, err
	}
	if head.ManifestSeq != expectedManifestSeq {
		return nil, ErrCommittedHeadConflict
	}
	return head, nil
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
	volumeID, store, err := m.storeForSegment(segment)
	if err != nil {
		return nil, err
	}
	cacheKey := segmentCacheKey(volumeID, segment.Key)
	if payload, ok := m.cache.get(cacheKey); ok {
		return cloneByteRange(payload, off, limit), nil
	}
	if segment.Length >= uint64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: segment %s length is too large", ErrCommittedStateIntegrity, segment.ID)
	}
	var payload []byte
	if segment.Encryption != nil {
		payload, err = m.encryption.decryptSegmentRange(store, volumeID, segment, 0, int64(segment.Length))
	} else {
		var reader io.ReadCloser
		reader, err = store.Get(segment.Key, 0, int64(segment.Length))
		if err == nil {
			payload, err = io.ReadAll(io.LimitReader(reader, int64(segment.Length)+1))
			closeErr := reader.Close()
			if err == nil {
				err = closeErr
			}
		}
	}
	if err != nil {
		if objectstore.IsNotFound(err) {
			return nil, fmt.Errorf("%w: segment %s is missing: %w", ErrCommittedStateIntegrity, segment.Key, err)
		}
		return nil, err
	}
	if err := verifySegmentPayload(segment, payload); err != nil {
		return nil, err
	}
	if int64(len(payload)) <= segmentCacheMaxBytes {
		m.cache.put(cacheKey, payload)
	}
	return cloneByteRange(payload, off, limit), nil
}

func verifySegmentPayload(segment *Segment, payload []byte) error {
	if segment == nil || uint64(len(payload)) != segment.Length {
		return fmt.Errorf("%w: segment length mismatch", ErrCommittedStateIntegrity)
	}
	if segment.SHA256 == "" {
		return nil
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != strings.ToLower(segment.SHA256) {
		return fmt.Errorf("%w: segment %s checksum mismatch", ErrCommittedStateIntegrity, segment.Key)
	}
	return nil
}

func (m *Materializer) LoadLatestManifest(ctx context.Context) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, ErrMaterializedManifestNotFound
	}
	if m.headStore != nil {
		head, err := m.loadCommittedHead(ctx)
		switch {
		case err == nil:
			manifest, loadErr := m.loadManifestByKey(ctx, head.ManifestKey)
			if loadErr != nil {
				if errors.Is(loadErr, ErrMaterializedManifestNotFound) || objectstore.IsNotFound(loadErr) {
					return nil, fmt.Errorf("%w: committed manifest %s is missing", ErrCommittedStateIntegrity, head.ManifestKey)
				}
				return nil, loadErr
			}
			if validateErr := validateManifestHead(head, manifest); validateErr != nil {
				return nil, validateErr
			}
			if validateErr := m.validateRecoveryManifest(head.ManifestKey, head.ManifestSeq, manifest); validateErr != nil {
				return nil, fmt.Errorf("%w: %w", ErrCommittedStateIntegrity, validateErr)
			}
			return manifest, nil
		case !errors.Is(err, ErrCommittedHeadNotFound):
			return nil, err
		}

		manifest, err := m.loadLegacyLatestManifest(ctx)
		if err != nil {
			return nil, err
		}
		head = committedHeadForManifest(manifest, manifestKey(manifest.ManifestSeq), nil)
		if err := m.headStore.CompareAndSwapCommittedHead(ctx, m.volumeID, nil, head); err != nil {
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
	state, manifest, err := m.loadLatestStateOwned(ctx)
	if err != nil {
		return nil, nil, err
	}
	started := time.Now()
	state = cloneState(state)
	format := StateFormatV1
	if manifest.Version == StateFormatV2 {
		format = StateFormatV2
	}
	m.observeOpenPhase("state_clone", "remote", format, started, -1, state, nil)
	return state, manifest, nil
}

func (m *Materializer) loadLatestStateOwned(ctx context.Context) (*SnapshotState, *Manifest, error) {
	manifest, err := m.LoadLatestManifest(ctx)
	if err != nil {
		return nil, nil, err
	}
	return manifest.State, manifest, nil
}

func (m *Materializer) loadLatestEngineState(ctx context.Context, metadataPath string, metadataCacheBytes int64) (*loadedEngineState, *Manifest, error) {
	if strings.TrimSpace(metadataPath) == "" {
		state, manifest, err := m.loadLatestStateOwned(ctx)
		return loadedEngineStateFromSnapshot(state), manifest, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	key := manifestLatestKey
	var committed *CommittedHead
	if m.headStore != nil {
		head, err := m.loadCommittedHead(ctx)
		switch {
		case err == nil:
			committed = head
			key = head.ManifestKey
		case !errors.Is(err, ErrCommittedHeadNotFound):
			return nil, nil, err
		}
	}
	state, manifest, err := m.loadManifestEngineStateByKey(ctx, key, metadataPath, metadataCacheBytes)
	if err != nil {
		if committed != nil && errors.Is(err, ErrInvalidInput) {
			return nil, nil, fmt.Errorf("%w: %w", ErrCommittedStateIntegrity, err)
		}
		return nil, nil, err
	}
	if committed != nil {
		if manifest.State != nil {
			if validateErr := m.validateRecoveryManifest(key, committed.ManifestSeq, manifest); validateErr != nil {
				state.close()
				return nil, nil, fmt.Errorf("%w: %w", ErrCommittedStateIntegrity, validateErr)
			}
		}
		if !committedHeadMatchesCheckpoint(committed, m.volumeID, state.checkpointSequence()) || manifest.ManifestSeq != committed.ManifestSeq {
			state.close()
			return nil, nil, fmt.Errorf("%w: committed head does not match manifest state", ErrCommittedStateIntegrity)
		}
		if validateErr := validateManifestHead(committed, manifest); validateErr != nil {
			state.close()
			return nil, nil, validateErr
		}
		return state, manifest, nil
	}
	if m.headStore != nil {
		head := committedHeadForManifest(manifest, manifestKey(manifest.ManifestSeq), nil)
		if err := m.headStore.CompareAndSwapCommittedHead(ctx, m.volumeID, nil, head); err != nil {
			state.close()
			if errors.Is(err, ErrCommittedHeadConflict) {
				return m.loadLatestEngineState(ctx, metadataPath, metadataCacheBytes)
			}
			return nil, nil, err
		}
	}
	return state, manifest, nil
}

func (m *Materializer) loadManifestEngineStateByKey(ctx context.Context, key, metadataPath string, metadataCacheBytes int64) (*loadedEngineState, *Manifest, error) {
	if strings.TrimSpace(key) == "" {
		return nil, nil, ErrMaterializedManifestNotFound
	}
	reader, err := m.store.Get(key, 0, -1)
	if err != nil {
		if objectstore.IsNotFound(err) {
			return nil, nil, ErrMaterializedManifestNotFound
		}
		return nil, nil, fmt.Errorf("get %s: %w", key, err)
	}
	buffered := bufio.NewReader(&contextReader{ctx: ctx, reader: reader})
	v2, err := hasStateV2Magic(buffered)
	if err != nil {
		_ = reader.Close()
		return nil, nil, fmt.Errorf("inspect %s: %w", key, err)
	}
	if !v2 {
		_ = reader.Close()
		manifest, err := m.loadManifestByKey(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		return loadedEngineStateFromSnapshot(manifest.State), manifest, nil
	}
	metadata, stream, decodeErr := newSQLiteMetadataStoreFromStateV2(
		ctx,
		metadataPath,
		buffered,
		m.volumeID,
		stateBlobAAD(m.volumeID, "object:"+key),
		StateV2Role_STATE_V2_ROLE_MANIFEST,
		m.encryption,
		metadataCacheBytes,
	)
	closeErr := reader.Close()
	if decodeErr != nil {
		return nil, nil, fmt.Errorf("decode %s: %w", key, decodeErr)
	}
	if closeErr != nil {
		_ = metadata.Close()
		return nil, nil, closeErr
	}
	state := &loadedEngineState{
		metadata: metadata, nextSeq: stream.Header.NextSeq, nextInode: stream.Header.NextInode,
		metadataPath: metadataPath, stateDigest: stream.Metadata.StateDigest,
	}
	manifest := &Manifest{
		Version:              StateFormatV2,
		VolumeID:             m.volumeID,
		ManifestSeq:          stream.Metadata.ManifestSeq,
		CheckpointSeq:        stream.Metadata.CheckpointSeq,
		CreatedAt:            stream.Metadata.CreatedAt,
		CommitID:             stream.Metadata.CommitID,
		StateDigest:          stream.Metadata.StateDigest,
		ManifestDigest:       stream.Metadata.ManifestDigest,
		ParentManifestKey:    stream.Metadata.ParentManifestKey,
		ParentManifestDigest: stream.Metadata.ParentManifestDigest,
		ParentCommitID:       stream.Metadata.ParentCommitID,
		ParentGeneration:     stream.Metadata.ParentGeneration,
	}
	if err := hydrateAndValidateManifest(manifest); err != nil {
		state.close()
		return nil, nil, err
	}
	root, ok := metadata.Node(RootInode)
	if stream.Header.NextSeq == 0 || stream.Header.NextSeq-1 != manifest.ManifestSeq || manifest.CheckpointSeq != manifest.ManifestSeq || !ok || root.Type != TypeDirectory {
		state.close()
		return nil, nil, fmt.Errorf("%w: manifest %s has inconsistent state", ErrInvalidInput, key)
	}
	return state, manifest, nil
}

func (m *Materializer) persistSnapshot(ctx context.Context, snapshotID string, state *SnapshotState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.Enabled() {
		return fmt.Errorf("%w: materializer is not configured", ErrInvalidInput)
	}
	if err := validateSnapshotID(snapshotID); err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("%w: snapshot state is required", ErrInvalidInput)
	}
	return m.putSnapshotState(ctx, snapshotObjectKey(snapshotID), state)
}

func (m *Materializer) loadSnapshot(ctx context.Context, snapshotID string) (*SnapshotState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, ErrSnapshotNotFound
	}
	if err := validateSnapshotID(snapshotID); err != nil {
		return nil, err
	}
	state, err := m.getSnapshotState(ctx, snapshotObjectKey(snapshotID))
	if err != nil {
		if objectstore.IsNotFound(err) {
			return nil, ErrSnapshotNotFound
		}
		return nil, fmt.Errorf("load snapshot object %s: %w", snapshotID, err)
	}
	normalizeState(state)
	defaultSegmentVolumeIDs(state, m.volumeID)
	return state, nil
}

func (m *Materializer) deleteSnapshot(ctx context.Context, snapshotID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.Enabled() {
		return nil
	}
	if err := validateSnapshotID(snapshotID); err != nil {
		return err
	}
	if err := m.store.Delete(snapshotObjectKey(snapshotID)); err != nil && !objectstore.IsNotFound(err) {
		return fmt.Errorf("delete snapshot object %s: %w", snapshotID, err)
	}
	return nil
}

func (m *Materializer) loadUniqueManifestAtOrBefore(ctx context.Context, cutoff time.Time, expectedSizeBytes int64) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, ErrMaterializedManifestNotFound
	}
	if cutoff.IsZero() {
		return nil, fmt.Errorf("%w: manifest cutoff is required", ErrInvalidInput)
	}
	if expectedSizeBytes < 0 {
		return nil, fmt.Errorf("%w: expected snapshot size must be non-negative", ErrInvalidInput)
	}
	headBefore, err := m.loadRecoveryHead(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := listObjectKeys(ctx, m.store, manifestDir+"/")
	if err != nil {
		return nil, err
	}

	var candidates []*Manifest
	for _, key := range keys {
		keySequence, ok := immutableManifestSequence(key)
		if !ok || keySequence > headBefore.ManifestSeq {
			continue
		}
		manifest, err := m.loadManifestByKey(ctx, key)
		if err != nil {
			return nil, err
		}
		if err := m.validateRecoveryManifest(key, keySequence, manifest); err != nil {
			return nil, err
		}
		if manifest.CreatedAt.After(cutoff) || StateStorageBytes(manifest.State) != expectedSizeBytes {
			continue
		}
		candidates = append(candidates, manifest)
	}
	if len(candidates) == 0 {
		return nil, ErrMaterializedManifestNotFound
	}
	if len(candidates) != 1 {
		return nil, fmt.Errorf("%w: %d immutable manifests match legacy snapshot metadata", ErrInvalidInput, len(candidates))
	}
	if err := m.validateRecoverySegments(ctx, candidates[0].State); err != nil {
		return nil, err
	}
	headAfter, err := m.loadRecoveryHead(ctx)
	if err != nil {
		return nil, err
	}
	if !sameCommittedHeadIdentity(headBefore, headAfter) {
		return nil, fmt.Errorf("%w: committed head changed during legacy snapshot recovery", ErrCommittedHeadConflict)
	}
	return candidates[0], nil
}

func (m *Materializer) loadRecoveryHead(ctx context.Context) (*CommittedHead, error) {
	if m.headStore != nil {
		head, err := m.headStore.LoadCommittedHead(ctx, m.volumeID)
		if err != nil {
			if errors.Is(err, ErrCommittedHeadNotFound) {
				return nil, ErrMaterializedManifestNotFound
			}
			return nil, err
		}
		if head == nil || head.VolumeID != m.volumeID || head.ManifestSeq == 0 ||
			head.CheckpointSeq != head.ManifestSeq || strings.TrimSpace(head.ManifestKey) == "" {
			return nil, fmt.Errorf("%w: invalid committed head for volume %s", ErrInvalidInput, m.volumeID)
		}
		copy := *head
		return &copy, nil
	}

	manifest, err := m.loadLegacyLatestManifest(ctx)
	if err != nil {
		return nil, err
	}
	if err := m.validateRecoveryManifest(manifestKey(manifest.ManifestSeq), manifest.ManifestSeq, manifest); err != nil {
		return nil, err
	}
	return committedHeadForManifest(manifest, manifestKey(manifest.ManifestSeq), nil), nil
}

func (m *Materializer) validateRecoveryManifest(key string, keySequence uint64, manifest *Manifest) error {
	if manifest == nil || manifest.State == nil {
		return fmt.Errorf("%w: manifest %s has no state", ErrInvalidInput, key)
	}
	if manifest.Version != StateFormatV1 && manifest.Version != StateFormatV2 {
		return fmt.Errorf("%w: manifest %s has unsupported version %d", ErrInvalidInput, key, manifest.Version)
	}
	if manifest.VolumeID != m.volumeID {
		return fmt.Errorf("%w: manifest %s belongs to volume %s", ErrInvalidInput, key, manifest.VolumeID)
	}
	if manifest.ManifestSeq != keySequence || manifest.CheckpointSeq != keySequence || checkpointSequence(manifest.State) != keySequence {
		return fmt.Errorf("%w: manifest %s has inconsistent sequence metadata", ErrInvalidInput, key)
	}
	if manifest.CreatedAt.IsZero() {
		return fmt.Errorf("%w: manifest %s has no creation time", ErrInvalidInput, key)
	}
	root := manifest.State.Nodes[RootInode]
	if root == nil || root.Type != TypeDirectory {
		return fmt.Errorf("%w: manifest %s has no valid root inode", ErrInvalidInput, key)
	}
	return nil
}

func (m *Materializer) validateRecoverySegments(ctx context.Context, state *SnapshotState) error {
	seen := make(map[string]struct{})
	for _, extents := range state.ColdFiles {
		for _, extent := range extents {
			if err := ctx.Err(); err != nil {
				return err
			}
			if extent.SegmentID == "" {
				continue
			}
			segment := state.Segments[extent.SegmentID]
			if segment == nil || segment.ID != extent.SegmentID {
				return fmt.Errorf("%w: recovery state is missing segment %s", ErrInvalidInput, extent.SegmentID)
			}
			if extent.Offset > segment.Length || extent.Length > segment.Length-extent.Offset {
				return fmt.Errorf("%w: recovery extent exceeds segment %s", ErrInvalidInput, extent.SegmentID)
			}
			if _, ok := seen[extent.SegmentID]; ok {
				continue
			}
			seen[extent.SegmentID] = struct{}{}
			if isInlineSegment(segment) {
				continue
			}
			if strings.TrimSpace(segment.Key) == "" {
				return fmt.Errorf("%w: recovery segment %s has no object key", ErrInvalidInput, segment.ID)
			}
			_, store, err := m.storeForSegment(segment)
			if err != nil {
				return err
			}
			if _, err := store.Head(segment.Key); err != nil {
				return fmt.Errorf("validate recovery segment %s: %w", segment.ID, err)
			}
		}
	}
	return nil
}

func (m *Materializer) validateCommittedStateSegments(ctx context.Context, state *SnapshotState) error {
	if state == nil {
		return fmt.Errorf("%w: state is required", ErrCommittedStateIntegrity)
	}
	if err := m.validateRecoverySegments(ctx, state); err != nil {
		return classifyCommittedSegmentValidationError(err)
	}
	return nil
}

func (m *Materializer) validateCommittedLoadedStateSegments(ctx context.Context, state *loadedEngineState) error {
	if state == nil {
		return fmt.Errorf("%w: state is required", ErrCommittedStateIntegrity)
	}
	if state.state != nil {
		return m.validateCommittedStateSegments(ctx, state.state)
	}
	sqliteState, ok := state.metadata.(*sqliteMetadataStore)
	if !ok || sqliteState == nil {
		return fmt.Errorf("%w: state metadata is required", ErrCommittedStateIntegrity)
	}
	if err := sqliteState.validateSegments(ctx, m); err != nil {
		return classifyCommittedSegmentValidationError(err)
	}
	return nil
}

func classifyCommittedSegmentValidationError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrInvalidInput) || errors.Is(err, ErrCommittedStateIntegrity) || objectstore.IsNotFound(err) {
		return fmt.Errorf("%w: %w", ErrCommittedStateIntegrity, err)
	}
	// Provider transport errors and 5xx responses are availability failures,
	// not proof that an immutable committed object is missing or corrupt.
	return err
}

type materializedSegment struct {
	ID         string
	VolumeID   string
	Key        string
	Payload    []byte
	SHA256     string
	Encryption *SegmentEncryption
}

func buildMaterializedState(manifestSeq uint64, commitID, volumeID string, state *SnapshotState, targetSize uint64) (*SnapshotState, []*materializedSegment, error) {
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
	inodes := make([]uint64, 0, len(state.Data)+len(state.ColdFiles))
	seen := make(map[uint64]struct{}, len(state.Data)+len(state.ColdFiles))
	for inode := range state.Data {
		seen[inode] = struct{}{}
		inodes = append(inodes, inode)
	}
	for inode, extents := range state.ColdFiles {
		if len(extents) == 0 {
			continue
		}
		if _, ok := seen[inode]; !ok {
			seen[inode] = struct{}{}
			inodes = append(inodes, inode)
		}
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })

	for _, inode := range inodes {
		node := state.Nodes[inode]
		if node == nil || node.Type == TypeDirectory {
			continue
		}
		var fileExtents []FileExtent
		if payload, ok := state.Data[inode]; ok && len(payload) > 0 {
			extents, err := builder.append(payload)
			if err != nil {
				return nil, nil, err
			}
			fileExtents = append(fileExtents, extents...)
		} else {
			extents, err := materializeFileExtents(builder, state, manifestState, inode)
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

func materializeFileExtents(builder *segmentBuilder, state, manifestState *SnapshotState, inode uint64) ([]FileExtent, error) {
	var out []FileExtent
	for _, extent := range state.ColdFiles[inode] {
		if extent.Length == 0 {
			continue
		}
		if extent.SegmentID == "" {
			out = append(out, extent)
			continue
		}
		existing := state.Segments[extent.SegmentID]
		if existing == nil {
			return nil, fmt.Errorf("%w: missing retained segment %s", ErrInvalidInput, extent.SegmentID)
		}
		if isInlineSegment(existing) {
			payload, err := inlineSegmentRange(existing, extent.Offset, extent.Length)
			if err != nil {
				return nil, err
			}
			extents, err := builder.append(payload)
			if err != nil {
				return nil, err
			}
			out = append(out, extents...)
			continue
		}
		out = append(out, extent)
		manifestState.Segments[extent.SegmentID] = cloneSegment(existing)
	}
	return out, nil
}

type segmentBuilder struct {
	manifestSeq uint64
	commitID    string
	volumeID    string
	targetSize  uint64
	nextIndex   int
	current     *materializedSegment
	segments    []*materializedSegment
}

func newSegmentBuilder(manifestSeq uint64, commitID, volumeID string, targetSize uint64) *segmentBuilder {
	if targetSize == 0 {
		targetSize = DefaultSegmentTargetSizeBytes
	}
	return &segmentBuilder{
		manifestSeq: manifestSeq,
		commitID:    commitID,
		volumeID:    volumeID,
		targetSize:  targetSize,
	}
}

func (b *segmentBuilder) append(payload []byte) ([]FileExtent, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var out []FileExtent
	for len(payload) > 0 {
		segment := b.ensureCurrent()
		space := b.targetSize - uint64(len(segment.Payload))
		if space == 0 {
			b.current = nil
			continue
		}
		n := len(payload)
		if uint64(n) > space {
			n = int(space)
		}
		offset := uint64(len(segment.Payload))
		segment.Payload = append(segment.Payload, payload[:n]...)
		out = append(out, FileExtent{
			SegmentID: segment.ID,
			Offset:    offset,
			Length:    uint64(n),
		})
		payload = payload[n:]
	}
	return out, nil
}

func (b *segmentBuilder) ensureCurrent() *materializedSegment {
	if b.current != nil && uint64(len(b.current.Payload)) < b.targetSize {
		return b.current
	}
	segmentID := fmt.Sprintf("%s-%d", b.commitID, b.nextIndex)
	if b.commitID == "" {
		segmentID = fmt.Sprintf("%020d-%d", b.manifestSeq, b.nextIndex)
	}
	b.nextIndex++
	b.current = &materializedSegment{
		ID:       segmentID,
		VolumeID: b.volumeID,
		Key:      fmt.Sprintf("%s/%s.bin", segmentDir, segmentID),
		Payload:  make([]byte, 0, int(b.targetSize)),
	}
	b.segments = append(b.segments, b.current)
	return b.current
}

func (b *segmentBuilder) finish() []*materializedSegment {
	for _, segment := range b.segments {
		sum := sha256.Sum256(segment.Payload)
		segment.SHA256 = hex.EncodeToString(sum[:])
	}
	return b.segments
}

func manifestKey(seq uint64, commitIDs ...string) string {
	if len(commitIDs) > 0 && strings.TrimSpace(commitIDs[0]) != "" {
		return fmt.Sprintf("%s/%020d-%s.json", manifestDir, seq, strings.TrimSpace(commitIDs[0]))
	}
	return fmt.Sprintf("%s/%020d.json", manifestDir, seq)
}

func snapshotObjectKey(snapshotID string) string {
	return fmt.Sprintf("snapshots/%s.json", snapshotID)
}

func immutableManifestSequence(key string) (uint64, bool) {
	const suffix = ".json"
	name := strings.TrimPrefix(key, manifestDir+"/")
	if name == key || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	base := strings.TrimSuffix(name, suffix)
	digits := base
	if separator := strings.IndexByte(base, '-'); separator >= 0 {
		digits = base[:separator]
	}
	if len(digits) != 20 {
		return 0, false
	}
	sequence, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return sequence, true
}

func checkpointSequence(state *SnapshotState) uint64 {
	if state == nil || state.NextSeq == 0 {
		return 0
	}
	return state.NextSeq - 1
}

func ensureMaterializableSequence(state *SnapshotState) {
	if state != nil && state.NextSeq <= 1 {
		state.NextSeq = 2
	}
}

func (m *Materializer) putManifest(ctx context.Context, key string, manifest *Manifest) error {
	if manifest == nil || manifest.State == nil {
		return fmt.Errorf("%w: manifest state is required", ErrInvalidInput)
	}
	if normalizedStateFormatVersion(manifest.Version) != StateFormatV2 {
		return m.putJSON(ctx, key, manifest)
	}
	return m.putMetadataStateV2(ctx, key, newEagerMetadataStore(manifest.State), manifest.State.NextSeq, manifest.State.NextInode, stateV2Metadata{
		Role: StateV2Role_STATE_V2_ROLE_MANIFEST, ManifestSeq: manifest.ManifestSeq,
		CheckpointSeq: manifest.CheckpointSeq, CreatedAt: manifest.CreatedAt,
		CommitID: manifest.CommitID, StateDigest: manifest.StateDigest, ManifestDigest: manifest.ManifestDigest,
		ParentManifestKey: manifest.ParentManifestKey, ParentManifestDigest: manifest.ParentManifestDigest,
		ParentCommitID: manifest.ParentCommitID, ParentGeneration: manifest.ParentGeneration,
	})
}

func (m *Materializer) getManifest(ctx context.Context, key string) (*Manifest, error) {
	result, legacy, err := m.readStateObject(ctx, key, StateV2Role_STATE_V2_ROLE_MANIFEST)
	if err != nil {
		return nil, err
	}
	if result != nil {
		manifest := &Manifest{
			Version:              StateFormatV2,
			VolumeID:             m.volumeID,
			ManifestSeq:          result.Metadata.ManifestSeq,
			CheckpointSeq:        result.Metadata.CheckpointSeq,
			CreatedAt:            result.Metadata.CreatedAt,
			CommitID:             result.Metadata.CommitID,
			StateDigest:          result.Metadata.StateDigest,
			ManifestDigest:       result.Metadata.ManifestDigest,
			ParentManifestKey:    result.Metadata.ParentManifestKey,
			ParentManifestDigest: result.Metadata.ParentManifestDigest,
			ParentCommitID:       result.Metadata.ParentCommitID,
			ParentGeneration:     result.Metadata.ParentGeneration,
			State:                result.State,
		}
		if err := hydrateAndValidateManifest(manifest); err != nil {
			return nil, err
		}
		return manifest, nil
	}
	started := time.Now()
	var manifest Manifest
	if err := json.Unmarshal(legacy, &manifest); err != nil {
		m.observeOpenPhase("decode", "remote", StateFormatV1, started, int64(len(legacy)), nil, err)
		return nil, fmt.Errorf("decode %s: %w", key, err)
	}
	if err := hydrateAndValidateManifest(&manifest); err != nil {
		return nil, err
	}
	m.observeOpenPhase("decode", "remote", StateFormatV1, started, int64(len(legacy)), manifest.State, nil)
	return &manifest, nil
}

func hydrateAndValidateManifest(manifest *Manifest) error {
	if manifest == nil {
		return fmt.Errorf("%w: manifest is required", ErrCommittedStateIntegrity)
	}
	if manifest.State != nil {
		stateDigest, err := snapshotStateDigest(manifest.State)
		if err != nil {
			return err
		}
		if manifest.StateDigest != "" && manifest.StateDigest != stateDigest {
			return fmt.Errorf("%w: manifest state digest mismatch", ErrCommittedStateIntegrity)
		}
		manifest.StateDigest = stateDigest
	}
	calculated := calculateManifestDigest(manifest)
	if manifest.ManifestDigest != "" && manifest.ManifestDigest != calculated {
		return fmt.Errorf("%w: manifest digest mismatch", ErrCommittedStateIntegrity)
	}
	manifest.ManifestDigest = calculated
	return nil
}

func validateManifestHead(head *CommittedHead, manifest *Manifest) error {
	if head == nil || manifest == nil || head.VolumeID != manifest.VolumeID ||
		head.ManifestSeq != manifest.ManifestSeq || head.CheckpointSeq != manifest.CheckpointSeq {
		return fmt.Errorf("%w: committed head does not identify the loaded manifest", ErrCommittedStateIntegrity)
	}
	if head.ManifestDigest != "" && head.ManifestDigest != manifest.ManifestDigest {
		return fmt.Errorf("%w: committed manifest digest mismatch", ErrCommittedStateIntegrity)
	}
	if head.CommitID != "" && head.CommitID != manifest.CommitID {
		return fmt.Errorf("%w: committed manifest id mismatch", ErrCommittedStateIntegrity)
	}
	return nil
}

func (m *Materializer) putSnapshotState(ctx context.Context, key string, state *SnapshotState) error {
	if m.stateFormatVersion != StateFormatV2 {
		return m.putJSON(ctx, key, state)
	}
	return m.putMetadataStateV2(ctx, key, newEagerMetadataStore(state), state.NextSeq, state.NextInode, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_SNAPSHOT})
}

func (m *Materializer) putMetadataStateV2(ctx context.Context, key string, metadata metadataStore, nextSeq, nextInode uint64, stateMetadata stateV2Metadata) error {
	file, err := os.CreateTemp("", "s0fs-state-v2-*.object")
	if err != nil {
		return fmt.Errorf("create %s spool: %w", key, err)
	}
	path := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(path)
	}()
	if err := writeMetadataStateV2(ctx, file, m.volumeID, stateBlobAAD(m.volumeID, "object:"+key), metadata, nextSeq, nextInode, stateMetadata, m.encryption); err != nil {
		return fmt.Errorf("encode %s: %w", key, err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := m.store.Put(key, file); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (m *Materializer) getSnapshotState(ctx context.Context, key string) (*SnapshotState, error) {
	result, legacy, err := m.readStateObject(ctx, key, StateV2Role_STATE_V2_ROLE_SNAPSHOT)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result.State, nil
	}
	started := time.Now()
	var state SnapshotState
	if err := json.Unmarshal(legacy, &state); err != nil {
		m.observeOpenPhase("decode", "remote", StateFormatV1, started, int64(len(legacy)), nil, err)
		return nil, fmt.Errorf("decode %s: %w", key, err)
	}
	m.observeOpenPhase("decode", "remote", StateFormatV1, started, int64(len(legacy)), &state, nil)
	return &state, nil
}

// readStateObject returns either a decoded v2 result or decrypted legacy JSON.
func (m *Materializer) readStateObject(ctx context.Context, key string, role StateV2Role) (*stateV2DecodeResult, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	started := time.Now()
	reader, err := m.store.Get(key, 0, -1)
	if err != nil {
		m.observeOpenPhase("object_read", "remote", 0, started, -1, nil, err)
		return nil, nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer reader.Close()
	buffered := bufio.NewReader(reader)
	v2, err := hasStateV2Magic(buffered)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect %s: %w", key, err)
	}
	if v2 {
		result, err := decodeStateV2Context(ctx, buffered, m.volumeID, stateBlobAAD(m.volumeID, "object:"+key), role, m.encryption)
		bytesRead := int64(-1)
		var state *SnapshotState
		if result != nil {
			bytesRead = result.Bytes
			state = result.State
		}
		m.observeOpenPhase("object_read_decode", "remote", StateFormatV2, started, bytesRead, state, err)
		if err != nil {
			return nil, nil, err
		}
		return result, nil, nil
	}
	payload, err := io.ReadAll(&contextReader{ctx: ctx, reader: buffered})
	if err != nil {
		m.observeOpenPhase("object_read", "remote", StateFormatV1, started, int64(len(payload)), nil, err)
		return nil, nil, fmt.Errorf("read %s: %w", key, err)
	}
	m.observeOpenPhase("object_read", "remote", StateFormatV1, started, int64(len(payload)), nil, nil)
	started = time.Now()
	if plaintext, encrypted, err := m.encryption.decryptBlobIfEncrypted(payload, stateBlobAAD(m.volumeID, "object:"+key)); encrypted || err != nil {
		if err != nil {
			m.observeOpenPhase("decrypt", "remote", StateFormatV1, started, int64(len(payload)), nil, err)
			return nil, nil, err
		}
		payload = plaintext
	}
	m.observeOpenPhase("decrypt", "remote", StateFormatV1, started, int64(len(payload)), nil, nil)
	return nil, payload, nil
}

func (m *Materializer) putJSON(ctx context.Context, key string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	payload, err = m.encryption.encryptBlob(payload, stateBlobAAD(m.volumeID, "object:"+key))
	if err != nil {
		return fmt.Errorf("encrypt %s: %w", key, err)
	}
	return m.putBytes(ctx, key, payload)
}

func (m *Materializer) observeOpenPhase(phase, source string, format int, started time.Time, bytes int64, state *SnapshotState, err error) {
	if m == nil || m.openObserver == nil {
		return
	}
	observation := OpenObservation{
		VolumeID: m.volumeID,
		Phase:    phase,
		Source:   source,
		Format:   format,
		Duration: time.Since(started),
		Bytes:    bytes,
		Err:      err,
	}
	if phase == "complete" && state != nil {
		observation.Nodes = len(state.Nodes)
		observation.DirectoryEntries = directoryEntryCount(state.Children)
		observation.Segments = len(state.Segments)
	}
	emitOpenObservation(m.openObserver, observation)
}

func (m *Materializer) loadManifestByKey(ctx context.Context, key string) (*Manifest, error) {
	if strings.TrimSpace(key) == "" {
		return nil, ErrMaterializedManifestNotFound
	}
	manifest, err := m.getManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	if manifest.State == nil {
		return nil, fmt.Errorf("materialized manifest %s has no state", key)
	}
	normalizeState(manifest.State)
	sourceVolumeID := manifest.VolumeID
	if strings.TrimSpace(sourceVolumeID) == "" {
		sourceVolumeID = m.volumeID
	}
	defaultSegmentVolumeIDs(manifest.State, sourceVolumeID)
	return manifest, nil
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

func (m *Materializer) storeForSegment(segment *Segment) (string, objectstore.Store, error) {
	if segment == nil {
		return "", nil, fmt.Errorf("%w: segment is required", ErrInvalidInput)
	}
	volumeID := strings.TrimSpace(segment.VolumeID)
	if volumeID == "" {
		volumeID = m.volumeID
	}
	if volumeID == "" || volumeID == m.volumeID {
		return m.volumeID, m.store, nil
	}
	if m.objectStoreForVolume == nil {
		return "", nil, fmt.Errorf("%w: segment %s belongs to volume %s but no object store resolver is configured", ErrInvalidInput, segment.ID, volumeID)
	}
	store, err := m.objectStoreForVolume(volumeID)
	if err != nil {
		return "", nil, err
	}
	if store == nil {
		return "", nil, fmt.Errorf("%w: object store resolver returned nil for volume %s", ErrInvalidInput, volumeID)
	}
	return volumeID, store, nil
}

func segmentCacheKey(volumeID, key string) string {
	return volumeID + "\x00" + key
}

func defaultSegmentVolumeIDs(state *SnapshotState, volumeID string) {
	if state == nil {
		return
	}
	for _, segment := range state.Segments {
		if segment != nil && strings.TrimSpace(segment.VolumeID) == "" {
			segment.VolumeID = volumeID
		}
	}
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
	// The cache takes ownership of payload. ReadSegmentRange returns cloned
	// ranges to callers, so cached segment bytes remain immutable after put.
	c.entries[key] = payload
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

func (c *segmentCache) estimatedMemoryBytes() int64 {
	if c == nil {
		return 0
	}
	// Admission charges the capacity rather than current occupancy so cache
	// growth cannot make an already-active engine bypass the node budget.
	return c.maxBytes + 1<<20
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
	if segment.Encryption != nil {
		enc := *segment.Encryption
		enc.WrappedKey = append([]byte(nil), segment.Encryption.WrappedKey...)
		enc.NoncePrefix = append([]byte(nil), segment.Encryption.NoncePrefix...)
		copy.Encryption = &enc
	}
	copy.InlineData = append([]byte(nil), segment.InlineData...)
	return &copy
}

func cloneSegmentForMaterialization(segment *Segment) *Segment {
	if segment == nil {
		return nil
	}
	clone := *segment
	if segment.Encryption != nil {
		enc := *segment.Encryption
		enc.WrappedKey = append([]byte(nil), segment.Encryption.WrappedKey...)
		enc.NoncePrefix = append([]byte(nil), segment.Encryption.NoncePrefix...)
		clone.Encryption = &enc
	}
	if isInlineSegment(segment) {
		// Inline segment payloads are immutable after creation; materialization
		// snapshots can share the backing bytes while the engine lock is released.
		clone.InlineData = segment.InlineData
	} else {
		clone.InlineData = append([]byte(nil), segment.InlineData...)
	}
	return &clone
}
