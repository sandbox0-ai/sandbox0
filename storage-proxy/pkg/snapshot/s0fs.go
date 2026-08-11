package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

type s0fsHeadRepository interface {
	GetS0FSCommittedHead(ctx context.Context, volumeID string) (*db.S0FSCommittedHead, error)
	CompareAndSwapS0FSCommittedHead(ctx context.Context, volumeID string, expected, head *db.S0FSCommittedHead) error
	BeginS0FSCommit(ctx context.Context, volumeID, commitID string, expected *db.S0FSCommittedHead, expiresAt time.Time) error
	RenewS0FSCommit(ctx context.Context, volumeID, commitID string, expiresAt time.Time) error
	AbortS0FSCommit(ctx context.Context, volumeID, commitID string) error
	AcquireS0FSGarbageCollection(ctx context.Context, volumeID, token string, expected *db.S0FSCommittedHead, expiresAt time.Time) error
	ValidateS0FSGarbageCollection(ctx context.Context, volumeID, token string, expected *db.S0FSCommittedHead) error
	ReleaseS0FSGarbageCollection(ctx context.Context, volumeID, token string) error
	StageS0FSGarbageCollection(ctx context.Context, volumeID, token string, expected *db.S0FSCommittedHead, candidates []string, deleteAfter time.Time) ([]string, error)
}

func snapshotDBHead(head *s0fs.CommittedHead) *db.S0FSCommittedHead {
	if head == nil {
		return nil
	}
	return &db.S0FSCommittedHead{
		VolumeID: head.VolumeID, ManifestSeq: head.ManifestSeq, CheckpointSeq: head.CheckpointSeq,
		ManifestKey: head.ManifestKey, ManifestDigest: head.ManifestDigest,
		CommitID: head.CommitID, Generation: head.Generation, UpdatedAt: head.UpdatedAt,
	}
}

func mapSnapshotS0FSConflict(err error) error {
	if errors.Is(err, db.ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}

type activeMountRepository interface {
	GetActiveMounts(ctx context.Context, volumeID string, heartbeatTimeout int) ([]*db.VolumeMount, error)
}

type sourceVolumeRepository interface {
	ListSandboxVolumesBySource(ctx context.Context, sourceVolumeID string) ([]*db.SandboxVolume, error)
}

type snapshotHeadStore struct {
	repo s0fsHeadRepository
}

func (s *snapshotHeadStore) LoadCommittedHead(ctx context.Context, volumeID string) (*s0fs.CommittedHead, error) {
	if s == nil || s.repo == nil {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	head, err := s.repo.GetS0FSCommittedHead(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, s0fs.ErrCommittedHeadNotFound
		}
		return nil, err
	}
	return &s0fs.CommittedHead{
		VolumeID:       head.VolumeID,
		ManifestSeq:    head.ManifestSeq,
		CheckpointSeq:  head.CheckpointSeq,
		ManifestKey:    head.ManifestKey,
		ManifestDigest: head.ManifestDigest,
		CommitID:       head.CommitID,
		Generation:     head.Generation,
		UpdatedAt:      head.UpdatedAt,
	}, nil
}

func (s *snapshotHeadStore) CompareAndSwapCommittedHead(ctx context.Context, volumeID string, expected, head *s0fs.CommittedHead) error {
	if s == nil || s.repo == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	var expectedDB *db.S0FSCommittedHead
	if expected != nil {
		expectedDB = &db.S0FSCommittedHead{
			VolumeID: expected.VolumeID, ManifestSeq: expected.ManifestSeq, CheckpointSeq: expected.CheckpointSeq,
			ManifestKey: expected.ManifestKey, ManifestDigest: expected.ManifestDigest, CommitID: expected.CommitID, Generation: expected.Generation,
		}
	}
	err := s.repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, expectedDB, &db.S0FSCommittedHead{
		VolumeID:       head.VolumeID,
		ManifestSeq:    head.ManifestSeq,
		CheckpointSeq:  head.CheckpointSeq,
		ManifestKey:    head.ManifestKey,
		ManifestDigest: head.ManifestDigest,
		CommitID:       head.CommitID,
		Generation:     head.Generation,
		UpdatedAt:      head.UpdatedAt,
	})
	if errors.Is(err, db.ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
}

func (s *snapshotHeadStore) BeginCommit(ctx context.Context, volumeID, commitID string, expected *s0fs.CommittedHead, expiresAt time.Time) error {
	return mapSnapshotS0FSConflict(s.repo.BeginS0FSCommit(ctx, volumeID, commitID, snapshotDBHead(expected), expiresAt))
}

func (s *snapshotHeadStore) RenewCommit(ctx context.Context, volumeID, commitID string, expiresAt time.Time) error {
	return mapSnapshotS0FSConflict(s.repo.RenewS0FSCommit(ctx, volumeID, commitID, expiresAt))
}

func (s *snapshotHeadStore) AbortCommit(ctx context.Context, volumeID, commitID string) error {
	return s.repo.AbortS0FSCommit(ctx, volumeID, commitID)
}

func (s *snapshotHeadStore) AcquireGarbageCollection(ctx context.Context, volumeID, token string, expected *s0fs.CommittedHead, expiresAt time.Time) error {
	return mapSnapshotS0FSConflict(s.repo.AcquireS0FSGarbageCollection(ctx, volumeID, token, snapshotDBHead(expected), expiresAt))
}

func (s *snapshotHeadStore) ReleaseGarbageCollection(ctx context.Context, volumeID, token string) error {
	return s.repo.ReleaseS0FSGarbageCollection(ctx, volumeID, token)
}

func (s *snapshotHeadStore) ValidateGarbageCollection(ctx context.Context, volumeID, token string, expected *s0fs.CommittedHead) error {
	return mapSnapshotS0FSConflict(s.repo.ValidateS0FSGarbageCollection(ctx, volumeID, token, snapshotDBHead(expected)))
}

func (s *snapshotHeadStore) StageGarbageCollection(ctx context.Context, volumeID, token string, expected *s0fs.CommittedHead, candidates []string, deleteAfter time.Time) ([]string, error) {
	due, err := s.repo.StageS0FSGarbageCollection(ctx, volumeID, token, snapshotDBHead(expected), candidates, deleteAfter)
	return due, mapSnapshotS0FSConflict(err)
}

func (m *Manager) s0fsConfig(teamID, volumeID string) (s0fs.Config, error) {
	cfg := s0fs.Config{
		VolumeID: volumeID,
		WALPath:  filepath.Join(m.config.CacheDir, "s0fs", volumeID, "engine.wal"),
	}
	encryption, err := volume.S0FSEncryptionConfig(m.config)
	if err != nil {
		return s0fs.Config{}, err
	}
	cfg.Encryption = encryption
	segmentTargetSize, err := volume.S0FSSegmentTargetSize(m.config)
	if err != nil {
		return s0fs.Config{}, err
	}
	cfg.SegmentTargetSize = segmentTargetSize
	stateFormatVersion, err := volume.S0FSStateFormatVersion(m.config)
	if err != nil {
		return s0fs.Config{}, err
	}
	cfg.StateFormatVersion = stateFormatVersion
	store, err := m.s0fsObjectStore(teamID, volumeID)
	if err != nil {
		return s0fs.Config{}, err
	}
	cfg.ObjectStore = store
	repo, ok := any(m.repo).(s0fsHeadRepository)
	if !ok || repo == nil {
		return s0fs.Config{}, fmt.Errorf("s0fs committed-head coordination repository is not configured")
	}
	cfg.HeadStore = &snapshotHeadStore{repo: repo}
	cfg.ObjectStoreForVolume = func(sourceVolumeID string) (objectstore.Store, error) {
		return m.s0fsObjectStore(teamID, sourceVolumeID)
	}
	return cfg, nil
}

// loadS0FSSnapshotState loads the durable snapshot object and performs a
// guarded, one-time recovery for snapshots created before snapshot state was
// persisted to object storage. The manifest fallback is deliberately narrow:
// the committed head and manifest structure must be stable, and exactly one
// state may match the PostgreSQL snapshot timestamp and storage size.
func (m *Manager) loadS0FSSnapshotState(ctx context.Context, cfg s0fs.Config, snapshot *db.Snapshot) (*s0fs.SnapshotState, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot is required")
	}
	state, err := s0fs.LoadSnapshot(ctx, cfg, snapshot.ID)
	if err == nil {
		return state, nil
	}
	if !errors.Is(err, s0fs.ErrSnapshotNotFound) {
		return nil, err
	}

	state, manifest, err := s0fs.RecoverSnapshotFromManifest(ctx, cfg, snapshot.CreatedAt, snapshot.SizeBytes)
	if err != nil {
		if errors.Is(err, s0fs.ErrMaterializedManifestNotFound) {
			return nil, fmt.Errorf("%w: no durable state or eligible legacy manifest", s0fs.ErrSnapshotNotFound)
		}
		return nil, err
	}
	if got := snapshotSizeBytes(state); got != snapshot.SizeBytes {
		return nil, fmt.Errorf(
			"%w: recovered manifest storage size %d does not match snapshot catalog size %d",
			s0fs.ErrSnapshotNotFound,
			got,
			snapshot.SizeBytes,
		)
	}
	if err := s0fs.PersistSnapshot(ctx, cfg, snapshot.ID, state); err != nil {
		return nil, fmt.Errorf("persist recovered snapshot state: %w", err)
	}
	m.logger.WithFields(logrus.Fields{
		"volume_id":    snapshot.VolumeID,
		"snapshot_id":  snapshot.ID,
		"manifest_seq": manifest.ManifestSeq,
	}).Warn("Recovered legacy s0fs snapshot state from an immutable manifest")
	return state, nil
}

func (m *Manager) hasMountedCtldOwner(ctx context.Context, volumeID string) (bool, error) {
	return m.hasMountedOwnerKind(ctx, volumeID, volume.OwnerKindCtld)
}

func (m *Manager) hasMountedStorageProxyOwner(ctx context.Context, volumeID string) (bool, error) {
	return m.hasMountedOwnerKind(ctx, volumeID, volume.OwnerKindStorageProxy)
}

func (m *Manager) hasActiveWritableMount(ctx context.Context, volumeID string) (bool, error) {
	mounts, err := m.activeMounts(ctx, volumeID)
	if err != nil {
		return false, err
	}
	for _, mount := range mounts {
		if mount == nil {
			continue
		}
		opts := volume.DecodeMountOptions(mount.MountOptions)
		if volume.NormalizeAccessMode(string(opts.AccessMode)) != volume.AccessModeROX {
			return true, nil
		}
	}
	return false, nil
}

func (m *Manager) hasMountedOwnerKind(ctx context.Context, volumeID, ownerKind string) (bool, error) {
	if ownerKind == "" {
		return false, nil
	}
	mounts, err := m.activeMounts(ctx, volumeID)
	if err != nil {
		return false, err
	}
	for _, mount := range mounts {
		if volume.DecodeMountOptions(mount.MountOptions).OwnerKind == ownerKind {
			return true, nil
		}
	}
	return false, nil
}

func (m *Manager) activeMounts(ctx context.Context, volumeID string) ([]*db.VolumeMount, error) {
	repo, ok := any(m.repo).(activeMountRepository)
	if !ok || repo == nil || volumeID == "" {
		return nil, nil
	}
	heartbeatTimeout := 15
	if m.config != nil && m.config.HeartbeatTimeout > 0 {
		heartbeatTimeout = m.config.HeartbeatTimeout
	}
	mounts, err := repo.GetActiveMounts(ctx, volumeID, heartbeatTimeout)
	if err != nil {
		return nil, fmt.Errorf("get active mounts: %w", err)
	}
	return mounts, nil
}

func (m *Manager) s0fsObjectStore(teamID, volumeID string) (objectstore.Store, error) {
	if m == nil || m.config == nil || teamID == "" || volumeID == "" || strings.TrimSpace(m.config.S3Bucket) == "" {
		return nil, nil
	}
	prefix, err := naming.S3VolumePrefix(teamID, volumeID)
	if err != nil {
		return nil, err
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:            m.config.ObjectStorageType,
		Bucket:          m.config.S3Bucket,
		Region:          m.config.S3Region,
		Endpoint:        m.config.S3Endpoint,
		AccessKey:       m.config.S3AccessKey,
		SecretKey:       m.config.S3SecretKey,
		SessionToken:    m.config.S3SessionToken,
		Metrics:         m.metrics,
		RequestObserver: m.requestObserver,
	})
	if err != nil {
		return nil, err
	}
	return objectstore.Prefix(store, prefix+"/s0fs/"), nil
}

func (m *Manager) openS0FSEngine(ctx context.Context, teamID, volumeID string) (*s0fs.Engine, func() error, error) {
	if volumeID == "" {
		return nil, nil, fmt.Errorf("volume id is required")
	}
	if m.volMgr != nil {
		if volCtx, err := m.volMgr.GetVolume(volumeID); err == nil && volCtx != nil && volCtx.IsS0FS() {
			return volCtx.S0FS, func() error { return nil }, nil
		}
	}

	cfg, err := m.s0fsConfig(teamID, volumeID)
	if err != nil {
		return nil, nil, err
	}
	engine, err := s0fs.Open(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return engine, engine.Close, nil
}

func cleanupS0FSVolume(volumeID string, cfg *config.StorageProxyConfig) error {
	if cfg == nil || volumeID == "" {
		return nil
	}
	return os.RemoveAll(filepath.Join(cfg.CacheDir, "s0fs", volumeID))
}

func snapshotSizeBytes(state *s0fs.SnapshotState) int64 {
	return s0fs.StateStorageBytes(state)
}

func (m *Manager) resolveS0FSForkState(ctx context.Context, teamID, sourceVolumeID string) (*s0fs.SnapshotState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := m.s0fsConfig(teamID, sourceVolumeID)
	if err != nil {
		return nil, err
	}
	materializer := s0fs.NewMaterializer(sourceVolumeID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
	materializer.SetEncryption(cfg.Encryption)
	if materializer == nil || !materializer.Enabled() {
		return nil, fmt.Errorf("%w: s0fs materializer is not configured", s0fs.ErrInvalidInput)
	}
	state, _, err := materializer.LoadLatestState(ctx)
	if err != nil {
		if !errors.Is(err, s0fs.ErrMaterializedManifestNotFound) {
			return nil, err
		}
		engine, closeFn, openErr := m.openS0FSEngine(ctx, teamID, sourceVolumeID)
		if openErr != nil {
			return nil, openErr
		}
		defer closeFn()
		state, err = engine.ExportState()
		if err != nil {
			return nil, err
		}
	}
	return s0fs.PrepareForkState(state, sourceVolumeID)
}

func (m *Manager) createS0FSSnapshot(ctx context.Context, req *CreateSnapshotRequest) (*db.Snapshot, error) {
	vol, err := m.repo.GetSandboxVolume(ctx, req.VolumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrVolumeNotFound
		}
		return nil, err
	}
	if vol.TeamID != req.TeamID {
		return nil, ErrVolumeNotFound
	}

	engine, closeFn, err := m.openS0FSEngine(ctx, vol.TeamID, req.VolumeID)
	if err != nil {
		return nil, err
	}
	defer closeFn()

	if m.volMgr != nil {
		if volCtx, getErr := m.volMgr.GetVolume(req.VolumeID); getErr == nil && volCtx != nil {
			_ = volCtx.FlushAll("")
		}
	}
	if _, err := engine.EnsureMaterialized(ctx); err != nil {
		return nil, err
	}

	snapshotID := uuid.New().String()
	state, err := engine.CreateSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}
	cfg, err := m.s0fsConfig(vol.TeamID, req.VolumeID)
	if err != nil {
		_ = engine.DeleteSnapshot(snapshotID)
		return nil, err
	}
	cleanupSnapshotState := func() {
		if cleanupErr := s0fs.DeleteSnapshot(context.Background(), cfg, snapshotID); cleanupErr != nil && !errors.Is(cleanupErr, s0fs.ErrSnapshotNotFound) {
			m.logger.WithError(cleanupErr).Warn("Failed to clean up uncommitted s0fs snapshot state")
		}
	}
	if err := s0fs.PersistSnapshot(ctx, cfg, snapshotID, state); err != nil {
		cleanupSnapshotState()
		return nil, fmt.Errorf("persist snapshot state: %w", err)
	}
	snapshot := &db.Snapshot{
		ID:          snapshotID,
		VolumeID:    req.VolumeID,
		TeamID:      req.TeamID,
		UserID:      req.UserID,
		RootInode:   int64(s0fs.RootInode),
		SourceInode: int64(s0fs.RootInode),
		Name:        req.Name,
		Description: req.Description,
		SizeBytes:   snapshotSizeBytes(state),
		CreatedAt:   time.Now(),
	}
	if err := m.enforceStorageObservationQuota(ctx, applyStorageObservationMetadata(
		m.snapshotStorageObservation(ctx, snapshot, snapshot.CreatedAt),
		req.StorageMetadata,
	)); err != nil {
		cleanupSnapshotState()
		return nil, err
	}
	if err := m.repo.CreateSnapshot(ctx, snapshot); err != nil {
		cleanupSnapshotState()
		return nil, err
	}
	if err := m.recordVolumeStorageState(ctx, vol, state, snapshot.CreatedAt); err != nil {
		return nil, err
	}
	if err := m.recordSnapshotStorageWithMetadata(ctx, snapshot, req.StorageMetadata); err != nil {
		return nil, err
	}
	if err := m.appendMeteringEvent(ctx, snapshotCreatedEvent(m.regionID(), m.clusterID, snapshot)); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (m *Manager) forkS0FSVolume(ctx context.Context, req *ForkVolumeRequest) (*db.SandboxVolume, error) {
	sourceVol, err := m.repo.GetSandboxVolume(ctx, req.SourceVolumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrVolumeNotFound
		}
		return nil, err
	}
	if sourceVol.TeamID != req.TeamID {
		return nil, ErrVolumeNotFound
	}
	if volume.NormalizeBackend(sourceVol.Backend) != volume.BackendS0FS {
		return nil, ErrUnsupportedBackend
	}

	if volume.NormalizeAccessMode(sourceVol.AccessMode) != volume.AccessModeROX {
		ctldMounted, err := m.hasMountedCtldOwner(ctx, req.SourceVolumeID)
		if err != nil {
			return nil, err
		}
		if ctldMounted {
			return nil, ErrMountedCtldOwner
		}
	}

	if m.volMgr != nil {
		if volCtx, getErr := m.volMgr.GetVolume(req.SourceVolumeID); getErr == nil && volCtx != nil {
			_ = volCtx.FlushAll("")
			if volCtx.IsS0FS() {
				materialization, err := volCtx.SyncMaterialize(ctx)
				if err != nil {
					return nil, err
				}
				if materialization.ObservationError != nil {
					return nil, materialization.ObservationError
				}
			}
		}
	}

	state, err := m.resolveS0FSForkState(ctx, sourceVol.TeamID, req.SourceVolumeID)
	if err != nil {
		return nil, err
	}

	defaultPosixUID := sourceVol.DefaultPosixUID
	defaultPosixGID := sourceVol.DefaultPosixGID
	if req.DefaultPosixUID != nil || req.DefaultPosixGID != nil {
		defaultPosixUID = req.DefaultPosixUID
		defaultPosixGID = req.DefaultPosixGID
	}

	accessMode := volume.AccessModeRWO
	if req.AccessMode != nil && strings.TrimSpace(*req.AccessMode) != "" {
		parsedMode, ok := volume.ParseAccessMode(*req.AccessMode)
		if !ok {
			return nil, ErrInvalidAccessMode
		}
		accessMode = parsedMode
	}

	newVolumeID := uuid.New().String()
	now := time.Now()
	sourceID := sourceVol.ID
	newVol := &db.SandboxVolume{
		ID:              newVolumeID,
		TeamID:          req.TeamID,
		UserID:          req.UserID,
		SourceVolumeID:  &sourceID,
		DefaultPosixUID: defaultPosixUID,
		DefaultPosixGID: defaultPosixGID,
		AccessMode:      string(accessMode),
		Backend:         volume.BackendS0FS,
		BackendConfig:   []byte(`{}`),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		if err := m.repo.CreateSandboxVolumeTx(ctx, tx, newVol); err != nil {
			return err
		}
		if err := m.appendStorageObservationTx(ctx, tx, m.volumeStorageObservation(ctx, newVol, 0, newVol.CreatedAt)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if success {
			return
		}
		_ = m.closeStorageObservation(context.Background(), m.volumeStorageObservation(context.Background(), newVol, 0, time.Now().UTC()))
		_ = cleanupS0FSVolume(newVolumeID, m.config)
		_ = m.repo.WithTx(context.Background(), func(tx pgx.Tx) error {
			err := m.repo.DeleteSandboxVolumeTx(context.Background(), tx, newVolumeID)
			if errors.Is(err, db.ErrNotFound) {
				return nil
			}
			return err
		})
	}()

	targetEngine, closeTarget, err := m.openS0FSEngine(ctx, req.TeamID, newVolumeID)
	if err != nil {
		return nil, err
	}
	if err := targetEngine.ReplaceState(state); err != nil {
		closeTarget()
		return nil, err
	}
	manifest, err := targetEngine.SyncMaterialize(ctx)
	if err != nil {
		closeTarget()
		return nil, err
	}
	if manifest != nil && manifest.State != nil {
		if err := m.recordVolumeStorageState(ctx, newVol, manifest.State, time.Now().UTC()); err != nil {
			closeTarget()
			return nil, err
		}
	}
	_ = closeTarget()

	if err := m.appendMeteringEvent(ctx, volumeForkedEvent(m.regionID(), m.clusterID, newVol)); err != nil {
		return nil, err
	}
	success = true
	return newVol, nil
}

func (m *Manager) createS0FSVolumeFromSnapshot(ctx context.Context, req *CreateVolumeFromSnapshotRequest) (*db.SandboxVolume, error) {
	snapshotRecord, err := m.repo.GetSnapshot(ctx, strings.TrimSpace(req.SnapshotID))
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrSnapshotNotFound
		}
		return nil, err
	}
	if snapshotRecord.TeamID != req.TeamID {
		return nil, ErrSnapshotNotFound
	}

	sourceVol, err := m.repo.GetSandboxVolume(ctx, snapshotRecord.VolumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrVolumeNotFound
		}
		return nil, err
	}
	if sourceVol.TeamID != req.TeamID {
		return nil, ErrVolumeNotFound
	}
	if volume.NormalizeBackend(sourceVol.Backend) != volume.BackendS0FS {
		return nil, ErrUnsupportedBackend
	}

	accessMode := volume.AccessModeRWO
	if strings.TrimSpace(req.AccessMode) != "" {
		parsedMode, ok := volume.ParseAccessMode(req.AccessMode)
		if !ok {
			return nil, ErrInvalidAccessMode
		}
		accessMode = parsedMode
	}

	cfg, err := m.s0fsConfig(snapshotRecord.TeamID, snapshotRecord.VolumeID)
	if err != nil {
		return nil, err
	}
	state, err := m.loadS0FSSnapshotState(ctx, cfg, snapshotRecord)
	if err != nil {
		return nil, err
	}
	forkState, err := s0fs.PrepareForkState(state, snapshotRecord.VolumeID)
	if err != nil {
		return nil, err
	}

	newVolumeID := uuid.New().String()
	now := time.Now()
	sourceID := snapshotRecord.VolumeID
	newVol := &db.SandboxVolume{
		ID:              newVolumeID,
		TeamID:          req.TeamID,
		UserID:          req.UserID,
		SourceVolumeID:  &sourceID,
		DefaultPosixUID: req.DefaultPosixUID,
		DefaultPosixGID: req.DefaultPosixGID,
		AccessMode:      string(accessMode),
		Backend:         volume.BackendS0FS,
		BackendConfig:   []byte(`{}`),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		if err := m.repo.CreateSandboxVolumeTx(ctx, tx, newVol); err != nil {
			return err
		}
		return m.appendStorageObservationTx(ctx, tx, applyStorageObservationMetadata(
			m.volumeStorageObservation(ctx, newVol, 0, newVol.CreatedAt),
			req.StorageMetadata,
		))
	}); err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if success {
			return
		}
		_ = m.closeStorageObservation(context.Background(), applyStorageObservationMetadata(
			m.volumeStorageObservation(context.Background(), newVol, 0, time.Now().UTC()),
			req.StorageMetadata,
		))
		_ = cleanupS0FSVolume(newVolumeID, m.config)
		_ = m.repo.WithTx(context.Background(), func(tx pgx.Tx) error {
			err := m.repo.DeleteSandboxVolumeTx(context.Background(), tx, newVolumeID)
			if errors.Is(err, db.ErrNotFound) {
				return nil
			}
			return err
		})
	}()

	targetEngine, closeTarget, err := m.openS0FSEngine(ctx, req.TeamID, newVolumeID)
	if err != nil {
		return nil, err
	}
	if err := targetEngine.ReplaceState(forkState); err != nil {
		closeTarget()
		return nil, err
	}
	manifest, err := targetEngine.SyncMaterialize(ctx)
	if err != nil {
		closeTarget()
		return nil, err
	}
	if manifest != nil && manifest.State != nil {
		if err := m.recordVolumeStorageStateWithMetadata(ctx, newVol, manifest.State, time.Now().UTC(), req.StorageMetadata); err != nil {
			closeTarget()
			return nil, err
		}
	}
	_ = closeTarget()

	if err := m.appendMeteringEvent(ctx, volumeCreatedEvent(m.regionID(), m.clusterID, newVol)); err != nil {
		return nil, err
	}
	success = true
	return newVol, nil
}

func (m *Manager) restoreS0FSSnapshot(ctx context.Context, req *RestoreSnapshotRequest, snapshot *db.Snapshot) error {
	cfg, err := m.s0fsConfig(snapshot.TeamID, req.VolumeID)
	if err != nil {
		return err
	}
	state, err := m.loadS0FSSnapshotState(ctx, cfg, snapshot)
	if err != nil {
		return err
	}
	engine, closeFn, err := m.openS0FSEngine(ctx, snapshot.TeamID, req.VolumeID)
	var manifest *s0fs.Manifest
	if err != nil {
		if !errors.Is(err, s0fs.ErrCommittedStateIntegrity) && !errors.Is(err, s0fs.ErrCommittedHeadConflict) {
			return err
		}
		manifest, err = s0fs.RepairCommittedState(ctx, cfg, state)
		if err != nil {
			return fmt.Errorf("repair s0fs from snapshot: %w", err)
		}
	} else {
		defer closeFn()
		if err := engine.RestoreState(state); err != nil {
			if !errors.Is(err, s0fs.ErrCommittedStateIntegrity) && !errors.Is(err, s0fs.ErrCommittedHeadConflict) {
				return err
			}
			manifest, err = s0fs.RepairCommittedState(ctx, cfg, state)
			if err != nil {
				return fmt.Errorf("repair failed s0fs engine from snapshot: %w", err)
			}
		} else {
			manifest, err = engine.SyncMaterialize(ctx)
			if err != nil {
				if !errors.Is(err, s0fs.ErrCommittedStateIntegrity) && !errors.Is(err, s0fs.ErrCommittedHeadConflict) {
					return err
				}
				manifest, err = s0fs.RepairCommittedState(ctx, cfg, state)
				if err != nil {
					return fmt.Errorf("repair conflicted s0fs restore from snapshot: %w", err)
				}
			}
		}
	}
	if manifest != nil && manifest.State != nil {
		vol := &db.SandboxVolume{
			ID:        req.VolumeID,
			TeamID:    snapshot.TeamID,
			UserID:    req.UserID,
			CreatedAt: snapshot.CreatedAt,
		}
		if volumeRecord, getErr := m.repo.GetSandboxVolume(ctx, req.VolumeID); getErr == nil {
			vol = volumeRecord
		}
		if err := m.recordVolumeStorageState(ctx, vol, manifest.State, time.Now().UTC()); err != nil {
			return err
		}
	}
	if m.volMgr != nil {
		if volCtx, getErr := m.volMgr.GetVolume(req.VolumeID); getErr == nil && volCtx != nil {
			_ = m.volMgr.UpdateVolumeRoot(req.VolumeID, fsmeta.RootInode)
		}
	}

	invalidateID := uuid.New().String()
	if m.volMgr == nil {
		return m.appendMeteringEvent(ctx, snapshotRestoredEvent(m.regionID(), m.clusterID, snapshot, req.VolumeID, req.TeamID, req.UserID))
	}
	participants, err := m.volMgr.BeginInvalidate(req.VolumeID, invalidateID)
	if err != nil {
		return err
	}
	if participants > 0 {
		m.publishInvalidateEvent(req.VolumeID, invalidateID)
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := m.volMgr.WaitForInvalidate(waitCtx, req.VolumeID, invalidateID); err != nil {
			return fmt.Errorf("%w: %v", ErrRemountTimeout, err)
		}
	}
	return m.appendMeteringEvent(ctx, snapshotRestoredEvent(m.regionID(), m.clusterID, snapshot, req.VolumeID, req.TeamID, req.UserID))
}

func (m *Manager) deleteS0FSSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	volumeRecord, err := m.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		return err
	}
	cfg, err := m.s0fsConfig(volumeRecord.TeamID, volumeID)
	if err != nil {
		return err
	}
	if err := s0fs.DeleteSnapshot(ctx, cfg, snapshotID); err != nil && !errors.Is(err, s0fs.ErrSnapshotNotFound) {
		return err
	}
	return nil
}

func (m *Manager) DeleteVolumeObjectsIfUnreferenced(ctx context.Context, vol *db.SandboxVolume) error {
	if vol == nil || strings.TrimSpace(vol.ID) == "" || strings.TrimSpace(vol.TeamID) == "" {
		return nil
	}
	if volume.NormalizeBackend(vol.Backend) != volume.BackendS0FS {
		return nil
	}
	safe, err := m.canCollectS0FSVolume(ctx, vol.ID)
	if err != nil {
		return err
	}
	if !safe {
		m.logger.WithField("volume_id", vol.ID).Info("Skipping s0fs volume object cleanup because references may still exist")
		return nil
	}
	store, err := m.s0fsObjectStore(vol.TeamID, vol.ID)
	if err != nil {
		return err
	}
	deleted, err := s0fs.DeleteAllObjects(ctx, store)
	if err != nil {
		return err
	}
	if err := cleanupS0FSVolume(vol.ID, m.config); err != nil {
		return err
	}
	if len(deleted) > 0 {
		m.logger.WithFields(map[string]any{
			"volume_id": vol.ID,
			"objects":   len(deleted),
		}).Info("Deleted unreferenced s0fs volume objects")
	}
	return nil
}

func (m *Manager) garbageCollectS0FSVolumeObjects(ctx context.Context, volumeID, teamID string) (*s0fs.GarbageCollectionResult, error) {
	if strings.TrimSpace(volumeID) == "" || strings.TrimSpace(teamID) == "" {
		return nil, nil
	}
	safe, err := m.canCollectS0FSVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if !safe {
		return nil, nil
	}

	cfg, err := m.s0fsConfig(teamID, volumeID)
	if err != nil {
		return nil, err
	}
	materializer := s0fs.NewMaterializer(volumeID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
	materializer.SetEncryption(cfg.Encryption)
	if materializer == nil || !materializer.Enabled() {
		return nil, nil
	}
	coordinator, ok := cfg.HeadStore.(s0fs.CommitCoordinator)
	if !ok || coordinator == nil {
		return nil, nil
	}
	head, err := cfg.HeadStore.LoadCommittedHead(ctx, volumeID)
	if errors.Is(err, s0fs.ErrCommittedHeadNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	token := uuid.NewString()
	if err := coordinator.AcquireGarbageCollection(ctx, volumeID, token, head, time.Now().UTC().Add(30*time.Minute)); err != nil {
		if errors.Is(err, s0fs.ErrCommittedHeadConflict) {
			return nil, nil
		}
		return nil, err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coordinator.ReleaseGarbageCollection(releaseCtx, volumeID, token)
	}()
	if safe, err = m.canCollectS0FSVolume(ctx, volumeID); err != nil || !safe {
		return nil, err
	}

	latestState, _, err := materializer.LoadLatestState(ctx)
	if err != nil {
		if errors.Is(err, s0fs.ErrMaterializedManifestNotFound) {
			return nil, nil
		}
		return nil, err
	}
	retainedStates := []*s0fs.SnapshotState{latestState}
	snapshots, err := m.repo.ListSnapshotsByVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		state, err := m.loadS0FSSnapshotState(ctx, cfg, snapshot)
		if err != nil {
			if errors.Is(err, s0fs.ErrSnapshotNotFound) {
				return nil, nil
			}
			return nil, err
		}
		retainedStates = append(retainedStates, state)
	}

	retainedManifests := map[string]struct{}{
		s0fsLegacyLatestManifestKey: {},
		head.ManifestKey:            {},
	}

	plan, err := materializer.PlanGarbageCollection(ctx, retainedStates, retainedManifests)
	if err != nil {
		return nil, err
	}
	due, err := coordinator.StageGarbageCollection(
		ctx, volumeID, token, head, plan.Candidates(), time.Now().UTC().Add(24*time.Hour),
	)
	if err != nil {
		return nil, err
	}
	plan = plan.RetainCandidates(due)
	if len(plan.Segments) == 0 && len(plan.Manifests) == 0 {
		return &s0fs.GarbageCollectionResult{}, nil
	}
	plan.SetDeleteGuard(func(runCtx context.Context) error {
		return coordinator.ValidateGarbageCollection(runCtx, volumeID, token, head)
	})
	result, err := plan.Apply(ctx)
	if err != nil {
		return nil, err
	}
	if len(result.DeletedSegments) > 0 || len(result.DeletedManifests) > 0 {
		m.logger.WithFields(map[string]any{
			"volume_id": volumeID,
			"segments":  len(result.DeletedSegments),
			"manifests": len(result.DeletedManifests),
		}).Info("Deleted unreferenced s0fs objects")
	}
	return result, nil
}

func (m *Manager) canCollectS0FSVolume(ctx context.Context, volumeID string) (bool, error) {
	if repo, ok := any(m.repo).(sourceVolumeRepository); ok && repo != nil {
		children, err := repo.ListSandboxVolumesBySource(ctx, volumeID)
		if err != nil {
			return false, err
		}
		if len(children) > 0 {
			return false, nil
		}
	} else {
		return false, nil
	}
	if repo, ok := any(m.repo).(activeMountRepository); ok && repo != nil {
		mounts, err := repo.GetActiveMounts(ctx, volumeID, m.heartbeatTimeout())
		if err != nil {
			return false, err
		}
		if len(mounts) > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (m *Manager) heartbeatTimeout() int {
	if m != nil && m.config != nil && m.config.HeartbeatTimeout > 0 {
		return m.config.HeartbeatTimeout
	}
	return 15
}

const s0fsLegacyLatestManifestKey = "manifests/latest.json"
