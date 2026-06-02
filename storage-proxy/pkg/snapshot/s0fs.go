package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
)

type s0fsHeadRepository interface {
	GetS0FSCommittedHead(ctx context.Context, volumeID string) (*db.S0FSCommittedHead, error)
	CompareAndSwapS0FSCommittedHead(ctx context.Context, volumeID string, expectedManifestSeq uint64, head *db.S0FSCommittedHead) error
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
		VolumeID:      head.VolumeID,
		ManifestSeq:   head.ManifestSeq,
		CheckpointSeq: head.CheckpointSeq,
		ManifestKey:   head.ManifestKey,
		UpdatedAt:     head.UpdatedAt,
	}, nil
}

func (s *snapshotHeadStore) CompareAndSwapCommittedHead(ctx context.Context, volumeID string, expectedManifestSeq uint64, head *s0fs.CommittedHead) error {
	if s == nil || s.repo == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	err := s.repo.CompareAndSwapS0FSCommittedHead(ctx, volumeID, expectedManifestSeq, &db.S0FSCommittedHead{
		VolumeID:      head.VolumeID,
		ManifestSeq:   head.ManifestSeq,
		CheckpointSeq: head.CheckpointSeq,
		ManifestKey:   head.ManifestKey,
		UpdatedAt:     head.UpdatedAt,
	})
	if errors.Is(err, db.ErrConflict) {
		return s0fs.ErrCommittedHeadConflict
	}
	return err
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
	store, err := m.s0fsObjectStore(teamID, volumeID)
	if err != nil {
		return s0fs.Config{}, err
	}
	cfg.ObjectStore = store
	if repo, ok := any(m.repo).(s0fsHeadRepository); ok {
		cfg.HeadStore = &snapshotHeadStore{repo: repo}
	}
	cfg.ObjectStoreForVolume = func(sourceVolumeID string) (objectstore.Store, error) {
		return m.s0fsObjectStore(teamID, sourceVolumeID)
	}
	return cfg, nil
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
		Type:         m.config.ObjectStorageType,
		Bucket:       m.config.S3Bucket,
		Region:       m.config.S3Region,
		Endpoint:     m.config.S3Endpoint,
		AccessKey:    m.config.S3AccessKey,
		SecretKey:    m.config.S3SecretKey,
		SessionToken: m.config.S3SessionToken,
		Metrics:      m.metrics,
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

type s0fsArchiveMeta struct {
	state *s0fs.SnapshotState
}

func (m *s0fsArchiveMeta) GetAttr(_ fsmeta.Context, inode fsmeta.Ino, attr *fsmeta.Attr) syscall.Errno {
	if attr == nil {
		return syscall.EINVAL
	}
	node, err := m.state.GetAttr(uint64(inode))
	if err != nil {
		return errnoForS0FSError(err)
	}
	*attr = fsmeta.Attr{
		Typ:       metaTypeForS0FS(node.Type),
		Mode:      uint16(node.Mode),
		Uid:       node.UID,
		Gid:       node.GID,
		Nlink:     node.Nlink,
		Length:    node.Size,
		Atime:     node.Atime.Unix(),
		Atimensec: uint32(node.Atime.Nanosecond()),
		Mtime:     node.Mtime.Unix(),
		Mtimensec: uint32(node.Mtime.Nanosecond()),
		Ctime:     node.Ctime.Unix(),
		Ctimensec: uint32(node.Ctime.Nanosecond()),
	}
	return 0
}

func (m *s0fsArchiveMeta) Readdir(_ fsmeta.Context, inode fsmeta.Ino, _ uint8, entries *[]*fsmeta.Entry) syscall.Errno {
	dirEntries, err := m.state.ReadDir(uint64(inode))
	if err != nil {
		return errnoForS0FSError(err)
	}
	out := make([]*fsmeta.Entry, 0, len(dirEntries))
	for _, entry := range dirEntries {
		attr := &fsmeta.Attr{}
		if errno := m.GetAttr(fsmeta.Background(), fsmeta.Ino(entry.Inode), attr); errno != 0 {
			return errno
		}
		out = append(out, &fsmeta.Entry{
			Inode: fsmeta.Ino(entry.Inode),
			Name:  []byte(entry.Name),
			Attr:  attr,
		})
	}
	*entries = out
	return 0
}

func (m *s0fsArchiveMeta) ReadLink(_ fsmeta.Context, inode fsmeta.Ino, target *[]byte) syscall.Errno {
	node, err := m.state.GetAttr(uint64(inode))
	if err != nil {
		return errnoForS0FSError(err)
	}
	if node.Type != s0fs.TypeSymlink {
		return syscall.EINVAL
	}
	*target = []byte(node.Target)
	return 0
}

type s0fsArchiveReader struct {
	reader *s0fs.SnapshotReader
}

func (r *s0fsArchiveReader) ReadFile(ctx context.Context, inode fsmeta.Ino, size uint64, w io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := r.reader.Read(uint64(inode), 0, size)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err = w.Write(data)
	return err
}

func metaTypeForS0FS(fileType s0fs.FileType) uint8 {
	switch fileType {
	case s0fs.TypeDirectory:
		return fsmeta.TypeDirectory
	case s0fs.TypeSymlink:
		return fsmeta.TypeSymlink
	default:
		return fsmeta.TypeFile
	}
}

func errnoForS0FSError(err error) syscall.Errno {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, s0fs.ErrNotFound), errors.Is(err, s0fs.ErrSnapshotNotFound):
		return syscall.ENOENT
	case errors.Is(err, s0fs.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, s0fs.ErrIsDir):
		return syscall.EISDIR
	case errors.Is(err, s0fs.ErrExists):
		return syscall.EEXIST
	case errors.Is(err, s0fs.ErrNotEmpty):
		return syscall.ENOTEMPTY
	default:
		return syscall.EIO
	}
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
		return nil, err
	}
	return s0fs.PrepareForkState(state, sourceVolumeID)
}

func (m *Manager) openS0FSSnapshotArchiveSession(ctx context.Context, volumeID, snapshotID string) (*snapshotArchiveSession, fsmeta.Ino, *fsmeta.Attr, error) {
	volumeRecord, err := m.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		return nil, 0, nil, err
	}
	cfg, err := m.s0fsConfig(volumeRecord.TeamID, volumeID)
	if err != nil {
		return nil, 0, nil, err
	}
	state, err := s0fs.LoadSnapshot(ctx, cfg, snapshotID)
	if err != nil {
		return nil, 0, nil, err
	}
	materializer := s0fs.NewMaterializer(volumeID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
	materializer.SetEncryption(cfg.Encryption)
	rootAttr := &fsmeta.Attr{}
	metaView := &s0fsArchiveMeta{state: state}
	if errno := metaView.GetAttr(fsmeta.Background(), fsmeta.RootInode, rootAttr); errno != 0 {
		return nil, 0, nil, errno
	}
	return &snapshotArchiveSession{
		meta:   metaView,
		reader: &s0fsArchiveReader{reader: s0fs.NewSnapshotReader(state, materializer)},
	}, fsmeta.RootInode, rootAttr, nil
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
		_ = engine.DeleteSnapshot(snapshotID)
		return nil, err
	}
	if err := m.repo.CreateSnapshot(ctx, snapshot); err != nil {
		_ = engine.DeleteSnapshot(snapshotID)
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
				manifest, err := volCtx.S0FS.SyncMaterialize(ctx)
				if err != nil {
					return nil, err
				}
				if manifest != nil && manifest.State != nil {
					if err := m.recordVolumeStorageState(ctx, sourceVol, manifest.State, time.Now().UTC()); err != nil {
						return nil, err
					}
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
	state, err := s0fs.LoadSnapshot(ctx, cfg, snapshotRecord.ID)
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
	engine, closeFn, err := m.openS0FSEngine(ctx, snapshot.TeamID, req.VolumeID)
	if err != nil {
		return err
	}
	defer closeFn()

	if err := engine.RestoreSnapshot(req.SnapshotID); err != nil {
		return err
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		return err
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
	engine, closeFn, err := m.openS0FSEngine(ctx, volumeRecord.TeamID, volumeID)
	if err != nil {
		return err
	}
	defer closeFn()
	if err := engine.DeleteSnapshot(snapshotID); err != nil && !errors.Is(err, s0fs.ErrSnapshotNotFound) {
		return err
	}
	return nil
}

func (m *Manager) DeleteVolumeObjectsIfUnreferenced(ctx context.Context, vol *db.SandboxVolume) error {
	if vol == nil || strings.TrimSpace(vol.ID) == "" || strings.TrimSpace(vol.TeamID) == "" {
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

	latestState, latestManifest, err := materializer.LoadLatestState(ctx)
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
		state, err := s0fs.LoadSnapshot(ctx, cfg, snapshot.ID)
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
	}
	if latestManifest.ManifestSeq > 0 {
		retainedManifests[s0fsManifestKey(latestManifest.ManifestSeq)] = struct{}{}
	}
	headBefore, err := m.currentS0FSManifestKey(ctx, volumeID, latestManifest)
	if err != nil {
		return nil, err
	}
	retainedManifests[headBefore] = struct{}{}

	plan, err := materializer.PlanGarbageCollection(ctx, retainedStates, retainedManifests)
	if err != nil {
		return nil, err
	}
	if len(plan.Segments) == 0 && len(plan.Manifests) == 0 {
		return &s0fs.GarbageCollectionResult{}, nil
	}

	safe, err = m.canCollectS0FSVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if !safe {
		return nil, nil
	}
	headAfter, err := m.currentS0FSManifestKey(ctx, volumeID, latestManifest)
	if err != nil {
		return nil, err
	}
	if headAfter != headBefore {
		m.logger.WithFields(map[string]any{
			"volume_id":   volumeID,
			"head_before": headBefore,
			"head_after":  headAfter,
		}).Info("Skipping s0fs garbage collection because committed head changed")
		return nil, nil
	}
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

func (m *Manager) currentS0FSManifestKey(ctx context.Context, volumeID string, fallback *s0fs.Manifest) (string, error) {
	if repo, ok := any(m.repo).(s0fsHeadRepository); ok && repo != nil {
		head, err := repo.GetS0FSCommittedHead(ctx, volumeID)
		if err == nil && strings.TrimSpace(head.ManifestKey) != "" {
			return head.ManifestKey, nil
		}
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return "", err
		}
	}
	if fallback != nil && fallback.ManifestSeq > 0 {
		return s0fsManifestKey(fallback.ManifestSeq), nil
	}
	return s0fsLegacyLatestManifestKey, nil
}

func (m *Manager) heartbeatTimeout() int {
	if m != nil && m.config != nil && m.config.HeartbeatTimeout > 0 {
		return m.config.HeartbeatTimeout
	}
	return 15
}

func s0fsManifestKey(seq uint64) string {
	return fmt.Sprintf("manifests/%020d.json", seq)
}

const s0fsLegacyLatestManifestKey = "manifests/latest.json"
