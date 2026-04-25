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

func (m *Manager) s0fsConfig(teamID, volumeID string) s0fs.Config {
	cfg := s0fs.Config{
		VolumeID: volumeID,
		WALPath:  filepath.Join(m.config.CacheDir, "s0fs", volumeID, "engine.wal"),
	}
	store, err := m.s0fsObjectStore(teamID, volumeID)
	if err == nil {
		cfg.ObjectStore = store
	}
	if repo, ok := any(m.repo).(s0fsHeadRepository); ok {
		cfg.HeadStore = &snapshotHeadStore{repo: repo}
	}
	cfg.ObjectStoreForVolume = func(sourceVolumeID string) (objectstore.Store, error) {
		return m.s0fsObjectStore(teamID, sourceVolumeID)
	}
	return cfg
}

func (m *Manager) hasMountedCtldOwner(ctx context.Context, volumeID string) (bool, error) {
	repo, ok := any(m.repo).(activeMountRepository)
	if !ok || repo == nil || volumeID == "" {
		return false, nil
	}
	heartbeatTimeout := 15
	if m.config != nil && m.config.HeartbeatTimeout > 0 {
		heartbeatTimeout = m.config.HeartbeatTimeout
	}
	mounts, err := repo.GetActiveMounts(ctx, volumeID, heartbeatTimeout)
	if err != nil {
		return false, fmt.Errorf("get active mounts: %w", err)
	}
	for _, mount := range mounts {
		if volume.DecodeMountOptions(mount.MountOptions).OwnerKind == volume.OwnerKindCtld {
			return true, nil
		}
	}
	return false, nil
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

	engine, err := s0fs.Open(ctx, m.s0fsConfig(teamID, volumeID))
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
	state *s0fs.SnapshotState
}

func (r *s0fsArchiveReader) ReadFile(ctx context.Context, inode fsmeta.Ino, size uint64, w io.Writer) error {
	data, err := r.state.Read(uint64(inode), 0, size)
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
	if state == nil {
		return 0
	}
	var size int64
	for _, payload := range state.Data {
		size += int64(len(payload))
	}
	return size
}

func (m *Manager) resolveS0FSForkState(ctx context.Context, teamID, sourceVolumeID string) (*s0fs.SnapshotState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg := m.s0fsConfig(teamID, sourceVolumeID)
	materializer := s0fs.NewMaterializer(sourceVolumeID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
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
	state, err := s0fs.LoadSnapshot(ctx, m.s0fsConfig(volumeRecord.TeamID, volumeID), snapshotID)
	if err != nil {
		return nil, 0, nil, err
	}
	rootAttr := &fsmeta.Attr{}
	metaView := &s0fsArchiveMeta{state: state}
	if errno := metaView.GetAttr(fsmeta.Background(), fsmeta.RootInode, rootAttr); errno != 0 {
		return nil, 0, nil, errno
	}
	return &snapshotArchiveSession{
		meta:   metaView,
		reader: &s0fsArchiveReader{state: state},
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
	if err := m.repo.CreateSnapshot(ctx, snapshot); err != nil {
		_ = engine.DeleteSnapshot(snapshotID)
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
				if _, err := volCtx.S0FS.SyncMaterialize(ctx); err != nil {
					return nil, err
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
		return nil
	}); err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if success {
			return
		}
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
	if _, err := targetEngine.SyncMaterialize(ctx); err != nil {
		closeTarget()
		return nil, err
	}
	_ = closeTarget()

	if err := m.appendMeteringEvent(ctx, volumeForkedEvent(m.regionID(), m.clusterID, newVol)); err != nil {
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
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		return err
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
