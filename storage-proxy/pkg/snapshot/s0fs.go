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
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/juicefs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func (m *Manager) s0fsConfig(teamID, volumeID string) s0fs.Config {
	cfg := s0fs.Config{
		VolumeID: volumeID,
		WALPath:  filepath.Join(m.config.CacheDir, "s0fs", volumeID, "engine.wal"),
	}
	store, err := m.s0fsObjectStore(teamID, volumeID)
	if err == nil {
		cfg.ObjectStore = store
	}
	return cfg
}

func (m *Manager) s0fsObjectStore(teamID, volumeID string) (object.ObjectStorage, error) {
	if m == nil || m.config == nil || teamID == "" || volumeID == "" || strings.TrimSpace(m.config.S3Bucket) == "" {
		return nil, nil
	}
	prefix, err := naming.S3VolumePrefix(teamID, volumeID)
	if err != nil {
		return nil, err
	}
	store, err := juicefs.CreateObjectStorage(juicefs.ObjectStorageConfig{
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
	return object.WithPrefix(store, prefix+"/s0fs/"), nil
}

func (m *Manager) shouldUseS0FS(volumeID string) bool {
	if volumeID == "" || m == nil || m.config == nil {
		return false
	}
	if m.volMgr != nil {
		if volCtx, err := m.volMgr.GetVolume(volumeID); err == nil && volCtx != nil {
			return volCtx.IsS0FS()
		}
	}
	if m.repo != nil {
		if volumeRecord, err := m.repo.GetSandboxVolume(context.Background(), volumeID); err == nil {
			return volume.ResolveBackendType(volumeRecord.BackendType) == volume.BackendS0FS
		}
	}
	baseDir := filepath.Join(m.config.CacheDir, "s0fs", volumeID)
	if _, err := os.Stat(filepath.Join(baseDir, "head.json")); err == nil {
		return true
	}
	if _, err := os.Stat(filepath.Join(baseDir, "engine.wal")); err == nil {
		return true
	}
	return false
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

func (m *s0fsArchiveMeta) GetAttr(_ meta.Context, inode meta.Ino, attr *meta.Attr) syscall.Errno {
	if attr == nil {
		return syscall.EINVAL
	}
	node, err := m.state.GetAttr(uint64(inode))
	if err != nil {
		return errnoForS0FSError(err)
	}
	*attr = meta.Attr{
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

func (m *s0fsArchiveMeta) Readdir(_ meta.Context, inode meta.Ino, _ uint8, entries *[]*meta.Entry) syscall.Errno {
	dirEntries, err := m.state.ReadDir(uint64(inode))
	if err != nil {
		return errnoForS0FSError(err)
	}
	out := make([]*meta.Entry, 0, len(dirEntries))
	for _, entry := range dirEntries {
		attr := &meta.Attr{}
		if errno := m.GetAttr(meta.Background(), meta.Ino(entry.Inode), attr); errno != 0 {
			return errno
		}
		out = append(out, &meta.Entry{
			Inode: meta.Ino(entry.Inode),
			Name:  []byte(entry.Name),
			Attr:  attr,
		})
	}
	*entries = out
	return 0
}

func (m *s0fsArchiveMeta) ReadLink(_ meta.Context, inode meta.Ino, target *[]byte) syscall.Errno {
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

func (r *s0fsArchiveReader) ReadFile(ctx context.Context, inode meta.Ino, size uint64, w io.Writer) error {
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
		return meta.TypeDirectory
	case s0fs.TypeSymlink:
		return meta.TypeSymlink
	default:
		return meta.TypeFile
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

func resolveS0FSForkState(ctx context.Context, source *s0fs.Engine) (*s0fs.SnapshotState, error) {
	if source == nil {
		return nil, fmt.Errorf("source engine is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return source.ExportState()
}

func (m *Manager) openS0FSSnapshotArchiveSession(ctx context.Context, volumeID, snapshotID string) (*snapshotArchiveSession, meta.Ino, *meta.Attr, error) {
	volumeRecord, err := m.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		return nil, 0, nil, err
	}
	state, err := s0fs.LoadSnapshot(ctx, m.s0fsConfig(volumeRecord.TeamID, volumeID), snapshotID)
	if err != nil {
		return nil, 0, nil, err
	}
	rootAttr := &meta.Attr{}
	metaView := &s0fsArchiveMeta{state: state}
	if errno := metaView.GetAttr(meta.Background(), meta.RootInode, rootAttr); errno != 0 {
		return nil, 0, nil, errno
	}
	return &snapshotArchiveSession{
		meta:   metaView,
		reader: &s0fsArchiveReader{state: state},
	}, meta.RootInode, rootAttr, nil
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

	sourceEngine, closeSource, err := m.openS0FSEngine(ctx, sourceVol.TeamID, req.SourceVolumeID)
	if err != nil {
		return nil, err
	}
	defer closeSource()

	if m.volMgr != nil {
		if volCtx, getErr := m.volMgr.GetVolume(req.SourceVolumeID); getErr == nil && volCtx != nil {
			_ = volCtx.FlushAll("")
		}
	}

	state, err := resolveS0FSForkState(ctx, sourceEngine)
	if err != nil {
		return nil, err
	}

	cacheSize := sourceVol.CacheSize
	if req.CacheSize != nil && strings.TrimSpace(*req.CacheSize) != "" {
		cacheSize = *req.CacheSize
	}
	if cacheSize == "" {
		cacheSize = "1G"
	}

	bufferSize := sourceVol.BufferSize
	if req.BufferSize != nil && strings.TrimSpace(*req.BufferSize) != "" {
		bufferSize = *req.BufferSize
	}
	if bufferSize == "" {
		bufferSize = "32M"
	}

	prefetch := sourceVol.Prefetch
	if req.Prefetch != nil {
		prefetch = *req.Prefetch
	}

	writeback := sourceVol.Writeback
	if req.Writeback != nil {
		writeback = *req.Writeback
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
	targetEngine, closeTarget, err := m.openS0FSEngine(ctx, req.TeamID, newVolumeID)
	if err != nil {
		return nil, err
	}
	if err := targetEngine.ReplaceState(state); err != nil {
		closeTarget()
		return nil, err
	}
	_ = closeTarget()

	now := time.Now()
	sourceID := sourceVol.ID
	newVol := &db.SandboxVolume{
		ID:              newVolumeID,
		TeamID:          req.TeamID,
		UserID:          req.UserID,
		SourceVolumeID:  &sourceID,
		DefaultPosixUID: defaultPosixUID,
		DefaultPosixGID: defaultPosixGID,
		CacheSize:       cacheSize,
		Prefetch:        prefetch,
		BufferSize:      bufferSize,
		Writeback:       writeback,
		AccessMode:      string(accessMode),
		BackendType:     volume.BackendS0FS,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if err := m.repo.WithTx(ctx, func(tx pgx.Tx) error {
		if err := m.repo.CreateSandboxVolumeTx(ctx, tx, newVol); err != nil {
			return err
		}
		return m.appendMeteringEventTx(ctx, tx, volumeForkedEvent(m.regionID(), m.clusterID, newVol))
	}); err != nil {
		_ = cleanupS0FSVolume(newVolumeID, m.config)
		return nil, err
	}

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
	if m.volMgr != nil {
		if volCtx, getErr := m.volMgr.GetVolume(req.VolumeID); getErr == nil && volCtx != nil {
			_ = m.volMgr.UpdateVolumeRoot(req.VolumeID, meta.RootInode)
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
