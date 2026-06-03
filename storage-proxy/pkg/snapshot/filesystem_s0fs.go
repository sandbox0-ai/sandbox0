package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

var (
	ErrFilesystemNotFound         = errors.New("filesystem not found")
	ErrFilesystemSnapshotNotFound = errors.New("filesystem snapshot not found")
	ErrFilesystemBusy             = errors.New("filesystem is busy, try again later")
)

type sandboxFilesystemRepository interface {
	CreateSandboxFilesystem(ctx context.Context, fs *db.SandboxFilesystem) error
	GetSandboxFilesystem(ctx context.Context, id string) (*db.SandboxFilesystem, error)
	DeleteSandboxFilesystem(ctx context.Context, id string) error
	CreateSandboxFilesystemSnapshot(ctx context.Context, snapshot *db.SandboxFilesystemSnapshot) error
	GetSandboxFilesystemSnapshot(ctx context.Context, filesystemID, snapshotID, teamID string) (*db.SandboxFilesystemSnapshot, error)
	FindSandboxFilesystemSnapshot(ctx context.Context, snapshotID, teamID string) (*db.SandboxFilesystemSnapshot, error)
	DeleteSandboxFilesystemSnapshot(ctx context.Context, filesystemID, snapshotID, teamID string) error
	RestoreSandboxFilesystemSnapshot(ctx context.Context, filesystemID, snapshotID, teamID string) (*db.SandboxFilesystem, error)
}

type sandboxFilesystemHeadRepository interface {
	GetSandboxFilesystemS0FSCommittedHead(ctx context.Context, filesystemID string) (*db.SandboxFilesystemS0FSCommittedHead, error)
	CompareAndSwapSandboxFilesystemS0FSCommittedHead(ctx context.Context, filesystemID string, expectedManifestSeq uint64, head *db.SandboxFilesystemS0FSCommittedHead) error
}

type activeSandboxFilesystemMountRepository interface {
	GetActiveSandboxFilesystemMounts(ctx context.Context, filesystemID string, heartbeatTimeout int) ([]*db.SandboxFilesystemMount, error)
}

type sandboxFilesystemHeadStore struct {
	repo sandboxFilesystemHeadRepository
}

func (s *sandboxFilesystemHeadStore) LoadCommittedHead(ctx context.Context, filesystemID string) (*s0fs.CommittedHead, error) {
	if s == nil || s.repo == nil {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	head, err := s.repo.GetSandboxFilesystemS0FSCommittedHead(ctx, filesystemID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, s0fs.ErrCommittedHeadNotFound
		}
		return nil, err
	}
	return &s0fs.CommittedHead{
		VolumeID:      head.FilesystemID,
		ManifestSeq:   head.ManifestSeq,
		CheckpointSeq: head.CheckpointSeq,
		ManifestKey:   head.ManifestKey,
		UpdatedAt:     head.UpdatedAt,
	}, nil
}

func (s *sandboxFilesystemHeadStore) CompareAndSwapCommittedHead(ctx context.Context, filesystemID string, expectedManifestSeq uint64, head *s0fs.CommittedHead) error {
	if s == nil || s.repo == nil {
		return s0fs.ErrCommittedHeadNotFound
	}
	err := s.repo.CompareAndSwapSandboxFilesystemS0FSCommittedHead(ctx, filesystemID, expectedManifestSeq, &db.SandboxFilesystemS0FSCommittedHead{
		FilesystemID:  head.VolumeID,
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

type staticSandboxFilesystemHeadStore struct {
	head *s0fs.CommittedHead
}

func (s staticSandboxFilesystemHeadStore) LoadCommittedHead(context.Context, string) (*s0fs.CommittedHead, error) {
	if s.head == nil || strings.TrimSpace(s.head.ManifestKey) == "" {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	clone := *s.head
	return &clone, nil
}

func (s staticSandboxFilesystemHeadStore) CompareAndSwapCommittedHead(context.Context, string, uint64, *s0fs.CommittedHead) error {
	return s0fs.ErrCommittedHeadConflict
}

type ForkSandboxFilesystemRequest struct {
	SourceFilesystemID string
	TeamID             string
	UserID             string
	TemplateID         *string
}

type CreateSandboxFilesystemFromSnapshotRequest struct {
	SnapshotID string
	TeamID     string
	UserID     string
	TemplateID *string
}

type CreateSandboxFilesystemSnapshotRequest struct {
	FilesystemID string
	TeamID       string
	UserID       string
	Name         string
	Description  string
}

type RestoreSandboxFilesystemSnapshotRequest struct {
	FilesystemID string
	SnapshotID   string
	TeamID       string
	UserID       string
}

func (m *Manager) ForkSandboxFilesystem(ctx context.Context, req *ForkSandboxFilesystemRequest) (*db.SandboxFilesystem, error) {
	if req == nil {
		return nil, fmt.Errorf("fork sandbox filesystem request is required")
	}
	repo, err := m.sandboxFilesystemRepo()
	if err != nil {
		return nil, err
	}
	source, err := repo.GetSandboxFilesystem(ctx, strings.TrimSpace(req.SourceFilesystemID))
	if err != nil {
		return nil, mapSandboxFilesystemError(err)
	}
	if source.TeamID != strings.TrimSpace(req.TeamID) {
		return nil, ErrFilesystemNotFound
	}
	if err := m.ensureSandboxFilesystemIdle(ctx, source.ID); err != nil {
		return nil, err
	}
	state, _, err := m.loadCurrentSandboxFilesystemState(ctx, source)
	if err != nil {
		return nil, err
	}
	state, err = s0fs.PrepareForkState(state, source.ID)
	if err != nil {
		return nil, err
	}

	templateID := req.TemplateID
	if templateID == nil {
		templateID = source.TemplateID
	}
	now := time.Now().UTC()
	sourceID := source.ID
	filesystem := &db.SandboxFilesystem{
		ID:                 uuid.NewString(),
		TeamID:             req.TeamID,
		UserID:             req.UserID,
		SourceFilesystemID: &sourceID,
		TemplateID:         templateID,
		BaseImageDigest:    source.BaseImageDigest,
		State:              db.SandboxFilesystemStateAvailable,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	return m.createSandboxFilesystemBranch(ctx, repo, filesystem, state)
}

func (m *Manager) CreateSandboxFilesystemFromSnapshot(ctx context.Context, req *CreateSandboxFilesystemFromSnapshotRequest) (*db.SandboxFilesystem, error) {
	if req == nil {
		return nil, fmt.Errorf("create sandbox filesystem from snapshot request is required")
	}
	repo, err := m.sandboxFilesystemRepo()
	if err != nil {
		return nil, err
	}
	snapshotRecord, err := repo.FindSandboxFilesystemSnapshot(ctx, strings.TrimSpace(req.SnapshotID), strings.TrimSpace(req.TeamID))
	if err != nil {
		return nil, mapSandboxFilesystemSnapshotError(err)
	}
	source, err := repo.GetSandboxFilesystem(ctx, snapshotRecord.FilesystemID)
	if err != nil {
		return nil, mapSandboxFilesystemError(err)
	}
	if source.TeamID != req.TeamID {
		return nil, ErrFilesystemNotFound
	}

	state, err := m.resolveSandboxFilesystemForkState(ctx, source.TeamID, source.ID, snapshotRecord.S0FSHead)
	if err != nil {
		return nil, err
	}
	templateID := req.TemplateID
	if templateID == nil {
		templateID = source.TemplateID
	}
	now := time.Now().UTC()
	sourceID := source.ID
	filesystem := &db.SandboxFilesystem{
		ID:                 uuid.NewString(),
		TeamID:             req.TeamID,
		UserID:             req.UserID,
		SourceFilesystemID: &sourceID,
		TemplateID:         templateID,
		BaseImageDigest:    snapshotRecord.BaseImageDigest,
		State:              db.SandboxFilesystemStateAvailable,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	return m.createSandboxFilesystemBranch(ctx, repo, filesystem, state)
}

func (m *Manager) CreateSandboxFilesystemSnapshot(ctx context.Context, req *CreateSandboxFilesystemSnapshotRequest) (*db.SandboxFilesystemSnapshot, error) {
	if req == nil {
		return nil, fmt.Errorf("create sandbox filesystem snapshot request is required")
	}
	repo, err := m.sandboxFilesystemRepo()
	if err != nil {
		return nil, err
	}
	filesystem, err := repo.GetSandboxFilesystem(ctx, strings.TrimSpace(req.FilesystemID))
	if err != nil {
		return nil, mapSandboxFilesystemError(err)
	}
	if filesystem.TeamID != strings.TrimSpace(req.TeamID) {
		return nil, ErrFilesystemNotFound
	}
	if err := m.ensureSandboxFilesystemIdle(ctx, filesystem.ID); err != nil {
		return nil, err
	}
	state, head, err := m.loadCurrentSandboxFilesystemState(ctx, filesystem)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	snapshotRecord := &db.SandboxFilesystemSnapshot{
		ID:              uuid.NewString(),
		FilesystemID:    filesystem.ID,
		TeamID:          req.TeamID,
		UserID:          req.UserID,
		BaseImageDigest: filesystem.BaseImageDigest,
		S0FSHead:        head,
		Name:            strings.TrimSpace(req.Name),
		Description:     req.Description,
		SizeBytes:       s0fs.StateStorageBytes(state),
		CreatedAt:       now,
	}
	if snapshotRecord.Name == "" {
		return nil, fmt.Errorf("snapshot name is required")
	}
	if err := repo.CreateSandboxFilesystemSnapshot(ctx, snapshotRecord); err != nil {
		return nil, err
	}
	return snapshotRecord, nil
}

func (m *Manager) RestoreSandboxFilesystemSnapshot(ctx context.Context, req *RestoreSandboxFilesystemSnapshotRequest) (*db.SandboxFilesystem, error) {
	if req == nil {
		return nil, fmt.Errorf("restore sandbox filesystem snapshot request is required")
	}
	repo, err := m.sandboxFilesystemRepo()
	if err != nil {
		return nil, err
	}
	filesystem, err := repo.GetSandboxFilesystem(ctx, strings.TrimSpace(req.FilesystemID))
	if err != nil {
		return nil, mapSandboxFilesystemError(err)
	}
	if filesystem.TeamID != strings.TrimSpace(req.TeamID) {
		return nil, ErrFilesystemNotFound
	}
	if err := m.ensureSandboxFilesystemIdle(ctx, filesystem.ID); err != nil {
		return nil, err
	}
	restored, err := repo.RestoreSandboxFilesystemSnapshot(ctx, filesystem.ID, strings.TrimSpace(req.SnapshotID), req.TeamID)
	if err != nil {
		return nil, mapSandboxFilesystemSnapshotError(err)
	}
	return restored, nil
}

func (m *Manager) createSandboxFilesystemBranch(ctx context.Context, repo sandboxFilesystemRepository, filesystem *db.SandboxFilesystem, state *s0fs.SnapshotState) (*db.SandboxFilesystem, error) {
	if err := repo.CreateSandboxFilesystem(ctx, filesystem); err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if success {
			return
		}
		_ = cleanupS0FSFilesystem(filesystem.ID, m.config)
		_ = repo.DeleteSandboxFilesystem(context.Background(), filesystem.ID)
	}()

	if !sandboxFilesystemStateHasCheckpoint(state) {
		updated, err := repo.GetSandboxFilesystem(ctx, filesystem.ID)
		if err != nil {
			return nil, err
		}
		success = true
		return updated, nil
	}

	engine, closeFn, err := m.openSandboxFilesystemS0FSEngine(ctx, filesystem.TeamID, filesystem.ID)
	if err != nil {
		return nil, err
	}
	if err := engine.ReplaceState(state); err != nil {
		closeFn()
		return nil, err
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		closeFn()
		return nil, err
	}
	if err := closeFn(); err != nil {
		return nil, err
	}
	updated, err := repo.GetSandboxFilesystem(ctx, filesystem.ID)
	if err != nil {
		return nil, err
	}
	success = true
	return updated, nil
}

func (m *Manager) resolveSandboxFilesystemForkState(ctx context.Context, teamID, sourceFilesystemID, manifestKey string) (*s0fs.SnapshotState, error) {
	if strings.TrimSpace(sourceFilesystemID) == "" {
		return nil, fmt.Errorf("%w: source filesystem id is required", s0fs.ErrInvalidInput)
	}
	if strings.TrimSpace(manifestKey) == "" {
		return emptySandboxFilesystemState(time.Now().UTC()), nil
	}
	state, err := m.loadSandboxFilesystemState(ctx, teamID, sourceFilesystemID, manifestKey)
	if err != nil {
		return nil, err
	}
	return s0fs.PrepareForkState(state, sourceFilesystemID)
}

func (m *Manager) loadCurrentSandboxFilesystemState(ctx context.Context, filesystem *db.SandboxFilesystem) (*s0fs.SnapshotState, string, error) {
	if filesystem == nil {
		return nil, "", fmt.Errorf("sandbox filesystem is required")
	}
	head, err := m.currentSandboxFilesystemManifestKey(ctx, filesystem.ID, nil)
	if errors.Is(err, s0fs.ErrMaterializedManifestNotFound) {
		var manifest *s0fs.Manifest
		var state *s0fs.SnapshotState
		state, manifest, err = m.ensureSandboxFilesystemMaterialized(ctx, filesystem)
		if err != nil {
			return nil, "", err
		}
		if !sandboxFilesystemStateHasCheckpoint(state) {
			return state, "", nil
		}
		head, err = m.currentSandboxFilesystemManifestKey(ctx, filesystem.ID, manifest)
	}
	if err != nil {
		return nil, "", err
	}
	state, err := m.loadSandboxFilesystemState(ctx, filesystem.TeamID, filesystem.ID, head)
	if err != nil {
		return nil, "", err
	}
	return state, head, nil
}

func (m *Manager) loadSandboxFilesystemState(ctx context.Context, teamID, filesystemID, manifestKey string) (*s0fs.SnapshotState, error) {
	cfg, err := m.sandboxFilesystemS0FSConfig(teamID, filesystemID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(manifestKey) != "" {
		head, err := committedHeadFromManifestKey(filesystemID, manifestKey, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		cfg.HeadStore = staticSandboxFilesystemHeadStore{head: head}
	}
	materializer := s0fs.NewMaterializer(filesystemID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
	if materializer == nil || !materializer.Enabled() {
		return nil, fmt.Errorf("%w: s0fs materializer is not configured", s0fs.ErrInvalidInput)
	}
	materializer.SetEncryption(cfg.Encryption)
	materializer.SetSegmentTargetSize(cfg.SegmentTargetSize)
	state, _, err := materializer.LoadLatestState(ctx)
	return state, err
}

func (m *Manager) ensureSandboxFilesystemMaterialized(ctx context.Context, filesystem *db.SandboxFilesystem) (*s0fs.SnapshotState, *s0fs.Manifest, error) {
	engine, closeFn, err := m.openSandboxFilesystemS0FSEngine(ctx, filesystem.TeamID, filesystem.ID)
	if err != nil {
		return nil, nil, err
	}
	defer closeFn()
	state := engine.SnapshotState()
	if !sandboxFilesystemStateHasCheckpoint(state) {
		return state, nil, nil
	}
	manifest, err := engine.EnsureMaterialized(ctx)
	if err != nil || manifest != nil {
		if manifest != nil && manifest.State != nil {
			state = manifest.State
		}
		return state, manifest, err
	}
	if err := engine.ReplaceState(engine.SnapshotState()); err != nil {
		return nil, nil, err
	}
	manifest, err = engine.SyncMaterialize(ctx)
	if manifest != nil && manifest.State != nil {
		state = manifest.State
	}
	return state, manifest, err
}

func (m *Manager) openSandboxFilesystemS0FSEngine(ctx context.Context, teamID, filesystemID string) (*s0fs.Engine, func() error, error) {
	cfg, err := m.sandboxFilesystemS0FSConfig(teamID, filesystemID)
	if err != nil {
		return nil, nil, err
	}
	engine, err := s0fs.Open(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return engine, engine.Close, nil
}

func (m *Manager) sandboxFilesystemS0FSConfig(teamID, filesystemID string) (s0fs.Config, error) {
	if m == nil || m.config == nil {
		return s0fs.Config{}, fmt.Errorf("storage proxy config is required")
	}
	cfg := s0fs.Config{
		VolumeID: filesystemID,
		WALPath:  filepath.Join(m.config.CacheDir, "s0fs-filesystems", filesystemID, "engine.wal"),
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
	store, err := m.sandboxFilesystemObjectStore(teamID, filesystemID)
	if err != nil {
		return s0fs.Config{}, err
	}
	cfg.ObjectStore = store
	if repo, ok := any(m.repo).(sandboxFilesystemHeadRepository); ok {
		cfg.HeadStore = &sandboxFilesystemHeadStore{repo: repo}
	}
	cfg.ObjectStoreForVolume = func(sourceFilesystemID string) (objectstore.Store, error) {
		return m.sandboxFilesystemObjectStore(teamID, sourceFilesystemID)
	}
	return cfg, nil
}

func (m *Manager) sandboxFilesystemObjectStore(teamID, filesystemID string) (objectstore.Store, error) {
	if m == nil || m.config == nil || teamID == "" || filesystemID == "" || strings.TrimSpace(m.config.S3Bucket) == "" {
		return nil, nil
	}
	prefix, err := naming.S3FilesystemPrefix(teamID, filesystemID)
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
	})
	if err != nil {
		return nil, err
	}
	return objectstore.Prefix(store, prefix+"/s0fs/"), nil
}

func (m *Manager) currentSandboxFilesystemManifestKey(ctx context.Context, filesystemID string, fallback *s0fs.Manifest) (string, error) {
	if repo, ok := any(m.repo).(sandboxFilesystemHeadRepository); ok && repo != nil {
		head, err := repo.GetSandboxFilesystemS0FSCommittedHead(ctx, filesystemID)
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
	return "", s0fs.ErrMaterializedManifestNotFound
}

func (m *Manager) ensureSandboxFilesystemIdle(ctx context.Context, filesystemID string) error {
	repo, ok := any(m.repo).(activeSandboxFilesystemMountRepository)
	if !ok || repo == nil {
		return nil
	}
	mounts, err := repo.GetActiveSandboxFilesystemMounts(ctx, filesystemID, m.heartbeatTimeout())
	if err != nil {
		return err
	}
	if len(mounts) > 0 {
		return ErrFilesystemBusy
	}
	return nil
}

func (m *Manager) sandboxFilesystemRepo() (sandboxFilesystemRepository, error) {
	repo, ok := any(m.repo).(sandboxFilesystemRepository)
	if !ok || repo == nil {
		return nil, fmt.Errorf("sandbox filesystem repository is not configured")
	}
	return repo, nil
}

func committedHeadFromManifestKey(filesystemID, manifestKey string, updatedAt time.Time) (*s0fs.CommittedHead, error) {
	seqText := strings.TrimPrefix(strings.TrimSpace(manifestKey), "manifests/")
	seqText = strings.TrimSuffix(seqText, ".json")
	manifestSeq, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil || manifestSeq == 0 {
		return nil, fmt.Errorf("invalid sandbox filesystem manifest key %q", manifestKey)
	}
	return &s0fs.CommittedHead{
		VolumeID:      filesystemID,
		ManifestSeq:   manifestSeq,
		CheckpointSeq: 0,
		ManifestKey:   manifestKey,
		UpdatedAt:     updatedAt,
	}, nil
}

func sandboxFilesystemStateHasCheckpoint(state *s0fs.SnapshotState) bool {
	return state != nil && state.NextSeq > 1
}

func emptySandboxFilesystemState(now time.Time) *s0fs.SnapshotState {
	return &s0fs.SnapshotState{
		NextSeq:   1,
		NextInode: s0fs.RootInode + 1,
		Nodes: map[uint64]*s0fs.Node{
			s0fs.RootInode: {
				Inode: s0fs.RootInode,
				Type:  s0fs.TypeDirectory,
				Mode:  0o755,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
		},
		Children: map[uint64]map[string]uint64{
			s0fs.RootInode: {},
		},
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]s0fs.FileExtent),
		Segments:  make(map[string]*s0fs.Segment),
	}
}

func cleanupS0FSFilesystem(filesystemID string, cfg *config.StorageProxyConfig) error {
	if cfg == nil || strings.TrimSpace(cfg.CacheDir) == "" || strings.TrimSpace(filesystemID) == "" {
		return nil
	}
	return os.RemoveAll(filepath.Join(cfg.CacheDir, "s0fs-filesystems", filesystemID))
}

func mapSandboxFilesystemError(err error) error {
	if errors.Is(err, db.ErrNotFound) {
		return ErrFilesystemNotFound
	}
	return err
}

func mapSandboxFilesystemSnapshotError(err error) error {
	if errors.Is(err, db.ErrNotFound) {
		return ErrFilesystemSnapshotNotFound
	}
	return err
}
