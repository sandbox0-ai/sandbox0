package volume

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sirupsen/logrus"
)

// S0FSBackend mounts the in-process active volume engine.
type S0FSBackend struct {
	logger    *logrus.Logger
	config    *config.StorageProxyConfig
	headStore s0fs.HeadStore
}

func NewS0FSBackend(logger *logrus.Logger, cfg *config.StorageProxyConfig, repo *db.Repository) *S0FSBackend {
	return &S0FSBackend{
		logger:    logger,
		config:    cfg,
		headStore: db.NewS0FSHeadStore(repo),
	}
}

func (b *S0FSBackend) MountVolume(ctx context.Context, req BackendMountRequest) (*VolumeContext, error) {
	if req.TeamID == "" {
		return nil, fmt.Errorf("missing team id for volume mount")
	}
	if b == nil || b.config == nil {
		return nil, fmt.Errorf("storage proxy config is not available")
	}
	cacheDir := filepath.Join(b.config.CacheDir, "s0fs", req.VolumeID)
	remoteStore, err := b.createObjectStorage(req)
	if err != nil {
		return nil, fmt.Errorf("create s0fs object storage: %w", err)
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:    req.VolumeID,
		WALPath:     filepath.Join(cacheDir, "engine.wal"),
		ObjectStore: remoteStore,
		HeadStore:   b.headStore,
	})
	if err != nil {
		return nil, fmt.Errorf("open s0fs engine: %w", err)
	}

	volCtx := &VolumeContext{
		VolumeID:  req.VolumeID,
		TeamID:    req.TeamID,
		Backend:   BackendS0FS,
		S0FS:      engine,
		Access:    req.AccessMode,
		MountedAt: req.MountedAt,
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}
	b.startMaterializer(volCtx)
	return volCtx, nil
}

func (b *S0FSBackend) UnmountVolume(ctx context.Context, volCtx *VolumeContext) error {
	if volCtx == nil || volCtx.S0FS == nil {
		return nil
	}
	if volCtx.materializeCancel != nil {
		volCtx.materializeCancel()
		if volCtx.materializeDone != nil {
			<-volCtx.materializeDone
		}
	}
	if _, err := volCtx.S0FS.SyncMaterialize(ctx); err != nil {
		return fmt.Errorf("materialize s0fs volume: %w", err)
	}
	return volCtx.S0FS.Close()
}

func (b *S0FSBackend) createObjectStorage(req BackendMountRequest) (objectstore.Store, error) {
	if b == nil || b.config == nil || strings.TrimSpace(b.config.S3Bucket) == "" {
		return nil, nil
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:         b.config.ObjectStorageType,
		Bucket:       b.config.S3Bucket,
		Region:       b.config.S3Region,
		Endpoint:     b.config.S3Endpoint,
		AccessKey:    b.config.S3AccessKey,
		SecretKey:    b.config.S3SecretKey,
		SessionToken: b.config.S3SessionToken,
		Metrics:      req.Metrics,
	})
	if err != nil {
		return nil, err
	}
	prefix := strings.Trim(req.S3Prefix, "/")
	if prefix == "" {
		prefix = strings.Trim(req.VolumeID, "/")
	}
	return objectstore.Prefix(store, prefix+"/s0fs/"), nil
}

func (b *S0FSBackend) startMaterializer(volCtx *VolumeContext) {
	if volCtx == nil || volCtx.S0FS == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	volCtx.materializeCancel = cancel
	volCtx.materializeDone = done

	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := volCtx.S0FS.SyncMaterialize(ctx); err != nil {
					b.logger.WithError(err).WithField("volume_id", volCtx.VolumeID).Warn("Failed to materialize s0fs volume")
				}
			}
		}
	}()
}
