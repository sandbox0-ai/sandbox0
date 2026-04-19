package volume

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sirupsen/logrus"
)

// S0FSBackend mounts the in-process active volume engine.
type S0FSBackend struct {
	logger *logrus.Logger
	config *config.StorageProxyConfig
}

func NewS0FSBackend(logger *logrus.Logger, cfg *config.StorageProxyConfig) *S0FSBackend {
	return &S0FSBackend{
		logger: logger,
		config: cfg,
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
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID: req.VolumeID,
		WALPath:  filepath.Join(cacheDir, "engine.wal"),
	})
	if err != nil {
		return nil, fmt.Errorf("open s0fs engine: %w", err)
	}

	return &VolumeContext{
		VolumeID:  req.VolumeID,
		TeamID:    req.TeamID,
		Backend:   BackendS0FS,
		S0FS:      engine,
		Config:    req.Config,
		Access:    req.AccessMode,
		MountedAt: req.MountedAt,
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}, nil
}

func (b *S0FSBackend) UnmountVolume(_ context.Context, volCtx *VolumeContext) error {
	if volCtx == nil || volCtx.S0FS == nil {
		return nil
	}
	return volCtx.S0FS.Close()
}
