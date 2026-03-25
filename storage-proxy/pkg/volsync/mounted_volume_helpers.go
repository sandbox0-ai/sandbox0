package volsync

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

var errLogicalPathNotFound = errors.New("logical path not found")

type mountedVolumeManager interface {
	MountVolume(ctx context.Context, s3Prefix, volumeID, teamID string, config *volume.VolumeConfig, accessMode volume.AccessMode) (string, time.Time, error)
	UnmountVolume(ctx context.Context, volumeID, sessionID string) error
	GetVolume(volumeID string) (*volume.VolumeContext, error)
}

func ensureMountedVolume(ctx context.Context, volMgr mountedVolumeManager, logger *logrus.Logger, volumeRecord *db.SandboxVolume) (*volume.VolumeContext, string, error) {
	if volMgr == nil || volumeRecord == nil {
		return nil, "", fmt.Errorf("mounted volume manager or volume record is nil")
	}

	volCtx, err := volMgr.GetVolume(volumeRecord.ID)
	if err == nil {
		return volCtx, "", nil
	}

	prefix, err := naming.S3VolumePrefix(volumeRecord.TeamID, volumeRecord.ID)
	if err != nil {
		return nil, "", fmt.Errorf("build s3 prefix: %w", err)
	}
	sessionID, _, err := volMgr.MountVolume(ctx, prefix, volumeRecord.ID, volumeRecord.TeamID, &volume.VolumeConfig{
		CacheSize:  volumeRecord.CacheSize,
		Prefetch:   volumeRecord.Prefetch,
		BufferSize: volumeRecord.BufferSize,
		Writeback:  volumeRecord.Writeback,
	}, volume.NormalizeAccessMode(volumeRecord.AccessMode))
	if err != nil {
		return nil, "", fmt.Errorf("mount volume: %w", err)
	}
	volCtx, err = volMgr.GetVolume(volumeRecord.ID)
	if err != nil {
		_ = volMgr.UnmountVolume(context.Background(), volumeRecord.ID, sessionID)
		return nil, "", fmt.Errorf("get mounted volume: %w", err)
	}
	if logger != nil {
		logger.WithField("volume_id", volumeRecord.ID).Debug("Mounted temporary volume session")
	}
	return volCtx, sessionID, nil
}

func cleanupMountedVolume(volMgr mountedVolumeManager, logger *logrus.Logger, volumeID, sessionID string) {
	if volMgr == nil || sessionID == "" {
		return
	}
	if err := volMgr.UnmountVolume(context.Background(), volumeID, sessionID); err != nil && logger != nil {
		logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to unmount temporary volume session")
	}
}

func logicalRootInode(volCtx *volume.VolumeContext) meta.Ino {
	if volCtx == nil || volCtx.RootInode == 0 {
		return meta.RootInode
	}
	return volCtx.RootInode
}

func cleanLogicalPath(raw string) (string, error) {
	cleaned := path.Clean("/" + strings.TrimSpace(raw))
	if cleaned == "/" {
		return "", fmt.Errorf("logical path %q is invalid", raw)
	}
	return cleaned, nil
}

func splitLogicalPath(raw string) ([]string, error) {
	cleaned, err := cleanLogicalPath(raw)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	if len(parts) == 0 {
		return nil, fmt.Errorf("logical path %q is invalid", raw)
	}
	return parts, nil
}

func lookupLogicalPath(volCtx *volume.VolumeContext, raw string) (meta.Ino, meta.Ino, string, *meta.Attr, error) {
	if volCtx == nil {
		return 0, 0, "", nil, fmt.Errorf("volume context is nil")
	}
	parts, err := splitLogicalPath(raw)
	if err != nil {
		return 0, 0, "", nil, err
	}

	current := logicalRootInode(volCtx)
	var attr meta.Attr
	for i, part := range parts {
		var next meta.Ino
		errno := volCtx.Meta.Lookup(meta.Background(), current, part, &next, &attr, false)
		if errno == syscall.ENOENT {
			return current, 0, part, nil, errLogicalPathNotFound
		}
		if errno != 0 {
			return 0, 0, "", nil, fmt.Errorf("lookup %q: %w", part, syscall.Errno(errno))
		}
		if i == len(parts)-1 {
			targetAttr := attr
			return current, next, part, &targetAttr, nil
		}
		current = next
	}

	return 0, 0, "", nil, fmt.Errorf("logical path %q resolution failed", raw)
}

func ensureLogicalParent(volCtx *volume.VolumeContext, raw string) (meta.Ino, string, error) {
	if volCtx == nil {
		return 0, "", fmt.Errorf("volume context is nil")
	}
	parts, err := splitLogicalPath(raw)
	if err != nil {
		return 0, "", err
	}
	baseName := parts[len(parts)-1]
	current := logicalRootInode(volCtx)
	var attr meta.Attr
	for _, part := range parts[:len(parts)-1] {
		var next meta.Ino
		errno := volCtx.Meta.Lookup(meta.Background(), current, part, &next, &attr, false)
		if errno == syscall.ENOENT {
			errno = volCtx.Meta.Mkdir(meta.Background(), current, part, 0o755, 0, 0, &next, &attr)
		}
		if errno != 0 {
			return 0, "", fmt.Errorf("ensure parent %q: %w", part, syscall.Errno(errno))
		}
		current = next
	}
	return current, baseName, nil
}
