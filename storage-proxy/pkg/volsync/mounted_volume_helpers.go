package volsync

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

var errLogicalPathNotFound = errors.New("logical path not found")

type mountedVolumeManager interface {
	MountVolume(ctx context.Context, s3Prefix, volumeID, teamID string, accessMode volume.AccessMode) (string, time.Time, error)
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
	sessionID, _, err := volMgr.MountVolume(ctx, prefix, volumeRecord.ID, volumeRecord.TeamID, volume.NormalizeAccessMode(volumeRecord.AccessMode))
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

func logicalRootInode(volCtx *volume.VolumeContext) fsmeta.Ino {
	if volCtx == nil || volCtx.RootInode == 0 {
		return fsmeta.RootInode
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

func lookupLogicalPath(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, raw string) (fsmeta.Ino, fsmeta.Ino, string, *fsmeta.Attr, error) {
	if volCtx == nil {
		return 0, 0, "", nil, fmt.Errorf("volume context is nil")
	}
	if volCtx.S0FS != nil {
		return lookupLogicalPathS0FS(volCtx, raw)
	}
	return 0, 0, "", nil, unsupportedVolumeBackend(volCtx)
}

func ensureLogicalParent(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, raw string) (fsmeta.Ino, string, error) {
	if volCtx == nil {
		return 0, "", fmt.Errorf("volume context is nil")
	}
	if volCtx.S0FS != nil {
		return ensureLogicalParentS0FS(volCtx, raw)
	}
	return 0, "", unsupportedVolumeBackend(volCtx)
}

func unsupportedVolumeBackend(volCtx *volume.VolumeContext) error {
	if volCtx == nil {
		return fmt.Errorf("volume context is nil")
	}
	backend := strings.TrimSpace(volCtx.Backend)
	if backend == "" {
		backend = "unknown"
	}
	return fmt.Errorf("storage backend %s is not supported", backend)
}

func lookupLogicalPathS0FS(volCtx *volume.VolumeContext, raw string) (fsmeta.Ino, fsmeta.Ino, string, *fsmeta.Attr, error) {
	parts, err := splitLogicalPath(raw)
	if err != nil {
		return 0, 0, "", nil, err
	}
	current := uint64(logicalRootInode(volCtx))
	for i, part := range parts {
		node, err := volCtx.S0FS.Lookup(current, part)
		if err != nil {
			if err == s0fs.ErrNotFound {
				return fsmeta.Ino(current), 0, part, nil, errLogicalPathNotFound
			}
			return 0, 0, "", nil, fmt.Errorf("lookup %q: %w", part, err)
		}
		if i == len(parts)-1 {
			attr := s0fsNodeToAttr(node)
			return fsmeta.Ino(current), fsmeta.Ino(node.Inode), part, attr, nil
		}
		current = node.Inode
	}
	return 0, 0, "", nil, fmt.Errorf("logical path %q resolution failed", raw)
}

func ensureLogicalParentS0FS(volCtx *volume.VolumeContext, raw string) (fsmeta.Ino, string, error) {
	parts, err := splitLogicalPath(raw)
	if err != nil {
		return 0, "", err
	}
	baseName := parts[len(parts)-1]
	current := uint64(logicalRootInode(volCtx))
	for _, part := range parts[:len(parts)-1] {
		node, err := volCtx.S0FS.Lookup(current, part)
		if err != nil {
			if err != s0fs.ErrNotFound {
				return 0, "", fmt.Errorf("lookup %q: %w", part, err)
			}
			node, err = volCtx.S0FS.Mkdir(current, part, 0o755)
			if err != nil && err != s0fs.ErrExists {
				return 0, "", fmt.Errorf("mkdir %q: %w", part, err)
			}
			if err == s0fs.ErrExists {
				node, err = volCtx.S0FS.Lookup(current, part)
				if err != nil {
					return 0, "", fmt.Errorf("lookup after mkdir %q: %w", part, err)
				}
			}
		}
		current = node.Inode
	}
	return fsmeta.Ino(current), baseName, nil
}

func s0fsNodeToAttr(node *s0fs.Node) *fsmeta.Attr {
	if node == nil {
		return nil
	}
	attr := &fsmeta.Attr{
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
	switch node.Type {
	case s0fs.TypeDirectory:
		attr.Typ = fsmeta.TypeDirectory
	case s0fs.TypeSymlink:
		attr.Typ = fsmeta.TypeSymlink
	default:
		attr.Typ = fsmeta.TypeFile
	}
	return attr
}
