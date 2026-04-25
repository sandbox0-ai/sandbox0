package volsync

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

const (
	entryKindFile      = "file"
	entryKindDirectory = "directory"
)

var errDefaultPosixIdentity = errors.New("volume default_posix_uid/default_posix_gid is required for sync apply")

const maxPOSIXID = int64(^uint32(0))

type replicaChangeApplier interface {
	ApplyChange(context.Context, *db.SandboxVolume, ChangeRequest) error
}

type VolumeChangeApplier struct {
	volMgr mountedVolumeManager
	logger *logrus.Logger
}

func NewVolumeChangeApplier(volMgr mountedVolumeManager, logger *logrus.Logger) *VolumeChangeApplier {
	return &VolumeChangeApplier{
		volMgr: volMgr,
		logger: logger,
	}
}

func (a *VolumeChangeApplier) ApplyChange(ctx context.Context, volumeRecord *db.SandboxVolume, change ChangeRequest) error {
	if a == nil || a.volMgr == nil || volumeRecord == nil {
		return nil
	}
	metaCtx, err := defaultSyncMetaContext(volumeRecord)
	if err != nil {
		return err
	}
	volCtx, sessionID, err := ensureMountedVolume(ctx, a.volMgr, a.logger, volumeRecord)
	if err != nil {
		return err
	}
	if err := volume.EnsureLazyRootPosixIdentity(volCtx, metaCtx.Uid(), metaCtx.Gid()); err != nil {
		return err
	}
	if sessionID != "" {
		defer cleanupMountedVolume(a.volMgr, a.logger, volumeRecord.ID, sessionID)
	}

	switch NormalizeEventType(change.EventType) {
	case db.SyncEventCreate:
		return a.applyCreate(volCtx, metaCtx, change)
	case db.SyncEventWrite:
		return a.applyWrite(volCtx, metaCtx, change)
	case db.SyncEventRemove:
		return a.applyRemove(volCtx, metaCtx, change)
	case db.SyncEventRename:
		return a.applyRename(volCtx, metaCtx, change)
	case db.SyncEventChmod:
		return a.applyChmod(volCtx, metaCtx, change)
	case db.SyncEventInvalidate:
		return nil
	default:
		return ErrInvalidChange
	}
}

func defaultSyncMetaContext(volumeRecord *db.SandboxVolume) (fsmeta.Context, error) {
	if volumeRecord == nil || volumeRecord.DefaultPosixUID == nil || volumeRecord.DefaultPosixGID == nil {
		return nil, errDefaultPosixIdentity
	}
	if *volumeRecord.DefaultPosixUID < 0 || *volumeRecord.DefaultPosixUID > maxPOSIXID {
		return nil, fmt.Errorf("default_posix_uid out of range: %d", *volumeRecord.DefaultPosixUID)
	}
	if *volumeRecord.DefaultPosixGID < 0 || *volumeRecord.DefaultPosixGID > maxPOSIXID {
		return nil, fmt.Errorf("default_posix_gid out of range: %d", *volumeRecord.DefaultPosixGID)
	}
	return fsmeta.NewContext(0, uint32(*volumeRecord.DefaultPosixUID), []uint32{uint32(*volumeRecord.DefaultPosixGID)}), nil
}

func (a *VolumeChangeApplier) applyCreate(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, change ChangeRequest) error {
	switch normalizeEntryKind(change.EntryKind) {
	case entryKindDirectory:
		parentIno, baseName, err := ensureLogicalParent(volCtx, metaCtx, change.Path)
		if err != nil {
			return err
		}
		if volCtx.S0FS != nil {
			node, err := volCtx.S0FS.Mkdir(uint64(parentIno), baseName, defaultMode(change.Mode, 0o755))
			if err != nil && err != s0fs.ErrExists {
				return fmt.Errorf("mkdir %q: %w", change.Path, err)
			}
			if err == nil {
				if ownerErr := volCtx.S0FS.SetOwner(node.Inode, metaCtx.Uid(), metaCtx.Gid()); ownerErr != nil {
					return fmt.Errorf("set directory owner %q: %w", change.Path, ownerErr)
				}
			}
			return nil
		}
		return unsupportedVolumeBackend(volCtx)
	case entryKindFile:
		return a.writeFile(volCtx, metaCtx, change.Path, change.ContentBase64, defaultMode(change.Mode, 0o644), true)
	default:
		return ErrInvalidChange
	}
}

func (a *VolumeChangeApplier) applyWrite(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, change ChangeRequest) error {
	if change.ContentBase64 == nil {
		return ErrInvalidChange
	}
	return a.writeFile(volCtx, metaCtx, change.Path, change.ContentBase64, defaultMode(change.Mode, 0o644), false)
}

func (a *VolumeChangeApplier) applyRemove(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, change ChangeRequest) error {
	parentIno, _, baseName, targetAttr, err := lookupLogicalPath(volCtx, metaCtx, change.Path)
	if err != nil {
		if err == errLogicalPathNotFound {
			return nil
		}
		return err
	}
	if targetAttr == nil {
		return nil
	}
	if volCtx.S0FS != nil {
		if targetAttr.Typ == fsmeta.TypeDirectory {
			if err := volCtx.S0FS.RemoveDir(uint64(parentIno), baseName); err != nil && err != s0fs.ErrNotFound {
				return fmt.Errorf("remove %q: %w", change.Path, err)
			}
			return nil
		}
		if err := volCtx.S0FS.Unlink(uint64(parentIno), baseName); err != nil && err != s0fs.ErrNotFound {
			return fmt.Errorf("remove %q: %w", change.Path, err)
		}
		return nil
	}
	return unsupportedVolumeBackend(volCtx)
}

func (a *VolumeChangeApplier) applyRename(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, change ChangeRequest) error {
	if strings.TrimSpace(change.OldPath) == "" || strings.TrimSpace(change.Path) == "" {
		return ErrInvalidChange
	}
	oldParentIno, _, oldBaseName, _, err := lookupLogicalPath(volCtx, metaCtx, change.OldPath)
	if err != nil {
		return err
	}
	newParentIno, newBaseName, err := ensureLogicalParent(volCtx, metaCtx, change.Path)
	if err != nil {
		return err
	}
	if volCtx.S0FS != nil {
		if err := volCtx.S0FS.Rename(uint64(oldParentIno), oldBaseName, uint64(newParentIno), newBaseName); err != nil {
			return fmt.Errorf("rename %q -> %q: %w", change.OldPath, change.Path, err)
		}
		return nil
	}
	return unsupportedVolumeBackend(volCtx)
}

func (a *VolumeChangeApplier) applyChmod(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, change ChangeRequest) error {
	if change.Mode == nil {
		return ErrInvalidChange
	}
	_, targetIno, _, targetAttr, err := lookupLogicalPath(volCtx, metaCtx, change.Path)
	if err != nil {
		return err
	}
	if targetAttr == nil {
		return ErrInvalidChange
	}
	if volCtx.S0FS != nil {
		if err := volCtx.S0FS.SetMode(uint64(targetIno), uint32(*change.Mode)); err != nil {
			return fmt.Errorf("chmod %q: %w", change.Path, err)
		}
		return nil
	}
	return unsupportedVolumeBackend(volCtx)
}

func (a *VolumeChangeApplier) writeFile(volCtx *volume.VolumeContext, metaCtx fsmeta.Context, logicalPath string, contentBase64 *string, mode uint32, createOnly bool) error {
	content, err := decodeContent(contentBase64)
	if err != nil {
		return err
	}
	parentIno, baseName, err := ensureLogicalParent(volCtx, metaCtx, logicalPath)
	if err != nil {
		return err
	}
	if volCtx.S0FS != nil {
		_, targetIno, _, targetAttr, lookupErr := lookupLogicalPath(volCtx, metaCtx, logicalPath)
		switch {
		case lookupErr == nil && createOnly:
			return nil
		case lookupErr == nil:
			if targetAttr == nil || targetAttr.Typ == fsmeta.TypeDirectory {
				return ErrInvalidChange
			}
			if err := volCtx.S0FS.Truncate(uint64(targetIno), uint64(len(content))); err != nil {
				return fmt.Errorf("truncate file %q: %w", logicalPath, err)
			}
			if len(content) == 0 {
				return nil
			}
			if _, err := volCtx.S0FS.Write(uint64(targetIno), 0, content); err != nil {
				return fmt.Errorf("write file %q: %w", logicalPath, err)
			}
			return nil
		case lookupErr != errLogicalPathNotFound:
			return lookupErr
		}

		node, err := volCtx.S0FS.CreateFile(uint64(parentIno), baseName, mode)
		if err != nil {
			return fmt.Errorf("create file %q: %w", logicalPath, err)
		}
		if err := volCtx.S0FS.SetOwner(node.Inode, metaCtx.Uid(), metaCtx.Gid()); err != nil {
			return fmt.Errorf("set file owner %q: %w", logicalPath, err)
		}
		if err := volCtx.S0FS.Truncate(node.Inode, uint64(len(content))); err != nil {
			return fmt.Errorf("truncate file %q: %w", logicalPath, err)
		}
		if len(content) == 0 {
			return nil
		}
		if _, err := volCtx.S0FS.Write(node.Inode, 0, content); err != nil {
			return fmt.Errorf("write file %q: %w", logicalPath, err)
		}
		return nil
	}

	return unsupportedVolumeBackend(volCtx)
}

func normalizeEntryKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", entryKindFile:
		return entryKindFile
	case entryKindDirectory:
		return entryKindDirectory
	default:
		return ""
	}
}

func decodeContent(contentBase64 *string) ([]byte, error) {
	if contentBase64 == nil {
		return []byte{}, nil
	}
	payload, err := base64.StdEncoding.DecodeString(*contentBase64)
	if err != nil {
		return nil, fmt.Errorf("decode content_base64: %w", err)
	}
	return payload, nil
}

func defaultMode(mode *uint32, fallback uint32) uint32 {
	if mode == nil || *mode == 0 {
		return fallback
	}
	return *mode
}
