package volsync

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

const (
	entryKindFile      = "file"
	entryKindDirectory = "directory"
)

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
	volCtx, sessionID, err := ensureMountedVolume(ctx, a.volMgr, a.logger, volumeRecord)
	if err != nil {
		return err
	}
	if sessionID != "" {
		defer cleanupMountedVolume(a.volMgr, a.logger, volumeRecord.ID, sessionID)
	}

	switch NormalizeEventType(change.EventType) {
	case db.SyncEventCreate:
		return a.applyCreate(volCtx, change)
	case db.SyncEventWrite:
		return a.applyWrite(volCtx, change)
	case db.SyncEventRemove:
		return a.applyRemove(volCtx, change)
	case db.SyncEventRename:
		return a.applyRename(volCtx, change)
	case db.SyncEventChmod:
		return a.applyChmod(volCtx, change)
	case db.SyncEventInvalidate:
		return nil
	default:
		return ErrInvalidChange
	}
}

func (a *VolumeChangeApplier) applyCreate(volCtx *volume.VolumeContext, change ChangeRequest) error {
	switch normalizeEntryKind(change.EntryKind) {
	case entryKindDirectory:
		parentIno, baseName, err := ensureLogicalParent(volCtx, change.Path)
		if err != nil {
			return err
		}
		var inode meta.Ino
		var attr meta.Attr
		errno := volCtx.Meta.Mkdir(meta.Background(), parentIno, baseName, uint16(defaultMode(change.Mode, 0o755)), 0, 0, &inode, &attr)
		if errno == syscall.EEXIST {
			return nil
		}
		if errno != 0 {
			return fmt.Errorf("mkdir %q: %w", change.Path, syscall.Errno(errno))
		}
		return nil
	case entryKindFile:
		return a.writeFile(volCtx, change.Path, change.ContentBase64, defaultMode(change.Mode, 0o644), true)
	default:
		return ErrInvalidChange
	}
}

func (a *VolumeChangeApplier) applyWrite(volCtx *volume.VolumeContext, change ChangeRequest) error {
	if change.ContentBase64 == nil {
		return ErrInvalidChange
	}
	return a.writeFile(volCtx, change.Path, change.ContentBase64, defaultMode(change.Mode, 0o644), false)
}

func (a *VolumeChangeApplier) applyRemove(volCtx *volume.VolumeContext, change ChangeRequest) error {
	parentIno, _, baseName, targetAttr, err := lookupLogicalPath(volCtx, change.Path)
	if err != nil {
		if err == errLogicalPathNotFound {
			return nil
		}
		return err
	}
	if targetAttr == nil {
		return nil
	}
	var removeCount uint64
	errno := volCtx.Meta.Remove(meta.Background(), parentIno, baseName, true, 4, &removeCount)
	if errno == syscall.ENOENT {
		return nil
	}
	if errno != 0 {
		return fmt.Errorf("remove %q: %w", change.Path, syscall.Errno(errno))
	}
	return nil
}

func (a *VolumeChangeApplier) applyRename(volCtx *volume.VolumeContext, change ChangeRequest) error {
	if strings.TrimSpace(change.OldPath) == "" || strings.TrimSpace(change.Path) == "" {
		return ErrInvalidChange
	}
	oldParentIno, _, oldBaseName, _, err := lookupLogicalPath(volCtx, change.OldPath)
	if err != nil {
		return err
	}
	newParentIno, newBaseName, err := ensureLogicalParent(volCtx, change.Path)
	if err != nil {
		return err
	}
	vfsCtx := vfs.NewLogContext(meta.Background())
	errno := volCtx.VFS.Rename(vfsCtx, oldParentIno, oldBaseName, newParentIno, newBaseName, 0)
	if errno != 0 {
		return fmt.Errorf("rename %q -> %q: %w", change.OldPath, change.Path, syscall.Errno(errno))
	}
	return nil
}

func (a *VolumeChangeApplier) applyChmod(volCtx *volume.VolumeContext, change ChangeRequest) error {
	if change.Mode == nil {
		return ErrInvalidChange
	}
	_, targetIno, _, targetAttr, err := lookupLogicalPath(volCtx, change.Path)
	if err != nil {
		return err
	}
	if targetAttr == nil {
		return ErrInvalidChange
	}
	clonedAttr := *targetAttr
	clonedAttr.Mode = uint16(*change.Mode)
	errno := volCtx.Meta.SetAttr(meta.Background(), targetIno, meta.SetAttrMode, 0, &clonedAttr)
	if errno != 0 {
		return fmt.Errorf("chmod %q: %w", change.Path, syscall.Errno(errno))
	}
	return nil
}

func (a *VolumeChangeApplier) writeFile(volCtx *volume.VolumeContext, logicalPath string, contentBase64 *string, mode uint32, createOnly bool) error {
	content, err := decodeContent(contentBase64)
	if err != nil {
		return err
	}
	parentIno, baseName, err := ensureLogicalParent(volCtx, logicalPath)
	if err != nil {
		return err
	}

	vfsCtx := vfs.NewLogContext(meta.Background())

	if createOnly {
		var existingIno meta.Ino
		var existingAttr meta.Attr
		errno := volCtx.Meta.Lookup(meta.Background(), parentIno, baseName, &existingIno, &existingAttr, false)
		if errno == 0 {
			return nil
		}
		if errno != syscall.ENOENT {
			return fmt.Errorf("lookup create target %q: %w", logicalPath, syscall.Errno(errno))
		}
	}

	entry, handleID, errno := volCtx.VFS.Create(vfsCtx, parentIno, baseName, uint16(mode), 0, syscall.O_WRONLY)
	if errno != 0 {
		if errno == syscall.EEXIST && !createOnly {
			_, targetIno, _, targetAttr, lookupErr := lookupLogicalPath(volCtx, logicalPath)
			if lookupErr != nil {
				return lookupErr
			}
			if targetAttr == nil || targetAttr.Typ == meta.TypeDirectory {
				return ErrInvalidChange
			}
			_, handleID, errno = volCtx.VFS.Open(vfsCtx, targetIno, syscall.O_WRONLY)
			if errno != 0 {
				return fmt.Errorf("open existing file %q: %w", logicalPath, syscall.Errno(errno))
			}
			entry = &meta.Entry{Inode: targetIno, Attr: targetAttr}
		} else {
			return fmt.Errorf("create file %q: %w", logicalPath, syscall.Errno(errno))
		}
	}
	defer volCtx.VFS.Release(vfsCtx, entry.Inode, handleID)

	if errno := volCtx.Meta.SetAttr(meta.Background(), entry.Inode, meta.SetAttrSize, 0, &meta.Attr{Length: uint64(len(content))}); errno != 0 {
		return fmt.Errorf("truncate file %q: %w", logicalPath, syscall.Errno(errno))
	}
	if len(content) == 0 {
		return nil
	}
	if errno := volCtx.VFS.Write(vfsCtx, entry.Inode, content, 0, handleID); errno != 0 {
		return fmt.Errorf("write file %q: %w", logicalPath, syscall.Errno(errno))
	}
	return nil
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
