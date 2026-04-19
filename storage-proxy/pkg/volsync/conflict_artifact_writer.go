package volsync

import (
	"context"
	"encoding/json"
	"fmt"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/legacyfs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

type ConflictArtifactWriter struct {
	volMgr mountedVolumeManager
	logger *logrus.Logger
}

type conflictArtifactPayload struct {
	Version         string           `json:"version"`
	ConflictID      string           `json:"conflict_id"`
	VolumeID        string           `json:"volume_id"`
	ReplicaID       *string          `json:"replica_id,omitempty"`
	Path            string           `json:"path"`
	ArtifactPath    string           `json:"artifact_path"`
	IncomingPath    *string          `json:"incoming_path,omitempty"`
	IncomingOldPath *string          `json:"incoming_old_path,omitempty"`
	ExistingSeq     *int64           `json:"existing_seq,omitempty"`
	Reason          string           `json:"reason"`
	Status          string           `json:"status"`
	Metadata        *json.RawMessage `json:"metadata,omitempty"`
	GeneratedAt     string           `json:"generated_at"`
}

func NewConflictArtifactWriter(volMgr mountedVolumeManager, logger *logrus.Logger) *ConflictArtifactWriter {
	return &ConflictArtifactWriter{
		volMgr: volMgr,
		logger: logger,
	}
}

func (w *ConflictArtifactWriter) MaterializeConflict(ctx context.Context, volumeRecord *db.SandboxVolume, conflict *db.SyncConflict) (*ArtifactMaterialization, error) {
	if w == nil || w.volMgr == nil || volumeRecord == nil || conflict == nil {
		return nil, nil
	}

	volCtx, sessionID, err := ensureMountedVolume(ctx, w.volMgr, w.logger, volumeRecord)
	if err != nil {
		return nil, err
	}
	if sessionID != "" {
		defer cleanupMountedVolume(w.volMgr, w.logger, volumeRecord.ID, sessionID)
	}

	payload, err := buildConflictArtifactPayload(conflict)
	if err != nil {
		return nil, err
	}
	if err := writeArtifactFile(volCtx, conflict.ArtifactPath, payload); err != nil {
		return nil, err
	}
	return &ArtifactMaterialization{SizeBytes: int64(len(payload))}, nil
}

func buildConflictArtifactPayload(conflict *db.SyncConflict) ([]byte, error) {
	payload := conflictArtifactPayload{
		Version:         "sandbox0.sync_conflict_artifact.v1",
		ConflictID:      conflict.ID,
		VolumeID:        conflict.VolumeID,
		ReplicaID:       conflict.ReplicaID,
		Path:            conflict.Path,
		ArtifactPath:    conflict.ArtifactPath,
		IncomingPath:    conflict.IncomingPath,
		IncomingOldPath: conflict.IncomingOldPath,
		ExistingSeq:     conflict.ExistingSeq,
		Reason:          conflict.Reason,
		Status:          conflict.Status,
		Metadata:        conflict.Metadata,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	return json.MarshalIndent(payload, "", "  ")
}

func writeArtifactFile(volCtx *volume.VolumeContext, artifactPath string, payload []byte) error {
	parentIno, baseName, targetIno, targetAttr, err := ensureArtifactParent(volCtx, artifactPath)
	if err != nil {
		return err
	}
	if targetAttr != nil {
		if targetAttr.Typ == fsmeta.TypeDirectory {
			return fmt.Errorf("artifact path %q points to an existing directory", artifactPath)
		}
		vfsCtx := legacyfs.NewLogContext(fsmeta.Background())
		if st := volCtx.VFS.Unlink(vfsCtx, parentIno, baseName); st != 0 && st != syscall.ENOENT {
			return fmt.Errorf("unlink existing artifact %q: %w", artifactPath, syscall.Errno(st))
		}
		_ = targetIno
	}

	vfsCtx := legacyfs.NewLogContext(fsmeta.Background())
	entry, handleID, errno := volCtx.VFS.Create(vfsCtx, parentIno, baseName, 0o644, 0, syscall.O_WRONLY)
	if errno != 0 {
		return fmt.Errorf("create artifact %q: %w", artifactPath, syscall.Errno(errno))
	}
	defer volCtx.VFS.Release(vfsCtx, entry.Inode, handleID)

	if len(payload) == 0 {
		return nil
	}
	if errno := volCtx.VFS.Write(vfsCtx, entry.Inode, payload, 0, handleID); errno != 0 {
		return fmt.Errorf("write artifact %q: %w", artifactPath, syscall.Errno(errno))
	}
	return nil
}

func ensureArtifactParent(volCtx *volume.VolumeContext, artifactPath string) (fsmeta.Ino, string, fsmeta.Ino, *fsmeta.Attr, error) {
	parentIno, baseName, err := ensureLogicalParent(volCtx, fsmeta.Background(), artifactPath)
	if err != nil {
		return 0, "", 0, nil, err
	}
	var targetIno fsmeta.Ino
	targetAttr := &fsmeta.Attr{}
	errno := volCtx.Meta.Lookup(fsmeta.Background(), parentIno, baseName, &targetIno, targetAttr, false)
	if errno == syscall.ENOENT {
		return parentIno, baseName, 0, nil, nil
	}
	if errno != 0 {
		return 0, "", 0, nil, fmt.Errorf("lookup artifact target %q: %w", artifactPath, syscall.Errno(errno))
	}
	return parentIno, baseName, targetIno, targetAttr, nil
}
