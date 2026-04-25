package volsync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
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
	if volCtx == nil || volCtx.S0FS == nil {
		return unsupportedVolumeBackend(volCtx)
	}
	if targetAttr != nil {
		if targetAttr.Typ == fsmeta.TypeDirectory {
			return fmt.Errorf("artifact path %q points to an existing directory", artifactPath)
		}
		if err := volCtx.S0FS.Unlink(uint64(parentIno), baseName); err != nil && err != s0fs.ErrNotFound {
			return fmt.Errorf("unlink existing artifact %q: %w", artifactPath, err)
		}
		_ = targetIno
	}

	node, err := volCtx.S0FS.CreateFile(uint64(parentIno), baseName, 0o644)
	if err != nil {
		return fmt.Errorf("create artifact %q: %w", artifactPath, err)
	}
	if len(payload) == 0 {
		return nil
	}
	if _, err := volCtx.S0FS.Write(node.Inode, 0, payload); err != nil {
		return fmt.Errorf("write artifact %q: %w", artifactPath, err)
	}
	return nil
}

func ensureArtifactParent(volCtx *volume.VolumeContext, artifactPath string) (fsmeta.Ino, string, fsmeta.Ino, *fsmeta.Attr, error) {
	parentIno, baseName, err := ensureLogicalParent(volCtx, fsmeta.Background(), artifactPath)
	if err != nil {
		return 0, "", 0, nil, err
	}
	if volCtx == nil || volCtx.S0FS == nil {
		return 0, "", 0, nil, unsupportedVolumeBackend(volCtx)
	}
	node, err := volCtx.S0FS.Lookup(uint64(parentIno), baseName)
	if err == s0fs.ErrNotFound {
		return parentIno, baseName, 0, nil, nil
	}
	if err != nil {
		return 0, "", 0, nil, fmt.Errorf("lookup artifact target %q: %w", artifactPath, err)
	}
	return parentIno, baseName, fsmeta.Ino(node.Inode), s0fsNodeToAttr(node), nil
}
