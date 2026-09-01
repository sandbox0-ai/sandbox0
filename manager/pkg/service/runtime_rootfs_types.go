package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
)

type SandboxRootFSProductStore interface {
	CreateRootFSSnapshot(context.Context, *sandboxstore.CreateRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error)
	ListRootFSSnapshots(context.Context, *sandboxstore.ListRootFSSnapshotsRequest) ([]*sandboxstore.RootFSSnapshot, error)
	GetRootFSSnapshot(context.Context, string, string) (*sandboxstore.RootFSSnapshot, error)
	DeleteRootFSSnapshot(context.Context, string, string) error
	ForkRootFSFilesystem(context.Context, *sandboxstore.ForkRootFSFilesystemRequest) (*sandboxstore.RootFSFilesystem, error)
	RestoreRootFSFromSnapshot(context.Context, *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error)
}

type sandboxRootFSSnapshotCreator interface {
	CreateRootFSSnapshot(context.Context, *sandboxstore.CreateRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error)
}

type sandboxRootFSRestorer interface {
	RestoreRootFSFromSnapshot(context.Context, *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error)
}

type CreateSandboxRootFSSnapshotRequest struct {
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
	OperationID string    `json:"-"`
	StartedAt   time.Time `json:"-"`
}

// SandboxRunningRootFSSnapshotter publishes an immutable checkpoint from the
// exact live writer without replacing or pausing the source runtime.
type SandboxRunningRootFSSnapshotter interface {
	CreateRunningSandboxRootFSSnapshot(context.Context, string, string, *CreateSandboxRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error)
}

type SandboxRootFSSnapshot struct {
	ID          string    `json:"id"`
	SandboxID   string    `json:"sandbox_id"`
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

type ListSandboxRootFSSnapshotsResponse struct {
	Snapshots []*SandboxRootFSSnapshot `json:"snapshots"`
	Count     int                      `json:"count"`
}

type RestoreSandboxRootFSRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

type RestoreSandboxRootFSResponse struct {
	SandboxID  string `json:"sandbox_id"`
	SnapshotID string `json:"snapshot_id"`
	Status     string `json:"status"`
}

type ForkSandboxRequest struct {
	Config      *ForkSandboxConfig `json:"config,omitempty"`
	OperationID string             `json:"-"`
	StartedAt   time.Time          `json:"-"`
}

type ForkSandboxConfig struct {
	TTL     *int32 `json:"ttl,omitempty"`
	HardTTL *int32 `json:"hard_ttl,omitempty"`
}

type ForkSandboxResponse struct {
	SourceSandboxID string              `json:"source_sandbox_id"`
	Sandbox         *managerapi.Sandbox `json:"sandbox"`
}

type RebaseSandboxRootFSRequest struct {
	TargetBaseArtifactDigest string    `json:"target_base_artifact_digest"`
	RollbackTTL              *int32    `json:"rollback_ttl,omitempty"`
	OperationID              string    `json:"-"`
	StartedAt                time.Time `json:"-"`
}

type RebaseSandboxRootFSResponse struct {
	SandboxID          string    `json:"sandbox_id"`
	GenerationID       string    `json:"generation_id"`
	BaseArtifactDigest string    `json:"base_artifact_digest"`
	RollbackExpiresAt  time.Time `json:"rollback_expires_at"`
	Status             string    `json:"status"`
}

func validateRootFSSandboxRecord(record *sandboxstore.SandboxRecord, sandboxID, teamID string, requirePaused bool) error {
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return apierror.NewNotFound("sandbox", sandboxID)
	}
	if record.TeamID != teamID {
		return apierror.NewForbidden("sandbox", sandboxID, fmt.Errorf("sandbox belongs to a different team"))
	}
	if requirePaused && record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		return fmt.Errorf("%w: current desired state is %s", ErrSandboxRootFSRequiresPausedSandbox, record.DesiredState)
	}
	return nil
}

func validateRootFSSourceSandboxRecord(record *sandboxstore.SandboxRecord, sandboxID, teamID string, now time.Time) error {
	if err := validateRootFSSandboxRecord(record, sandboxID, teamID, false); err != nil {
		return err
	}
	if record.DesiredState != sandboxstore.SandboxDesiredStatePaused && record.DesiredState != sandboxstore.SandboxDesiredStateActive {
		return fmt.Errorf("%w: current desired state is %s", ErrSandboxRootFSSourceRequiresRunningOrPaused, record.DesiredState)
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(now) {
		return apierror.NewNotFound("sandbox", sandboxID)
	}
	return nil
}

func sandboxRootFSSnapshotFromStore(snapshot *sandboxstore.RootFSSnapshot) *SandboxRootFSSnapshot {
	if snapshot == nil {
		return nil
	}
	return &SandboxRootFSSnapshot{
		ID: snapshot.ID, SandboxID: snapshot.SourceSandboxID,
		Name: snapshot.Name, Description: snapshot.Description,
		CreatedAt: snapshot.CreatedAt, ExpiresAt: snapshot.ExpiresAt,
	}
}

func generateRootFSSnapshotID() string {
	return "rootfs-snapshot-" + uuid.NewString()
}
