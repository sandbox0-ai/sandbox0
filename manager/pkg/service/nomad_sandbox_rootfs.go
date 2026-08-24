package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

// NomadSandboxRootFSStore is the durable block-COW product boundary used by
// the public Nomad snapshot and restore API.
type NomadSandboxRootFSStore interface {
	SandboxRootFSProductStore
	GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error)
	GetRootFSFilesystem(context.Context, string) (*sandboxstore.RootFSFilesystem, error)
	WithSandboxLock(context.Context, string, func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error) error
}

// NomadSandboxRootFSService exposes snapshots only for stable paused
// block-COW heads. Running checkpoints remain owned by explicit lifecycle
// orchestration.
type NomadSandboxRootFSService struct {
	store NomadSandboxRootFSStore
	now   func() time.Time
}

// NewNomadSandboxRootFSService creates a PostgreSQL-only RootFS product
// service.
func NewNomadSandboxRootFSService(
	store NomadSandboxRootFSStore,
	now func() time.Time,
) (*NomadSandboxRootFSService, error) {
	if store == nil {
		return nil, fmt.Errorf("Nomad sandbox rootfs store is required")
	}
	if now == nil {
		now = time.Now
	}
	return &NomadSandboxRootFSService{store: store, now: now}, nil
}

// CreateSandboxRootFSSnapshot pins the current paused durable generation.
func (s *NomadSandboxRootFSService) CreateSandboxRootFSSnapshot(
	ctx context.Context,
	sandboxID, teamID string,
	request *CreateSandboxRootFSSnapshotRequest,
) (*SandboxRootFSSnapshot, error) {
	if request == nil {
		request = &CreateSandboxRootFSSnapshotRequest{}
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if !request.ExpiresAt.IsZero() && !request.ExpiresAt.After(s.now().UTC()) {
		return nil, ErrRootFSSnapshotExpired
	}
	if err := s.requirePausedBlockSandbox(ctx, sandboxID, teamID); err != nil {
		return nil, err
	}

	var snapshot *sandboxstore.RootFSSnapshot
	snapshotID := generateRootFSSnapshotID()
	err := s.store.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateNomadRootFSSandboxRecord(record, sandboxID, teamID, true); err != nil {
			return err
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}
		if activeTxn != nil {
			return apierror.NewConflict("sandbox", sandboxID,
				fmt.Errorf("sandbox lifecycle %s is %s", activeTxn.Kind, activeTxn.Phase))
		}
		creator := sandboxRootFSSnapshotCreator(s.store)
		if txCreator, ok := tx.(sandboxRootFSSnapshotCreator); ok {
			creator = txCreator
		}
		var createErr error
		snapshot, createErr = creator.CreateRootFSSnapshot(lockCtx, &sandboxstore.CreateRootFSSnapshotRequest{
			SandboxID: sandboxID, SnapshotID: snapshotID,
			Name: strings.TrimSpace(request.Name), Description: strings.TrimSpace(request.Description),
			ExpiresAt: request.ExpiresAt,
		})
		return createErr
	})
	if err != nil {
		return nil, err
	}
	return sandboxRootFSSnapshotFromStore(snapshot), nil
}

// ListSandboxRootFSSnapshots lists public snapshots for one owned Nomad
// sandbox.
func (s *NomadSandboxRootFSService) ListSandboxRootFSSnapshots(
	ctx context.Context,
	sandboxID, teamID string,
) (*ListSandboxRootFSSnapshotsResponse, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if err := validateNomadRootFSSandboxRecord(record, sandboxID, teamID, false); err != nil {
		return nil, err
	}
	snapshots, err := s.store.ListRootFSSnapshots(ctx, &sandboxstore.ListRootFSSnapshotsRequest{
		SandboxID: sandboxID, TeamID: teamID,
	})
	if err != nil {
		return nil, err
	}
	result := make([]*SandboxRootFSSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil || template.IsBuildSnapshotID(snapshot.ID) {
			continue
		}
		if err := validateNomadRootFSSnapshot(snapshot); err != nil {
			return nil, err
		}
		result = append(result, sandboxRootFSSnapshotFromStore(snapshot))
	}
	return &ListSandboxRootFSSnapshotsResponse{Snapshots: result, Count: len(result)}, nil
}

// GetSandboxRootFSSnapshot gets one public block-COW snapshot.
func (s *NomadSandboxRootFSService) GetSandboxRootFSSnapshot(
	ctx context.Context,
	snapshotID, teamID string,
) (*SandboxRootFSSnapshot, error) {
	snapshotID = strings.TrimSpace(snapshotID)
	teamID = strings.TrimSpace(teamID)
	if snapshotID == "" || teamID == "" || template.IsBuildSnapshotID(snapshotID) {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	snapshot, err := s.store.GetRootFSSnapshot(ctx, snapshotID, teamID)
	if err != nil {
		return nil, err
	}
	if err := validateNomadRootFSSnapshot(snapshot); err != nil {
		return nil, err
	}
	return sandboxRootFSSnapshotFromStore(snapshot), nil
}

// DeleteSandboxRootFSSnapshot deletes one public block-COW snapshot.
func (s *NomadSandboxRootFSService) DeleteSandboxRootFSSnapshot(
	ctx context.Context,
	snapshotID, teamID string,
) error {
	snapshotID = strings.TrimSpace(snapshotID)
	teamID = strings.TrimSpace(teamID)
	if snapshotID == "" || teamID == "" || template.IsBuildSnapshotID(snapshotID) {
		return sandboxstore.ErrRootFSSnapshotNotFound
	}
	snapshot, err := s.store.GetRootFSSnapshot(ctx, snapshotID, teamID)
	if err != nil {
		return err
	}
	if err := validateNomadRootFSSnapshot(snapshot); err != nil {
		return err
	}
	return s.store.DeleteRootFSSnapshot(ctx, snapshotID, teamID)
}

// RestoreSandboxRootFS installs a snapshot only while the target has no live
// writer or lifecycle transaction.
func (s *NomadSandboxRootFSService) RestoreSandboxRootFS(
	ctx context.Context,
	sandboxID, teamID string,
	request *RestoreSandboxRootFSRequest,
) (*RestoreSandboxRootFSResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	snapshotID := strings.TrimSpace(request.SnapshotID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if snapshotID == "" || template.IsBuildSnapshotID(snapshotID) {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	snapshot, err := s.store.GetRootFSSnapshot(ctx, snapshotID, teamID)
	if err != nil {
		return nil, err
	}
	if err := validateNomadRootFSSnapshot(snapshot); err != nil {
		return nil, err
	}
	err = s.store.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := validateNomadRootFSSandboxRecord(record, sandboxID, teamID, true); err != nil {
			return err
		}
		activeTxn, err := tx.GetActiveLifecycleTxn(lockCtx, sandboxID)
		if err != nil {
			return err
		}
		if activeTxn != nil {
			return apierror.NewConflict("sandbox", sandboxID,
				fmt.Errorf("sandbox lifecycle %s is %s", activeTxn.Kind, activeTxn.Phase))
		}
		restorer := sandboxRootFSRestorer(s.store)
		if txRestorer, ok := tx.(sandboxRootFSRestorer); ok {
			restorer = txRestorer
		}
		_, restoreErr := restorer.RestoreRootFSFromSnapshot(lockCtx, &sandboxstore.RestoreRootFSFromSnapshotRequest{
			SandboxID: sandboxID, SnapshotID: snapshotID, TeamID: teamID,
		})
		return restoreErr
	})
	if err != nil {
		return nil, err
	}
	return &RestoreSandboxRootFSResponse{
		SandboxID: sandboxID, SnapshotID: snapshotID, Status: managerapi.SandboxStatusPaused,
	}, nil
}

func (s *NomadSandboxRootFSService) requirePausedBlockSandbox(ctx context.Context, sandboxID, teamID string) error {
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	if err := validateNomadRootFSSandboxRecord(record, sandboxID, teamID, true); err != nil {
		return err
	}
	filesystem, err := s.store.GetRootFSFilesystem(ctx, sandboxID)
	if err != nil {
		return err
	}
	if filesystem == nil ||
		strings.TrimSpace(filesystem.HeadGenerationID) == "" {
		return fmt.Errorf("%w: sandbox %s has no block-COW head", sandboxstore.ErrRootFSFilesystemNotFound, sandboxID)
	}
	return nil
}

func validateNomadRootFSSandboxRecord(record *sandboxstore.SandboxRecord, sandboxID, teamID string, requirePaused bool) error {
	if err := validateRootFSSandboxRecord(record, sandboxID, teamID, requirePaused); err != nil {
		return err
	}
	return nil
}

func validateNomadRootFSSnapshot(snapshot *sandboxstore.RootFSSnapshot) error {
	if snapshot == nil ||
		strings.TrimSpace(snapshot.HeadGenerationID) == "" {
		return sandboxstore.ErrRootFSSnapshotNotFound
	}
	return nil
}
