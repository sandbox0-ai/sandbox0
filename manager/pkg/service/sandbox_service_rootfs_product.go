package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
)

var ErrSandboxRootFSStoreUnavailable = errors.New("sandbox rootfs store is unavailable")
var ErrSandboxRootFSRequiresPausedSandbox = errors.New("sandbox rootfs operation requires a paused sandbox")

type SandboxRootFSProductStore interface {
	CreateRootFSSnapshot(ctx context.Context, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error)
	ListRootFSSnapshots(ctx context.Context, req *ListRootFSSnapshotsRequest) ([]*RootFSSnapshot, error)
	GetRootFSSnapshot(ctx context.Context, snapshotID, teamID string) (*RootFSSnapshot, error)
	DeleteRootFSSnapshot(ctx context.Context, snapshotID, teamID string) error
	ForkRootFSFilesystem(ctx context.Context, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error)
	RestoreRootFSFromSnapshot(ctx context.Context, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error)
}

type CreateSandboxRootFSSnapshotRequest struct {
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
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

type ForkSandboxRequest struct{}

type ForkSandboxResponse struct {
	SourceSandboxID string   `json:"source_sandbox_id"`
	Sandbox         *Sandbox `json:"sandbox"`
}

func (s *SandboxService) CreateSandboxRootFSSnapshot(ctx context.Context, sandboxID, teamID string, req *CreateSandboxRootFSSnapshotRequest) (*SandboxRootFSSnapshot, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	if req == nil {
		req = &CreateSandboxRootFSSnapshotRequest{}
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	var snapshot *RootFSSnapshot
	err = s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, _ SandboxStoreTx, record *SandboxRecord) error {
		if err := validateRootFSSandboxRecord(record, sandboxID, teamID, true); err != nil {
			return err
		}
		var createErr error
		snapshot, createErr = store.CreateRootFSSnapshot(lockCtx, &CreateRootFSSnapshotRequest{
			SandboxID:   sandboxID,
			SnapshotID:  generateRootFSSnapshotID(),
			Name:        strings.TrimSpace(req.Name),
			Description: strings.TrimSpace(req.Description),
			ExpiresAt:   req.ExpiresAt,
		})
		return createErr
	})
	if err != nil {
		return nil, err
	}
	return sandboxRootFSSnapshotFromStore(snapshot), nil
}

func (s *SandboxService) ListSandboxRootFSSnapshots(ctx context.Context, sandboxID, teamID string) (*ListSandboxRootFSSnapshotsResponse, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if err := s.requireRootFSSandboxOwnership(ctx, sandboxID, teamID); err != nil {
		return nil, err
	}
	snapshots, err := store.ListRootFSSnapshots(ctx, &ListRootFSSnapshotsRequest{
		SandboxID: sandboxID,
		TeamID:    teamID,
	})
	if err != nil {
		return nil, err
	}
	out := make([]*SandboxRootFSSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, sandboxRootFSSnapshotFromStore(snapshot))
	}
	return &ListSandboxRootFSSnapshotsResponse{
		Snapshots: out,
		Count:     len(out),
	}, nil
}

func (s *SandboxService) GetSandboxRootFSSnapshot(ctx context.Context, snapshotID, teamID string) (*SandboxRootFSSnapshot, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	snapshot, err := store.GetRootFSSnapshot(ctx, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID))
	if err != nil {
		return nil, err
	}
	return sandboxRootFSSnapshotFromStore(snapshot), nil
}

func (s *SandboxService) DeleteSandboxRootFSSnapshot(ctx context.Context, snapshotID, teamID string) error {
	store, err := s.rootFSProductStore()
	if err != nil {
		return err
	}
	return store.DeleteRootFSSnapshot(ctx, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID))
}

func (s *SandboxService) RestoreSandboxRootFS(ctx context.Context, sandboxID, teamID string, req *RestoreSandboxRootFSRequest) (*RestoreSandboxRootFSResponse, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	if _, err := store.GetRootFSSnapshot(ctx, snapshotID, teamID); err != nil {
		return nil, err
	}
	err = s.sandboxStore.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, _ SandboxStoreTx, record *SandboxRecord) error {
		if err := validateRootFSSandboxRecord(record, sandboxID, teamID, true); err != nil {
			return err
		}
		_, restoreErr := store.RestoreRootFSFromSnapshot(lockCtx, &RestoreRootFSFromSnapshotRequest{
			SandboxID:  sandboxID,
			SnapshotID: snapshotID,
			TeamID:     teamID,
		})
		return restoreErr
	})
	if err != nil {
		return nil, err
	}
	return &RestoreSandboxRootFSResponse{
		SandboxID:  sandboxID,
		SnapshotID: snapshotID,
		Status:     SandboxStatusPaused,
	}, nil
}

func (s *SandboxService) ForkSandbox(ctx context.Context, sourceSandboxID, teamID, userID string, _ *ForkSandboxRequest) (*ForkSandboxResponse, error) {
	store, err := s.rootFSProductStore()
	if err != nil {
		return nil, err
	}
	sourceSandboxID = strings.TrimSpace(sourceSandboxID)
	teamID = strings.TrimSpace(teamID)
	userID = strings.TrimSpace(userID)
	if sourceSandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}

	var source *SandboxRecord
	if err := s.sandboxStore.WithSandboxLock(ctx, sourceSandboxID, func(lockCtx context.Context, _ SandboxStoreTx, record *SandboxRecord) error {
		if err := validateRootFSSandboxRecord(record, sourceSandboxID, teamID, true); err != nil {
			return err
		}
		source = cloneSandboxRecordForRootFSProduct(record)
		return nil
	}); err != nil {
		return nil, err
	}
	template, err := s.templateForSandboxRecord(source)
	if err != nil {
		return nil, err
	}
	targetID, err := s.generateAvailableForkSandboxID(ctx, template)
	if err != nil {
		return nil, err
	}
	now := s.clock.Now().UTC()
	target := &SandboxRecord{
		ID:                targetID,
		TeamID:            teamID,
		UserID:            userID,
		TemplateID:        source.TemplateID,
		TemplateName:      source.TemplateName,
		TemplateNamespace: source.TemplateNamespace,
		ClusterID:         source.ClusterID,
		Status:            SandboxStatusPaused,
		Config:            cloneSandboxConfigValue(source.Config),
		TemplateSpec:      *source.TemplateSpec.DeepCopy(),
		ClaimedAt:         now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	err = s.sandboxStore.WithSandboxLock(ctx, sourceSandboxID, func(lockCtx context.Context, _ SandboxStoreTx, record *SandboxRecord) error {
		if err := validateRootFSSandboxRecord(record, sourceSandboxID, teamID, true); err != nil {
			return err
		}
		if err := s.sandboxStore.UpsertSandbox(lockCtx, target); err != nil {
			return err
		}
		if _, err := store.ForkRootFSFilesystem(lockCtx, &ForkRootFSFilesystemRequest{
			SourceSandboxID: sourceSandboxID,
			TargetSandboxID: targetID,
			TargetTeamID:    teamID,
		}); err != nil {
			if cleanupErr := s.sandboxStore.MarkSandboxDeleted(lockCtx, targetID, s.clock.Now().UTC()); cleanupErr != nil && s.logger != nil {
				s.logger.Warn("Failed to clean up sandbox record after rootfs fork failure",
					zap.String("sandboxID", targetID),
					zap.Error(cleanupErr),
				)
			}
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &ForkSandboxResponse{
		SourceSandboxID: sourceSandboxID,
		Sandbox:         s.recordToSandbox(target),
	}, nil
}

func (s *SandboxService) rootFSProductStore() (SandboxRootFSProductStore, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, ErrSandboxRootFSStoreUnavailable
	}
	store, ok := s.sandboxStore.(SandboxRootFSProductStore)
	if !ok {
		return nil, ErrSandboxRootFSStoreUnavailable
	}
	return store, nil
}

func (s *SandboxService) generateAvailableForkSandboxID(ctx context.Context, template *v1alpha1.SandboxTemplate) (string, error) {
	if s == nil || s.sandboxStore == nil {
		return "", ErrSandboxRootFSStoreUnavailable
	}
	for i := 0; i < 8; i++ {
		sandboxID, err := s.generateStableSandboxID(template)
		if err != nil {
			return "", err
		}
		existing, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
		if err != nil {
			if errors.Is(err, ErrSandboxRecordNotFound) {
				return sandboxID, nil
			}
			return "", err
		}
		if existing == nil {
			return sandboxID, nil
		}
	}
	return "", fmt.Errorf("failed to allocate fork sandbox id")
}

func (s *SandboxService) requireRootFSSandboxOwnership(ctx context.Context, sandboxID, teamID string) error {
	if s == nil || s.sandboxStore == nil {
		return ErrSandboxRootFSStoreUnavailable
	}
	record, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	return validateRootFSSandboxRecord(record, sandboxID, teamID, false)
}

func validateRootFSSandboxRecord(record *SandboxRecord, sandboxID, teamID string, requirePaused bool) error {
	if record == nil || record.Status == SandboxStatusDeleted {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
	}
	if record.TeamID != teamID {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "sandbox"}, sandboxID, fmt.Errorf("sandbox belongs to a different team"))
	}
	if requirePaused && record.Status != SandboxStatusPaused {
		return fmt.Errorf("%w: current status is %s", ErrSandboxRootFSRequiresPausedSandbox, record.Status)
	}
	return nil
}

func sandboxRootFSSnapshotFromStore(snapshot *RootFSSnapshot) *SandboxRootFSSnapshot {
	if snapshot == nil {
		return nil
	}
	return &SandboxRootFSSnapshot{
		ID:          snapshot.ID,
		SandboxID:   snapshot.SourceSandboxID,
		Name:        snapshot.Name,
		Description: snapshot.Description,
		CreatedAt:   snapshot.CreatedAt,
		ExpiresAt:   snapshot.ExpiresAt,
	}
}

func generateRootFSSnapshotID() string {
	return "rootfs-snapshot-" + utilrand.String(10)
}

func cloneSandboxRecordForRootFSProduct(record *SandboxRecord) *SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	clone.Config = cloneSandboxConfigValue(record.Config)
	clone.Mounts = append([]ClaimMount(nil), record.Mounts...)
	clone.TemplateSpec = *record.TemplateSpec.DeepCopy()
	return &clone
}

func cloneSandboxConfigValue(cfg SandboxConfig) SandboxConfig {
	cloned := cloneSandboxConfig(&cfg)
	if cloned == nil {
		return SandboxConfig{}
	}
	return *cloned
}
