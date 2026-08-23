package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// NomadSandboxProjectionStore is the durable read model required by the
// public Nomad sandbox query API.
type NomadSandboxProjectionStore interface {
	GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error)
	ListSandboxes(context.Context, *sandboxstore.ListSandboxesRequest) ([]*sandboxstore.SandboxRecord, error)
	GetActiveLifecycleTxn(context.Context, string) (*sandboxstore.SandboxLifecycleTxn, error)
	GetRuntimeSlotBySandboxID(context.Context, string) (*sandboxstore.RuntimeSlot, error)
}

// NomadSandboxReader projects public sandbox state only from regional durable
// records and runtime-slot authority. It never consults a Kubernetes Pod cache.
type NomadSandboxReader struct {
	store NomadSandboxProjectionStore
}

// NewNomadSandboxReader creates the fail-closed Nomad public query service.
func NewNomadSandboxReader(store NomadSandboxProjectionStore) (*NomadSandboxReader, error) {
	if store == nil {
		return nil, fmt.Errorf("Nomad sandbox projection store is required")
	}
	return &NomadSandboxReader{store: store}, nil
}

// GetSandbox returns one sandbox with its runtime state projected from the
// exact current runtime slot.
func (r *NomadSandboxReader) GetSandbox(ctx context.Context, sandboxID string) (*managerapi.Sandbox, error) {
	record, err := r.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get sandbox record: %w", err)
	}
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	if record.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
		return nil, fmt.Errorf("sandbox %s does not use the Nomad runtime", sandboxID)
	}
	projected := sandboxRecordToSandbox(record)
	if record.DesiredState != sandboxstore.SandboxDesiredStateActive {
		return projected, nil
	}
	activeTxn, err := r.store.GetActiveLifecycleTxn(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get active sandbox lifecycle txn: %w", err)
	}
	return r.projectActive(ctx, record, activeTxn, projected)
}

// ListSandboxes returns a stable public projection after applying runtime
// status filters and pagination to the authoritative regional records.
func (r *NomadSandboxReader) ListSandboxes(ctx context.Context, request *sandboxstore.ListSandboxesRequest) (*ListSandboxesResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("sandbox list request is required")
	}
	normalized := *request
	if normalized.Offset < 0 {
		return nil, fmt.Errorf("sandbox list offset must not be negative")
	}
	if normalized.Limit <= 0 {
		normalized.Limit = 50
	}
	if normalized.Limit > 200 {
		normalized.Limit = 200
	}
	records, err := r.store.ListSandboxes(ctx, &normalized)
	if err != nil {
		return nil, err
	}
	summaries := make([]*SandboxSummary, 0, len(records))
	for _, record := range records {
		if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
			continue
		}
		if record.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
			return nil, fmt.Errorf("sandbox %s does not use the Nomad runtime", record.ID)
		}
		projected := sandboxRecordToSandbox(record)
		if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
			projected, err = r.projectActive(ctx, record, nil, projected)
			if err != nil {
				return nil, err
			}
		}
		if normalized.Status != "" && projected.Status != normalized.Status {
			continue
		}
		if normalized.Paused != nil && projected.Paused != *normalized.Paused {
			continue
		}
		summaries = append(summaries, &SandboxSummary{
			ID: projected.ID, TemplateID: projected.TemplateID,
			Status: projected.Status, Paused: projected.Paused,
			RuntimeGeneration: projected.RuntimeGeneration,
			CreatedAt:         projected.CreatedAt, ExpiresAt: projected.ExpiresAt,
			HardExpiresAt: projected.HardExpiresAt, UpdatedAt: projected.UpdatedAt,
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].CreatedAt.After(summaries[j].CreatedAt)
	})
	totalCount := len(summaries)
	if normalized.Offset >= totalCount {
		summaries = []*SandboxSummary{}
	} else {
		end := normalized.Offset + normalized.Limit
		if end > totalCount {
			end = totalCount
		}
		summaries = summaries[normalized.Offset:end]
	}
	return &ListSandboxesResponse{
		Sandboxes: summaries, Count: totalCount,
		HasMore: normalized.Offset+len(summaries) < totalCount,
	}, nil
}

// GetSandboxStatus returns the legacy status envelope from the same durable
// projection used by GetSandbox.
func (r *NomadSandboxReader) GetSandboxStatus(ctx context.Context, sandboxID string) (map[string]any, error) {
	sandbox, err := r.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	return sandboxStatusResponse(sandbox), nil
}

func (r *NomadSandboxReader) projectActive(
	ctx context.Context,
	record *sandboxstore.SandboxRecord,
	activeTxn *sandboxstore.SandboxLifecycleTxn,
	projected *managerapi.Sandbox,
) (*managerapi.Sandbox, error) {
	slot, err := r.store.GetRuntimeSlotBySandboxID(ctx, record.ID)
	if err != nil {
		if errors.Is(err, sandboxstore.ErrRuntimeSlotNotFound) {
			if record.RuntimeID != "" {
				projected.Status = managerapi.SandboxStatusFailed
			}
			return projected, nil
		}
		return nil, fmt.Errorf("get Nomad runtime slot projection: %w", err)
	}
	return projectNomadSandboxSlot(record, activeTxn, projected, slot)
}

func projectNomadSandboxSlot(
	record *sandboxstore.SandboxRecord,
	activeTxn *sandboxstore.SandboxLifecycleTxn,
	projected *managerapi.Sandbox,
	slot *sandboxstore.RuntimeSlot,
) (*managerapi.Sandbox, error) {
	if slot == nil || slot.SandboxID != record.ID {
		return nil, fmt.Errorf("Nomad runtime slot projection does not match sandbox %s", record.ID)
	}
	projected.RuntimeID = slot.AllocationID
	switch slot.State {
	case sandboxstore.RuntimeSlotStateActive:
		if slot.ProcdInstanceID == "" || len(slot.CommandReadyDigest) != sha256.Size ||
			slot.CommandReadyAt.IsZero() || !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) {
			projected.Status = managerapi.SandboxStatusFailed
			return projected, nil
		}
		if err := runtimeslot.ValidateNomadProcdAddress(slot.ProcdAddress); err != nil {
			projected.Status = managerapi.SandboxStatusFailed
			return projected, nil
		}
		projected.Status = managerapi.SandboxStatusRunning
		projected.InternalAddr = slot.ProcdAddress
	case sandboxstore.RuntimeSlotStateClaiming, sandboxstore.RuntimeSlotStateStarting:
		projected.Status = managerapi.SandboxStatusStarting
	case sandboxstore.RuntimeSlotStateQuiescing, sandboxstore.RuntimeSlotStateOrphaned:
		projected.Status = managerapi.SandboxStatusFailed
	default:
		projected.Status = managerapi.SandboxStatusFailed
	}
	if sandboxLifecycleTxnHidesCommittedRuntime(activeTxn) {
		projected.InternalAddr = ""
	}
	return projected, nil
}

func sandboxStatusResponse(sandbox *managerapi.Sandbox) map[string]any {
	return map[string]any{
		"sandbox_id": sandbox.ID, "template_id": sandbox.TemplateID,
		"team_id": sandbox.TeamID, "user_id": sandbox.UserID,
		"runtime_id": sandbox.RuntimeID, "status": sandbox.Status,
		"claimed_at": sandbox.ClaimedAt.Format(time.RFC3339),
		"expires_at": sandbox.ExpiresAt, "hard_expires_at": sandbox.HardExpiresAt,
		"created_at": sandbox.CreatedAt.Format(time.RFC3339),
	}
}
