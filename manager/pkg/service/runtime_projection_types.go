package service

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
)

type ListSandboxesResponse struct {
	Sandboxes []*SandboxSummary `json:"sandboxes"`
	Count     int               `json:"count"`
	HasMore   bool              `json:"has_more"`
}

type SandboxSummary struct {
	ID                string     `json:"id"`
	TemplateID        string     `json:"template_id"`
	Status            string     `json:"status"`
	Paused            bool       `json:"paused"`
	RuntimeGeneration int64      `json:"runtime_generation"`
	CreatedAt         time.Time  `json:"created_at"`
	ExpiresAt         *time.Time `json:"expires_at"`
	HardExpiresAt     *time.Time `json:"hard_expires_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func sandboxRecordToSandbox(record *sandboxstore.SandboxRecord) *managerapi.Sandbox {
	if record == nil {
		return nil
	}
	autoResume := true
	if record.Config.AutoResume != nil {
		autoResume = *record.Config.AutoResume
	}
	return &managerapi.Sandbox{
		ID: record.ID, TemplateID: record.TemplateID, TeamID: record.TeamID, UserID: record.UserID,
		Status:     sandboxStatusFromDesiredState(record.DesiredState),
		Paused:     record.DesiredState == sandboxstore.SandboxDesiredStatePaused,
		AutoResume: autoResume, Resources: cloneSandboxResourceConfig(record.Config.Resources),
		Services:  cloneSandboxAppServices(record.Config.Services),
		RuntimeID: record.RuntimeID, RuntimeGeneration: record.RuntimeGeneration,
		ExpiresAt: optionalTime(record.ExpiresAt), HardExpiresAt: optionalTime(record.HardExpiresAt),
		ClaimedAt: record.ClaimedAt, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}
}

func sandboxStatusFromDesiredState(desiredState string) string {
	switch desiredState {
	case sandboxstore.SandboxDesiredStatePaused:
		return managerapi.SandboxStatusPaused
	case sandboxstore.SandboxDesiredStateTerminating, sandboxstore.SandboxDesiredStateDeleted:
		return managerapi.SandboxStatusTerminating
	default:
		return managerapi.SandboxStatusStarting
	}
}

func sandboxLifecycleTxnHidesCommittedRuntime(txn *sandboxstore.SandboxLifecycleTxn) bool {
	if txn == nil {
		return false
	}
	return txn.Kind == sandboxstore.SandboxLifecycleKindResume || txn.Kind == sandboxstore.SandboxLifecycleKindPause
}

func sandboxLifecycleTxnCancelRequested(txn *sandboxstore.SandboxLifecycleTxn) bool {
	return txn != nil && !txn.CancelRequestedAt.IsZero()
}

func sandboxLifecycleTxnCancelableAutoPause(txn *sandboxstore.SandboxLifecycleTxn) bool {
	if txn == nil || txn.Kind != sandboxstore.SandboxLifecycleKindPause ||
		txn.Source != sandboxstore.SandboxLifecycleSourceAuto || !txn.Cancelable {
		return false
	}
	switch txn.Phase {
	case sandboxstore.SandboxLifecyclePhasePreparing,
		sandboxstore.SandboxLifecyclePhaseBarriered,
		sandboxstore.SandboxLifecyclePhasePublishing:
		return true
	default:
		return false
	}
}
