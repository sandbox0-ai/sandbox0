package nomadclaim

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type checkpointPlanner interface {
	ClaimCheckpoint(context.Context, runtimeslotclaim.Request, protocol.CheckpointRestoreAuthority) (*runtimeslotclaim.Result, error)
}

type checkpointPauseStore interface {
	RequestNomadSandboxMemoryPause(context.Context, string) (*sandboxstore.NomadSandboxMemoryPauseCandidate, error)
}

// PauseMemorySandboxAndWait reserves the explicit memory operation. The shared
// checkpoint worker captures before source cleanup; ordinary Nomad stop and
// filesystem-pause enqueuers must not run against the still-live source.
func (s *Service) PauseMemorySandboxAndWait(ctx context.Context, sandboxID string) (*service.PauseSandboxResponse, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	store, ok := s.store.(checkpointPauseStore)
	if !ok {
		return nil, fmt.Errorf("%w: memory pause authority is unavailable", service.ErrSandboxLifecycleUnavailable)
	}
	candidate, err := store.RequestNomadSandboxMemoryPause(ctx, sandboxID)
	if err != nil {
		return nil, mapNomadSandboxPauseError(sandboxID, err)
	}
	if candidate == nil || candidate.SandboxID != sandboxID || candidate.OperationID == "" {
		return nil, fmt.Errorf("%w: memory pause authority changed owner", service.ErrSandboxLifecycleUnavailable)
	}
	status := managerapi.SandboxStatusStarting
	if candidate.AlreadyPaused {
		status = managerapi.SandboxStatusPaused
	}
	return &service.PauseSandboxResponse{SandboxID: sandboxID, Paused: candidate.AlreadyPaused, Status: status}, nil
}

// ResumeMemorySandboxAndWait is the explicit manager boundary for restoring a
// retained process image. The existing resume entry points keep cold defaults.
func (s *Service) ResumeMemorySandboxAndWait(ctx context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	record, _, err := s.resumeNomadSandboxMode(ctx, sandboxID, true)
	if err != nil {
		return nil, err
	}
	return &managerapi.ResumeSandboxResponse{SandboxID: record.ID, Resumed: true}, nil
}

// ResumeSandboxAutomaticallyAndWait restores retained memory when this paused
// sandbox owns a checkpoint. Only the proven absence of a checkpoint permits
// the original filesystem resume path; restore failures never start cold.
func (s *Service) ResumeSandboxAutomaticallyAndWait(ctx context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	response, err := s.ResumeMemorySandboxAndWait(ctx, sandboxID)
	if errors.Is(err, sandboxstore.ErrNomadCheckpointNotRetained) {
		return s.ResumeSandboxAndWait(ctx, sandboxID)
	}
	return response, err
}

// bindCheckpointResumePlan rejects configuration drift instead of silently
// applying only part of a mutable template to preserved processes. Session reset
// is an activation flag; it is retained from capture and never rerun on restore.
func bindCheckpointResumePlan(candidate *sandboxstore.NomadSandboxResumeCandidate, plan *nomadResumePlan) error {
	a := candidate.Checkpoint
	if a == nil || a.Assignment.Validate() != nil || a.LifecycleEpoch <= 0 ||
		a.Assignment.OperationID != candidate.OperationID || a.Assignment.Target.SandboxID != candidate.SandboxID ||
		a.Assignment.Target.TeamID != candidate.Record.TeamID || a.Assignment.Target.RuntimeGeneration != candidate.RuntimeGeneration ||
		candidate.CheckpointCompatibilityDigest != plan.runtimeClass.CompatibilityDigest {
		return fmt.Errorf("retained memory requires its exact runtime compatibility and admitted assignment")
	}
	target := a.Assignment.Target
	expected := plan.assignment
	expected.RuntimeGeneration = target.RuntimeGeneration
	expected.ResetCopiedSessionState = target.ResetCopiedSessionState
	expectedRevision, err := expected.Revision()
	if err != nil {
		return err
	}
	capturedRevision, err := target.Revision()
	if err != nil || expectedRevision != capturedRevision {
		return fmt.Errorf("sandbox configuration changed since memory capture")
	}
	plan.assignment = target
	return nil
}
