package nomadclaim

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
)

type checkpointForkStore interface {
	GetNomadSandboxRunningMemoryFork(context.Context, string) (*sandboxstore.NomadRunningMemoryFork, error)
	RequestNomadSandboxRunningMemoryFork(context.Context, *sandboxstore.NomadSandboxForkRequest) (*sandboxstore.NomadRunningMemoryFork, error)
}

// completeNomadMemoryFork shares ordinary target derivation. A running source
// is captured once by the checkpoint worker; its cleanup atomically retains
// the child and admits parent restore. Pending responses are exact-retryable.
func (s *Service) completeNomadMemoryFork(ctx context.Context, source, target *sandboxstore.SandboxRecord, operation string, accepted bool) (*service.ForkSandboxResponse, error) {
	request := &sandboxstore.NomadSandboxForkRequest{OperationID: operation, SourceSandboxID: source.ID, ExpectedTeamID: source.TeamID, Target: target, Memory: true}
	if !accepted && source.DesiredState == sandboxstore.SandboxDesiredStatePaused {
		child, err := s.store.ForkNomadPausedSandbox(ctx, request)
		if err != nil {
			return nil, mapNomadForkError("fork paused memory sandbox", source.ID, err)
		}
		return nomadForkResponse(source.ID, child), nil
	}
	if _, ok := s.planner.(checkpointPlanner); !ok {
		return nil, fmt.Errorf("%w: memory restore planner is unavailable", service.ErrSandboxLifecycleUnavailable)
	}
	store, ok := s.store.(checkpointForkStore)
	if !ok {
		return nil, fmt.Errorf("%w: memory fork authority is unavailable", service.ErrSandboxLifecycleUnavailable)
	}
	intent, err := store.RequestNomadSandboxRunningMemoryFork(ctx, request)
	if err != nil {
		return nil, mapNomadForkError("request running memory fork", source.ID, err)
	}
	if intent == nil || intent.OperationID != operation || intent.SourceSandboxID != source.ID || intent.Target == nil || intent.Target.ID != target.ID {
		return nil, fmt.Errorf("%w: memory fork authority changed identity", service.ErrSandboxLifecycleUnavailable)
	}
	if intent.ParentResumeOperationID == "" {
		capture, err := s.store.GetLifecycleTxn(ctx, intent.CaptureOperationID)
		if err != nil {
			return nil, err
		}
		if capture != nil && capture.Phase == sandboxstore.SandboxLifecyclePhaseAborted {
			return nil, apierror.NewConflict("sandbox", source.ID, fmt.Errorf("memory fork capture was canceled"))
		}
		if source.DesiredState == sandboxstore.SandboxDesiredStateTerminating || source.DesiredState == sandboxstore.SandboxDesiredStateDeleted ||
			(!source.HardExpiresAt.IsZero() && !source.HardExpiresAt.After(s.now())) {
			return nil, apierror.NewConflict("sandbox", source.ID, fmt.Errorf("memory fork source expired or terminated"))
		}
		return nil, fmt.Errorf("%w: memory fork capture is pending", service.ErrSandboxLifecycleUnavailable)
	}
	resume, err := s.store.GetLifecycleTxn(ctx, intent.ParentResumeOperationID)
	if err != nil {
		return nil, err
	}
	if resume != nil && resume.SandboxID == source.ID && resume.Kind == sandboxstore.SandboxLifecycleKindResume && resume.Phase == sandboxstore.SandboxLifecyclePhaseAborted {
		return nil, apierror.NewConflict("sandbox", source.ID, fmt.Errorf("memory fork parent restore failed; a new explicit resume is required"))
	}
	if err := s.ResumeMemorySandboxOperation(ctx, source.ID, intent.ParentResumeOperationID); err != nil {
		return nil, err
	}
	resume, err = s.store.GetLifecycleTxn(ctx, intent.ParentResumeOperationID)
	if err != nil {
		return nil, err
	}
	if resume == nil || resume.SandboxID != source.ID || resume.Kind != sandboxstore.SandboxLifecycleKindResume || resume.Phase != sandboxstore.SandboxLifecyclePhaseCommitted {
		return nil, fmt.Errorf("%w: memory fork parent restore is pending", service.ErrSandboxLifecycleUnavailable)
	}
	// This also verifies memory custody and the immutable fork target digest.
	// An expired child reports failure only after its parent has resumed.
	child, err := s.store.ForkNomadPausedSandbox(ctx, request)
	if err != nil {
		return nil, mapNomadForkError("complete running memory fork", source.ID, err)
	}
	return nomadForkResponse(source.ID, child), nil
}
