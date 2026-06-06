package service

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

// PauseSandboxResponse represents the response from pausing a sandbox.
type PauseSandboxResponse struct {
	SandboxID     string                `json:"sandbox_id"`
	Paused        bool                  `json:"paused"`
	PowerState    SandboxPowerState     `json:"power_state"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                `json:"updated_memory,omitempty"`
	UpdatedCPU    string                `json:"updated_cpu,omitempty"`
}

// ResumeSandboxResponse represents the response from resuming a sandbox.
type ResumeSandboxResponse struct {
	SandboxID      string            `json:"sandbox_id"`
	Resumed        bool              `json:"resumed"`
	PowerState     SandboxPowerState `json:"power_state"`
	RestoredMemory string            `json:"restored_memory,omitempty"`
}

// PauseSandbox records a desired paused state for ctld reconciliation.
func (s *SandboxService) PauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	return s.RequestPauseSandbox(ctx, sandboxID)
}

// RequestPauseSandbox records a desired paused state and reconciles it asynchronously.
func (s *SandboxService) RequestPauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	if err := s.requireCtldPowerTransitions(); err != nil {
		return nil, err
	}
	state, err := s.requestSandboxPowerState(ctx, sandboxID, SandboxPowerStatePaused)
	if err != nil {
		return nil, err
	}
	return &PauseSandboxResponse{
		SandboxID:  sandboxID,
		Paused:     true,
		PowerState: state,
	}, nil
}

// PauseSandboxAndWait records a desired paused state and waits until the sandbox observes it.
func (s *SandboxService) PauseSandboxAndWait(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	resp, err := s.RequestPauseSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if resp.PowerState.Desired == SandboxPowerStatePaused && resp.PowerState.Observed == SandboxPowerStatePaused && resp.PowerState.Phase == SandboxPowerPhaseStable {
		return resp, nil
	}
	waitCtx, cancel := sandboxPowerTransitionContext(ctx)
	defer cancel()
	state, err := s.waitForSandboxPowerState(waitCtx, sandboxID, SandboxPowerStatePaused, resp.PowerState.DesiredGeneration)
	if err != nil {
		return &PauseSandboxResponse{SandboxID: sandboxID, Paused: state.Observed == SandboxPowerStatePaused, PowerState: state}, err
	}
	resp.PowerState = state
	resp.Paused = true
	return resp, nil
}

// ResumeSandbox records a desired active state for ctld reconciliation.
func (s *SandboxService) ResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	return s.RequestResumeSandbox(ctx, sandboxID)
}

// RequestResumeSandbox records a desired active state and reconciles it asynchronously.
func (s *SandboxService) RequestResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	if err := s.requireCtldPowerTransitions(); err != nil {
		return nil, err
	}
	state, err := s.requestSandboxPowerState(ctx, sandboxID, SandboxPowerStateActive)
	if err != nil {
		return nil, err
	}
	return &ResumeSandboxResponse{
		SandboxID:  sandboxID,
		Resumed:    true,
		PowerState: state,
	}, nil
}

// ResumeSandboxAndWait records a desired active state and waits until the sandbox observes it.
func (s *SandboxService) ResumeSandboxAndWait(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	resp, err := s.RequestResumeSandbox(ctx, sandboxID)
	if err != nil {
		if k8serrors.IsNotFound(err) && s.sandboxStore != nil {
			waitCtx, cancel := sandboxRestoreContext(ctx)
			defer cancel()
			sandbox, restoreErr := s.RestoreCleanedSandboxRuntime(waitCtx, sandboxID)
			if restoreErr != nil {
				return nil, restoreErr
			}
			generation := int64(0)
			if sandbox != nil {
				generation = sandbox.PowerState.DesiredGeneration
			}
			return cleanedSandboxResumeResponse(sandboxID, generation), nil
		}
		return nil, err
	}
	if resp.PowerState.Desired == SandboxPowerStateActive && resp.PowerState.Observed == SandboxPowerStateActive && resp.PowerState.Phase == SandboxPowerPhaseStable {
		return resp, nil
	}
	waitCtx, cancel := sandboxPowerTransitionContext(ctx)
	defer cancel()
	state, err := s.waitForSandboxPowerState(waitCtx, sandboxID, SandboxPowerStateActive, resp.PowerState.DesiredGeneration)
	if err != nil {
		return &ResumeSandboxResponse{SandboxID: sandboxID, Resumed: state.Observed == SandboxPowerStateActive, PowerState: state}, err
	}
	resp.PowerState = state
	resp.Resumed = true
	return resp, nil
}

// RequestPauseSandboxByID records the desired paused state for controller-driven reconciliation.
func (s *SandboxService) RequestPauseSandboxByID(ctx context.Context, sandboxID string) error {
	_, err := s.RequestPauseSandbox(ctx, sandboxID)
	return err
}

// PauseSandboxByID records the desired paused state for compatibility with older controller callers.
//
// Deprecated: use RequestPauseSandboxByID for declarative pause requests.
func (s *SandboxService) PauseSandboxByID(ctx context.Context, sandboxID string) error {
	return s.RequestPauseSandboxByID(ctx, sandboxID)
}

func (s *SandboxService) requireCtldPowerTransitions() error {
	if s == nil || !s.config.CtldEnabled {
		return ErrSandboxPowerRequiresCtld
	}
	return nil
}

// TerminateSandboxByID implements the SandboxTerminator interface from controller package.
// It wraps TerminateSandbox and returns only the error.
func (s *SandboxService) TerminateSandboxByID(ctx context.Context, sandboxID string) error {
	return s.TerminateSandbox(ctx, sandboxID)
}

// GetSandboxResourceUsage gets the resource usage of a sandbox.
func (s *SandboxService) GetSandboxResourceUsage(ctx context.Context, sandboxID string) (*SandboxResourceUsage, error) {
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	// Call procd stats API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	statsResp, err := s.procdClient.Stats(ctx, procdAddress, internalToken)
	if err != nil {
		return nil, fmt.Errorf("call procd stats: %w", err)
	}

	return &statsResp.SandboxResourceUsage, nil
}
