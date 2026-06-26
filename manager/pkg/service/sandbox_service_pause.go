package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
)

// PauseSandboxResponse represents the response from pausing a sandbox.
type PauseSandboxResponse struct {
	SandboxID     string                `json:"sandbox_id"`
	Paused        bool                  `json:"paused"`
	Status        string                `json:"status,omitempty"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                `json:"updated_memory,omitempty"`
	UpdatedCPU    string                `json:"updated_cpu,omitempty"`
}

// ResumeSandboxResponse represents the response from resuming a sandbox.
type ResumeSandboxResponse struct {
	SandboxID      string `json:"sandbox_id"`
	Resumed        bool   `json:"resumed"`
	RestoredMemory string `json:"restored_memory,omitempty"`
}

// PauseSandbox accepts a checkpointed pause request and returns the lifecycle state.
func (s *SandboxService) PauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	status, err := s.RequestPauseSandboxRuntime(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	return &PauseSandboxResponse{
		SandboxID: sandboxID,
		Paused:    status == SandboxStatusPaused,
		Status:    status,
	}, nil
}

// PauseSandboxAndWait accepts a pause request. Checkpoint completion is asynchronous.
func (s *SandboxService) PauseSandboxAndWait(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	return s.PauseSandbox(ctx, sandboxID)
}

// ResumeSandbox creates or reuses a runtime and restores the latest rootfs checkpoint.
func (s *SandboxService) ResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := strings.TrimSpace(sandboxID)
	if key == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	resultCh := s.resumeGroup.DoChan(key, func() (any, error) {
		restoreCtx, cancel := sandboxRestoreContext(context.Background())
		defer cancel()
		_, err := s.ResumePausedSandboxRuntime(restoreCtx, key)
		if err != nil {
			return nil, err
		}
		return &ResumeSandboxResponse{
			SandboxID: key,
			Resumed:   true,
		}, nil
	})
	select {
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		resp, ok := result.Val.(*ResumeSandboxResponse)
		if !ok || resp == nil {
			return nil, fmt.Errorf("resume sandbox returned invalid result")
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ResumeSandboxAndWait creates or reuses a runtime and restores the latest rootfs checkpoint.
func (s *SandboxService) ResumeSandboxAndWait(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	return s.ResumeSandbox(ctx, sandboxID)
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
