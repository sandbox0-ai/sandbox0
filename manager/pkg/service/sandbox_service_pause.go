package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
)

// PauseSandboxResponse represents the response from pausing a sandbox.
type PauseSandboxResponse struct {
	SandboxID     string                         `json:"sandbox_id"`
	Paused        bool                           `json:"paused"`
	Status        string                         `json:"status,omitempty"`
	ResourceUsage *procdapi.SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                         `json:"updated_memory,omitempty"`
	UpdatedCPU    string                         `json:"updated_cpu,omitempty"`
}

const teamPauseBatchSize = 100

// TeamPauseResult summarizes accepted pause requests for one team's running
// sandboxes in the local data-plane cluster.
type TeamPauseResult struct {
	Requested int `json:"requested"`
}

type activeSandboxIDLister interface {
	ListActiveSandboxIDs(context.Context, string, string, string, int) ([]string, error)
}

// PauseActiveSandboxesForTeam requests durable checkpoint pauses for every
// running sandbox owned by teamID in this manager's cluster. The operation is
// idempotent: existing pause transactions are reused, and a keyset scan makes
// one enforcement pass visit each currently active identity only once while
// asynchronous checkpoint work is still pending.
func (s *SandboxService) PauseActiveSandboxesForTeam(ctx context.Context, teamID string) (TeamPauseResult, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return TeamPauseResult{}, fmt.Errorf("team_id is required")
	}
	if s == nil || s.sandboxStore == nil {
		return TeamPauseResult{}, fmt.Errorf("sandbox store is unavailable")
	}

	lister, ok := s.sandboxStore.(activeSandboxIDLister)
	if !ok {
		return TeamPauseResult{}, fmt.Errorf("sandbox store does not support listing active sandbox ids")
	}

	var result TeamPauseResult
	afterSandboxID := ""
	for {
		ids, err := lister.ListActiveSandboxIDs(ctx, teamID, s.config.ClusterID, afterSandboxID, teamPauseBatchSize)
		if err != nil {
			return result, fmt.Errorf("list active sandboxes for team: %w", err)
		}
		if len(ids) == 0 {
			return result, nil
		}
		for _, sandboxID := range ids {
			if _, err := s.requestBillingPauseSandboxRuntime(ctx, sandboxID); err != nil {
				return result, fmt.Errorf("pause sandbox %q: %w", sandboxID, err)
			}
			result.Requested++
		}
		if len(ids) < teamPauseBatchSize {
			return result, nil
		}
		afterSandboxID = ids[len(ids)-1]
	}
}

// PauseSandbox accepts a checkpointed pause request and returns the lifecycle state.
func (s *SandboxService) PauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	status, err := s.RequestPauseSandboxRuntime(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	return &PauseSandboxResponse{
		SandboxID: sandboxID,
		Paused:    status == managerapi.SandboxStatusPaused,
		Status:    status,
	}, nil
}

// PauseSandboxAndWait accepts a pause request. Checkpoint completion is asynchronous.
func (s *SandboxService) PauseSandboxAndWait(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	return s.PauseSandbox(ctx, sandboxID)
}

// ResumeSandbox creates or reuses a runtime and restores the latest rootfs checkpoint.
func (s *SandboxService) ResumeSandbox(ctx context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
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
		return &managerapi.ResumeSandboxResponse{
			SandboxID: key,
			Resumed:   true,
		}, nil
	})
	select {
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		resp, ok := result.Val.(*managerapi.ResumeSandboxResponse)
		if !ok || resp == nil {
			return nil, fmt.Errorf("resume sandbox returned invalid result")
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ResumeSandboxAndWait creates or reuses a runtime and restores the latest rootfs checkpoint.
func (s *SandboxService) ResumeSandboxAndWait(ctx context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	return s.ResumeSandbox(ctx, sandboxID)
}

// TerminateSandboxByID implements the SandboxTerminator interface from controller package.
// It wraps TerminateSandbox and returns only the error.
func (s *SandboxService) TerminateSandboxByID(ctx context.Context, sandboxID string) error {
	return s.TerminateSandbox(ctx, sandboxID)
}

// GetSandboxResourceUsage gets the resource usage of a sandbox.
func (s *SandboxService) GetSandboxResourceUsage(ctx context.Context, sandboxID string) (*procdapi.SandboxResourceUsage, error) {
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
