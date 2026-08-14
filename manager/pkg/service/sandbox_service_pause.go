package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
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
	txnID         string
}

// PauseSandbox accepts a checkpointed pause request and returns the lifecycle state.
func (s *SandboxService) PauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	status, txnID, err := s.requestPauseSandboxRuntime(ctx, sandboxID, pauseSandboxRuntimeOptions{source: sandboxstore.SandboxLifecycleSourceManual})
	if err != nil {
		return nil, err
	}
	return &PauseSandboxResponse{
		SandboxID: sandboxID,
		Paused:    status == managerapi.SandboxStatusPaused,
		Status:    status,
		txnID:     txnID,
	}, nil
}

// PauseSandboxAndWait accepts a pause request and waits for the durable
// checkpoint transaction to finish.
func (s *SandboxService) PauseSandboxAndWait(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	pauseCtx, cancel := sandboxRestoreContext(ctx)
	defer cancel()

	response, err := s.PauseSandbox(pauseCtx, sandboxID)
	if err != nil || response.Paused || s.sandboxStore == nil {
		return response, err
	}
	if err := s.waitForSandboxLifecycleTxnExit(pauseCtx, sandboxID); err != nil {
		return nil, fmt.Errorf("wait for sandbox pause: %w", err)
	}
	sandbox, err := s.GetSandbox(pauseCtx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get sandbox after pause: %w", err)
	}
	if sandbox == nil || sandbox.Status != managerapi.SandboxStatusPaused || !sandbox.Paused {
		if response.txnID != "" {
			txn, txnErr := s.sandboxStore.GetLifecycleTxn(pauseCtx, response.txnID)
			if txnErr != nil {
				return nil, fmt.Errorf("load sandbox pause outcome: %w", txnErr)
			}
			if outcomeErr := sandboxPauseOutcomeError(txn); outcomeErr != nil {
				return nil, outcomeErr
			}
		}
		status := "unknown"
		if sandbox != nil && strings.TrimSpace(sandbox.Status) != "" {
			status = sandbox.Status
		}
		return nil, fmt.Errorf("sandbox pause did not complete: status is %q", status)
	}
	return &PauseSandboxResponse{
		SandboxID: sandboxID,
		Paused:    true,
		Status:    managerapi.SandboxStatusPaused,
	}, nil
}

func sandboxPauseOutcomeError(txn *sandboxstore.SandboxLifecycleTxn) error {
	if txn == nil || txn.Kind != sandboxstore.SandboxLifecycleKindPause || txn.Phase != sandboxstore.SandboxLifecyclePhaseAborted {
		return nil
	}
	reason := strings.TrimSpace(txn.Error)
	if reason == "" {
		reason = "checkpoint transaction aborted"
	}
	if strings.HasPrefix(reason, sandboxLifecycleTxnUnavailablePrefix) {
		reason = strings.TrimSpace(strings.TrimPrefix(reason, sandboxLifecycleTxnUnavailablePrefix))
		return fmt.Errorf("sandbox pause checkpoint failed: %w", &ctldapi.RequestError{
			StatusCode: http.StatusServiceUnavailable,
			Message:    reason,
		})
	}
	return fmt.Errorf("sandbox pause checkpoint failed: %w", errors.New(reason))
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
