package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

// ListSandboxesResponse represents the response from listing sandboxes
type ListSandboxesResponse struct {
	Sandboxes []apispec.SandboxSummary `json:"sandboxes"`
	Count     int                      `json:"count"`
	HasMore   bool                     `json:"has_more"`
}

// ListSandboxesOptions represents options for listing sandboxes
type ListSandboxesOptions struct {
	Status     string
	TemplateID string
	Paused     *bool
	Limit      *int
	Offset     *int
}

func (s *Session) ListSandboxes(ctx context.Context, t ContractT, opts *ListSandboxesOptions) (*ListSandboxesResponse, int, error) {
	specPath := "/api/v1/sandboxes"
	requestPath := "/api/v1/sandboxes"

	// Build query parameters
	if opts != nil {
		query := url.Values{}
		if opts.Status != "" {
			query.Set("status", opts.Status)
		}
		if opts.TemplateID != "" {
			query.Set("template_id", opts.TemplateID)
		}
		if opts.Paused != nil {
			if *opts.Paused {
				query.Set("paused", "true")
			} else {
				query.Set("paused", "false")
			}
		}
		if opts.Limit != nil {
			query.Set("limit", fmt.Sprintf("%d", *opts.Limit))
		}
		if opts.Offset != nil {
			query.Set("offset", fmt.Sprintf("%d", *opts.Offset))
		}
		if len(query) > 0 {
			requestPath = requestPath + "?" + query.Encode()
		}
	}

	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list sandboxes failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success {
		return nil, status, fmt.Errorf("list sandboxes response indicates failure")
	}
	return &ListSandboxesResponse{
		Sandboxes: resp.Data.Sandboxes,
		Count:     resp.Data.Count,
		HasMore:   resp.Data.HasMore,
	}, status, nil
}

func (s *Session) ClaimSandbox(ctx context.Context, t ContractT, template string) (*apispec.ClaimResponse, error) {
	req := apispec.ClaimRequest{Template: &template}
	return s.ClaimSandboxWithRequest(ctx, t, req)
}

func (s *Session) ClaimSandboxWithRequest(ctx context.Context, t ContractT, req apispec.ClaimRequest) (*apispec.ClaimResponse, error) {
	resp, _, err := s.ClaimSandboxDetailed(ctx, t, req)
	return resp, err
}

func (s *Session) ClaimSandboxDetailed(ctx context.Context, t ContractT, req apispec.ClaimRequest) (*apispec.ClaimResponse, int, error) {
	if s.teamID == "" || s.userID == "" {
		return nil, 0, fmt.Errorf("team or user id missing")
	}
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("claim sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessClaimResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil || resp.Data.SandboxId == "" {
		return nil, status, fmt.Errorf("claim sandbox response missing id")
	}
	return resp.Data, status, nil
}

func (s *Session) DeleteSandbox(ctx context.Context, t ContractT, sandboxID string) error {
	if sandboxID == "" {
		return nil
	}
	specPath := "/api/v1/sandboxes/{id}"
	requestPath := "/api/v1/sandboxes/" + sandboxID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return fmt.Errorf("delete sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	return nil
}

func (s *Session) GetSandbox(ctx context.Context, t ContractT, sandboxID string) (*apispec.Sandbox, int, error) {
	specPath := "/api/v1/sandboxes/{id}"
	requestPath := "/api/v1/sandboxes/" + sandboxID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("get sandbox response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) UpdateSandbox(ctx context.Context, t ContractT, sandboxID string, config apispec.SandboxUpdateConfig) (*apispec.Sandbox, int, error) {
	specPath := "/api/v1/sandboxes/{id}"
	requestPath := "/api/v1/sandboxes/" + sandboxID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, apispec.SandboxUpdateRequest{
		Config: &config,
	}, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("update sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("update sandbox response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) GetSandboxStatus(ctx context.Context, t ContractT, sandboxID string) (*apispec.SandboxStatus, int, error) {
	specPath := "/api/v1/sandboxes/{id}/status"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/status"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get sandbox status failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxStatusResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("get sandbox status response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) RefreshSandbox(ctx context.Context, t ContractT, sandboxID string) (*apispec.RefreshResponse, int, error) {
	specPath := "/api/v1/sandboxes/{id}/refresh"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/refresh"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, apispec.SandboxRefreshRequest{}, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("refresh sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessRefreshResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("refresh sandbox response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) PauseSandbox(ctx context.Context, t ContractT, sandboxID string) (*apispec.PauseSandboxResponse, int, error) {
	specPath := "/api/v1/sandboxes/{id}/pause"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/pause"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("pause sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessPauseSandboxResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("pause sandbox response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ResumeSandbox(ctx context.Context, t ContractT, sandboxID string) (*apispec.ResumeSandboxResponse, int, error) {
	specPath := "/api/v1/sandboxes/{id}/resume"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/resume"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("resume sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessResumeSandboxResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("resume sandbox response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) CreateSandboxRootFSSnapshot(ctx context.Context, t ContractT, sandboxID string, req apispec.CreateSandboxRootFSSnapshotRequest) (*apispec.SandboxRootFSSnapshot, int, error) {
	specPath := "/api/v1/sandboxes/{id}/snapshots"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/snapshots"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create sandbox rootfs snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxRootFSSnapshotResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("create sandbox rootfs snapshot response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListSandboxRootFSSnapshots(ctx context.Context, t ContractT, sandboxID string) (*apispec.SandboxRootFSSnapshotList, int, error) {
	specPath := "/api/v1/sandboxes/{id}/snapshots"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/snapshots"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list sandbox rootfs snapshots failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxRootFSSnapshotListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("list sandbox rootfs snapshots response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) GetSandboxRootFSSnapshot(ctx context.Context, t ContractT, snapshotID string) (*apispec.SandboxRootFSSnapshot, int, error) {
	specPath := "/api/v1/sandbox-rootfs-snapshots/{snapshot_id}"
	requestPath := "/api/v1/sandbox-rootfs-snapshots/" + snapshotID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get sandbox rootfs snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxRootFSSnapshotResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("get sandbox rootfs snapshot response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) DeleteSandboxRootFSSnapshot(ctx context.Context, t ContractT, snapshotID string) (int, error) {
	specPath := "/api/v1/sandbox-rootfs-snapshots/{snapshot_id}"
	requestPath := "/api/v1/sandbox-rootfs-snapshots/" + snapshotID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, fmt.Errorf("delete sandbox rootfs snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) RestoreSandboxRootFS(ctx context.Context, t ContractT, sandboxID string, req apispec.RestoreSandboxRootFSRequest) (*apispec.RestoreSandboxRootFSResponse, int, error) {
	specPath := "/api/v1/sandboxes/{id}/rootfs/restore"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/rootfs/restore"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("restore sandbox rootfs failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessRestoreSandboxRootFSResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("restore sandbox rootfs response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ForkSandbox(ctx context.Context, t ContractT, sandboxID string) (*apispec.ForkSandboxResponse, int, error) {
	specPath := "/api/v1/sandboxes/{id}/fork"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/fork"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, apispec.ForkSandboxRequest{}, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("fork sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessForkSandboxResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("fork sandbox response missing data")
	}
	return resp.Data, status, nil
}
