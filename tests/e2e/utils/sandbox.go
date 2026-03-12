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
	if s.teamID == "" || s.userID == "" {
		return nil, fmt.Errorf("team or user id missing")
	}
	req := apispec.ClaimRequest{
		Template: &template,
	}
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, "/api/v1/sandboxes", "/api/v1/sandboxes", req, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusCreated {
		return nil, fmt.Errorf("claim sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessClaimResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil || resp.Data.SandboxId == "" {
		return nil, fmt.Errorf("claim sandbox response missing id")
	}
	return resp.Data, nil
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
