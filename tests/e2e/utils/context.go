package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) CreateContext(ctx context.Context, t ContractT, sandboxID string, req apispec.CreateContextRequest) (*apispec.ContextResponse, int, error) {
	specPath := "/api/v1/sandboxes/{id}/contexts"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/contexts"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create context failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessContextResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("create context response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListContexts(ctx context.Context, t ContractT, sandboxID string) ([]apispec.ContextResponse, int, error) {
	specPath := "/api/v1/sandboxes/{id}/contexts"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/contexts"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list contexts failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessContextListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil || resp.Data.Contexts == nil {
		return nil, status, nil
	}
	return *resp.Data.Contexts, status, nil
}

func (s *Session) DeleteContext(ctx context.Context, t ContractT, sandboxID, contextID string) (int, error) {
	specPath := "/api/v1/sandboxes/{id}/contexts/{ctx_id}"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/contexts/" + contextID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return status, fmt.Errorf("delete context failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}
