package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) CreateFunctionFromSandbox(ctx context.Context, t ContractT, sandboxID, serviceID, name string) (*apispec.FunctionRecord, int, error) {
	specPath := "/api/v1/functions"
	requestPath := "/api/v1/functions"
	req := apispec.FunctionCreateRequest{
		Name: &name,
		Source: apispec.FunctionSourceRequest{
			SandboxId: sandboxID,
			ServiceId: serviceID,
		},
	}
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated && status != http.StatusConflict {
		return nil, status, fmt.Errorf("create function failed with status %d: %s", status, formatAPIError(body))
	}
	if status == http.StatusConflict {
		return nil, status, fmt.Errorf("function name already exists: %s", formatAPIError(body))
	}
	var resp apispec.SuccessFunctionCreateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil {
		return nil, status, fmt.Errorf("create function response missing data")
	}
	return &resp.Data.Function, status, nil
}

func (s *Session) GetFunction(ctx context.Context, t ContractT, functionID string) (*apispec.FunctionRecord, int, error) {
	specPath := "/api/v1/functions/{id}"
	requestPath := "/api/v1/functions/" + functionID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get function failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessFunctionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil {
		return nil, status, fmt.Errorf("get function response missing data")
	}
	return &resp.Data.Function, status, nil
}

func (s *Session) UpdateFunction(ctx context.Context, t ContractT, functionID string, req apispec.FunctionUpdateRequest) (*apispec.FunctionRecord, int, error) {
	specPath := "/api/v1/functions/{id}"
	requestPath := "/api/v1/functions/" + functionID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("update function failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessFunctionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil {
		return nil, status, fmt.Errorf("update function response missing data")
	}
	return &resp.Data.Function, status, nil
}

func (s *Session) DeleteFunction(ctx context.Context, t ContractT, functionID string) (int, error) {
	specPath := "/api/v1/functions/{id}"
	requestPath := "/api/v1/functions/" + functionID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return status, fmt.Errorf("delete function failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) ListFunctionAliases(ctx context.Context, t ContractT, functionID string) ([]apispec.FunctionAlias, int, error) {
	specPath := "/api/v1/functions/{id}/aliases"
	requestPath := "/api/v1/functions/" + functionID + "/aliases"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list function aliases failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessFunctionAliasListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil {
		return nil, status, fmt.Errorf("list function aliases response missing data")
	}
	return resp.Data.Aliases, status, nil
}

func (s *Session) GetFunctionAlias(ctx context.Context, t ContractT, functionID, alias string) (*apispec.FunctionAlias, int, error) {
	specPath := "/api/v1/functions/{id}/aliases/{alias}"
	requestPath := "/api/v1/functions/" + functionID + "/aliases/" + alias
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get function alias failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessFunctionAliasResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil {
		return nil, status, fmt.Errorf("get function alias response missing data")
	}
	return &resp.Data.Alias, status, nil
}

func (s *Session) GetFunctionRevision(ctx context.Context, t ContractT, functionID string, revisionNumber int32) (*apispec.FunctionRevision, int, error) {
	specPath := "/api/v1/functions/{id}/revisions/{revision_number}"
	requestPath := fmt.Sprintf("/api/v1/functions/%s/revisions/%d", functionID, revisionNumber)
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get function revision failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessFunctionRevisionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil {
		return nil, status, fmt.Errorf("get function revision response missing data")
	}
	return &resp.Data.Revision, status, nil
}

func (s *Session) GetFunctionRuntime(ctx context.Context, t ContractT, functionID string) (*apispec.FunctionRuntimeStatus, int, error) {
	return s.functionRuntimeAction(ctx, t, http.MethodGet, "/api/v1/functions/{id}/runtime", "/api/v1/functions/"+functionID+"/runtime")
}

func (s *Session) RestartFunctionRuntime(ctx context.Context, t ContractT, functionID string) (*apispec.FunctionRuntimeStatus, int, error) {
	return s.functionRuntimeAction(ctx, t, http.MethodPost, "/api/v1/functions/{id}/runtime/restart", "/api/v1/functions/"+functionID+"/runtime/restart")
}

func (s *Session) RecycleFunctionRuntime(ctx context.Context, t ContractT, functionID string) (*apispec.FunctionRuntimeStatus, int, error) {
	return s.functionRuntimeAction(ctx, t, http.MethodPost, "/api/v1/functions/{id}/runtime/recycle", "/api/v1/functions/"+functionID+"/runtime/recycle")
}

func (s *Session) functionRuntimeAction(ctx context.Context, t ContractT, method, specPath, requestPath string) (*apispec.FunctionRuntimeStatus, int, error) {
	status, body, err := s.doJSONSpecRequest(t, ctx, method, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("function runtime request failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessFunctionRuntimeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil {
		return nil, status, fmt.Errorf("function runtime response missing data")
	}
	return &resp.Data.Runtime, status, nil
}
