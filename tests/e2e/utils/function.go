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
