package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) GetNetworkPolicy(ctx context.Context, t ContractT, sandboxID string) (*apispec.TplSandboxNetworkPolicy, int, *apispec.ErrorEnvelope, error) {
	specPath := "/api/v1/sandboxes/{id}/network"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/network"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, nil, err
	}
	if status != http.StatusOK {
		apiErr, err := decodeErrorEnvelope(body)
		if err != nil {
			return nil, status, nil, fmt.Errorf("get network policy failed with status %d: %s", status, formatAPIError(body))
		}
		return nil, status, apiErr, nil
	}
	var resp apispec.SuccessSandboxNetworkPolicyResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil, fmt.Errorf("get network policy response missing data")
	}
	return resp.Data, status, nil, nil
}
