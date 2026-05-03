package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) GetNetworkPolicy(ctx context.Context, t ContractT, sandboxID string) (*apispec.SandboxNetworkPolicy, int, *apispec.ErrorEnvelope, error) {
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

func (s *Session) UpdateNetworkPolicy(ctx context.Context, t ContractT, sandboxID string, req apispec.SandboxNetworkPolicy) (*apispec.SandboxNetworkPolicy, int, *apispec.ErrorEnvelope, error) {
	specPath := "/api/v1/sandboxes/{id}/network"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/network"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, nil, err
	}
	if status != http.StatusOK {
		apiErr, err := decodeErrorEnvelope(body)
		if err != nil {
			return nil, status, nil, fmt.Errorf("update network policy failed with status %d: %s", status, formatAPIError(body))
		}
		return nil, status, apiErr, nil
	}
	var resp apispec.SuccessSandboxNetworkPolicyResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil, fmt.Errorf("update network policy response missing data")
	}
	return resp.Data, status, nil, nil
}

func (s *Session) UpdatePublicGateway(ctx context.Context, t ContractT, sandboxID string, policy apispec.PublicGatewayConfig) (*apispec.PublicGatewayConfig, string, int, error) {
	specPath := "/api/v1/sandboxes/{id}/public-gateway"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/public-gateway"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, policy, true)
	if err != nil {
		return nil, "", status, err
	}
	if status != http.StatusOK {
		return nil, "", status, fmt.Errorf("update public gateway failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessPublicGatewayResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, "", status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, "", status, fmt.Errorf("update public gateway response missing data")
	}
	exposureDomain := ""
	if resp.Data.ExposureDomain != nil {
		exposureDomain = *resp.Data.ExposureDomain
	}
	return &resp.Data.PublicGateway, exposureDomain, status, nil
}

func (s *Session) GetPublicGateway(ctx context.Context, t ContractT, sandboxID string) (*apispec.PublicGatewayConfig, string, int, error) {
	specPath := "/api/v1/sandboxes/{id}/public-gateway"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/public-gateway"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, "", status, err
	}
	if status != http.StatusOK {
		return nil, "", status, fmt.Errorf("get public gateway failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessPublicGatewayResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, "", status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, "", status, fmt.Errorf("get public gateway response missing data")
	}
	exposureDomain := ""
	if resp.Data.ExposureDomain != nil {
		exposureDomain = *resp.Data.ExposureDomain
	}
	return &resp.Data.PublicGateway, exposureDomain, status, nil
}
