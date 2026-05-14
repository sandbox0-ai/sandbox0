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

func (s *Session) UpdateSandboxServices(ctx context.Context, t ContractT, sandboxID string, services []apispec.SandboxAppService) ([]apispec.SandboxAppServiceView, string, int, error) {
	specPath := "/api/v1/sandboxes/{id}/services"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/services"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, apispec.SandboxServicesUpdateRequest{
		Services: services,
	}, true)
	if err != nil {
		return nil, "", status, err
	}
	if status != http.StatusOK {
		return nil, "", status, fmt.Errorf("update sandbox services failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxServicesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, "", status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, "", status, fmt.Errorf("update sandbox services response missing data")
	}
	exposureDomain := ""
	if resp.Data.ExposureDomain != nil {
		exposureDomain = *resp.Data.ExposureDomain
	}
	return resp.Data.Services, exposureDomain, status, nil
}

func (s *Session) GetSandboxServices(ctx context.Context, t ContractT, sandboxID string) ([]apispec.SandboxAppServiceView, string, int, error) {
	specPath := "/api/v1/sandboxes/{id}/services"
	requestPath := "/api/v1/sandboxes/" + sandboxID + "/services"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, "", status, err
	}
	if status != http.StatusOK {
		return nil, "", status, fmt.Errorf("get sandbox services failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxServicesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, "", status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, "", status, fmt.Errorf("get sandbox services response missing data")
	}
	exposureDomain := ""
	if resp.Data.ExposureDomain != nil {
		exposureDomain = *resp.Data.ExposureDomain
	}
	return resp.Data.Services, exposureDomain, status, nil
}
