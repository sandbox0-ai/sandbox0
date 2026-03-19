package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) ListCredentialSources(ctx context.Context, t ContractT) ([]apispec.CredentialSourceMetadata, error) {
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, "/api/v1/credential-sources", "/api/v1/credential-sources", nil, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list credential sources failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessCredentialSourceListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, fmt.Errorf("list credential sources response missing data")
	}
	return *resp.Data, nil
}

func (s *Session) CreateCredentialSource(ctx context.Context, t ContractT, req apispec.CredentialSourceWriteRequest) (*apispec.CredentialSourceMetadata, error) {
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, "/api/v1/credential-sources", "/api/v1/credential-sources", req, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusCreated {
		return nil, fmt.Errorf("create credential source failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessCredentialSourceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, fmt.Errorf("create credential source response missing data")
	}
	return resp.Data, nil
}

func (s *Session) GetCredentialSource(ctx context.Context, t ContractT, name string) (*apispec.CredentialSourceMetadata, int, *apispec.ErrorEnvelope, error) {
	specPath := "/api/v1/credential-sources/{name}"
	requestPath := "/api/v1/credential-sources/" + name
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, nil, err
	}
	if status != http.StatusOK {
		apiErr, err := decodeErrorEnvelope(body)
		if err != nil {
			return nil, status, nil, fmt.Errorf("get credential source failed with status %d: %s", status, formatAPIError(body))
		}
		return nil, status, apiErr, nil
	}
	var resp apispec.SuccessCredentialSourceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil, fmt.Errorf("get credential source response missing data")
	}
	return resp.Data, status, nil, nil
}

func (s *Session) UpdateCredentialSource(ctx context.Context, t ContractT, name string, req apispec.CredentialSourceWriteRequest) (*apispec.CredentialSourceMetadata, error) {
	specPath := "/api/v1/credential-sources/{name}"
	requestPath := "/api/v1/credential-sources/" + name
	req.Name = name
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, req, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("update credential source failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessCredentialSourceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, fmt.Errorf("update credential source response missing data")
	}
	return resp.Data, nil
}

func (s *Session) DeleteCredentialSource(ctx context.Context, name string) (int, *apispec.ErrorEnvelope, error) {
	requestPath := "/api/v1/credential-sources/" + name
	status, body, err := s.doJSONRequest(ctx, http.MethodDelete, requestPath, nil, true)
	if err != nil {
		return 0, nil, err
	}
	if status >= http.StatusBadRequest {
		apiErr, decodeErr := decodeErrorEnvelope(body)
		if decodeErr != nil {
			return status, nil, fmt.Errorf("delete credential source failed with status %d: %s", status, string(body))
		}
		return status, apiErr, nil
	}
	return status, nil, nil
}
