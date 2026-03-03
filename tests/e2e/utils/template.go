package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/infra/pkg/apispec"
)

func (s *Session) ListTemplates(ctx context.Context, t ContractT) ([]apispec.Template, error) {
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, "/api/v1/templates", "/api/v1/templates", nil, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list templates failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessTemplateListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil || resp.Data.Templates == nil {
		return nil, nil
	}
	return *resp.Data.Templates, nil
}

func (s *Session) CreateTemplate(ctx context.Context, t ContractT, template apispec.TemplateCreateRequest) (*apispec.Template, error) {
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, "/api/v1/templates", "/api/v1/templates", template, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusCreated {
		return nil, fmt.Errorf("create template failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessTemplateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, fmt.Errorf("create template response missing data")
	}
	return resp.Data, nil
}

func (s *Session) GetTemplate(ctx context.Context, t ContractT, templateID string) (*apispec.Template, error) {
	if templateID == "" {
		return nil, fmt.Errorf("template id is required")
	}
	specPath := "/api/v1/templates/{id}"
	requestPath := "/api/v1/templates/" + templateID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("get template failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessTemplateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, fmt.Errorf("get template response missing data")
	}
	return resp.Data, nil
}

func (s *Session) UpdateTemplate(ctx context.Context, t ContractT, templateID string, template apispec.TemplateUpdateRequest) (*apispec.Template, error) {
	if templateID == "" {
		return nil, fmt.Errorf("template id is required")
	}
	specPath := "/api/v1/templates/{id}"
	requestPath := "/api/v1/templates/" + templateID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, template, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("update template failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessTemplateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, fmt.Errorf("update template response missing data")
	}
	return resp.Data, nil
}

func (s *Session) DeleteTemplate(ctx context.Context, t ContractT, templateID string) error {
	if templateID == "" {
		return fmt.Errorf("template id is required")
	}
	specPath := "/api/v1/templates/{id}"
	requestPath := "/api/v1/templates/" + templateID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return fmt.Errorf("delete template failed with status %d: %s", status, formatAPIError(body))
	}
	return nil
}

func CloneTemplateForCreate(base apispec.Template, name string) apispec.TemplateCreateRequest {
	return apispec.TemplateCreateRequest{
		TemplateId: name,
		Spec:       cloneSandboxTemplateSpec(base.Spec),
	}
}

func cloneSandboxTemplateSpec(base apispec.SandboxTemplateSpec) apispec.SandboxTemplateSpec {
	spec := base
	return spec
}
