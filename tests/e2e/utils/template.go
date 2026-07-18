package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

type TemplateFromSandboxCreateRequest struct {
	TemplateID string `json:"template_id"`
	SandboxID  string `json:"sandbox_id"`
}

type TemplateFromSandboxCreationStatus struct {
	State       string `json:"state"`
	Stage       string `json:"stage"`
	OutputImage string `json:"outputImage,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Message     string `json:"message,omitempty"`
}

type TemplateFromSandboxView struct {
	TemplateID string  `json:"template_id"`
	TeamID     *string `json:"team_id,omitempty"`
	Status     *struct {
		Creation *TemplateFromSandboxCreationStatus `json:"creation,omitempty"`
	} `json:"status,omitempty"`
}

type successTemplateFromSandboxResponse struct {
	Success bool                     `json:"success"`
	Data    *TemplateFromSandboxView `json:"data"`
}

func (s *Session) CreateTemplateFromSandboxDetailed(
	ctx context.Context,
	t ContractT,
	request TemplateFromSandboxCreateRequest,
	idempotencyKey string,
) (*TemplateFromSandboxView, int, error) {
	headers := make(http.Header)
	if idempotencyKey != "" {
		headers.Set("Idempotency-Key", idempotencyKey)
	}
	status, body, err := s.doJSONSpecRequestWithHeaders(
		t,
		ctx,
		http.MethodPost,
		"/api/v1/templates/from-sandbox",
		"/api/v1/templates/from-sandbox",
		request,
		true,
		headers,
	)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusAccepted {
		return nil, status, fmt.Errorf("create template from sandbox failed with status %d: %s", status, formatAPIError(body))
	}
	var response successTemplateFromSandboxResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, status, err
	}
	if !response.Success || response.Data == nil {
		return nil, status, fmt.Errorf("create template from sandbox response missing data")
	}
	return response.Data, status, nil
}

func (s *Session) GetTemplateFromSandboxView(ctx context.Context, t ContractT, templateID string) (*TemplateFromSandboxView, int, error) {
	if templateID == "" {
		return nil, 0, fmt.Errorf("template id is required")
	}
	specPath := "/api/v1/templates/{id}"
	requestPath := "/api/v1/templates/" + templateID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get template failed with status %d: %s", status, formatAPIError(body))
	}
	var response successTemplateFromSandboxResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, status, err
	}
	if !response.Success || response.Data == nil {
		return nil, status, fmt.Errorf("get template response missing data")
	}
	return response.Data, status, nil
}

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
	resp, _, err := s.CreateTemplateDetailed(ctx, t, template)
	return resp, err
}

func (s *Session) CreateSystemTemplate(
	ctx context.Context,
	env *framework.ScenarioEnv,
	template apispec.TemplateCreateRequest,
) (*apispec.Template, error) {
	status, body, err := s.doInternalSystemJSONRequest(ctx, env, http.MethodPost, "/api/v1/templates", template)
	if err != nil {
		return nil, err
	}
	if status != http.StatusCreated {
		return nil, fmt.Errorf("create system template failed with status %d: %s", status, formatAPIError(body))
	}
	var response apispec.SuccessTemplateResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if !response.Success || response.Data == nil {
		return nil, fmt.Errorf("create system template response missing data")
	}
	return response.Data, nil
}

func (s *Session) CreateTemplateDetailed(ctx context.Context, t ContractT, template apispec.TemplateCreateRequest) (*apispec.Template, int, error) {
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, "/api/v1/templates", "/api/v1/templates", template, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create template failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessTemplateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("create template response missing data")
	}
	return resp.Data, status, nil
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

func (s *Session) DeleteSystemTemplate(ctx context.Context, env *framework.ScenarioEnv, templateID string) error {
	if templateID == "" {
		return fmt.Errorf("template id is required")
	}
	status, body, err := s.doInternalSystemJSONRequest(
		ctx,
		env,
		http.MethodDelete,
		"/api/v1/templates/"+templateID,
		nil,
	)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return fmt.Errorf("delete system template failed with status %d: %s", status, formatAPIError(body))
	}
	return nil
}

func CloneTemplateForCreate(base apispec.Template, name string) apispec.TemplateCreateRequest {
	return apispec.TemplateCreateRequest{
		TemplateId: name,
		Spec:       base.Spec,
	}
}
