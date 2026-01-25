package cases

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	managerapi "github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/tests/e2e/framework"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type apiSession struct {
	baseURL   string
	apiPrefix string
	token     string
	teamID    string
	userID    string
	client    *http.Client
}

type loginResponse struct {
	AccessToken string    `json:"access_token"`
	User        *userInfo `json:"user"`
}

type userInfo struct {
	ID            string  `json:"id"`
	DefaultTeamID *string `json:"default_team_id"`
}

type teamListResponse struct {
	Teams []teamInfo `json:"teams"`
}

type teamInfo struct {
	ID string `json:"id"`
}

type claimResponse struct {
	SandboxID    string  `json:"sandbox_id"`
	Status       string  `json:"status"`
	ProcdAddress string  `json:"procd_address"`
	PodName      string  `json:"pod_name"`
	Template     string  `json:"template"`
	Namespace    string  `json:"namespace"`
	ClusterID    *string `json:"cluster_id,omitempty"`
}

type templateListResponse struct {
	Templates []managerapi.SandboxTemplate `json:"templates"`
	Count     int                          `json:"count"`
}

func newAPISession(env *framework.ScenarioEnv, useEdge bool) (*apiSession, func(), error) {
	if env == nil {
		return nil, nil, fmt.Errorf("scenario env is required")
	}
	serviceName := env.Infra.Name + "-internal-gateway"
	if useEdge {
		serviceName = env.Infra.Name + "-edge-gateway"
	}

	port, err := framework.GetServicePort(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, serviceName)
	if err != nil {
		return nil, nil, err
	}

	baseURL, cleanup, err := framework.PortForwardService(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, serviceName, port)
	if err != nil {
		return nil, nil, err
	}

	session := &apiSession{
		baseURL:   baseURL,
		apiPrefix: "/api/v1",
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
	return session, cleanup, nil
}

func (s *apiSession) login(ctx context.Context, email, password string) error {
	if s == nil {
		return fmt.Errorf("api session is nil")
	}
	req := map[string]string{
		"email":    email,
		"password": password,
	}
	status, body, err := s.doRequest(ctx, http.MethodPost, "/auth/login", req, false)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("login failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}

	resp, apiErr, err := decodeSpecResponse[loginResponse](body)
	if err != nil {
		return err
	}
	if apiErr != nil {
		return fmt.Errorf("login error: %s", apiErr.Message)
	}
	if resp == nil || resp.AccessToken == "" || resp.User == nil || resp.User.ID == "" {
		return fmt.Errorf("login response missing user or token")
	}

	s.token = resp.AccessToken
	s.userID = resp.User.ID

	if resp.User.DefaultTeamID != nil && *resp.User.DefaultTeamID != "" {
		s.teamID = *resp.User.DefaultTeamID
		return nil
	}

	teams, err := s.listTeams(ctx)
	if err != nil {
		return err
	}
	if len(teams) == 0 || teams[0].ID == "" {
		return fmt.Errorf("no team available for user %s", resp.User.ID)
	}
	s.teamID = teams[0].ID
	return nil
}

func (s *apiSession) listTeams(ctx context.Context) ([]teamInfo, error) {
	status, body, err := s.doRequest(ctx, http.MethodGet, "/teams", nil, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list teams failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	resp, apiErr, err := decodeSpecResponse[teamListResponse](body)
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, fmt.Errorf("list teams error: %s", apiErr.Message)
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Teams, nil
}

func (s *apiSession) doAPIRequest(ctx context.Context, method, path string, body any) (int, []byte, error) {
	return s.doRequest(ctx, method, s.apiPrefix+path, body, true)
}

func (s *apiSession) doRawAPIRequest(ctx context.Context, method, path string, body []byte, contentType string) (int, []byte, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return s.doRawRequest(ctx, method, s.apiPrefix+path, body, contentType, true)
}

func (s *apiSession) claimSandbox(ctx context.Context, template string) (*claimResponse, error) {
	if s.teamID == "" || s.userID == "" {
		return nil, fmt.Errorf("team or user id missing")
	}
	req := map[string]any{
		"team_id":  s.teamID,
		"user_id":  s.userID,
		"template": template,
	}
	status, body, err := s.doAPIRequest(ctx, http.MethodPost, "/sandboxes", req)
	if err != nil {
		return nil, err
	}
	if status != http.StatusCreated {
		return nil, fmt.Errorf("claim sandbox failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	resp, apiErr, err := decodeSpecResponse[claimResponse](body)
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, fmt.Errorf("claim sandbox error: %s", apiErr.Message)
	}
	if resp == nil || resp.SandboxID == "" {
		return nil, fmt.Errorf("claim sandbox response missing id")
	}
	return resp, nil
}

func (s *apiSession) deleteSandbox(ctx context.Context, sandboxID string) error {
	if sandboxID == "" {
		return nil
	}
	status, body, err := s.doAPIRequest(ctx, http.MethodDelete, "/sandboxes/"+sandboxID, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return fmt.Errorf("delete sandbox failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (s *apiSession) listTemplates(ctx context.Context) ([]managerapi.SandboxTemplate, error) {
	status, body, err := s.doAPIRequest(ctx, http.MethodGet, "/templates", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list templates failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	resp, apiErr, err := decodeSpecResponse[templateListResponse](body)
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, fmt.Errorf("list templates error: %s", apiErr.Message)
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Templates, nil
}

func (s *apiSession) createTemplate(ctx context.Context, template managerapi.SandboxTemplate) (*managerapi.SandboxTemplate, error) {
	status, body, err := s.doAPIRequest(ctx, http.MethodPost, "/templates", template)
	if err != nil {
		return nil, err
	}
	if status != http.StatusCreated {
		return nil, fmt.Errorf("create template failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	resp, apiErr, err := decodeSpecResponse[managerapi.SandboxTemplate](body)
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, fmt.Errorf("create template error: %s", apiErr.Message)
	}
	return resp, nil
}

func (s *apiSession) updateTemplate(ctx context.Context, templateID string, template managerapi.SandboxTemplate) (*managerapi.SandboxTemplate, error) {
	if templateID == "" {
		return nil, fmt.Errorf("template id is required")
	}
	status, body, err := s.doAPIRequest(ctx, http.MethodPut, "/templates/"+templateID, template)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("update template failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	resp, apiErr, err := decodeSpecResponse[managerapi.SandboxTemplate](body)
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, fmt.Errorf("update template error: %s", apiErr.Message)
	}
	return resp, nil
}

func (s *apiSession) deleteTemplate(ctx context.Context, templateID string) error {
	if templateID == "" {
		return fmt.Errorf("template id is required")
	}
	status, body, err := s.doAPIRequest(ctx, http.MethodDelete, "/templates/"+templateID, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return fmt.Errorf("delete template failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	return nil
}

func cloneTemplateForCreate(base managerapi.SandboxTemplate, name string) managerapi.SandboxTemplate {
	template := managerapi.SandboxTemplate{
		TypeMeta: metav1.TypeMeta{
			Kind:       "SandboxTemplate",
			APIVersion: managerapi.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   base.Namespace,
			Labels:      base.Labels,
			Annotations: base.Annotations,
		},
		Spec: base.Spec,
	}
	return template
}

func (s *apiSession) doRequest(ctx context.Context, method, path string, body any, withAuth bool) (int, []byte, error) {
	if s == nil {
		return 0, nil, fmt.Errorf("api session is nil")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	var payload io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal request body: %w", err)
		}
		payload = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, payload)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if withAuth && s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

func (s *apiSession) doRawRequest(ctx context.Context, method, path string, body []byte, contentType string, withAuth bool) (int, []byte, error) {
	if s == nil {
		return 0, nil, fmt.Errorf("api session is nil")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	var payload io.Reader
	if len(body) > 0 {
		payload = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, payload)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if withAuth && s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

func decodeSpecResponse[T any](body []byte) (*T, *spec.Error, error) {
	return spec.DecodeResponse[T](bytes.NewReader(body))
}
