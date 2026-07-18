package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Session struct {
	baseURL   string
	apiPrefix string
	token     string
	teamID    string
	userID    string
	client    *http.Client
}

func NewAPISession(env *framework.ScenarioEnv, useEdge bool) (*Session, func(), error) {
	if env == nil {
		return nil, nil, fmt.Errorf("scenario env is required")
	}
	serviceName := env.Infra.Name + "-cluster-gateway"
	if useEdge {
		serviceName = env.Infra.Name + "-regional-gateway"
	}

	port, err := framework.GetServicePort(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, serviceName)
	if err != nil {
		return nil, nil, err
	}

	baseURL, cleanup, err := framework.PortForwardService(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, serviceName, port)
	if err != nil {
		return nil, nil, err
	}

	session := &Session{
		baseURL:   baseURL,
		apiPrefix: "/api/v1",
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
	return session, cleanup, nil
}

func (s *Session) doJSONSpecRequest(
	t ContractT,
	ctx context.Context,
	method string,
	specPath string,
	requestPath string,
	body any,
	withAuth bool,
) (int, []byte, error) {
	return s.doJSONSpecRequestWithHeaders(t, ctx, method, specPath, requestPath, body, withAuth, nil)
}

func (s *Session) doJSONSpecRequestWithHeaders(
	t ContractT,
	ctx context.Context,
	method string,
	specPath string,
	requestPath string,
	body any,
	withAuth bool,
	headers http.Header,
) (int, []byte, error) {
	if t != nil {
		ValidateRequestExample(t, method, specPath, defaultContentType, body)
	}
	status, respBody, err := s.doJSONRequestWithHeaders(ctx, method, requestPath, body, withAuth, headers)
	if err != nil {
		return 0, nil, err
	}
	if t != nil && shouldValidateResponse(t, method, specPath, status) {
		ValidateResponseExample(t, method, specPath, status, defaultContentType, respBody)
	}
	return status, respBody, nil
}

func (s *Session) doRawSpecRequest(
	t ContractT,
	ctx context.Context,
	method string,
	specPath string,
	requestPath string,
	body []byte,
	requestContentType string,
	responseContentType string,
	withAuth bool,
) (int, []byte, error) {
	if t != nil {
		ValidateRequestExample(t, method, specPath, requestContentType, body)
	}
	status, respBody, err := s.doRawRequest(ctx, method, requestPath, body, requestContentType, withAuth)
	if err != nil {
		return 0, nil, err
	}
	if t != nil && responseContentType != "" && shouldValidateResponse(t, method, specPath, status) {
		ValidateResponseExample(t, method, specPath, status, responseContentType, respBody)
	}
	return status, respBody, nil
}

func (s *Session) doJSONRequest(ctx context.Context, method, path string, body any, withAuth bool) (int, []byte, error) {
	return s.doJSONRequestWithHeaders(ctx, method, path, body, withAuth, nil)
}

func (s *Session) doJSONRequestWithHeaders(ctx context.Context, method, path string, body any, withAuth bool, headers http.Header) (int, []byte, error) {
	if s == nil {
		return 0, nil, fmt.Errorf("api session is nil")
	}
	path = ensureLeadingSlash(path)

	payload, contentType, err := encodeJSONRequestBody(body)
	if err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, payload)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	s.setAuthHeaders(req, withAuth)

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	return readResponseBody(resp)
}

func (s *Session) doRawRequest(ctx context.Context, method, path string, body []byte, contentType string, withAuth bool) (int, []byte, error) {
	if s == nil {
		return 0, nil, fmt.Errorf("api session is nil")
	}
	path = ensureLeadingSlash(path)

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
	s.setAuthHeaders(req, withAuth)

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	return readResponseBody(resp)
}

func encodeJSONRequestBody(body any) (io.Reader, string, error) {
	if body == nil {
		return nil, "", nil
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, "", fmt.Errorf("marshal request body: %w", err)
	}
	return bytes.NewReader(raw), defaultContentType, nil
}

func readResponseBody(resp *http.Response) (int, []byte, error) {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

func (s *Session) setAuthHeaders(req *http.Request, withAuth bool) {
	if !withAuth || s.token == "" {
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	if s.teamID != "" {
		req.Header.Set(internalauth.TeamIDHeader, s.teamID)
	}
}

func ensureLeadingSlash(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}
	return "/" + path
}

func formatAPIError(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	apiErr, err := decodeErrorEnvelope(body)
	if err != nil || apiErr == nil {
		return strings.TrimSpace(string(body))
	}
	return strings.TrimSpace(apiErr.Error.Message)
}

func decodeErrorEnvelope(body []byte) (*apispec.ErrorEnvelope, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var apiErr apispec.ErrorEnvelope
	if err := json.Unmarshal(body, &apiErr); err != nil {
		return nil, err
	}
	return &apiErr, nil
}

func shouldValidateResponse(t ContractT, method, path string, status int) bool {
	t.Helper()
	spec := loadOpenAPISpec(t)
	operation, err := findOperation(spec, method, path)
	if err != nil || operation == nil || operation.Responses == nil {
		return false
	}
	responseRef := operation.Responses.Status(status)
	if responseRef == nil {
		responseRef = operation.Responses.Default()
	}
	return responseRef != nil && responseRef.Value != nil
}
