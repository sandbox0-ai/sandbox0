package utils

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	gatewayspec "github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

type PutTeamQuotaRequest struct {
	LimitValue int64  `json:"limit_value"`
	IntervalMS *int64 `json:"interval_ms,omitempty"`
	BurstValue *int64 `json:"burst_value,omitempty"`
}

func (s *Session) PutTeamQuota(ctx context.Context, env *framework.ScenarioEnv, dimension quota.Dimension, limitValue int64) (*quota.Policy, int, error) {
	return s.putTeamQuotaPolicy(ctx, env, dimension, PutTeamQuotaRequest{LimitValue: limitValue})
}

func (s *Session) PutTeamRateQuota(ctx context.Context, env *framework.ScenarioEnv, dimension quota.Dimension, limitValue, intervalMS, burstValue int64) (*quota.Policy, int, error) {
	return s.putTeamQuotaPolicy(ctx, env, dimension, PutTeamQuotaRequest{
		LimitValue: limitValue,
		IntervalMS: &intervalMS,
		BurstValue: &burstValue,
	})
}

func (s *Session) putTeamQuotaPolicy(ctx context.Context, env *framework.ScenarioEnv, dimension quota.Dimension, request PutTeamQuotaRequest) (*quota.Policy, int, error) {
	path, err := s.teamQuotaInternalPath(dimension)
	if err != nil {
		return nil, 0, err
	}
	status, body, err := s.doInternalSystemJSONRequest(ctx, env, http.MethodPut, path, request)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("put team quota failed with status %d: %s", status, formatAPIError(body))
	}
	resp, apiErr, err := gatewayspec.DecodeResponse[quota.Policy](bytes.NewReader(body))
	if err != nil {
		return nil, status, err
	}
	if apiErr != nil {
		return nil, status, fmt.Errorf("put team quota failed: %s", apiErr.Message)
	}
	return resp, status, nil
}

func (s *Session) DeleteTeamQuota(ctx context.Context, env *framework.ScenarioEnv, dimension quota.Dimension) (int, error) {
	path, err := s.teamQuotaInternalPath(dimension)
	if err != nil {
		return 0, err
	}
	status, body, err := s.doInternalSystemJSONRequest(ctx, env, http.MethodDelete, path, nil)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, fmt.Errorf("delete team quota failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) teamQuotaInternalPath(dimension quota.Dimension) (string, error) {
	if s == nil {
		return "", fmt.Errorf("api session is nil")
	}
	teamID := strings.TrimSpace(s.teamID)
	if teamID == "" {
		return "", fmt.Errorf("team_id is required")
	}
	return "/internal/v1/teams/" + url.PathEscape(teamID) + "/quotas/" + url.PathEscape(string(dimension)), nil
}

func (s *Session) doInternalSystemJSONRequest(ctx context.Context, env *framework.ScenarioEnv, method, path string, body any) (int, []byte, error) {
	if s == nil {
		return 0, nil, fmt.Errorf("api session is nil")
	}
	baseURL, cleanup, err := managerInternalBaseURL(ctx, env)
	if err != nil {
		return 0, nil, err
	}
	defer cleanup()

	token, err := managerInternalSystemToken(ctx, env)
	if err != nil {
		return 0, nil, err
	}

	path = ensureLeadingSlash(path)
	payload, contentType, err := encodeJSONRequestBody(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, payload)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	return readResponseBody(resp)
}

func managerInternalBaseURL(ctx context.Context, env *framework.ScenarioEnv) (string, func(), error) {
	if env == nil {
		return "", nil, fmt.Errorf("scenario env is required")
	}
	serviceName := env.Infra.Name + "-manager"
	port, err := framework.GetServicePort(ctx, env.Config.Kubeconfig, env.Infra.Namespace, serviceName)
	if err != nil {
		return "", nil, err
	}
	return framework.PortForwardService(ctx, env.Config.Kubeconfig, env.Infra.Namespace, serviceName, port)
}

func managerInternalSystemToken(ctx context.Context, env *framework.ScenarioEnv) (string, error) {
	if env == nil {
		return "", fmt.Errorf("scenario env is required")
	}
	secretName := env.Infra.Name + "-sandbox0-internal-jwt-data-plane"
	output, err := framework.KubectlOutput(
		ctx,
		env.Config.Kubeconfig,
		"get",
		"secret",
		secretName,
		"--namespace",
		env.Infra.Namespace,
		"-o",
		"jsonpath={.data.private\\.key}",
	)
	if err != nil {
		return "", err
	}
	encoded := strings.TrimSpace(output)
	if encoded == "" {
		return "", fmt.Errorf("secret %q private.key is empty", secretName)
	}
	privateKeyPEM, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("decode secret %q private.key: %w", secretName, err)
	}
	privateKey, err := internalauth.LoadEd25519PrivateKey(privateKeyPEM)
	if err != nil {
		return "", err
	}
	generator := internalauth.NewGenerator(internalauth.DefaultGeneratorConfig(internalauth.ServiceClusterGateway, privateKey))
	return generator.GenerateSystem(internalauth.ServiceManager, internalauth.GenerateOptions{Permissions: []string{"*:*"}})
}
