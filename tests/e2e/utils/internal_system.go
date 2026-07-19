package utils

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func (s *Session) doInternalSystemJSONRequest(
	ctx context.Context,
	env *framework.ScenarioEnv,
	method string,
	path string,
	body any,
) (int, []byte, error) {
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
	request, err := http.NewRequestWithContext(ctx, method, baseURL+path, payload)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	request.Header.Set(internalauth.DefaultTokenHeader, token)

	response, err := s.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	return readResponseBody(response)
}

func managerInternalBaseURL(
	ctx context.Context,
	env *framework.ScenarioEnv,
) (string, func(), error) {
	if env == nil {
		return "", nil, fmt.Errorf("scenario env is required")
	}
	serviceName := env.Infra.Name + "-manager"
	port, err := framework.GetServicePort(
		ctx,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		serviceName,
	)
	if err != nil {
		return "", nil, err
	}
	return framework.PortForwardService(
		ctx,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		serviceName,
		port,
	)
}

func managerInternalSystemToken(
	ctx context.Context,
	env *framework.ScenarioEnv,
) (string, error) {
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
	generator := internalauth.NewGenerator(
		internalauth.DefaultGeneratorConfig(internalauth.ServiceClusterGateway, privateKey),
	)
	return generator.GenerateSystem(
		internalauth.ServiceManager,
		internalauth.GenerateOptions{Permissions: []string{"*:*"}},
	)
}
