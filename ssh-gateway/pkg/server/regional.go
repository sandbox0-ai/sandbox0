package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
	"go.uber.org/zap"
)

// RegionalSandboxResolver resolves sandbox runtime targets through the
// regional-gateway internal SSH routing surface.
type RegionalSandboxResolver struct {
	baseURL         string
	internalAuthGen *internalauth.Generator
	httpClient      *http.Client
	logger          *zap.Logger
}

func NewRegionalSandboxResolver(baseURL string, internalAuthGen *internalauth.Generator, logger *zap.Logger, timeout time.Duration) *RegionalSandboxResolver {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &RegionalSandboxResolver{
		baseURL:         strings.TrimRight(baseURL, "/"),
		internalAuthGen: internalAuthGen,
		httpClient:      &http.Client{Timeout: timeout},
		logger:          logger,
	}
}

func (r *RegionalSandboxResolver) ResolveSandbox(ctx context.Context, sandboxID, userID string) (*sharedssh.ResolvedTarget, error) {
	if strings.TrimSpace(r.baseURL) == "" {
		return nil, fmt.Errorf("regional gateway URL is required")
	}
	if strings.TrimSpace(userID) == "" {
		return nil, fmt.Errorf("userID is required")
	}

	token, err := r.internalAuthGen.GenerateSystem(internalauth.ServiceRegionalGateway, internalauth.GenerateOptions{})
	if err != nil {
		return nil, fmt.Errorf("generate regional-gateway token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/internal/v1/sandboxes/"+sandboxID+"/ssh-target", nil)
	if err != nil {
		return nil, fmt.Errorf("create regional-gateway request: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set(internalauth.UserIDHeader, userID)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call regional-gateway ssh target endpoint: %w", err)
	}
	defer resp.Body.Close()

	target, apiErr, err := spec.DecodeResponse[sharedssh.ResolvedTarget](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode regional-gateway ssh target response: %w", err)
	}
	if err := resolveRegionalSandboxError(resp.StatusCode, apiErr); err != nil {
		return nil, err
	}
	if target == nil {
		return nil, fmt.Errorf("regional-gateway ssh target response was empty")
	}
	return target, nil
}

func resolveRegionalSandboxError(statusCode int, apiErr *spec.Error) error {
	switch statusCode {
	case http.StatusOK:
		return nil
	case http.StatusForbidden:
		return ErrSandboxAccessDenied
	case http.StatusNotFound:
		return ErrSandboxUnavailable
	case http.StatusServiceUnavailable:
		if apiErr != nil && strings.Contains(strings.ToLower(apiErr.Message), "waking up") {
			return ErrSandboxWakingUp
		}
		return ErrSandboxUnavailable
	default:
		if apiErr != nil && apiErr.Message != "" {
			return errors.New(apiErr.Message)
		}
		return fmt.Errorf("regional-gateway ssh target request failed with status %d", statusCode)
	}
}
