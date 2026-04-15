package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

type httpEgressAuthResolver struct {
	baseURL       string
	client        *http.Client
	tokenProvider EgressAuthTokenProvider
}

type EgressAuthTokenProvider interface {
	Token(ctx context.Context) (string, error)
}

func NewHTTPEgressAuthResolver(baseURL string, timeout time.Duration, tokenProvider EgressAuthTokenProvider) egressAuthResolver {
	return NewHTTPEgressAuthResolverWithHTTPClient(baseURL, timeout, tokenProvider, nil)
}

func NewHTTPEgressAuthResolverWithHTTPClient(baseURL string, timeout time.Duration, tokenProvider EgressAuthTokenProvider, client *http.Client) egressAuthResolver {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return noopEgressAuthResolver{}
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	return &httpEgressAuthResolver{
		baseURL:       strings.TrimRight(baseURL, "/"),
		tokenProvider: tokenProvider,
		client:        client,
	}
}

func (r *httpEgressAuthResolver) Resolve(ctx context.Context, req *egressauth.ResolveRequest) (*egressauth.ResolveResponse, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("egress auth resolver client is not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("resolve request is nil")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal resolve request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+"/internal/v1/egress-auth/resolve", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build resolve request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if r.tokenProvider != nil {
		token, err := r.tokenProvider.Token(ctx)
		if err != nil {
			return nil, fmt.Errorf("generate internal token: %w", err)
		}
		if token == "" {
			return nil, fmt.Errorf("generate internal token: empty token")
		}
		httpReq.Header.Set("X-Internal-Token", token)
	}

	resp, err := r.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("resolve egress auth: %w", err)
	}
	defer resp.Body.Close()

	payload, apiErr, err := spec.DecodeResponse[egressauth.ResolveResponse](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode resolve response: %w", err)
	}
	if resp.StatusCode >= 400 {
		if apiErr != nil {
			return nil, fmt.Errorf("resolve egress auth: %s", apiErr.Message)
		}
		return nil, fmt.Errorf("resolve egress auth: resolver returned status %d", resp.StatusCode)
	}
	if apiErr != nil {
		return nil, fmt.Errorf("resolve egress auth: %s", apiErr.Message)
	}
	return payload, nil
}
