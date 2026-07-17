package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const defaultExecutionScopeResolveTimeout = 100 * time.Millisecond
const defaultExecutionScopeResolveConcurrency = 32

var errExecutionScopeResolverSaturated = errors.New("execution scope resolver is saturated")

type executionScopeResolveRequest struct {
	SandboxIP  string
	TeamID     string
	SandboxID  string
	Transport  string
	LocalIP    string
	LocalPort  int
	RemoteIP   string
	RemotePort int
}

type executionScopeResolver interface {
	Resolve(context.Context, executionScopeResolveRequest) (*sandboxobservability.ExecutionScope, error)
}

type httpExecutionScopeResolver struct {
	procdPort int
	timeout   time.Duration
	client    *http.Client
	generator *internalauth.Generator
	slots     chan struct{}
}

func NewHTTPExecutionScopeResolver(procdPort int, generator *internalauth.Generator, client *http.Client) executionScopeResolver {
	if client == nil {
		client = &http.Client{
			Transport: &http.Transport{
				Proxy:               nil,
				DialContext:         (&net.Dialer{Timeout: defaultExecutionScopeResolveTimeout}).DialContext,
				DisableCompression:  true,
				DisableKeepAlives:   false,
				MaxIdleConnsPerHost: 16,
			},
		}
	}
	return &httpExecutionScopeResolver{
		procdPort: procdPort,
		timeout:   defaultExecutionScopeResolveTimeout,
		client:    client,
		generator: generator,
		slots:     make(chan struct{}, defaultExecutionScopeResolveConcurrency),
	}
}

func WithExecutionScopeResolver(resolver executionScopeResolver) ServerOption {
	return func(server *Server) {
		if server != nil {
			server.executionScopeResolver = resolver
		}
	}
}

func (r *httpExecutionScopeResolver) Resolve(ctx context.Context, request executionScopeResolveRequest) (*sandboxobservability.ExecutionScope, error) {
	if r == nil || r.generator == nil || r.client == nil || r.procdPort <= 0 {
		return nil, nil
	}
	if strings.TrimSpace(request.SandboxIP) == "" || strings.TrimSpace(request.TeamID) == "" {
		return nil, nil
	}
	localIP := net.ParseIP(strings.TrimSpace(request.LocalIP))
	remoteIP := net.ParseIP(strings.TrimSpace(request.RemoteIP))
	if localIP == nil || remoteIP == nil ||
		request.LocalPort <= 0 || request.LocalPort > 65535 ||
		request.RemotePort <= 0 || request.RemotePort > 65535 {
		return nil, nil
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	default:
		// Attribution is observability metadata and must never become network
		// backpressure when procd lookups are saturated.
		return nil, errExecutionScopeResolverSaturated
	}
	values := url.Values{}
	values.Set("transport", request.Transport)
	values.Set("local_ip", localIP.String())
	values.Set("local_port", strconv.Itoa(request.LocalPort))
	values.Set("remote_ip", remoteIP.String())
	values.Set("remote_port", strconv.Itoa(request.RemotePort))
	endpoint := url.URL{
		Scheme:   "http",
		Host:     net.JoinHostPort(request.SandboxIP, strconv.Itoa(r.procdPort)),
		Path:     "/api/v1/execution-scopes/resolve",
		RawQuery: values.Encode(),
	}
	requestCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	token, err := r.generator.Generate(internalauth.ServiceProcd, request.TeamID, "", internalauth.GenerateOptions{
		SandboxID: request.SandboxID,
	})
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set(internalauth.DefaultTokenHeader, token)
	response, err := r.client.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil, fmt.Errorf("procd execution scope resolve returned status %d", response.StatusCode)
	}
	var payload struct {
		ExecutionScope *sandboxobservability.ExecutionScope `json:"execution_scope"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 16*1024)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode procd execution scope: %w", err)
	}
	if payload.ExecutionScope == nil {
		return nil, nil
	}
	if err := sandboxobservability.ValidateExecutionScope(*payload.ExecutionScope); err != nil {
		return nil, fmt.Errorf("invalid procd execution scope: %w", err)
	}
	return payload.ExecutionScope, nil
}

func cloneExecutionScope(scope *sandboxobservability.ExecutionScope) *sandboxobservability.ExecutionScope {
	if scope == nil {
		return nil
	}
	cloned := *scope
	return &cloned
}

func equalExecutionScopes(left, right *sandboxobservability.ExecutionScope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
