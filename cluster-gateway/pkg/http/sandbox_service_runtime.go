package http

import (
	"bytes"
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

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

const (
	sandboxServiceRuntimeServiceIDEnv = "SANDBOX0_SERVICE_ID"
	sandboxServiceRuntimePortEnv      = "SANDBOX0_SERVICE_PORT"
	sandboxServiceRuntimeTypeEnv      = "SANDBOX0_SERVICE_RUNTIME"
	sandboxServiceReadinessInterval   = 200 * time.Millisecond
)

var errSandboxServiceReadinessTimeout = errors.New("sandbox service did not become ready")

type procdSessionListResponse struct {
	Sessions []procdSessionResponse `json:"sessions"`
}

type procdSessionResponse struct {
	ID    string           `json:"id"`
	Spec  procdSessionSpec `json:"spec"`
	Phase string           `json:"phase"`
}

type procdSessionSpec struct {
	Name      string                `json:"name,omitempty"`
	Command   []string              `json:"command"`
	CWD       string                `json:"cwd,omitempty"`
	Env       map[string]string     `json:"env,omitempty"`
	IO        procdSessionIO        `json:"io,omitempty"`
	Lifecycle procdSessionLifecycle `json:"lifecycle,omitempty"`
}

type procdSessionIO struct {
	Mode string `json:"mode"`
}

type procdSessionLifecycle struct {
	DesiredState    string              `json:"desired_state"`
	Restart         procdSessionRestart `json:"restart"`
	RuntimeRecovery string              `json:"runtime_recovery"`
}

type procdSessionRestart struct {
	Policy string `json:"policy"`
}

func (s *Server) ensureSandboxServiceRuntime(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService, route *mgr.SandboxAppServiceRoute) error {
	if service == nil || service.Runtime == nil {
		return nil
	}
	switch service.Runtime.Type {
	case mgr.SandboxAppServiceRuntimeCMD:
		if len(service.Runtime.Command) == 0 {
			return errors.New("cmd runtime command is required")
		}
		if err := s.ensureSandboxServiceCMDSession(ctx, sandbox, service); err != nil {
			return err
		}
		return s.waitSandboxServiceReady(ctx, sandbox, service, route)
	default:
		return nil
	}
}

func (s *Server) ensureSandboxServiceCMDSession(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService) error {
	var list procdSessionListResponse
	if err := s.doSandboxProcdJSON(ctx, sandbox, http.MethodGet, "/api/v1/sessions", nil, &list); err != nil {
		return err
	}
	desired := sandboxServiceCMDSessionSpec(service)
	for _, candidate := range list.Sessions {
		if candidate.Spec.Name != desired.Name {
			continue
		}
		if !sandboxServiceSessionSpecMatches(candidate.Spec, desired) {
			var updated procdSessionResponse
			return s.doSandboxProcdJSON(ctx, sandbox, http.MethodPut, "/api/v1/sessions/"+url.PathEscape(candidate.ID), desired, &updated)
		}
		switch candidate.Phase {
		case "pending", "starting", "running", "backoff":
			return nil
		default:
			request := map[string]string{"state": "running"}
			var updated procdSessionResponse
			return s.doSandboxProcdJSON(ctx, sandbox, http.MethodPut, "/api/v1/sessions/"+url.PathEscape(candidate.ID)+"/desired-state", request, &updated)
		}
	}
	return s.createSandboxServiceCMDSession(ctx, sandbox, service)
}

func sandboxServiceSessionSpecMatches(value, desired procdSessionSpec) bool {
	if value.Name != desired.Name || !equalStrings(value.Command, desired.Command) {
		return false
	}
	if strings.TrimSpace(value.CWD) != strings.TrimSpace(desired.CWD) || value.IO.Mode != desired.IO.Mode {
		return false
	}
	if value.Lifecycle.DesiredState != desired.Lifecycle.DesiredState ||
		value.Lifecycle.Restart.Policy != desired.Lifecycle.Restart.Policy ||
		value.Lifecycle.RuntimeRecovery != desired.Lifecycle.RuntimeRecovery {
		return false
	}
	if len(value.Env) != len(desired.Env) {
		return false
	}
	for key, desiredValue := range desired.Env {
		if value.Env[key] != desiredValue {
			return false
		}
	}
	return true
}

func sandboxServiceCMDSessionSpec(service *mgr.SandboxAppService) procdSessionSpec {
	return procdSessionSpec{
		Name:    sandboxServiceRuntimeSessionName(service.ID),
		Command: service.Runtime.Command,
		CWD:     service.Runtime.CWD,
		Env:     sandboxServiceRuntimeEnvVars(service),
		IO:      procdSessionIO{Mode: "pipes"},
		Lifecycle: procdSessionLifecycle{
			DesiredState: "running",
			Restart: procdSessionRestart{
				Policy: "on_failure",
			},
			RuntimeRecovery: "restart",
		},
	}
}

func (s *Server) createSandboxServiceCMDSession(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService) error {
	req := sandboxServiceCMDSessionSpec(service)
	var created procdSessionResponse
	headers := http.Header{"Idempotency-Key": []string{"sandbox-service-runtime:" + service.ID}}
	return s.doSandboxProcdJSONWithHeaders(ctx, sandbox, http.MethodPost, "/api/v1/sessions", req, headers, &created)
}

func sandboxServiceRuntimeSessionName(serviceID string) string {
	return "sandbox-service:" + serviceID
}

func sandboxServiceRuntimeEnvVars(service *mgr.SandboxAppService) map[string]string {
	env := make(map[string]string, len(service.Runtime.EnvVars)+3)
	for key, value := range service.Runtime.EnvVars {
		env[key] = value
	}
	env[sandboxServiceRuntimeServiceIDEnv] = service.ID
	env[sandboxServiceRuntimePortEnv] = strconv.Itoa(service.Port)
	env[sandboxServiceRuntimeTypeEnv] = mgr.SandboxAppServiceRuntimeCMD
	return env
}

func (s *Server) doSandboxProcdJSON(ctx context.Context, sandbox *mgr.Sandbox, method, path string, payload any, out any) error {
	return s.doSandboxProcdJSONWithHeaders(ctx, sandbox, method, path, payload, nil, out)
}

func (s *Server) doSandboxProcdJSONWithHeaders(ctx context.Context, sandbox *mgr.Sandbox, method, path string, payload any, headers http.Header, out any) error {
	if sandbox == nil {
		return errors.New("sandbox is required")
	}
	procdURL, err := functionProcdURL(sandbox.InternalAddr, path)
	if err != nil {
		return err
	}
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode procd request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, procdURL.String(), body)
	if err != nil {
		return err
	}
	token, err := s.functionProcdToken(sandbox)
	if err != nil {
		return err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		if message, ok := spec.DecodeErrorMessage(data); ok {
			return fmt.Errorf("procd returned %d: %s", resp.StatusCode, message)
		}
		return fmt.Errorf("procd returned %d", resp.StatusCode)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	value, apiErr, err := spec.DecodeResponse[json.RawMessage](resp.Body)
	if err != nil {
		return err
	}
	if apiErr != nil {
		return errors.New(apiErr.Message)
	}
	if value == nil || len(*value) == 0 || string(*value) == "null" {
		return nil
	}
	if err := json.Unmarshal(*value, out); err != nil {
		return err
	}
	return nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (s *Server) waitSandboxServiceReady(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService, route *mgr.SandboxAppServiceRoute) error {
	timeout := s.sandboxServiceReadinessTimeout(route)
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(sandboxServiceReadinessInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		ready, err := s.checkSandboxServiceReady(readyCtx, sandbox, service)
		if ready {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-readyCtx.Done():
			if lastErr != nil {
				return fmt.Errorf("%w: %v", errSandboxServiceReadinessTimeout, lastErr)
			}
			return errSandboxServiceReadinessTimeout
		case <-ticker.C:
		}
	}
}

func (s *Server) sandboxServiceReadinessTimeout(route *mgr.SandboxAppServiceRoute) time.Duration {
	if route != nil && route.TimeoutSeconds > 0 {
		return time.Duration(route.TimeoutSeconds) * time.Second
	}
	if s != nil && s.cfg != nil && s.cfg.ProxyTimeout.Duration > 0 {
		return s.cfg.ProxyTimeout.Duration
	}
	return 10 * time.Second
}

func (s *Server) checkSandboxServiceReady(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService) (bool, error) {
	targetURL, err := withPort(sandbox.InternalAddr, service.Port)
	if err != nil {
		return false, err
	}
	if service.HealthCheck != nil && strings.TrimSpace(service.HealthCheck.Path) != "" {
		return s.checkSandboxServiceHTTPReady(ctx, targetURL, service.HealthCheck.Path)
	}
	return checkSandboxServiceTCPReady(ctx, targetURL)
}

func (s *Server) checkSandboxServiceHTTPReady(ctx context.Context, targetURL *url.URL, path string) (bool, error) {
	healthURL := *targetURL
	healthURL.Path = sandboxServiceHealthPath(path)
	healthURL.RawQuery = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL.String(), nil)
	if err != nil {
		return false, err
	}
	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode >= 200 && resp.StatusCode < 400, nil
}

func checkSandboxServiceTCPReady(ctx context.Context, targetURL *url.URL) (bool, error) {
	host := targetURL.Hostname()
	port := targetURL.Port()
	if host == "" || port == "" {
		return false, errors.New("service target address is missing host or port")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return false, err
	}
	_ = conn.Close()
	return true, nil
}

func sandboxServiceHealthPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || strings.HasPrefix(path, "/") {
		return path
	}
	return "/" + path
}
