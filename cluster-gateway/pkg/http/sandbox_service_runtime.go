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
	"slices"
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

type procdContextListResponse struct {
	Contexts []procdContextResponse `json:"contexts"`
}

type procdContextResponse struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	Command []string          `json:"command,omitempty"`
	CWD     string            `json:"cwd"`
	EnvVars map[string]string `json:"env_vars"`
	Running bool              `json:"running"`
	Paused  bool              `json:"paused"`
}

type procdCreateContextRequest struct {
	Type    string                        `json:"type"`
	Cmd     *procdCreateCMDContextRequest `json:"cmd,omitempty"`
	CWD     string                        `json:"cwd,omitempty"`
	EnvVars map[string]string             `json:"env_vars,omitempty"`
}

type procdCreateCMDContextRequest struct {
	Command []string `json:"command"`
}

type procdEnsureNextJSRuntimeRequest struct {
	ServiceID string            `json:"service_id"`
	Port      int               `json:"port"`
	CWD       string            `json:"cwd,omitempty"`
	EnvVars   map[string]string `json:"env_vars,omitempty"`
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
		running, err := s.sandboxServiceCMDContextRunning(ctx, sandbox, service)
		if err != nil {
			return err
		}
		if !running {
			if err := s.createSandboxServiceCMDContext(ctx, sandbox, service); err != nil {
				return err
			}
		}
		return s.waitSandboxServiceReady(ctx, sandbox, service, route)
	case mgr.SandboxAppServiceRuntimeNextJS:
		if err := s.ensureSandboxServiceNextJSRuntime(ctx, sandbox, service); err != nil {
			return err
		}
		return s.waitSandboxServiceReady(ctx, sandbox, service, route)
	default:
		return nil
	}
}

func (s *Server) sandboxServiceCMDContextRunning(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService) (bool, error) {
	var list procdContextListResponse
	if err := s.doSandboxProcdJSON(ctx, sandbox, http.MethodGet, "/api/v1/contexts", nil, &list); err != nil {
		return false, err
	}
	for _, candidate := range list.Contexts {
		if sandboxServiceContextMatchesCMD(candidate, service) {
			return true, nil
		}
	}
	return false, nil
}

func sandboxServiceContextMatchesCMD(ctx procdContextResponse, service *mgr.SandboxAppService) bool {
	if service == nil || service.Runtime == nil {
		return false
	}
	if ctx.Type != "cmd" || !ctx.Running || ctx.Paused {
		return false
	}
	if ctx.EnvVars[sandboxServiceRuntimeServiceIDEnv] != service.ID {
		return false
	}
	if !slices.Equal(ctx.Command, service.Runtime.Command) {
		return false
	}
	return strings.TrimSpace(ctx.CWD) == strings.TrimSpace(service.Runtime.CWD)
}

func (s *Server) createSandboxServiceCMDContext(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService) error {
	req := procdCreateContextRequest{
		Type: "cmd",
		Cmd: &procdCreateCMDContextRequest{
			Command: service.Runtime.Command,
		},
		CWD:     service.Runtime.CWD,
		EnvVars: sandboxServiceRuntimeEnvVars(service),
	}
	var created procdContextResponse
	return s.doSandboxProcdJSON(ctx, sandbox, http.MethodPost, "/api/v1/contexts", req, &created)
}

func sandboxServiceRuntimeEnvVars(service *mgr.SandboxAppService) map[string]string {
	env := make(map[string]string, len(service.Runtime.EnvVars)+3)
	for key, value := range service.Runtime.EnvVars {
		env[key] = value
	}
	env[sandboxServiceRuntimeServiceIDEnv] = service.ID
	env[sandboxServiceRuntimePortEnv] = strconv.Itoa(service.Port)
	env[sandboxServiceRuntimeTypeEnv] = service.Runtime.Type
	return env
}

func (s *Server) ensureSandboxServiceNextJSRuntime(ctx context.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService) error {
	req := procdEnsureNextJSRuntimeRequest{
		ServiceID: service.ID,
		Port:      service.Port,
		CWD:       service.Runtime.CWD,
		EnvVars:   sandboxServiceRuntimeEnvVars(service),
	}
	return s.doSandboxProcdJSON(ctx, sandbox, http.MethodPost, "/api/v1/services/nextjs/ensure", req, nil)
}

func (s *Server) doSandboxProcdJSON(ctx context.Context, sandbox *mgr.Sandbox, method, path string, payload any, out any) error {
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
