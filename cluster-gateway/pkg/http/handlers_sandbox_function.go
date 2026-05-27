package http

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"go.uber.org/zap"
)

func sandboxServiceIsFunction(service *mgr.SandboxAppService) bool {
	return service != nil &&
		service.Runtime != nil &&
		service.Runtime.Type == mgr.SandboxAppServiceRuntimeFunction &&
		service.Runtime.Function != nil
}

func (s *Server) executeSandboxFunctionExposure(c *gin.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService, route *mgr.SandboxAppServiceRoute) {
	if sandbox == nil || service == nil || service.Runtime == nil || service.Runtime.Function == nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid function service")
		return
	}
	body, err := readSandboxFunctionBody(c.Request.Body)
	if err != nil {
		if errors.Is(err, errSandboxFunctionBodyTooLarge) {
			spec.JSONError(c, http.StatusRequestEntityTooLarge, "request_too_large", "function request body is too large")
			return
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	procdURL, err := url.Parse(sandbox.InternalAddr)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid sandbox address")
		return
	}
	procdURL.Path = "/api/v1/functions/execute"
	procdURL.RawQuery = ""

	fn := service.Runtime.Function
	source := sandboxfunction.Source{
		Type:     fn.Source.Type,
		Filename: fn.Source.Filename,
		Code:     fn.Source.Code,
	}
	source.Digest = sandboxfunction.InlineDigest(source.Filename, source.Code)

	timeout := sandboxFunctionTimeout(route)
	execReq := sandboxfunction.ExecuteRequest{
		ServiceID: service.ID,
		Runtime:   fn.Runtime,
		Handler:   fn.Handler,
		Source:    source,
		EnvVars:   service.Runtime.EnvVars,
		Request: sandboxfunction.HTTPRequest{
			Method:     c.Request.Method,
			Path:       c.Request.URL.Path,
			RawQuery:   c.Request.URL.RawQuery,
			Headers:    externalRequestHeaders(c.Request.Header),
			BodyBase64: base64.StdEncoding.EncodeToString(body),
		},
		TimeoutMS: int(timeout / time.Millisecond),
	}
	if route != nil {
		execReq.RouteID = route.ID
	}
	payload, err := json.Marshal(execReq)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to encode function request")
		return
	}
	token, err := s.functionProcdToken(sandbox)
	if err != nil {
		s.logger.Error("Failed to generate function procd token",
			zap.String("sandbox_id", sandbox.ID),
			zap.String("team_id", sandbox.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	upReq, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, procdURL.String(), bytes.NewReader(payload))
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	upReq, cancel := proxy.ApplyRequestTimeout(upReq, timeout)
	defer cancel()
	upReq.Header.Set("Content-Type", "application/json")
	upReq.Header.Set(internalauth.DefaultTokenHeader, token)
	upReq.Header.Set(internalauth.TeamIDHeader, sandbox.TeamID)
	if sandbox.UserID != "" {
		upReq.Header.Set(internalauth.UserIDHeader, sandbox.UserID)
	}

	resp, err := s.outboundHTTPClient().Do(upReq)
	if err != nil {
		if proxy.IsTimeoutError(err) {
			spec.JSONError(c, http.StatusGatewayTimeout, spec.CodeUnavailable, "function execution timed out")
			return
		}
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "failed to connect to sandbox function runtime")
		return
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "failed to read function runtime response")
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
		return
	}

	var envelope struct {
		Success bool                            `json:"success"`
		Data    sandboxfunction.ExecuteResponse `json:"data"`
		Error   *spec.Error                     `json:"error,omitempty"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil || !envelope.Success {
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "function runtime returned an invalid response")
		return
	}
	writeSandboxFunctionResponse(c, envelope.Data)
}

func (s *Server) functionProcdToken(sandbox *mgr.Sandbox) (string, error) {
	if s.internalAuthGen == nil {
		return "", errors.New("internal auth generator unavailable")
	}
	return s.internalAuthGen.Generate(internalauth.ServiceProcd, sandbox.TeamID, sandbox.UserID, internalauth.GenerateOptions{
		SandboxID: sandbox.ID,
	})
}

var errSandboxFunctionBodyTooLarge = errors.New("function request body too large")

func readSandboxFunctionBody(body io.Reader) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	limited := io.LimitReader(body, sandboxfunction.MaxHTTPRequestBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(data) > sandboxfunction.MaxHTTPRequestBytes {
		return nil, errSandboxFunctionBodyTooLarge
	}
	return data, nil
}

func sandboxFunctionTimeout(route *mgr.SandboxAppServiceRoute) time.Duration {
	if route != nil && route.TimeoutSeconds > 0 {
		return time.Duration(route.TimeoutSeconds) * time.Second
	}
	return time.Duration(sandboxfunction.DefaultTimeoutMS) * time.Millisecond
}

func externalRequestHeaders(headers http.Header) map[string][]string {
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		if isHopByHopHeader(key) {
			continue
		}
		copied := make([]string, 0, len(values))
		for _, value := range values {
			copied = append(copied, value)
		}
		out[http.CanonicalHeaderKey(key)] = copied
	}
	return out
}

func writeSandboxFunctionResponse(c *gin.Context, response sandboxfunction.ExecuteResponse) {
	for key, values := range response.Headers {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			c.Writer.Header().Add(key, value)
		}
	}
	body, err := base64.StdEncoding.DecodeString(response.BodyBase64)
	if err != nil {
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "function returned invalid body")
		return
	}
	status := response.Status
	if status == 0 {
		status = http.StatusOK
	}
	c.Data(status, c.Writer.Header().Get("Content-Type"), body)
}

func isHopByHopHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}
