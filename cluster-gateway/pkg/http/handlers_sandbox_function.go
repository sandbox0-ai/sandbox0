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
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
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
	if proxy.IsWebSocketUpgrade(c.Request) {
		s.proxySandboxFunctionWebSocket(c, sandbox, service, route)
		return
	}
	if sandboxFunctionWantsStream(c.Request) {
		s.streamSandboxFunctionExposure(c, sandbox, service, route)
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

	timeout := sandboxFunctionTimeout(route)
	execReq := buildSandboxFunctionExecuteRequest(c.Request, service, route, base64.StdEncoding.EncodeToString(body), int(timeout/time.Millisecond))
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

func (s *Server) streamSandboxFunctionExposure(c *gin.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService, route *mgr.SandboxAppServiceRoute) {
	body, err := readSandboxFunctionBody(c.Request.Body)
	if err != nil {
		if errors.Is(err, errSandboxFunctionBodyTooLarge) {
			spec.JSONError(c, http.StatusRequestEntityTooLarge, "request_too_large", "function request body is too large")
			return
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	procdURL, err := functionProcdURL(sandbox.InternalAddr, "/api/v1/functions/stream")
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid sandbox address")
		return
	}
	execReq := buildSandboxFunctionExecuteRequest(c.Request, service, route, base64.StdEncoding.EncodeToString(body), sandboxFunctionTimeoutMS(route))
	payload, err := json.Marshal(execReq)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to encode function request")
		return
	}
	token, err := s.functionProcdToken(sandbox)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}
	upReq, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, procdURL.String(), bytes.NewReader(payload))
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	upReq.Header.Set("Content-Type", "application/json")
	upReq.Header.Set(internalauth.DefaultTokenHeader, token)
	upReq.Header.Set(internalauth.TeamIDHeader, sandbox.TeamID)
	if sandbox.UserID != "" {
		upReq.Header.Set(internalauth.UserIDHeader, sandbox.UserID)
	}
	if route != nil && route.TimeoutSeconds > 0 {
		var cancel func()
		upReq, cancel = proxy.ApplyRequestTimeout(upReq, sandboxFunctionTimeout(route))
		defer cancel()
	}

	if err := proxy.DisableResponseWriteDeadline(c.Writer); err != nil {
		s.logger.Debug("Failed to disable function stream response deadline", zap.Error(err))
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
	for key, values := range resp.Header {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			c.Writer.Header().Add(key, value)
		}
	}
	c.Writer.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(flushingWriter{ResponseWriter: c.Writer}, resp.Body)
}

func (s *Server) proxySandboxFunctionWebSocket(c *gin.Context, sandbox *mgr.Sandbox, service *mgr.SandboxAppService, route *mgr.SandboxAppServiceRoute) {
	procdURL, err := functionProcdURL(sandbox.InternalAddr, "/api/v1/functions/ws")
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid sandbox address")
		return
	}
	switch procdURL.Scheme {
	case "https":
		procdURL.Scheme = "wss"
	default:
		procdURL.Scheme = "ws"
	}
	token, err := s.functionProcdToken(sandbox)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}
	headers := http.Header{}
	headers.Set(internalauth.DefaultTokenHeader, token)
	headers.Set(internalauth.TeamIDHeader, sandbox.TeamID)
	if sandbox.UserID != "" {
		headers.Set(internalauth.UserIDHeader, sandbox.UserID)
	}
	upstream, _, err := websocket.DefaultDialer.DialContext(c.Request.Context(), procdURL.String(), headers)
	if err != nil {
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "failed to connect to sandbox function runtime")
		return
	}
	defer upstream.Close()
	execReq := buildSandboxFunctionExecuteRequest(c.Request, service, route, "", 0)
	if err := upstream.WriteJSON(execReq); err != nil {
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "failed to initialize sandbox function runtime")
		return
	}

	if err := proxy.DisableResponseDeadlines(c.Writer); err != nil {
		s.logger.Debug("Failed to disable function websocket response deadlines", zap.Error(err))
	}
	downstream, err := functionWebSocketUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		s.logger.Error("Function websocket upgrade failed", zap.Error(err))
		return
	}
	defer downstream.Close()
	if err := proxy.DisableConnectionDeadlines(downstream.UnderlyingConn()); err != nil {
		s.logger.Debug("Failed to clear function websocket connection deadlines", zap.Error(err))
	}

	closeBoth := sync.OnceFunc(func() {
		_ = downstream.Close()
		_ = upstream.Close()
	})
	errCh := make(chan error, 2)
	go relayWebSocketMessages(upstream, downstream, errCh)
	go relayWebSocketMessages(downstream, upstream, errCh)
	<-errCh
	closeBoth()
}

func buildSandboxFunctionExecuteRequest(r *http.Request, service *mgr.SandboxAppService, route *mgr.SandboxAppServiceRoute, bodyBase64 string, timeoutMS int) sandboxfunction.ExecuteRequest {
	fn := service.Runtime.Function
	source := sandboxfunction.Source{
		Type:     fn.Source.Type,
		Filename: sandboxfunction.DefaultFilename,
		Code:     fn.Source.Code,
	}
	execReq := sandboxfunction.ExecuteRequest{
		ServiceID:      service.ID,
		Runtime:        fn.Runtime,
		Handler:        fn.Handler,
		MaxConcurrency: fn.MaxConcurrency,
		Source:         source,
		EnvVars:        service.Runtime.EnvVars,
		Request: sandboxfunction.HTTPRequest{
			Method:     r.Method,
			Path:       r.URL.Path,
			RawQuery:   r.URL.RawQuery,
			Headers:    externalRequestHeaders(r.Header),
			BodyBase64: bodyBase64,
		},
		TimeoutMS: timeoutMS,
	}
	if route != nil {
		execReq.RouteID = route.ID
	}
	return execReq
}

func functionProcdURL(raw, path string) (*url.URL, error) {
	procdURL, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	procdURL.Path = path
	procdURL.RawQuery = ""
	return procdURL, nil
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

func sandboxFunctionTimeoutMS(route *mgr.SandboxAppServiceRoute) int {
	if route != nil && route.TimeoutSeconds > 0 {
		return int((time.Duration(route.TimeoutSeconds) * time.Second) / time.Millisecond)
	}
	return 0
}

func sandboxFunctionWantsStream(r *http.Request) bool {
	if r == nil {
		return false
	}
	return headerContainsToken(r.Header.Get("Accept"), "text/event-stream")
}

func headerContainsToken(value, token string) bool {
	for _, part := range strings.Split(value, ",") {
		mediaType := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if strings.EqualFold(mediaType, token) {
			return true
		}
	}
	return false
}

func externalRequestHeaders(headers http.Header) map[string][]string {
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		if isHopByHopHeader(key) {
			continue
		}
		copied := make([]string, 0, len(values))
		copied = append(copied, values...)
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

var functionWebSocketUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func relayWebSocketMessages(src, dst *websocket.Conn, errCh chan<- error) {
	for {
		messageType, data, err := src.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		if err := dst.WriteMessage(messageType, data); err != nil {
			errCh <- err
			return
		}
	}
}

type flushingWriter struct {
	http.ResponseWriter
}

func (w flushingWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, err
}
