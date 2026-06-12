package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

func (s *Server) enforceHTTPPolicyForHTTPRequest(req *adapterRequest, httpReq *http.Request, writeDenied func(int, []byte) error) error {
	if req == nil || httpReq == nil {
		return nil
	}
	host := req.Host
	if host == "" {
		host = normalizeHost(httpReq.Host)
	}
	rule := policy.MatchHTTPProtocolRule(req.Compiled, host, req.DestPort, httpReq)
	if rule == nil {
		return nil
	}
	allowed, reason := policy.AllowHTTPRequest(rule, httpReq)
	action := "allow"
	if !allowed {
		action = "deny"
	}
	audit := protocolOperationAudit{
		RuleName:  rule.Name,
		Protocol:  "http",
		Operation: "request",
		Object:    httpRequestAuditObject(httpReq),
		Action:    action,
		Reason:    reason,
	}
	req.appendProtocolAudit(audit)
	if allowed {
		return nil
	}
	body := []byte("HTTP request denied by Sandbox0 protocol policy\n")
	if writeDenied != nil {
		if err := writeDenied(http.StatusForbidden, body); err != nil {
			return fmt.Errorf("write http policy response: %w", err)
		}
	}
	return fmt.Errorf("%w: %s", errProtocolPolicyDenied, reason)
}

func httpRequestAuditObject(req *http.Request) string {
	if req == nil {
		return ""
	}
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	path := "/"
	if req.URL != nil && req.URL.Path != "" {
		path = req.URL.Path
	}
	if method == "" {
		return path
	}
	return method + " " + path
}

func writeHTTPProtocolPolicyResponse(conn io.Writer, status int, body []byte) error {
	if conn == nil {
		return nil
	}
	resp := &http.Response{
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		ContentLength: int64(len(body)),
		Body:          io.NopCloser(bytes.NewReader(body)),
		Header:        make(http.Header),
		Close:         true,
	}
	resp.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp.Header.Set("Connection", "close")
	return resp.Write(conn)
}
