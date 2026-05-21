package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

const maxMCPRequestBodyBytes int64 = 1 << 20

var errProtocolPolicyDenied = errors.New("protocol policy denied request")

type protocolOperationAudit struct {
	RuleName  string `json:"rule_name,omitempty"`
	Protocol  string `json:"protocol,omitempty"`
	Operation string `json:"operation,omitempty"`
	Object    string `json:"object,omitempty"`
	Action    string `json:"action,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type jsonRPCMessage struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpCallToolParams struct {
	Name string `json:"name"`
}

type mcpPolicyDecision struct {
	Denied bool
	Status int
	Body   []byte
	Audit  []protocolOperationAudit
	Reason string
}

func (s *Server) enforceMCPPolicyForHTTPRequest(req *adapterRequest, httpReq *http.Request, writeDenied func(int, []byte) error) error {
	if req == nil || httpReq == nil {
		return nil
	}
	host := req.Host
	if host == "" {
		host = normalizeHost(httpReq.Host)
	}
	rule := policy.MatchMCPProtocolRule(req.Compiled, host, req.DestPort, httpReq)
	if rule == nil {
		return nil
	}
	if httpReq.Method != http.MethodPost {
		return nil
	}
	if httpReq.ContentLength < 0 {
		decision := deniedMCPDecision(rule, nil, "tools/call", "unsupported_streaming_body")
		req.appendProtocolAudit(decision.Audit...)
		if writeDenied != nil {
			if err := writeDenied(decision.Status, decision.Body); err != nil {
				return fmt.Errorf("write mcp policy response: %w", err)
			}
		}
		return fmt.Errorf("%w: %s", errProtocolPolicyDenied, decision.Reason)
	}
	body, err := readAndResetHTTPBody(httpReq, maxMCPRequestBodyBytes)
	if err != nil {
		req.appendProtocolAudit(protocolOperationAudit{
			RuleName: rule.Name,
			Protocol: "mcp",
			Action:   "deny",
			Reason:   "body_read_failed",
		})
		if writeDenied != nil {
			body, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      nil,
				"error": map[string]any{
					"code":    -32003,
					"message": "MCP request denied by Sandbox0 protocol policy",
					"data": map[string]string{
						"reason": "body_read_failed",
					},
				},
			})
			_ = writeDenied(http.StatusRequestEntityTooLarge, body)
		}
		return fmt.Errorf("%w: read mcp request body: %v", errProtocolPolicyDenied, err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	decision := evaluateMCPRequestBody(rule, body)
	req.appendProtocolAudit(decision.Audit...)
	if !decision.Denied {
		return nil
	}
	if writeDenied != nil {
		if err := writeDenied(decision.Status, decision.Body); err != nil {
			return fmt.Errorf("write mcp policy response: %w", err)
		}
	}
	return fmt.Errorf("%w: %s", errProtocolPolicyDenied, decision.Reason)
}

func readAndResetHTTPBody(req *http.Request, limit int64) ([]byte, error) {
	if req == nil || req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	limited := io.LimitReader(req.Body, limit+1)
	body, err := io.ReadAll(limited)
	_ = req.Body.Close()
	if err != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
		return body, err
	}
	if int64(len(body)) > limit {
		req.Body = io.NopCloser(bytes.NewReader(nil))
		req.ContentLength = 0
		req.TransferEncoding = nil
		return nil, fmt.Errorf("mcp request body exceeds %d bytes", limit)
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.TransferEncoding = nil
	return body, nil
}

func evaluateMCPRequestBody(rule *policy.CompiledProtocolRule, body []byte) mcpPolicyDecision {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return mcpPolicyDecision{}
	}
	if body[0] == '[' {
		var messages []jsonRPCMessage
		if err := json.Unmarshal(body, &messages); err != nil {
			return deniedMCPDecision(rule, nil, "", "invalid_json")
		}
		var audits []protocolOperationAudit
		for _, msg := range messages {
			decision := evaluateMCPMessage(rule, msg)
			if decision.Denied {
				decision.Audit = append(audits, decision.Audit...)
				return decision
			}
			if len(decision.Audit) > 0 {
				audits = append(audits, decision.Audit...)
			}
		}
		return mcpPolicyDecision{Audit: audits}
	}
	var msg jsonRPCMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return deniedMCPDecision(rule, nil, "", "invalid_json")
	}
	return evaluateMCPMessage(rule, msg)
}

func evaluateMCPMessage(rule *policy.CompiledProtocolRule, msg jsonRPCMessage) mcpPolicyDecision {
	method := strings.TrimSpace(msg.Method)
	if method == "" {
		return mcpPolicyDecision{}
	}
	switch method {
	case "tools/call":
		var params mcpCallToolParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return deniedMCPDecision(rule, msg.ID, "tools/call", "invalid_tool_params")
		}
		tool := strings.TrimSpace(params.Name)
		allowed, reason := policy.AllowMCPTool(rule, tool)
		action := "allow"
		if !allowed {
			action = "deny"
		}
		audit := protocolOperationAudit{
			RuleName:  rule.Name,
			Protocol:  "mcp",
			Operation: "tools/call",
			Object:    tool,
			Action:    action,
			Reason:    reason,
		}
		if !allowed {
			return deniedMCPDecisionWithAudit(rule, msg.ID, audit)
		}
		return mcpPolicyDecision{Audit: []protocolOperationAudit{audit}}
	case "tools/list":
		return mcpPolicyDecision{Audit: []protocolOperationAudit{{
			RuleName:  rule.Name,
			Protocol:  "mcp",
			Operation: "tools/list",
			Action:    "allow",
			Reason:    "metadata_only",
		}}}
	default:
		return mcpPolicyDecision{}
	}
}

func deniedMCPDecision(rule *policy.CompiledProtocolRule, id json.RawMessage, operation string, reason string) mcpPolicyDecision {
	audit := protocolOperationAudit{
		Protocol:  "mcp",
		Operation: operation,
		Action:    "deny",
		Reason:    reason,
	}
	if rule != nil {
		audit.RuleName = rule.Name
	}
	return deniedMCPDecisionWithAudit(rule, id, audit)
}

func deniedMCPDecisionWithAudit(_ *policy.CompiledProtocolRule, id json.RawMessage, audit protocolOperationAudit) mcpPolicyDecision {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"error": map[string]any{
			"code":    -32003,
			"message": "MCP request denied by Sandbox0 protocol policy",
			"data": map[string]string{
				"reason": audit.Reason,
			},
		},
	})
	return mcpPolicyDecision{
		Denied: true,
		Status: http.StatusOK,
		Body:   body,
		Audit:  []protocolOperationAudit{audit},
		Reason: audit.Reason,
	}
}

func writeMCPPolicyHTTPResponse(conn io.Writer, status int, body []byte) error {
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
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Set("Connection", "close")
	return resp.Write(conn)
}
