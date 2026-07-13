package sandboxobservability

import (
	"encoding/json"
	"math"
	"strings"
)

// NetworkProtocolOperation is a bounded L7 operation observed within one
// network flow. It contains policy metadata only; request and response bodies
// are intentionally excluded.
type NetworkProtocolOperation struct {
	RuleName  string `json:"rule_name,omitempty"`
	Protocol  string `json:"protocol,omitempty"`
	Operation string `json:"operation,omitempty"`
	Object    string `json:"object,omitempty"`
	Action    string `json:"action,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// NetworkAuditAttributes is the canonical, bounded network audit payload.
// Raw proxy and credential-resolution error strings are deliberately absent:
// they remain local diagnostic data because they can contain upstream or
// credential details that must not enter the compliance ledger.
type NetworkAuditAttributes struct {
	FlowID                      string                     `json:"flow_id,omitempty"`
	SourceIP                    string                     `json:"src_ip,omitempty"`
	DestinationIP               string                     `json:"dest_ip,omitempty"`
	DestinationPort             int64                      `json:"dest_port,omitempty"`
	Transport                   string                     `json:"transport,omitempty"`
	Protocol                    string                     `json:"protocol,omitempty"`
	Host                        string                     `json:"host,omitempty"`
	ClassifierResult            string                     `json:"classifier_result,omitempty"`
	Action                      string                     `json:"action,omitempty"`
	Reason                      string                     `json:"reason,omitempty"`
	Outcome                     string                     `json:"outcome,omitempty"`
	DurationMS                  int64                      `json:"duration_ms,omitempty"`
	EgressBytes                 int64                      `json:"egress_bytes,omitempty"`
	IngressBytes                int64                      `json:"ingress_bytes,omitempty"`
	Adapter                     string                     `json:"adapter,omitempty"`
	AdapterCapability           string                     `json:"adapter_capability,omitempty"`
	AuthRuleName                string                     `json:"auth_rule_name,omitempty"`
	AuthRef                     string                     `json:"auth_ref,omitempty"`
	AuthFailurePolicy           string                     `json:"auth_failure_policy,omitempty"`
	AuthBypassed                bool                       `json:"auth_bypassed,omitempty"`
	AuthBypassReason            string                     `json:"auth_bypass_reason,omitempty"`
	AuthEnforcement             string                     `json:"auth_enforcement,omitempty"`
	AuthResolved                bool                       `json:"auth_resolved,omitempty"`
	AuthCacheHit                bool                       `json:"auth_cache_hit,omitempty"`
	ProtocolOperations          []NetworkProtocolOperation `json:"protocol_operations,omitempty"`
	ProtocolOperationsTruncated bool                       `json:"protocol_operations_truncated,omitempty"`
}

// CanonicalMap converts typed attributes to the representation stored and
// signed by the audit ledger.
func (attributes NetworkAuditAttributes) CanonicalMap() map[string]any {
	encoded, err := json.Marshal(attributes)
	if err != nil {
		return map[string]any{}
	}
	result := make(map[string]any)
	if err := json.Unmarshal(encoded, &result); err != nil {
		return map[string]any{}
	}
	return result
}

// SanitizeNetworkAuditAttributes projects untrusted producer attributes onto
// the canonical network audit schema and applies all ledger size bounds.
func SanitizeNetworkAuditAttributes(raw map[string]any) NetworkAuditAttributes {
	var result NetworkAuditAttributes
	result.FlowID = boundedNetworkAuditString(raw["flow_id"])
	result.SourceIP = boundedNetworkAuditString(raw["src_ip"])
	result.DestinationIP = boundedNetworkAuditString(raw["dest_ip"])
	result.Transport = boundedNetworkAuditString(raw["transport"])
	result.Protocol = boundedNetworkAuditString(raw["protocol"])
	result.Host = boundedNetworkAuditString(raw["host"])
	result.ClassifierResult = boundedNetworkAuditString(raw["classifier_result"])
	result.Action = boundedNetworkAuditString(raw["action"])
	result.Reason = boundedNetworkAuditString(raw["reason"])
	result.Outcome = boundedNetworkAuditString(raw["outcome"])
	result.Adapter = boundedNetworkAuditString(raw["adapter"])
	result.AdapterCapability = boundedNetworkAuditString(raw["adapter_capability"])
	result.AuthRuleName = boundedNetworkAuditString(raw["auth_rule_name"])
	result.AuthRef = boundedNetworkAuditString(raw["auth_ref"])
	result.AuthFailurePolicy = boundedNetworkAuditString(raw["auth_failure_policy"])
	result.AuthBypassReason = boundedNetworkAuditString(raw["auth_bypass_reason"])
	result.AuthEnforcement = boundedNetworkAuditString(raw["auth_enforcement"])

	if value, ok := networkAuditInteger(raw["dest_port"]); ok && value > 0 && value <= 65535 {
		result.DestinationPort = value
	}
	if value, ok := networkAuditInteger(raw["duration_ms"]); ok && value >= 0 {
		result.DurationMS = value
	}
	if value, ok := networkAuditInteger(raw["egress_bytes"]); ok && value >= 0 {
		result.EgressBytes = value
	}
	if value, ok := networkAuditInteger(raw["ingress_bytes"]); ok && value >= 0 {
		result.IngressBytes = value
	}
	result.AuthBypassed, _ = raw["auth_bypassed"].(bool)
	result.AuthResolved, _ = raw["auth_resolved"].(bool)
	result.AuthCacheHit, _ = raw["auth_cache_hit"].(bool)

	result.ProtocolOperations, result.ProtocolOperationsTruncated = sanitizeNetworkProtocolOperations(raw["protocol_operations"])
	if explicitlyTruncated, _ := raw["protocol_operations_truncated"].(bool); explicitlyTruncated {
		result.ProtocolOperationsTruncated = true
	}
	return result
}

func boundedNetworkAuditString(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	bounded, _ := TruncateJSONStringContent(text, MaxNetworkAuditScalarFieldEncodedBytes)
	return bounded
}

func boundedNetworkProtocolString(value any) (string, bool) {
	text, ok := value.(string)
	if !ok {
		return "", true
	}
	return TruncateJSONStringContent(text, MaxNetworkAuditProtocolFieldEncodedBytes)
}

func networkAuditInteger(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case uint:
		if uint64(number) <= math.MaxInt64 {
			return int64(number), true
		}
	case uint8:
		return int64(number), true
	case uint16:
		return int64(number), true
	case uint32:
		return int64(number), true
	case uint64:
		if number <= math.MaxInt64 {
			return int64(number), true
		}
	case float32:
		value := float64(number)
		if validNetworkAuditFloatInteger(value) {
			return int64(value), true
		}
	case float64:
		if validNetworkAuditFloatInteger(number) {
			return int64(number), true
		}
	case json.Number:
		integer, err := number.Int64()
		return integer, err == nil
	}
	return 0, false
}

func validNetworkAuditFloatInteger(value float64) bool {
	const maxInt64Exclusive = float64(uint64(1) << 63)
	return !math.IsNaN(value) && !math.IsInf(value, 0) && math.Trunc(value) == value &&
		value >= math.MinInt64 && value < maxInt64Exclusive
}

func sanitizeNetworkProtocolOperations(raw any) ([]NetworkProtocolOperation, bool) {
	if raw == nil {
		return nil, false
	}
	operations, ok := raw.([]any)
	if !ok {
		return nil, true
	}
	limit := len(operations)
	truncated := false
	if limit > MaxNetworkAuditProtocolOperations {
		limit = MaxNetworkAuditProtocolOperations
		truncated = true
	}
	result := make([]NetworkProtocolOperation, 0, limit)
	for _, rawOperation := range operations[:limit] {
		fields, ok := rawOperation.(map[string]any)
		if !ok {
			truncated = true
			continue
		}
		operation := NetworkProtocolOperation{}
		var changed bool
		operation.RuleName, changed = boundedNetworkProtocolField(fields, "rule_name")
		truncated = truncated || changed
		operation.Protocol, changed = boundedNetworkProtocolField(fields, "protocol")
		truncated = truncated || changed
		operation.Operation, changed = boundedNetworkProtocolField(fields, "operation")
		truncated = truncated || changed
		operation.Object, changed = boundedNetworkProtocolField(fields, "object")
		truncated = truncated || changed
		operation.Action, changed = boundedNetworkProtocolField(fields, "action")
		truncated = truncated || changed
		operation.Reason, changed = boundedNetworkProtocolField(fields, "reason")
		truncated = truncated || changed
		for key := range fields {
			switch key {
			case "rule_name", "protocol", "operation", "object", "action", "reason":
			default:
				truncated = true
			}
		}
		result = append(result, operation)
	}
	return result, truncated
}

func boundedNetworkProtocolField(fields map[string]any, name string) (string, bool) {
	value, ok := fields[name]
	if !ok {
		return "", false
	}
	return boundedNetworkProtocolString(value)
}

// NetworkAuditAction returns the canonical action derived from producer
// attributes after sanitization.
func NetworkAuditAction(attributes NetworkAuditAttributes) string {
	if strings.EqualFold(attributes.Action, "deny") {
		return "network.deny"
	}
	return "network.connect"
}
