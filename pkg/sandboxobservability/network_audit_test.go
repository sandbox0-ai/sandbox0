package sandboxobservability

import (
	"strings"
	"testing"
)

func TestSanitizeNetworkAuditAttributesDefinesCanonicalBoundary(t *testing.T) {
	long := strings.Repeat("x", MaxNetworkAuditScalarFieldEncodedBytes+32)
	raw := map[string]any{
		"flow_id":            "flow-1",
		"dest_port":          443.0,
		"duration_ms":        -1,
		"host":               long,
		"action":             "deny",
		"error":              "upstream secret",
		"auth_resolve_error": "credential secret",
		"unknown":            "drop",
		"protocol_operations": []any{
			map[string]any{"protocol": "mcp", "operation": "tools/call", "unknown": "drop"},
		},
	}

	attributes := SanitizeNetworkAuditAttributes(raw)
	canonical := attributes.CanonicalMap()
	if attributes.DestinationPort != 443 || attributes.DurationMS != 0 {
		t.Fatalf("numeric normalization = %+v", attributes)
	}
	if len(attributes.Host) != MaxNetworkAuditScalarFieldEncodedBytes {
		t.Fatalf("bounded host length = %d", len(attributes.Host))
	}
	if NetworkAuditAction(attributes) != "network.deny" {
		t.Fatalf("NetworkAuditAction() = %q", NetworkAuditAction(attributes))
	}
	for _, key := range []string{"error", "auth_resolve_error", "unknown"} {
		if _, ok := canonical[key]; ok {
			t.Fatalf("canonical attributes retained %q: %#v", key, canonical)
		}
	}
	if !attributes.ProtocolOperationsTruncated {
		t.Fatal("unknown protocol operation field did not mark truncation")
	}
}

func TestNetworkProtocolOperationMissingOptionalFieldsIsNotTruncated(t *testing.T) {
	attributes := SanitizeNetworkAuditAttributes(map[string]any{
		"protocol_operations": []any{map[string]any{
			"protocol":  "mcp",
			"operation": "tools/call",
		}},
	})
	if attributes.ProtocolOperationsTruncated {
		t.Fatal("missing optional protocol fields marked the operation as truncated")
	}
	if len(attributes.ProtocolOperations) != 1 || attributes.ProtocolOperations[0].Operation != "tools/call" {
		t.Fatalf("protocol operations = %#v", attributes.ProtocolOperations)
	}
}
