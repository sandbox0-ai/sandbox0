package sandboxobservability

import (
	"encoding/json"
	"strings"
	"unicode/utf8"
)

const (
	// MaxNetworkAuditProtocolOperations bounds the protocol-operation history
	// attached to one network flow.
	MaxNetworkAuditProtocolOperations = 64
	// MaxNetworkAuditProtocolFieldEncodedBytes is a JSON-encoded content budget,
	// excluding the surrounding quotes, for each protocol-operation field.
	MaxNetworkAuditProtocolFieldEncodedBytes = 64
	// MaxNetworkAuditScalarFieldEncodedBytes is the corresponding budget for
	// each fixed top-level network-audit string attribute.
	MaxNetworkAuditScalarFieldEncodedBytes = 1024
)

// TruncateJSONStringContent limits a string by its encoding/json content size.
// Measuring encoded runes avoids undercounting control and HTML-escaped bytes.
func TruncateJSONStringContent(value string, maxEncodedBytes int) (string, bool) {
	if maxEncodedBytes <= 0 {
		return "", value != ""
	}
	var bounded strings.Builder
	encodedBytes := 0
	changed := false
	for len(value) > 0 {
		r, width := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && width == 1 {
			changed = true
		}
		encoded, err := json.Marshal(string(r))
		if err != nil {
			return bounded.String(), true
		}
		contentBytes := len(encoded) - 2
		if encodedBytes+contentBytes > maxEncodedBytes {
			return bounded.String(), true
		}
		bounded.WriteRune(r)
		encodedBytes += contentBytes
		value = value[width:]
	}
	return bounded.String(), changed
}
