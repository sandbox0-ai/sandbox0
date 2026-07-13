package sandboxobservability

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateJSONStringContent(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		limit       int
		wantChanged bool
	}{
		{name: "unchanged", value: "request", limit: 64},
		{name: "ascii", value: strings.Repeat("a", 100), limit: 8, wantChanged: true},
		{name: "escaped", value: strings.Repeat("\x01<&", 20), limit: 12, wantChanged: true},
		{name: "unicode", value: strings.Repeat("界", 10), limit: 7, wantChanged: true},
		{name: "invalid utf8", value: string([]byte{'a', 0xff, 'b'}), limit: 64, wantChanged: true},
		{name: "zero budget", value: "a", limit: 0, wantChanged: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounded, changed := TruncateJSONStringContent(tt.value, tt.limit)
			if changed != tt.wantChanged {
				t.Fatalf("changed = %t, want %t", changed, tt.wantChanged)
			}
			if !utf8.ValidString(bounded) {
				t.Fatalf("bounded value is not valid UTF-8: %q", bounded)
			}
			encoded, err := json.Marshal(bounded)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			if contentBytes := len(encoded) - 2; contentBytes > max(tt.limit, 0) {
				t.Fatalf("encoded content bytes = %d, want <= %d", contentBytes, max(tt.limit, 0))
			}
		})
	}
}
