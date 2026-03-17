package egressauth

import (
	"encoding/json"
	"testing"
	"time"
)

func TestResolveResponseMarshalUsesDirectivesWireModel(t *testing.T) {
	expiresAt := time.Unix(123, 0).UTC()
	payload, err := json.Marshal(&ResolveResponse{
		AuthRef:   "example-api",
		Headers:   map[string]string{"Authorization": "Bearer test-token"},
		ExpiresAt: &expiresAt,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, ok := raw["headers"]; ok {
		t.Fatalf("wire payload unexpectedly contains legacy headers field: %s", payload)
	}
	if _, ok := raw["directives"]; !ok {
		t.Fatalf("wire payload missing directives field: %s", payload)
	}
}

func TestResolveResponseUnmarshalHydratesCompatibilityHeaders(t *testing.T) {
	payload := []byte(`{
		"authRef":"example-api",
		"directives":[
			{"kind":"http_headers","httpHeaders":{"headers":{"Authorization":"Bearer test-token"}}}
		]
	}`)

	var resp ResolveResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := resp.Headers["Authorization"]; got != "Bearer test-token" {
		t.Fatalf("authorization header = %q", got)
	}
}
