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

func TestResolveResponseMarshalPreservesTLSClientCertificateDirective(t *testing.T) {
	payload, err := json.Marshal(NewTLSClientCertificateResolveResponse("example-cert", &TLSClientCertificateDirective{
		CertificatePEM: "cert",
		PrivateKeyPEM:  "key",
		CAPEM:          "ca",
	}, nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var resp ResolveResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Directives) != 1 || resp.Directives[0].TLSClientCertificate == nil {
		t.Fatalf("unexpected directives: %#v", resp.Directives)
	}
	if resp.Directives[0].TLSClientCertificate.CertificatePEM != "cert" {
		t.Fatalf("certificate pem = %q", resp.Directives[0].TLSClientCertificate.CertificatePEM)
	}
}

func TestResolveResponseMarshalPreservesUsernamePasswordDirective(t *testing.T) {
	payload, err := json.Marshal(NewUsernamePasswordResolveResponse("corp-proxy", &UsernamePasswordDirective{
		Username: "alice",
		Password: "secret",
	}, nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var resp ResolveResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Directives) != 1 || resp.Directives[0].UsernamePassword == nil {
		t.Fatalf("unexpected directives: %#v", resp.Directives)
	}
	if resp.Directives[0].UsernamePassword.Username != "alice" {
		t.Fatalf("username = %q", resp.Directives[0].UsernamePassword.Username)
	}
}

func TestResolveResponseMarshalPreservesPlaceholderSubstitutionDirective(t *testing.T) {
	payload, err := json.Marshal(NewPlaceholderSubstitutionResolveResponse("example-api", &PlaceholderSubstitutionDirective{
		Replacements: []PlaceholderSubstitutionReplacement{{
			Placeholder: "s0env_test_token",
			Value:       "resolved-secret",
			Locations: []PlaceholderSubstitutionLocation{
				PlaceholderSubstitutionLocationHeader,
				PlaceholderSubstitutionLocationQuery,
				PlaceholderSubstitutionLocationBody,
			},
		}},
	}, nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var resp ResolveResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Headers) != 0 {
		t.Fatalf("legacy headers = %#v, want none", resp.Headers)
	}
	if len(resp.Directives) != 1 || resp.Directives[0].PlaceholderSubstitution == nil {
		t.Fatalf("unexpected directives: %#v", resp.Directives)
	}
	replacements := resp.Directives[0].PlaceholderSubstitution.Replacements
	if len(replacements) != 1 {
		t.Fatalf("replacement count = %d, want 1", len(replacements))
	}
	if replacements[0].Value != "resolved-secret" {
		t.Fatalf("value = %q", replacements[0].Value)
	}
	cloned := CloneResolveResponse(&resp)
	replacements[0].Value = "mutated"
	if cloned.Directives[0].PlaceholderSubstitution.Replacements[0].Value != "resolved-secret" {
		t.Fatalf("clone value = %q", cloned.Directives[0].PlaceholderSubstitution.Replacements[0].Value)
	}
}
