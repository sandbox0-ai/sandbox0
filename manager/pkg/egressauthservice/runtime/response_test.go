package runtime

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

func TestResolveResponseFactoriesBuildTypedDirectives(t *testing.T) {
	expiresAt := time.Unix(123, 0).UTC()
	tests := []struct {
		name string
		resp *egressauth.ResolveResponse
		kind egressauth.ResolveDirectiveKind
	}{
		{
			name: "http headers",
			resp: newHTTPHeadersResolveResponse("headers", map[string]string{"Authorization": "Bearer token"}, &expiresAt),
			kind: egressauth.ResolveDirectiveKindHTTPHeaders,
		},
		{
			name: "placeholder substitution",
			resp: newPlaceholderSubstitutionResolveResponse("placeholder", &egressauth.PlaceholderSubstitutionDirective{
				Replacements: []egressauth.PlaceholderSubstitutionReplacement{{
					Placeholder: "s0env_token",
					Value:       "token",
					Locations:   []egressauth.PlaceholderSubstitutionLocation{egressauth.PlaceholderSubstitutionLocationHeader},
				}},
			}, &expiresAt),
			kind: egressauth.ResolveDirectiveKindPlaceholderSubstitution,
		},
		{
			name: "tls client certificate",
			resp: newTLSClientCertificateResolveResponse("tls", &egressauth.TLSClientCertificateDirective{
				CertificatePEM: "cert",
				PrivateKeyPEM:  "key",
			}, &expiresAt),
			kind: egressauth.ResolveDirectiveKindTLSClientCertificate,
		},
		{
			name: "username password",
			resp: newUsernamePasswordResolveResponse("password", &egressauth.UsernamePasswordDirective{
				Username: "alice",
				Password: "secret",
			}, &expiresAt),
			kind: egressauth.ResolveDirectiveKindUsernamePassword,
		},
		{
			name: "ssh proxy",
			resp: newSSHProxyResolveResponse("ssh", &egressauth.SSHProxyDirective{
				PrivateKeyPEM: "key",
			}, &expiresAt),
			kind: egressauth.ResolveDirectiveKindSSHProxy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.resp == nil || len(tt.resp.Directives) != 1 {
				t.Fatalf("response = %#v, want one directive", tt.resp)
			}
			if got := tt.resp.Directives[0].Kind; got != tt.kind {
				t.Fatalf("directive kind = %q, want %q", got, tt.kind)
			}
			if tt.resp.ExpiresAt == &expiresAt || tt.resp.ExpiresAt == nil || !tt.resp.ExpiresAt.Equal(expiresAt) {
				t.Fatalf("expires at = %p (%v), want independent copy of %v", tt.resp.ExpiresAt, tt.resp.ExpiresAt, expiresAt)
			}
		})
	}
}

func TestResolveResponseFactoriesCopyMutableInputs(t *testing.T) {
	headers := map[string]string{"Authorization": "Bearer token"}
	response := newHTTPHeadersResolveResponse("headers", headers, nil)
	headers["Authorization"] = "mutated"
	if got := response.Headers["Authorization"]; got != "Bearer token" {
		t.Fatalf("headers were not copied: %q", got)
	}

	directive := &egressauth.PlaceholderSubstitutionDirective{
		Replacements: []egressauth.PlaceholderSubstitutionReplacement{{
			Placeholder: "s0env_token",
			Value:       "token",
			Locations:   []egressauth.PlaceholderSubstitutionLocation{egressauth.PlaceholderSubstitutionLocationHeader},
		}},
	}
	response = newPlaceholderSubstitutionResolveResponse("placeholder", directive, nil)
	directive.Replacements[0].Value = "mutated"
	if got := response.Directives[0].PlaceholderSubstitution.Replacements[0].Value; got != "token" {
		t.Fatalf("placeholder replacements were not copied: %q", got)
	}
}
