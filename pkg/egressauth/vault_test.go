package egressauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestVaultResolverPutAndResolveKV2(t *testing.T) {
	var wrote map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Vault-Token"); got != "test-token" {
			t.Fatalf("unexpected vault token %q", got)
		}
		if r.URL.Path != "/v1/secret/data/sandbox0/credential-sources/team-1/proxy/1" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		switch r.Method {
		case http.MethodPost:
			var body struct {
				Data map[string]any `json:"data"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode write body: %v", err)
			}
			wrote = body.Data
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"data": map[string]any{
						"user": "alice",
						"pass": "secret",
					},
				},
			})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	resolver, err := NewVaultResolver([]VaultConnectionConfig{
		{
			Name:                "default",
			Provider:            CredentialSourceExternalProviderHashiCorpVault,
			Address:             server.URL,
			TokenFile:           tokenFile,
			DefaultMount:        "secret",
			KVVersion:           2,
			AllowedPathPrefixes: []string{"sandbox0/credential-sources/{{teamID}}/"},
		},
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	ref := &CredentialSourceExternalRefSpec{
		Path: "sandbox0/credential-sources/team-1/proxy/1",
		Fields: map[string]string{
			"username": "user",
			"password": "pass",
		},
	}
	spec := CredentialSourceSecretSpec{
		StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
			Username: "alice",
			Password: "secret",
		},
	}
	if err := resolver.Put(context.Background(), "team-1", "static_username_password", ref, spec); err != nil {
		t.Fatalf("put: %v", err)
	}
	if wrote["user"] != "alice" || wrote["pass"] != "secret" {
		t.Fatalf("unexpected written values: %#v", wrote)
	}

	resolved, err := resolver.Resolve(context.Background(), "team-1", "static_username_password", ref)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.StaticUsernamePassword == nil || resolved.StaticUsernamePassword.Username != "alice" || resolved.StaticUsernamePassword.Password != "secret" {
		t.Fatalf("unexpected resolved spec: %#v", resolved)
	}
}

func TestVaultResolverRejectsPathOutsideAllowedPrefix(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	resolver, err := NewVaultResolver([]VaultConnectionConfig{
		{
			Name:                "default",
			Address:             "http://127.0.0.1:8200",
			TokenFile:           tokenFile,
			AllowedPathPrefixes: []string{"sandbox0/credential-sources/{{teamID}}/"},
		},
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	err = resolver.Put(context.Background(), "team-1", "static_headers", &CredentialSourceExternalRefSpec{
		Path: "other/team-1/source",
	}, CredentialSourceSecretSpec{
		StaticHeaders: &StaticHeadersSourceSpec{Values: map[string]string{"Authorization": "Bearer token"}},
	})
	if err == nil {
		t.Fatal("expected path prefix validation error")
	}
}
