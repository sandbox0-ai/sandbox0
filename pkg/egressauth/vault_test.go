package egressauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
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
	if err := resolver.Put(context.Background(), "team-1", "static_username_password", ref, spec, false); err != nil {
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
	}, false)
	if err == nil {
		t.Fatal("expected path prefix validation error")
	}
}

func TestVaultResolverBoundsAndDeletesManagedKV2Source(t *testing.T) {
	var metadataWrites, dataWrites, metadataDeletes int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/secret/metadata/sandbox0/credential-sources/team-1/proxy":
			var body struct {
				MaxVersions int `json:"max_versions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode metadata body: %v", err)
			}
			if body.MaxVersions != 1 {
				t.Fatalf("max_versions = %d, want 1", body.MaxVersions)
			}
			metadataWrites++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/secret/data/sandbox0/credential-sources/team-1/proxy":
			dataWrites++
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/secret/metadata/sandbox0/credential-sources/team-1/proxy":
			metadataDeletes++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected vault request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	resolver, err := NewVaultResolver([]VaultConnectionConfig{{
		Name:                "default",
		Address:             server.URL,
		TokenFile:           tokenFile,
		DefaultMount:        "secret",
		AllowedPathPrefixes: []string{"sandbox0/credential-sources/{{teamID}}/"},
	}})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	ref := defaultVaultSourceRef("team-1", "proxy")
	spec := CredentialSourceSecretSpec{
		StaticHeaders: &StaticHeadersSourceSpec{Values: map[string]string{"Authorization": "Bearer token"}},
	}
	for range 2 {
		if err := resolver.Put(context.Background(), "team-1", "static_headers", ref, spec, true); err != nil {
			t.Fatalf("put managed source: %v", err)
		}
	}
	if err := resolver.Delete(context.Background(), "team-1", ref); err != nil {
		t.Fatalf("delete managed source: %v", err)
	}
	if metadataWrites != 2 || dataWrites != 2 || metadataDeletes != 1 {
		t.Fatalf(
			"vault calls metadata writes=%d data writes=%d metadata deletes=%d",
			metadataWrites,
			dataWrites,
			metadataDeletes,
		)
	}
}

func TestDefaultVaultSourceRefIsStable(t *testing.T) {
	first := defaultVaultSourceRef("team-1", "proxy")
	second := defaultVaultSourceRef("team-1", "proxy")
	if first.Path != "sandbox0/credential-sources/team-1/proxy" || second.Path != first.Path {
		t.Fatalf("default managed paths = %q and %q", first.Path, second.Path)
	}
}

func TestVaultResolverRejectsOversizedReferenceBeforeNetwork(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	resolver, err := NewVaultResolver([]VaultConnectionConfig{{
		Name:      "default",
		Address:   server.URL,
		TokenFile: tokenFile,
	}})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	ref := &CredentialSourceExternalRefSpec{
		Path: strings.Repeat("p", int(MaxCredentialExternalRefPathBytes)+1),
	}
	spec := CredentialSourceSecretSpec{
		StaticHeaders: &StaticHeadersSourceSpec{Values: map[string]string{"token": "secret"}},
	}

	if _, err := resolver.Resolve(
		context.Background(),
		"team-1",
		"static_headers",
		ref,
	); !resourceguard.IsTooLarge(err) {
		t.Fatalf("Resolve() error = %v, want TooLargeError", err)
	}
	if err := resolver.Put(
		context.Background(),
		"team-1",
		"static_headers",
		ref,
		spec,
		false,
	); !resourceguard.IsTooLarge(err) {
		t.Fatalf("Put() error = %v, want TooLargeError", err)
	}
	if err := resolver.Delete(
		context.Background(),
		"team-1",
		ref,
	); !resourceguard.IsTooLarge(err) {
		t.Fatalf("Delete() error = %v, want TooLargeError", err)
	}
	if requests != 0 {
		t.Fatalf("Vault request count = %d, want 0", requests)
	}
}
