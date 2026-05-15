package functionapi

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestHTTPRuntimeControllerDeletesSandboxThroughInternalRoute(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceClusterGateway,
		PublicKey:          publicKey,
		AllowedCallers:     []string{internalauth.ServiceClusterGateway},
		ClockSkewTolerance: time.Second,
	})

	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		token := r.Header.Get(internalauth.DefaultTokenHeader)
		if token == "" {
			t.Fatal("missing internal auth token")
		}
		claims, err := validator.Validate(token)
		if err != nil {
			t.Fatalf("validate token: %v", err)
		}
		if claims.TeamID != "team-1" {
			t.Fatalf("team id = %q, want team-1", claims.TeamID)
		}
		if claims.UserID != "user-1" {
			t.Fatalf("user id = %q, want user-1", claims.UserID)
		}
		if !hasPermission(claims.Permissions, authn.PermSandboxDelete) {
			t.Fatalf("token missing %s permission", authn.PermSandboxDelete)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceClusterGateway,
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	controller := NewHTTPRuntimeController(
		StaticClusterGatewayURLResolver(server.URL),
		generator,
		server.Client(),
	)
	err = controller.DeleteRuntimeSandbox(context.Background(), &authn.AuthContext{
		TeamID:      "team-1",
		UserID:      "user-1",
		Permissions: []string{authn.PermSandboxDelete},
	}, "sandbox-1")
	if err != nil {
		t.Fatalf("DeleteRuntimeSandbox() error = %v", err)
	}
	if gotPath != "/internal/v1/sandboxes/sandbox-1" {
		t.Fatalf("path = %q, want internal sandbox route", gotPath)
	}
}

func hasPermission(permissions []string, want string) bool {
	for _, permission := range permissions {
		if permission == want || permission == "*" {
			return true
		}
	}
	return false
}
