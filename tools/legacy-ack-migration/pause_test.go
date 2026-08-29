package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyackmigration"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestParseLoopbackManagerURL(t *testing.T) {
	for _, value := range []string{
		"https://127.0.0.1:8080",
		"http://manager:8080",
		"http://127.0.0.1:8080/api",
		"http://127.0.0.1",
	} {
		if _, err := parseLoopbackManagerURL(value); err == nil {
			t.Fatalf("parseLoopbackManagerURL(%q) succeeded", value)
		}
	}
	parsed, err := parseLoopbackManagerURL("http://127.0.0.1:18080")
	if err != nil || parsed.String() != "http://127.0.0.1:18080" {
		t.Fatalf("parseLoopbackManagerURL() = %v, %v", parsed, err)
	}
}

func TestRequestManagerPauseUsesTeamBoundOneShotToken(t *testing.T) {
	privatePEM, publicPEM, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := internalauth.LoadEd25519PrivateKey(privatePEM)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := internalauth.LoadEd25519PublicKey(publicPEM)
	if err != nil {
		t.Fatal(err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target: "manager", PublicKey: publicKey, AllowedCallers: []string{"cluster-gateway"},
	})
	var reads, pauses atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/sandboxes/sandbox-1" &&
			request.URL.Path != "/api/v1/sandboxes/sandbox-1/pause" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		claims, validateErr := validator.ValidateWithOptions(
			request.Header.Get(internalauth.DefaultTokenHeader),
			internalauth.ValidateOptions{RequireTeamID: true},
		)
		if validateErr != nil {
			t.Errorf("ValidateWithOptions() error = %v", validateErr)
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if claims.TeamID != "team-1" || claims.SandboxID != "sandbox-1" ||
			len(claims.Permissions) != 1 || claims.Permissions[0] != "*:*" {
			t.Errorf("claims = %#v", claims)
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		switch request.Method {
		case http.MethodGet:
			reads.Add(1)
		case http.MethodPost:
			pauses.Add(1)
		default:
			t.Errorf("unexpected request method %s", request.Method)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	managerURL, err := parseLoopbackManagerURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: "cluster-gateway", PrivateKey: privateKey,
	})
	if err := verifyManagerSandboxAccess(
		context.Background(), server.Client(), generator, managerURL,
		[]legacyackmigration.Sandbox{{ID: "sandbox-1", TeamID: "team-1", UserID: "user-1"}},
	); err != nil {
		t.Fatalf("verifyManagerSandboxAccess() error = %v", err)
	}
	token, err := generator.Generate("manager", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: []string{"*:*"}, SandboxID: "sandbox-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := requestManagerPause(context.Background(), server.Client(), managerURL, "sandbox-1", token)
	if err != nil || status != http.StatusOK {
		t.Fatalf("requestManagerPause() = %d, %v", status, err)
	}
	if reads.Load() != 1 || pauses.Load() != 1 {
		t.Fatalf("manager requests = reads %d, pauses %d", reads.Load(), pauses.Load())
	}
}

func TestSameLiveSandboxSetIgnoresOrderingButNotOwnership(t *testing.T) {
	left := &legacyackmigration.Catalog{Sandboxes: []legacyackmigration.Sandbox{
		{ID: "sandbox-1", TeamID: "team-1", DesiredState: sandboxstore.SandboxDesiredStateActive},
		{ID: "sandbox-2", TeamID: "team-2", DesiredState: sandboxstore.SandboxDesiredStatePaused},
	}}
	right := &legacyackmigration.Catalog{Sandboxes: []legacyackmigration.Sandbox{
		{ID: "sandbox-2", TeamID: "team-2", DesiredState: sandboxstore.SandboxDesiredStatePaused},
		{ID: "sandbox-1", TeamID: "team-1", DesiredState: sandboxstore.SandboxDesiredStatePaused},
	}}
	if !sameLiveSandboxSet(left, right) {
		t.Fatal("sameLiveSandboxSet() rejected reordered identities")
	}
	right.Sandboxes[1].TeamID = "other-team"
	if sameLiveSandboxSet(left, right) {
		t.Fatal("sameLiveSandboxSet() accepted ownership drift")
	}
}
