package internalgateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/pkg/auth"
	gatewayjwt "github.com/sandbox0-ai/infra/pkg/gateway/auth/jwt"
	gatewaydb "github.com/sandbox0-ai/infra/pkg/gateway/db"
)

func TestInternalGatewayIntegration_AuthRequired(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", resp.StatusCode)
	}
}

func TestInternalGatewayIntegration_PermissionDenied(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)
	token := newInternalToken(t, env.edgeGen, []string{auth.PermSandboxRead})

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d", resp.StatusCode)
	}
}

func TestInternalGatewayIntegration_VolumeEndpointsRequirePermissions(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)
	readToken := newInternalToken(t, env.edgeGen, []string{auth.PermSandboxRead})
	writeToken := newInternalToken(t, env.edgeGen, []string{auth.PermSandboxWrite})

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxvolumes", readToken, map[string]any{
		"name": "vol-1",
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for create, got %d", resp.StatusCode)
	}

	resp, _ = doGatewayRequest(t, env.server.Client(), http.MethodDelete, env.server.URL+"/api/v1/sandboxvolumes/vol-1", readToken, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for delete, got %d", resp.StatusCode)
	}

	resp, _ = doGatewayRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxvolumes", writeToken, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for list, got %d", resp.StatusCode)
	}
}

func TestInternalGatewayIntegration_PublicAuthJWT(t *testing.T) {
	dbPool, repo, _ := newGatewayTestDB(t)

	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(managerServer.Close)

	env := newGatewayPublicTestEnv(t, managerServer.URL, "", dbPool, "test-jwt-secret", "internal-gateway", keys)

	user := &gatewaydb.User{
		Email:         "jwt-user@example.com",
		Name:          "JWT User",
		PasswordHash:  "x",
		EmailVerified: true,
		IsAdmin:       false,
	}
	ctx := context.Background()
	team, _, err := repo.CreateUserWithDefaultTeam(ctx, user, "JWT Team")
	if err != nil {
		t.Fatalf("create user/team: %v", err)
	}

	issuer := gatewayjwt.NewIssuer("internal-gateway", "test-jwt-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair(user.ID, team.ID, "admin", user.Email, user.Name, user.IsAdmin)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	resp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", tokens.AccessToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d", resp.StatusCode)
	}
}

func TestInternalGatewayIntegration_PublicAuthAPIKey(t *testing.T) {
	dbPool, repo, _ := newGatewayTestDB(t)

	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(managerServer.Close)

	env := newGatewayPublicTestEnv(t, managerServer.URL, "", dbPool, "test-jwt-secret", "internal-gateway", keys)

	user := &gatewaydb.User{
		Email:         "apikey-user@example.com",
		Name:          "API Key User",
		PasswordHash:  "x",
		EmailVerified: true,
		IsAdmin:       false,
	}
	ctx := context.Background()
	team, _, err := repo.CreateUserWithDefaultTeam(ctx, user, "API Key Team")
	if err != nil {
		t.Fatalf("create user/team: %v", err)
	}

	_, keyValue, err := repo.CreateAPIKey(ctx, team.ID, user.ID, "test-key", "user", []string{"admin"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	resp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", keyValue, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d", resp.StatusCode)
	}
}
