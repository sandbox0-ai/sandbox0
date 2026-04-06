package clustergateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gatewayapikey "github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayidentity "github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
)

func TestClusterGatewayIntegration_AuthRequired(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

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

func TestClusterGatewayIntegration_PermissionDenied(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)
	token := newInternalToken(t, env.edgeGen, []string{authn.PermSandboxRead})

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d", resp.StatusCode)
	}
}

func TestClusterGatewayIntegration_VolumeEndpointsRequirePermissions(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)
	readToken := newInternalToken(t, env.edgeGen, []string{authn.PermSandboxRead})
	writeToken := newInternalToken(t, env.edgeGen, []string{authn.PermSandboxWrite})

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

func TestClusterGatewayIntegration_PublicAuthJWT(t *testing.T) {
	dbPool, identityRepo, _, _ := newGatewayTestDB(t)

	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(managerServer.Close)

	env := newGatewayPublicTestEnv(t, managerServer.URL, "", dbPool, "test-jwt-secret", "cluster-gateway", keys)

	user := &gatewayidentity.User{
		Email:         "jwt-user@example.com",
		Name:          "JWT User",
		PasswordHash:  "x",
		EmailVerified: true,
		IsAdmin:       false,
	}
	ctx := context.Background()
	team, _, err := identityRepo.CreateUserWithInitialTeam(ctx, user, "JWT Team", nil)
	if err != nil {
		t.Fatalf("create user/team: %v", err)
	}

	issuer := authn.NewIssuer("cluster-gateway", "test-jwt-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, []authn.TeamGrant{{TeamID: team.ID, TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	resp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", tokens.AccessToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d", resp.StatusCode)
	}
}

func TestClusterGatewayIntegration_PublicAuthAPIKey(t *testing.T) {
	dbPool, identityRepo, apiKeyRepo, _ := newGatewayTestDB(t)

	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(managerServer.Close)

	env := newGatewayPublicTestEnv(t, managerServer.URL, "", dbPool, "test-jwt-secret", "cluster-gateway", keys)

	user := &gatewayidentity.User{
		Email:         "apikey-user@example.com",
		Name:          "API Key User",
		PasswordHash:  "x",
		EmailVerified: true,
		IsAdmin:       false,
	}
	ctx := context.Background()
	team, _, err := identityRepo.CreateUserWithInitialTeam(ctx, user, "API Key Team", nil)
	if err != nil {
		t.Fatalf("create user/team: %v", err)
	}
	if _, err := dbPool.Exec(ctx, `UPDATE teams SET home_region_id = $2 WHERE id = $1`, team.ID, "aws-us-east-1"); err != nil {
		t.Fatalf("set team home region: %v", err)
	}

	_, keyValue, err := apiKeyRepo.CreateAPIKey(ctx, team.ID, "aws-us-east-1", user.ID, "test-key", "user", []string{"admin"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	resp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", keyValue, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d", resp.StatusCode)
	}
}

func TestClusterGatewayIntegration_APIKeyCanBeCreatedWithoutLocalTeamOrUserRow(t *testing.T) {
	dbPool, identityRepo, apiKeyRepo, _ := newGatewayTestDB(t)

	user := &gatewayidentity.User{
		Email:         "apikey-region-route@example.com",
		Name:          "API Key Region Route User",
		PasswordHash:  "x",
		EmailVerified: true,
		IsAdmin:       false,
	}
	ctx := context.Background()
	team, _, err := identityRepo.CreateUserWithInitialTeam(ctx, user, "Region Route Team", nil)
	if err != nil {
		t.Fatalf("create user/team: %v", err)
	}

	if _, err := dbPool.Exec(ctx, `DELETE FROM teams WHERE id = $1`, team.ID); err != nil {
		t.Fatalf("delete local team row: %v", err)
	}
	if _, err := dbPool.Exec(ctx, `DELETE FROM users WHERE id = $1`, user.ID); err != nil {
		t.Fatalf("delete local user row: %v", err)
	}

	key, keyValue, err := apiKeyRepo.CreateAPIKey(ctx, team.ID, "aws-us-east-1", user.ID, "test-key", "service", []string{"developer"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create api key without local team or user row: %v", err)
	}
	if key.TeamID != team.ID {
		t.Fatalf("expected team id %s, got %s", team.ID, key.TeamID)
	}
	if got, err := gatewayapikey.ParseRegionIDFromKey(keyValue); err != nil {
		t.Fatalf("parse region from api key: %v", err)
	} else if got != "aws-us-east-1" {
		t.Fatalf("expected region id aws-us-east-1, got %s", got)
	}

	validated, err := apiKeyRepo.ValidateAPIKey(ctx, keyValue)
	if err != nil {
		t.Fatalf("validate api key: %v", err)
	}
	if validated.TeamID != team.ID {
		t.Fatalf("expected validated team id %s, got %s", team.ID, validated.TeamID)
	}
}

func TestClusterGatewayIntegration_PublicAuthUserResponseIncludesDefaultTeamHomeRegion(t *testing.T) {
	dbPool, identityRepo, _, _ := newGatewayTestDB(t)

	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(managerServer.Close)

	env := newGatewayPublicTestEnv(t, managerServer.URL, "", dbPool, "test-jwt-secret", "cluster-gateway", keys)

	user := &gatewayidentity.User{
		Email:         "me-user@example.com",
		Name:          "Me User",
		PasswordHash:  "x",
		EmailVerified: true,
		IsAdmin:       false,
	}
	ctx := context.Background()
	team, _, err := identityRepo.CreateUserWithInitialTeam(ctx, user, "Me Team", nil)
	if err != nil {
		t.Fatalf("create user/team: %v", err)
	}
	if _, err := dbPool.Exec(ctx, `UPDATE teams SET home_region_id = $2 WHERE id = $1`, team.ID, "aws-us-east-1"); err != nil {
		t.Fatalf("set team home region: %v", err)
	}

	issuer := authn.NewIssuer("cluster-gateway", "test-jwt-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, []authn.TeamGrant{{TeamID: team.ID, TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	resp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodGet, env.server.URL+"/users/me", tokens.AccessToken, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d", resp.StatusCode)
	}

	var body struct {
		Data struct {
			ID    string `json:"id"`
			Email string `json:"email"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Data.ID != user.ID {
		t.Fatalf("expected user id %q, got %q", user.ID, body.Data.ID)
	}
	if body.Data.Email != user.Email {
		t.Fatalf("expected email %q, got %q", user.Email, body.Data.Email)
	}
}

func TestClusterGatewayIntegration_PublicAuthTeamsAcceptHomeRegionID(t *testing.T) {
	dbPool, identityRepo, _, _ := newGatewayTestDB(t)

	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeClusterGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(managerServer.Close)

	env := newGatewayPublicTestEnv(t, managerServer.URL, "", dbPool, "test-jwt-secret", "cluster-gateway", keys)

	user := &gatewayidentity.User{
		Email:         "team-admin@example.com",
		Name:          "Team Admin",
		PasswordHash:  "x",
		EmailVerified: true,
		IsAdmin:       false,
	}
	ctx := context.Background()
	team, _, err := identityRepo.CreateUserWithInitialTeam(ctx, user, "Admin Team", nil)
	if err != nil {
		t.Fatalf("create user/team: %v", err)
	}

	issuer := authn.NewIssuer("cluster-gateway", "test-jwt-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, []authn.TeamGrant{{TeamID: team.ID, TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	resp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodPost, env.server.URL+"/teams", tokens.AccessToken, map[string]any{
		"name":           "Regional Team",
		"home_region_id": "aws-us-east-1",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected created, got %d", resp.StatusCode)
	}

	var createBody struct {
		Data struct {
			ID           string  `json:"id"`
			HomeRegionID *string `json:"home_region_id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&createBody); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if createBody.Data.HomeRegionID == nil || *createBody.Data.HomeRegionID != "aws-us-east-1" {
		t.Fatalf("expected created team home region aws-us-east-1, got %#v", createBody.Data.HomeRegionID)
	}

	updateResp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodPut, env.server.URL+"/teams/"+createBody.Data.ID, tokens.AccessToken, map[string]any{
		"home_region_id": "aws-us-west-2",
	})
	defer updateResp.Body.Close()
	if updateResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected conflict, got %d", updateResp.StatusCode)
	}

	var updateBody struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(updateResp.Body).Decode(&updateBody); err != nil {
		t.Fatalf("decode update response: %v", err)
	}
	if updateBody.Error.Message != "team home region cannot be changed after creation" {
		t.Fatalf("expected immutable home region error, got %#v", updateBody.Error.Message)
	}

	getResp, _ := doGatewayRequestWithBearer(t, env.server.Client(), http.MethodGet, env.server.URL+"/teams/"+createBody.Data.ID, tokens.AccessToken, nil)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected get team ok, got %d", getResp.StatusCode)
	}

	var getBody struct {
		Data struct {
			HomeRegionID *string `json:"home_region_id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&getBody); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if getBody.Data.HomeRegionID == nil || *getBody.Data.HomeRegionID != "aws-us-east-1" {
		t.Fatalf("expected persisted team home region aws-us-east-1, got %#v", getBody.Data.HomeRegionID)
	}
}
