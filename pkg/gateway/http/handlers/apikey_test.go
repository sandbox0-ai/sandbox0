package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func TestNormalizeCreateAPIKeyRoles(t *testing.T) {
	tests := []struct {
		name    string
		roles   []string
		want    []string
		wantErr bool
	}{
		{
			name:  "defaults to developer",
			roles: nil,
			want:  []string{"developer"},
		},
		{
			name:  "trims and deduplicates roles",
			roles: []string{" admin ", "developer", "admin"},
			want:  []string{"admin", "developer"},
		},
		{
			name:    "rejects empty role",
			roles:   []string{"viewer", " "},
			wantErr: true,
		},
		{
			name:    "rejects unsupported role",
			roles:   []string{"owner"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeCreateAPIKeyRoles(tt.roles)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize roles: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("roles = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeCreateAPIKeyScope(t *testing.T) {
	tests := []struct {
		name    string
		scope   string
		want    string
		wantErr bool
	}{
		{name: "defaults to team", want: apikey.ScopeTeam},
		{name: "accepts team", scope: "team", want: apikey.ScopeTeam},
		{name: "accepts platform", scope: "platform", want: apikey.ScopePlatform},
		{name: "rejects unknown", scope: "admin", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeCreateAPIKeyScope(tt.scope)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize scope: %v", err)
			}
			if got != tt.want {
				t.Fatalf("scope = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCanGrantAPIKeyRolesRequiresPermissionSubset(t *testing.T) {
	tests := []struct {
		name       string
		callerRole string
		keyRoles   []string
		want       bool
	}{
		{name: "admin can grant admin", callerRole: "admin", keyRoles: []string{"admin"}, want: true},
		{name: "admin can grant viewer", callerRole: "admin", keyRoles: []string{"viewer"}, want: true},
		{name: "developer cannot grant admin", callerRole: "developer", keyRoles: []string{"admin"}, want: false},
		{name: "developer can grant builder", callerRole: "developer", keyRoles: []string{"builder"}, want: true},
		{name: "developer can grant viewer", callerRole: "developer", keyRoles: []string{"viewer"}, want: true},
		{name: "builder can grant builder", callerRole: "builder", keyRoles: []string{"builder"}, want: true},
		{name: "builder cannot grant viewer", callerRole: "builder", keyRoles: []string{"viewer"}, want: false},
		{name: "viewer can grant viewer", callerRole: "viewer", keyRoles: []string{"viewer"}, want: true},
		{name: "viewer cannot grant builder", callerRole: "viewer", keyRoles: []string{"builder"}, want: false},
		{name: "viewer cannot grant default developer", callerRole: "viewer", keyRoles: []string{"developer"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authCtx := &authn.AuthContext{
				TeamRole:    tt.callerRole,
				Permissions: authn.ExpandRolePermissions(tt.callerRole),
			}
			if got := canGrantAPIKeyRoles(authCtx, tt.keyRoles); got != tt.want {
				t.Fatalf("canGrantAPIKeyRoles() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCanGrantAPIKeyRolesAllowsWildcardPermissions(t *testing.T) {
	authCtx := &authn.AuthContext{Permissions: []string{"*"}}
	if !canGrantAPIKeyRoles(authCtx, []string{"admin"}) {
		t.Fatal("expected wildcard permissions to grant admin API key roles")
	}
}

func TestCreateAPIKeyAllowsPlatformScopeForSystemAdminJWT(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	authCtx := &authn.AuthContext{
		AuthMethod:    authn.AuthMethodJWT,
		TeamID:        "team-1",
		UserID:        "user-1",
		IsSystemAdmin: true,
		Permissions:   []string{"*"},
	}
	store := &fakeAPIKeyStore{}
	rec := performCreateAPIKeyRequestWithStore(t, store, authCtx, map[string]any{
		"name":       "platform-key",
		"scope":      "platform",
		"expires_in": "never",
	})

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if store.createdScope != apikey.ScopePlatform {
		t.Fatalf("created scope = %q, want %q", store.createdScope, apikey.ScopePlatform)
	}
	if len(store.createdRoles) != 0 {
		t.Fatalf("created roles = %v, want empty", store.createdRoles)
	}
	if !store.createdExpiresAt.After(time.Now().AddDate(99, 0, 0)) {
		t.Fatalf("expected never expiration to be about 100 years out, got %s", store.createdExpiresAt)
	}

	response, apiErr, err := spec.DecodeResponse[CreateAPIKeyResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
	if response.Scope != apikey.ScopePlatform || len(response.Roles) != 0 {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func TestCreateAPIKeyRejectsPlatformScopeWithoutSystemAdminJWT(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	tests := []struct {
		name    string
		authCtx *authn.AuthContext
	}{
		{
			name: "team admin jwt",
			authCtx: &authn.AuthContext{
				AuthMethod:  authn.AuthMethodJWT,
				TeamID:      "team-1",
				UserID:      "user-1",
				TeamRole:    "admin",
				Permissions: authn.ExpandRolePermissions("admin"),
			},
		},
		{
			name: "platform api key cannot create more platform keys",
			authCtx: &authn.AuthContext{
				AuthMethod:    authn.AuthMethodAPIKey,
				TeamID:        "team-1",
				UserID:        "user-1",
				APIKeyID:      "key-1",
				IsSystemAdmin: true,
				Permissions:   []string{"*"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := performCreateAPIKeyRequest(t, tt.authCtx, map[string]any{
				"name":  "platform-key",
				"scope": "platform",
			})
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
		})
	}
}

func TestCreateAPIKeyRejectsPlatformRoles(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	authCtx := &authn.AuthContext{
		AuthMethod:    authn.AuthMethodJWT,
		TeamID:        "team-1",
		UserID:        "user-1",
		IsSystemAdmin: true,
		Permissions:   []string{"*"},
	}
	rec := performCreateAPIKeyRequest(t, authCtx, map[string]any{
		"name":  "platform-key",
		"scope": "platform",
		"roles": []string{"admin"},
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestFilterVisibleAPIKeysHidesPlatformKeysOutsideSystemAdminJWT(t *testing.T) {
	keys := []*apikey.APIKey{
		{ID: "team-key", Scope: apikey.ScopeTeam},
		{ID: "platform-key", Scope: apikey.ScopePlatform},
	}

	nonAdmin := &authn.AuthContext{AuthMethod: authn.AuthMethodJWT, UserID: "user-1"}
	got := filterVisibleAPIKeys(nonAdmin, keys)
	if len(got) != 1 || got[0].ID != "team-key" {
		t.Fatalf("visible keys for non-admin = %#v", got)
	}

	platformAPIKey := &authn.AuthContext{AuthMethod: authn.AuthMethodAPIKey, UserID: "user-1", IsSystemAdmin: true}
	got = filterVisibleAPIKeys(platformAPIKey, keys)
	if len(got) != 1 || got[0].ID != "team-key" {
		t.Fatalf("visible keys for platform api key = %#v", got)
	}

	systemAdminJWT := &authn.AuthContext{AuthMethod: authn.AuthMethodJWT, UserID: "user-1", IsSystemAdmin: true}
	got = filterVisibleAPIKeys(systemAdminJWT, keys)
	if len(got) != 2 {
		t.Fatalf("visible keys for system admin jwt = %#v", got)
	}
}

func TestDeleteAPIKeyRejectsPlatformKeyForTeamAdmin(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	store := &fakeAPIKeyStore{
		key: &apikey.APIKey{
			ID:     "key-1",
			TeamID: "team-1",
			Scope:  apikey.ScopePlatform,
		},
	}
	authCtx := &authn.AuthContext{
		AuthMethod:  authn.AuthMethodJWT,
		TeamID:      "team-1",
		UserID:      "user-1",
		TeamRole:    "admin",
		Permissions: authn.ExpandRolePermissions("admin"),
	}
	rec := performDeleteAPIKeyRequest(t, store, authCtx, "key-1")

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if store.deletedID != "" {
		t.Fatalf("deleted id = %q, want empty", store.deletedID)
	}
}

func TestCreateAPIKeyRejectsRoleEscalation(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	authCtx := &authn.AuthContext{
		AuthMethod:  authn.AuthMethodJWT,
		TeamID:      "team-1",
		UserID:      "user-1",
		TeamRole:    "viewer",
		Permissions: authn.ExpandRolePermissions("viewer"),
	}
	rec := performCreateAPIKeyRequest(t, authCtx, map[string]any{
		"name":  "admin-key",
		"roles": []string{"admin"},
	})

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	_, apiErr, err := spec.DecodeResponse[map[string]any](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Code != spec.CodeForbidden {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
}

func TestCreateAPIKeyRejectsDefaultDeveloperEscalation(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	authCtx := &authn.AuthContext{
		AuthMethod:  authn.AuthMethodJWT,
		TeamID:      "team-1",
		UserID:      "user-1",
		TeamRole:    "viewer",
		Permissions: authn.ExpandRolePermissions("viewer"),
	}
	rec := performCreateAPIKeyRequest(t, authCtx, map[string]any{
		"name": "default-key",
	})

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
}

func TestCreateAPIKeyRejectsUnsupportedRole(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	authCtx := &authn.AuthContext{
		AuthMethod:  authn.AuthMethodJWT,
		TeamID:      "team-1",
		UserID:      "user-1",
		TeamRole:    "admin",
		Permissions: authn.ExpandRolePermissions("admin"),
	}
	rec := performCreateAPIKeyRequest(t, authCtx, map[string]any{
		"name":  "owner-key",
		"roles": []string{"owner"},
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func performCreateAPIKeyRequest(t *testing.T, authCtx *authn.AuthContext, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	return performCreateAPIKeyRequestWithStore(t, nil, authCtx, body)
}

func performCreateAPIKeyRequestWithStore(t *testing.T, store apiKeyStore, authCtx *authn.AuthContext, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	handler := &APIKeyHandler{
		keys:     store,
		regionID: "aws-us-east-1",
		logger:   zap.NewNop(),
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", authCtx)
		c.Next()
	})
	router.POST("/api-keys", handler.CreateAPIKey)

	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api-keys", bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func performDeleteAPIKeyRequest(t *testing.T, store apiKeyStore, authCtx *authn.AuthContext, keyID string) *httptest.ResponseRecorder {
	t.Helper()

	handler := &APIKeyHandler{
		keys:   store,
		logger: zap.NewNop(),
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", authCtx)
		c.Next()
	})
	router.DELETE("/api-keys/:id", handler.DeleteAPIKey)

	req := httptest.NewRequest(http.MethodDelete, "/api-keys/"+keyID, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

type fakeAPIKeyStore struct {
	key              *apikey.APIKey
	createdScope     string
	createdRoles     []string
	createdExpiresAt time.Time
	deletedID        string
	deactivatedID    string
}

func (s *fakeAPIKeyStore) CreateAPIKey(_ context.Context, teamID, _ string, userID, name, scope string, roles []string, expiresAt time.Time) (*apikey.APIKey, string, error) {
	s.createdScope = scope
	s.createdRoles = append([]string(nil), roles...)
	s.createdExpiresAt = expiresAt
	return &apikey.APIKey{
		ID:        "key-1",
		TeamID:    teamID,
		CreatedBy: userID,
		Name:      name,
		Scope:     scope,
		Roles:     roles,
		IsActive:  true,
		ExpiresAt: expiresAt,
		CreatedAt: time.Now(),
	}, "s0_aws-us-east-1_test", nil
}

func (s *fakeAPIKeyStore) GetAPIKeysByTeamID(context.Context, string) ([]*apikey.APIKey, error) {
	return []*apikey.APIKey{s.key}, nil
}

func (s *fakeAPIKeyStore) GetAPIKeysByUserID(context.Context, string) ([]*apikey.APIKey, error) {
	return []*apikey.APIKey{s.key}, nil
}

func (s *fakeAPIKeyStore) GetAPIKeyByID(context.Context, string) (*apikey.APIKey, error) {
	return s.key, nil
}

func (s *fakeAPIKeyStore) DeleteAPIKey(_ context.Context, id string) error {
	s.deletedID = id
	return nil
}

func (s *fakeAPIKeyStore) DeactivateAPIKey(_ context.Context, id string) error {
	s.deactivatedID = id
	return nil
}
