package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
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

	handler := &APIKeyHandler{
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
