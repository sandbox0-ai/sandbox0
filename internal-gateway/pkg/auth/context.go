package auth

import (
	"context"
)

type contextKey string

const authContextKey contextKey = "auth_context"

// AuthContext contains authentication information for a request
type AuthContext struct {
	// AuthMethod indicates how the request was authenticated
	AuthMethod AuthMethod

	// TeamID is the team this request belongs to
	TeamID string

	// UserID is the user ID (only present for JWT auth)
	UserID string

	// APIKeyID is the API key ID (only present for API key auth)
	APIKeyID string

	// Roles are the roles assigned to the authenticated entity
	Roles []string

	// Permissions are the specific permissions granted
	Permissions []string
}

// AuthMethod defines authentication methods
type AuthMethod string

const (
	AuthMethodAPIKey   AuthMethod = "api_key"
	AuthMethodJWT      AuthMethod = "jwt"
	AuthMethodInternal AuthMethod = "internal"
)

// Predefined permissions
const (
	// Sandbox permissions
	PermSandboxCreate = "sandbox:create"
	PermSandboxRead   = "sandbox:read"
	PermSandboxWrite  = "sandbox:write"
	PermSandboxDelete = "sandbox:delete"

	// Template permissions
	PermTemplateCreate = "template:create"
	PermTemplateRead   = "template:read"
	PermTemplateWrite  = "template:write"
	PermTemplateDelete = "template:delete"

	// SandboxVolume permissions
	PermSandboxVolumeCreate = "sandboxvolume:create"
	PermSandboxVolumeRead   = "sandboxvolume:read"
	PermSandboxVolumeWrite  = "sandboxvolume:write"
	PermSandboxVolumeDelete = "sandboxvolume:delete"
)

// RolePermissions maps roles to their permissions
var RolePermissions = map[string][]string{
	"admin": {
		"*:*", // All permissions
	},
	"developer": {
		PermSandboxCreate,
		PermSandboxRead,
		PermSandboxWrite,
		PermSandboxDelete,
		PermTemplateRead,
		PermSandboxVolumeCreate,
		PermSandboxVolumeRead,
		PermSandboxVolumeWrite,
		PermSandboxVolumeDelete,
	},
	"viewer": {
		PermSandboxRead,
		PermTemplateRead,
		PermSandboxVolumeRead,
	},
}

// WithAuthContext adds auth context to the context
func WithAuthContext(ctx context.Context, authCtx *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, authCtx)
}

// FromContext extracts auth context from the context
func FromContext(ctx context.Context) *AuthContext {
	if v := ctx.Value(authContextKey); v != nil {
		return v.(*AuthContext)
	}
	return nil
}

// HasPermission checks if the auth context has a specific permission
func (ac *AuthContext) HasPermission(permission string) bool {
	// Check direct permissions
	for _, p := range ac.Permissions {
		if p == permission || p == "*:*" {
			return true
		}
	}

	// Check role-based permissions
	for _, role := range ac.Roles {
		if perms, ok := RolePermissions[role]; ok {
			for _, p := range perms {
				if p == permission || p == "*:*" {
					return true
				}
			}
		}
	}

	return false
}

// HasRole checks if the auth context has a specific role
func (ac *AuthContext) HasRole(role string) bool {
	for _, r := range ac.Roles {
		if r == role {
			return true
		}
	}
	return false
}
