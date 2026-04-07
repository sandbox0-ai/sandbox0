package authn

import (
	"context"
)

type contextKey string

const authContextKey contextKey = "auth_context"

// AuthMethod defines authentication methods.
type AuthMethod string

const (
	AuthMethodAPIKey   AuthMethod = "api_key"
	AuthMethodJWT      AuthMethod = "jwt"
	AuthMethodInternal AuthMethod = "internal"
)

// PrincipalKind identifies the authenticated principal type.
type PrincipalKind string

const (
	PrincipalKindHuman    PrincipalKind = "human"
	PrincipalKindAPIKey   PrincipalKind = "api_key"
	PrincipalKindInternal PrincipalKind = "internal"
)

// Principal is a transport-agnostic identity for authorization decisions.
type Principal struct {
	Kind     PrincipalKind
	TeamID   string
	UserID   string
	APIKeyID string
}

// AuthContext contains authentication information for a request.
type AuthContext struct {
	AuthMethod AuthMethod
	TeamID     string
	UserID     string
	APIKeyID   string

	TeamRole      string
	IsSystemAdmin bool
	Permissions   []string
}

// Principal returns the normalized principal for this auth context.
func (ac *AuthContext) Principal() Principal {
	switch ac.AuthMethod {
	case AuthMethodAPIKey:
		return Principal{Kind: PrincipalKindAPIKey, TeamID: ac.TeamID, APIKeyID: ac.APIKeyID}
	case AuthMethodInternal:
		return Principal{Kind: PrincipalKindInternal, TeamID: ac.TeamID, UserID: ac.UserID}
	default:
		return Principal{Kind: PrincipalKindHuman, TeamID: ac.TeamID, UserID: ac.UserID}
	}
}

// Predefined permissions.
const (
	PermSandboxCreate = "sandbox:create"
	PermSandboxRead   = "sandbox:read"
	PermSandboxWrite  = "sandbox:write"
	PermSandboxDelete = "sandbox:delete"

	PermTemplateCreate = "template:create"
	PermTemplateRead   = "template:read"
	PermTemplateWrite  = "template:write"
	PermTemplateDelete = "template:delete"

	PermRegistryWrite = "registry:write"

	PermCredentialSourceRead   = "credentialsource:read"
	PermCredentialSourceWrite  = "credentialsource:write"
	PermCredentialSourceDelete = "credentialsource:delete"

	PermSandboxVolumeCreate = "sandboxvolume:create"
	PermSandboxVolumeRead   = "sandboxvolume:read"
	PermSandboxVolumeWrite  = "sandboxvolume:write"
	PermSandboxVolumeDelete = "sandboxvolume:delete"

	PermSandboxVolumeFileRead  = "sandboxvolumefile:read"
	PermSandboxVolumeFileWrite = "sandboxvolumefile:write"
)

// RolePermissions maps team roles to their permissions.
var RolePermissions = map[string][]string{
	"admin": {
		PermSandboxCreate,
		PermSandboxRead,
		PermSandboxWrite,
		PermSandboxDelete,
		PermTemplateCreate,
		PermTemplateRead,
		PermTemplateWrite,
		PermTemplateDelete,
		PermRegistryWrite,
		PermCredentialSourceRead,
		PermCredentialSourceWrite,
		PermCredentialSourceDelete,
		PermSandboxVolumeCreate,
		PermSandboxVolumeRead,
		PermSandboxVolumeWrite,
		PermSandboxVolumeDelete,
		PermSandboxVolumeFileRead,
		PermSandboxVolumeFileWrite,
	},
	"developer": {
		PermSandboxCreate,
		PermSandboxRead,
		PermSandboxWrite,
		PermSandboxDelete,
		PermTemplateRead,
		PermRegistryWrite,
		PermCredentialSourceRead,
		PermCredentialSourceWrite,
		PermCredentialSourceDelete,
		PermSandboxVolumeCreate,
		PermSandboxVolumeRead,
		PermSandboxVolumeWrite,
		PermSandboxVolumeDelete,
		PermSandboxVolumeFileRead,
		PermSandboxVolumeFileWrite,
	},
	"builder": {
		PermTemplateRead,
		PermRegistryWrite,
	},
	"viewer": {
		PermSandboxRead,
		PermTemplateRead,
		PermCredentialSourceRead,
		PermSandboxVolumeRead,
		PermSandboxVolumeFileRead,
	},
}

// WithAuthContext adds auth context to the context.
func WithAuthContext(ctx context.Context, authCtx *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, authCtx)
}

// FromContext extracts auth context from the context.
func FromContext(ctx context.Context) *AuthContext {
	if v := ctx.Value(authContextKey); v != nil {
		return v.(*AuthContext)
	}
	return nil
}

// HasPermission checks if the auth context has a specific permission.
func (ac *AuthContext) HasPermission(permission string) bool {
	for _, p := range ac.Permissions {
		if p == permission || p == "*:*" || p == "*" {
			return true
		}
	}
	return false
}

// HasRole checks if the auth context has a specific role.
func (ac *AuthContext) HasRole(role string) bool {
	return ac.TeamRole == role
}

// ExpandRolePermissions expands a team role into permissions.
func ExpandRolePermissions(role string) []string {
	permSet := make(map[string]struct{})
	if perms, ok := RolePermissions[role]; ok {
		for _, p := range perms {
			permSet[p] = struct{}{}
		}
	}

	permissions := make([]string, 0, len(permSet))
	for p := range permSet {
		permissions = append(permissions, p)
	}
	return permissions
}

// ExpandRolesPermissions expands a list of roles into permissions.
func ExpandRolesPermissions(roles []string) []string {
	permSet := make(map[string]struct{})
	for _, role := range roles {
		if perms, ok := RolePermissions[role]; ok {
			for _, p := range perms {
				permSet[p] = struct{}{}
			}
		}
	}

	permissions := make([]string, 0, len(permSet))
	for p := range permSet {
		permissions = append(permissions, p)
	}
	return permissions
}
