package internalauth

import (
	"context"
)

// contextKey is the key type for storing claims in context.
type contextKey struct{}

// claimsContextKey is the key used to store Claims in context.
var claimsContextKey = contextKey{}

// WithClaims adds the claims to the context.
func WithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

// ClaimsFromContext retrieves the claims from the context.
// Returns nil if no claims are present.
func ClaimsFromContext(ctx context.Context) *Claims {
	if claims, ok := ctx.Value(claimsContextKey).(*Claims); ok {
		return claims
	}
	return nil
}

// MustClaimsFromContext retrieves the claims from the context,
// panicking if not present.
func MustClaimsFromContext(ctx context.Context) *Claims {
	claims := ClaimsFromContext(ctx)
	if claims == nil {
		panic("internalauth: no claims in context")
	}
	return claims
}

// GetTeamID is a convenience function to get the team ID from context.
func GetTeamID(ctx context.Context) string {
	if claims := ClaimsFromContext(ctx); claims != nil {
		return claims.TeamID
	}
	return ""
}

// GetUserID is a convenience function to get the user ID from context.
func GetUserID(ctx context.Context) string {
	if claims := ClaimsFromContext(ctx); claims != nil {
		return claims.UserID
	}
	return ""
}

// GetCaller is a convenience function to get the caller from context.
func GetCaller(ctx context.Context) string {
	if claims := ClaimsFromContext(ctx); claims != nil {
		return claims.Caller
	}
	return ""
}

// GetPermissions is a convenience function to get the permissions from context.
func GetPermissions(ctx context.Context) []string {
	if claims := ClaimsFromContext(ctx); claims != nil {
		return claims.Permissions
	}
	return nil
}

// HasPermission checks if the context has a specific permission.
func HasPermission(ctx context.Context, permission string) bool {
	if claims := ClaimsFromContext(ctx); claims != nil {
		for _, p := range claims.Permissions {
			if p == permission || p == "*" {
				return true
			}
		}
	}
	return false
}

// HasAnyPermission checks if the context has any of the specified permissions.
func HasAnyPermission(ctx context.Context, permissions ...string) bool {
	if claims := ClaimsFromContext(ctx); claims != nil {
		for _, required := range permissions {
			for _, p := range claims.Permissions {
				if p == required || p == "*" {
					return true
				}
			}
		}
	}
	return false
}

// HasAllPermissions checks if the context has all of the specified permissions.
func HasAllPermissions(ctx context.Context, permissions ...string) bool {
	return hasPermissions(GetPermissions(ctx), permissions)
}
