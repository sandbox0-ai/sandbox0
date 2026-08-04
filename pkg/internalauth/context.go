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

// HasAllPermissions checks if the context has all of the specified permissions.
func HasAllPermissions(ctx context.Context, permissions ...string) bool {
	var have []string
	if claims := ClaimsFromContext(ctx); claims != nil {
		have = claims.Permissions
	}
	return hasPermissions(have, permissions)
}
