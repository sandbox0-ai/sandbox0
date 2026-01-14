// Package auth provides shared authentication context and permission definitions
// for the sandbox0 gateway services.
//
// This package defines:
//   - AuthContext for storing authentication state in request context
//   - AuthMethod types (api_key, jwt, internal)
//   - Permission constants and role-based permission mappings
//
// Usage:
//
//	// Add auth context to request
//	ctx = auth.WithAuthContext(ctx, &auth.AuthContext{
//	    AuthMethod: auth.AuthMethodAPIKey,
//	    TeamID:     "team-123",
//	    Roles:      []string{"developer"},
//	})
//
//	// Retrieve auth context
//	authCtx := auth.FromContext(ctx)
//	if authCtx != nil && authCtx.HasPermission(auth.PermSandboxCreate) {
//	    // authorized
//	}
package auth
