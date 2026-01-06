package internalauth

import (
	"context"
	"net/http"
)

// Transport is an HTTP RoundTripper that automatically adds internal tokens
// to outgoing requests.
//
// Usage:
//
//	client := &http.Client{
//	    Transport: internalauth.NewTransport(generator, http.DefaultTransport),
//	}
type Transport struct {
	generator *Generator
	// TargetFn determines the target service for each request.
	// If nil, uses a default target.
	TargetFn func(*http.Request) string
	// TeamIDFn determines the team ID for each request.
	// If nil, uses a default team ID.
	TeamIDFn func(*http.Request) string
	// PermissionsFn determines the permissions for each request.
	// If nil, uses default permissions.
	PermissionsFn func(*http.Request) []string
	// TokenHeader is the header name to set. Default: X-Internal-Token.
	TokenHeader string
	// Base is the underlying RoundTripper. If nil, uses http.DefaultTransport.
	Base http.RoundTripper
}

// NewTransport creates a new Transport with the given generator.
//
// Example:
//
//	transport := NewTransport(generator,
//	    WithTargetFromRequest,
//	    WithTeamIDFromContext,
//	    http.DefaultTransport,
//	)
func NewTransport(generator *Generator, opts ...TransportOption) *Transport {
	t := &Transport{
		generator:   generator,
		TokenHeader: DefaultTokenHeader,
		Base:        http.DefaultTransport,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// TransportOption configures a Transport.
type TransportOption func(*Transport)

// WithTarget sets a static target for all requests.
func WithTarget(target string) TransportOption {
	return func(t *Transport) {
		t.TargetFn = func(*http.Request) string { return target }
	}
}

// WithTargetFn sets a function to determine the target per request.
func WithTargetFn(fn func(*http.Request) string) TransportOption {
	return func(t *Transport) {
		t.TargetFn = fn
	}
}

// WithTeamID sets a static team ID for all requests.
func WithTeamID(teamID string) TransportOption {
	return func(t *Transport) {
		t.TeamIDFn = func(*http.Request) string { return teamID }
	}
}

// WithTeamIDFn sets a function to determine the team ID per request.
func WithTeamIDFn(fn func(*http.Request) string) TransportOption {
	return func(t *Transport) {
		t.TeamIDFn = fn
	}
}

// WithPermissions sets static permissions for all requests.
func WithPermissions(permissions ...string) TransportOption {
	return func(t *Transport) {
		t.PermissionsFn = func(*http.Request) []string { return permissions }
	}
}

// WithPermissionsFn sets a function to determine permissions per request.
func WithPermissionsFn(fn func(*http.Request) []string) TransportOption {
	return func(t *Transport) {
		t.PermissionsFn = fn
	}
}

// WithTokenHeader sets a custom token header name.
func WithTokenHeader(header string) TransportOption {
	return func(t *Transport) {
		t.TokenHeader = header
	}
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	req = req.Clone(req.Context())

	// Determine target
	target := "unknown"
	if t.TargetFn != nil {
		target = t.TargetFn(req)
	}

	// Determine team ID
	teamID := ""
	if t.TeamIDFn != nil {
		teamID = t.TeamIDFn(req)
	}

	// Determine permissions
	var permissions []string
	if t.PermissionsFn != nil {
		permissions = t.PermissionsFn(req)
	}

	// Generate token
	token, err := t.generator.Generate(target, teamID, "", GenerateOptions{
		Permissions: permissions,
	})
	if err != nil {
		return nil, err
	}

	// Add token to request
	req.Header.Set(t.TokenHeader, token)

	// Make the request
	if t.Base == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.Base.RoundTrip(req)
}

// Helper functions for common Transport configurations

// TargetFromRequest extracts the target from the request host.
// Example: "storage-proxy.sandbox0-system.svc.cluster.local" -> "storage-proxy"
func TargetFromRequest(req *http.Request) string {
	host := req.URL.Host
	if host == "" {
		host = req.Host
	}
	// Extract service name from host
	// Assumes format: <service>.<namespace>.svc.<cluster>
	if len(host) > 0 {
		for i, c := range host {
			if c == '.' {
				return host[:i]
			}
		}
	}
	return host
}

// TeamIDFromContext extracts the team ID from the request context.
func TeamIDFromContext(req *http.Request) string {
	return GetTeamID(req.Context())
}

// UserIDFromContext extracts the user ID from the request context.
func UserIDFromContext(req *http.Request) string {
	return GetUserID(req.Context())
}

// PermissionsFromContext extracts permissions from the request context.
func PermissionsFromContext(req *http.Request) []string {
	return GetPermissions(req.Context())
}

// NewAuthenticatedClient creates an HTTP client that automatically adds
// internal tokens to requests.
//
// Example:
//
//	client := internalauth.NewAuthenticatedClient(generator, "storage-proxy")
//	resp, err := client.Get("http://storage-proxy:8081/api/v1/sandboxvolumes")
func NewAuthenticatedClient(generator *Generator, target string) *http.Client {
	return &http.Client{
		Transport: NewTransport(generator,
			WithTarget(target),
			WithTeamIDFn(TeamIDFromContext),
			WithPermissionsFn(PermissionsFromContext),
		),
	}
}

// RequestContextInjector adds authentication context to a request.
// This is useful for making authenticated requests with a standard http.Client.
//
// Usage:
//
//	req, _ := http.NewRequest("GET", url, nil)
//	req = internalauth.NewRequestContext(req, "team-123", "user-456", perms)
//	// Then use with a Transport that extracts from context
func NewRequestContext(req *http.Request, teamID, userID string, permissions []string) *http.Request {
	claims := &Claims{
		TeamID:      teamID,
		UserID:      userID,
		Permissions: permissions,
	}
	ctx := WithClaims(req.Context(), claims)
	return req.WithContext(ctx)
}

// ContextWithTeam creates a context with team ID for making authenticated requests.
func ContextWithTeam(ctx context.Context, teamID string) context.Context {
	return WithClaims(ctx, &Claims{TeamID: teamID})
}

// ContextWithUser creates a context with team and user ID for making authenticated requests.
func ContextWithUser(ctx context.Context, teamID, userID string) context.Context {
	return WithClaims(ctx, &Claims{TeamID: teamID, UserID: userID})
}
