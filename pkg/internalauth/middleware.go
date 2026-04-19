package internalauth

import (
	"net/http"
	"strings"
)

const (
	// DefaultTokenHeader is the default header name for the internal token.
	DefaultTokenHeader = "X-Internal-Token"

	// AuthorizationHeader is the Authorization header for Bearer tokens.
	AuthorizationHeader = "Authorization"

	// TeamIDHeader is the header name for passing the team ID to internal services.
	TeamIDHeader = "X-Team-ID"

	// UserIDHeader is the header name for passing the user ID to internal services.
	UserIDHeader = "X-User-ID"

	// TokenForProcdHeader is the header name used to pass a storage token to procd.
	TokenForProcdHeader = "X-Token-For-Procd"

	// VolumeSessionIDHeader is the header for a storage-proxy mount session identifier.
	VolumeSessionIDHeader = "X-Volume-Session-ID"

	// VolumeSessionSecretHeader is the header for a storage-proxy mount session secret.
	VolumeSessionSecretHeader = "X-Volume-Session-Secret"

	// VolumeIDHeader carries the volume identifier for stream-scoped session auth.
	VolumeIDHeader = "X-Volume-ID"
)

// TokenExtractor extracts the internal token from an HTTP request.
type TokenExtractor interface {
	ExtractToken(r *http.Request) (string, error)
}

// HeaderExtractor extracts tokens from a specified header.
type HeaderExtractor struct {
	HeaderName string
	Prefix     string // Optional prefix (e.g., "Bearer ")
}

// ExtractToken extracts the token from the configured header.
func (e *HeaderExtractor) ExtractToken(r *http.Request) (string, error) {
	header := r.Header.Get(e.HeaderName)
	if header == "" {
		return "", ErrInvalidToken
	}

	// Strip prefix if configured
	if e.Prefix != "" {
		if !strings.HasPrefix(header, e.Prefix) {
			return "", ErrInvalidToken
		}
		header = strings.TrimPrefix(header, e.Prefix)
	}

	return strings.TrimSpace(header), nil
}

// MultiExtractor tries multiple extractors in order until one succeeds.
type MultiExtractor []TokenExtractor

// ExtractToken tries each extractor in sequence.
func (m MultiExtractor) ExtractToken(r *http.Request) (string, error) {
	for _, extractor := range m {
		token, err := extractor.ExtractToken(r)
		if err == nil && token != "" {
			return token, nil
		}
	}
	return "", ErrInvalidToken
}

// DefaultExtractor returns a token extractor that checks both
// X-Internal-Token and Authorization: Bearer <token> headers.
func DefaultExtractor() TokenExtractor {
	return MultiExtractor{
		&HeaderExtractor{HeaderName: DefaultTokenHeader},
		&HeaderExtractor{HeaderName: AuthorizationHeader, Prefix: "Bearer "},
	}
}

// AuthMiddleware creates an HTTP middleware that validates internal tokens.
//
// Usage:
//
//	middleware := internalauth.AuthMiddleware(validator, internalauth.DefaultExtractor())
//	http.Handle("/api", middleware(handler))
func AuthMiddleware(validator *Validator, extractor TokenExtractor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract token
			token, err := extractor.ExtractToken(r)
			if err != nil {
				http.Error(w, "missing or invalid token", http.StatusUnauthorized)
				return
			}

			// Validate token
			claims, err := validator.Validate(token)
			if err != nil {
				http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
				return
			}

			// Add claims to request context for handlers to use
			r = r.WithContext(WithClaims(r.Context(), claims))

			next.ServeHTTP(w, r)
		})
	}
}

// OptionalAuthMiddleware creates a middleware that validates tokens if present,
// but continues without claims if no token is provided.
//
// This is useful for endpoints that accept both authenticated and
// unauthenticated requests.
func OptionalAuthMiddleware(validator *Validator, extractor TokenExtractor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, err := extractor.ExtractToken(r)
			if err == nil && token != "" {
				if claims, validateErr := validator.Validate(token); validateErr == nil {
					r = r.WithContext(WithClaims(r.Context(), claims))
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RequirePermissions creates a middleware that checks if the request
// has the required permissions.
//
// Must be used after AuthMiddleware.
func RequirePermissions(permissions ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := ClaimsFromContext(r.Context())
			if claims == nil {
				http.Error(w, "no authentication context", http.StatusUnauthorized)
				return
			}

			if !hasPermissions(claims.Permissions, permissions) {
				http.Error(w, "insufficient permissions", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RequireTeam creates a middleware that checks if the request
// is for a specific team.
//
// Must be used after AuthMiddleware.
func RequireTeam(teamID string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := ClaimsFromContext(r.Context())
			if claims == nil {
				http.Error(w, "no authentication context", http.StatusUnauthorized)
				return
			}

			if claims.TeamID != teamID {
				http.Error(w, "team mismatch", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
