package internalauth

const (
	// DefaultTokenHeader is the default header name for the internal token.
	DefaultTokenHeader = "X-Internal-Token"

	// AuthorizationHeader is the Authorization header for Bearer tokens.
	AuthorizationHeader = "Authorization"

	// TeamIDHeader is the header name for passing the team ID to internal services.
	TeamIDHeader = "X-Team-ID"

	// UserIDHeader is the header name for passing the user ID to internal services.
	UserIDHeader = "X-User-ID"
)
