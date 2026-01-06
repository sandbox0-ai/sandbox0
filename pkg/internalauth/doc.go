// Package internalauth provides internal token-based authentication for
// inter-service communication within the sandbox0 infrastructure.
//
// # Architecture Overview
//
// The package implements a JWT-based authentication scheme where any service
// can generate tokens to authenticate with other services. This enables:
//
//   - Zero-trust inter-service communication
//   - Token-based authentication with short-lived tokens (default 30s)
//   - Caller and target validation to prevent token reuse
//   - Replay attack detection (optional)
//
// # Components
//
//   - Generator: Creates signed JWT tokens for a caller service
//   - Validator: Validates tokens for a target service
//   - Middleware: HTTP middleware for automatic token validation
//   - Transport: HTTP RoundTripper for automatic token injection
//
// # Basic Usage
//
//	// In the caller service (e.g., internal-gateway)
//	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
//	    Caller: "internal-gateway",
//	    Secret: []byte(os.Getenv("INTERNAL_JWT_SECRET")),
//	})
//
//	token, err := generator.Generate("storage-proxy", "team-123", "user-456",
//	    internalauth.GenerateOptions{
//	        Permissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
//	    })
//
//	// Make authenticated request
//	req, _ := http.NewRequest("POST", url, body)
//	req.Header.Set("X-Internal-Token", token)
//	client.Do(req)
//
//	// In the target service (e.g., storage-proxy)
//	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
//	    Target: "storage-proxy",
//	    Secret: []byte(os.Getenv("INTERNAL_JWT_SECRET")),
//	})
//
//	middleware := internalauth.AuthMiddleware(validator, internalauth.DefaultExtractor())
//	http.Handle("/api/", middleware(handler))
//
// # HTTP Client with Automatic Token Injection
//
//	// Create an auto-authenticating HTTP client
//	client := internalauth.NewAuthenticatedClient(generator, "storage-proxy")
//
//	// Add team context to request
//	req, _ := http.NewRequest("GET", "http://storage-proxy:8081/api/v1/volumes", nil)
//	req = req.WithContext(internalauth.ContextWithTeam(req.Context(), "team-123"))
//
//	// Token is automatically added
//	resp, err := client.Do(req)
//
// # Middleware Usage
//
//	// Simple auth middleware
//	mux := http.NewServeMux()
//	authMiddleware := internalauth.AuthMiddleware(validator, internalauth.DefaultExtractor())
//
//	// Apply to all routes
//	mux.Handle("/api/", authMiddleware(handler))
//
//	// Require specific permissions
//	mux.Handle("/api/admin/", authMiddleware(
//	    internalauth.RequirePermissions("admin:*")(handler),
//	))
//
//	// Require specific team
//	mux.Handle("/api/teams/", authMiddleware(
//	    internalauth.RequireTeam("team-123")(handler),
//	))
//
// # Accessing Claims in Handlers
//
//	func handler(w http.ResponseWriter, r *http.Request) {
//	    claims := internalauth.ClaimsFromContext(r.Context())
//	    teamID := claims.TeamID
//	    userID := claims.UserID
//	    caller := claims.Caller
//
//	    // Or use convenience functions
//	    teamID := internalauth.GetTeamID(r.Context())
//	    hasPermission := internalauth.HasPermission(r.Context(), "sandboxvolume:write")
//	}
//
// # Token Structure
//
// The JWT token contains the following claims:
//
//	{
//	  "iss": "internal-gateway",      // Issuer (caller service)
//	  "sub": "team-123",              // Subject (team ID)
//	  "aud": "storage-proxy",         // Audience (target service)
//	  "iat": 1706745600,              // Issued at
//	  "exp": 1706745630,              // Expires at (30s default)
//	  "jti": "unique-id",             // JWT ID (for replay detection)
//	  "caller": "internal-gateway",   // Caller service
//	  "target": "storage-proxy",      // Target service
//	  "team_id": "team-123",          // Team ID
//	  "user_id": "user-456",          // User ID (optional)
//	  "permissions": ["..."],         // Granted permissions
//	  "request_id": "req-abc"         // Request ID (optional)
//	}
//
// # Security Considerations
//
//   1. Secret Management: The INTERNAL_JWT_SECRET must be:
//      - At least 256 bits (32 bytes) of random data
//      - Stored securely (environment variables, K8s secrets, Vault)
//      - Rotated regularly (recommend 90 days)
//      - Never committed to code or exposed in logs
//
//   2. Token Lifetime: Short TTL (30s default) means:
//      - Tokens expire quickly if leaked
//      - No need for token revocation
//      - Clock synchronization is important
//
//   3. Network Security: While tokens are signed, use:
//      - HTTPS/mTLS in production
//      - Network policies to restrict who can connect
//      - Service mesh for additional security
//
//   4. Replay Attack Detection: Enable ReplayDetectionEnabled in ValidatorConfig
//      to prevent token reuse. This adds memory overhead for tracking used JTIs.
//
// # Configuration
//
// Environment variables (recommended):
//
//	INTERNAL_JWT_SECRET=your-256-bit-secret-here
//
// Generate a secure secret:
//
//	openssl rand -base64 32
//
// # Service Registration
//
// Common service names for use as Caller/Target:
//
//	- "internal-gateway"  - API gateway and router
//	- "manager"           - Template and sandbox management
//	- "procd"             - Sandbox process manager
//	- "storage-proxy"     - Storage and volume management
//	- "e2b-gateway"       - E2B compatibility layer
package internalauth
