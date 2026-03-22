// Package internalauth provides internal token-based authentication for
// inter-service communication within the sandbox0 infrastructure.
//
// # Architecture Overview
//
// The package implements a JWT-based authentication scheme using Ed25519
// asymmetric encryption where any service can generate tokens to authenticate
// with other services. This enables:
//
//   - Zero-trust inter-service communication
//   - Token-based authentication with short-lived tokens (default 30s)
//   - Caller and target validation to prevent token reuse
//   - Untrusted services can verify tokens without storing secrets
//   - Replay attack detection (optional)
//
// # Components
//
//   - Generator: Creates signed JWT tokens for a caller service (uses private key)
//   - Validator: Validates tokens for a target service (uses public key only)
//   - Middleware: HTTP middleware for automatic token validation
//   - Transport: HTTP RoundTripper for automatic token injection
//
// # Ed25519 Asymmetric Signing
//
// The package uses Ed25519 for all token operations:
//
//   - Generator uses private key to sign tokens
//   - Validator only needs public key to verify tokens
//   - Ideal for untrusted services like procd that should not hold secrets
//
// # Basic Usage
//
//	// Step 1: Generate key pair (one-time, run in secure environment)
//	privateKeyPEM, publicKeyPEM, err := internalauth.GenerateEd25519KeyPair()
//	os.WriteFile("/secrets/internal_jwt_private.key", privateKeyPEM, 0600)
//	os.WriteFile("/config/internal_jwt_public.key", publicKeyPEM, 0644)
//
//	// Step 2: Generator (cluster-gateway) - load private key and sign
//	privateKey, _ := internalauth.LoadEd25519PrivateKeyFromFile("/secrets/internal_jwt_private.key")
//	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
//	    Caller:     "cluster-gateway",
//	    PrivateKey: privateKey,
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
//	// Step 3: Validator (storage-proxy/procd) - load public key and verify
//	publicKey, _ := internalauth.LoadEd25519PublicKeyFromFile("/config/internal_jwt_public.key")
//	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
//	    Target:    "storage-proxy",
//	    PublicKey: publicKey,
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
//	  "iss": "cluster-gateway",      // Issuer (caller service)
//	  "sub": "team-123",              // Subject (team ID)
//	  "aud": "storage-proxy",         // Audience (target service)
//	  "iat": 1706745600,              // Issued at
//	  "exp": 1706745630,              // Expires at (30s default)
//	  "jti": "unique-id",             // JWT ID (for replay detection)
//	  "caller": "cluster-gateway",   // Caller service
//	  "target": "storage-proxy",      // Target service
//	  "team_id": "team-123",          // Team ID
//	  "user_id": "user-456",          // User ID (optional)
//	  "permissions": ["..."]          // Granted permissions
//	}
//
// # Security Considerations
//
//  1. Key Management:
//     - Private key must be stored securely (environment variables, K8s secrets, Vault)
//     - Private key should only be on trusted services (cluster-gateway)
//     - Public key can be embedded in untrusted service images or mounted as config
//     - Rotate keys regularly (recommend 90 days)
//     - Never commit keys to code or expose them in logs
//
//  2. Token Lifetime: Short TTL (30s default) means:
//     - Tokens expire quickly if leaked
//     - No need for token revocation
//     - Clock synchronization is important
//
//  3. Network Security: While tokens are signed, use:
//     - HTTPS/mTLS in production
//     - Network policies to restrict who can connect
//     - Service mesh for additional security
//
//  4. Replay Attack Detection: Enable ReplayDetectionEnabled in ValidatorConfig
//     to prevent token reuse. This adds memory overhead for tracking used JTIs.
//
// # Key Generation
//
// Generate Ed25519 key pairs using the provided functions:
//
//	// Generate new key pair
//	privateKeyPEM, publicKeyPEM, err := internalauth.GenerateEd25519KeyPair()
//
//	// Save to files
//	os.WriteFile("private.key", privateKeyPEM, 0600)
//	os.WriteFile("public.key", publicKeyPEM, 0644)
//
// # Service Registration
//
// Common service names for use as Caller/Target:
//
//   - "cluster-gateway"  - API gateway and router
//   - "manager"           - Template and sandbox management
//   - "procd"             - Sandbox process manager (untrusted, uses public key only)
//   - "storage-proxy"     - Storage and volume management
//   - "e2b-gateway"       - E2B compatibility layer
package internalauth
