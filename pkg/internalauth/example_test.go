package internalauth_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/huangzhihao/sandbox0/infra/pkg/internalauth"
)

func ExampleGenerator() {
	// Create a generator for the caller service
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: "internal-gateway",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
		TTL:    30 * time.Second,
	})

	// Generate a token for calling storage-proxy
	token, err := generator.Generate(
		"storage-proxy",                        // target
		"team-123",                             // team ID
		"user-456",                             // user ID
		internalauth.GenerateOptions{           // options
			Permissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
			RequestID:  "req-abc123",
			UserID:     "user-456",
		},
	)
	if err != nil {
		panic(err)
	}

	// Output is a JWT token that changes each time
	_ = token
	fmt.Println("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
	// Output: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
}

func ExampleValidator() {
	// First, generate a token (in real usage, this comes from the caller service)
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: "internal-gateway",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
	})
	token, _ := generator.Generate("storage-proxy", "team-123", "user-456",
		internalauth.GenerateOptions{
			Permissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
		})

	// Create a validator for the target service
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target: "storage-proxy",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
	})

	// Validate the token
	claims, err := validator.Validate(token)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Caller: %s, Team: %s, Permissions: %v\n",
		claims.Caller, claims.TeamID, claims.Permissions)
	// Output: Caller: internal-gateway, Team: team-123, Permissions: [sandboxvolume:read sandboxvolume:write]
}

func ExampleAuthMiddleware() {
	// Create validator
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target: "storage-proxy",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
	})

	// Create middleware
	middleware := internalauth.AuthMiddleware(
		validator,
		internalauth.DefaultExtractor(),
	)

	// Create a handler that uses the claims
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := internalauth.ClaimsFromContext(r.Context())
		if claims != nil {
			fmt.Fprintf(w, "Hello team %s from %s", claims.TeamID, claims.Caller)
		}
	})

	// Wrap handler with middleware
	wrappedHandler := middleware(handler)

	// Test the handler
	req := httptest.NewRequest("GET", "/api/test", nil)
	req.Header.Set("X-Internal-Token", "valid-token-here")
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)
}

func Example_authenticatedClient() {
	// Create generator
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: "internal-gateway",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
	})

	// Create an auto-authenticating HTTP client
	client := internalauth.NewAuthenticatedClient(generator, "storage-proxy")

	// Make a request with team context
	req, _ := http.NewRequest("GET", "http://storage-proxy:8081/api/v1/volumes", nil)
	req = req.WithContext(internalauth.ContextWithTeam(req.Context(), "team-123"))

	// Token is automatically added to the request
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Println(resp.StatusCode)
}

func ExampleTransport() {
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: "internal-gateway",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
	})

	// Create a custom transport with dynamic target selection
	transport := internalauth.NewTransport(generator,
		internalauth.WithTargetFn(func(req *http.Request) string {
			// Extract target from request host
			host := req.URL.Host
			if idx := strings.Index(host, "."); idx > 0 {
				return host[:idx]
			}
			return host
		}),
		internalauth.WithTeamIDFn(internalauth.TeamIDFromContext),
		internalauth.WithPermissionsFn(internalauth.PermissionsFromContext),
	)

	client := &http.Client{Transport: transport}
	_ = client
}

func Example_context() {
	// Create a request with authentication context
	req, _ := http.NewRequest("GET", "http://service/api", nil)

	// Add team and user context
	req = internalauth.NewRequestContext(req, "team-123", "user-456",
		[]string{"resource:read", "resource:write"})

	// In a handler, extract the claims
	claims := internalauth.ClaimsFromContext(req.Context())
	fmt.Printf("Team: %s, User: %s\n", claims.TeamID, claims.UserID)

	// Or use convenience functions
	teamID := internalauth.GetTeamID(req.Context())
	hasPerm := internalauth.HasPermission(req.Context(), "resource:read")
	_, _ = teamID, hasPerm
}

func ExampleRequirePermissions() {
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target: "admin-service",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
	})

	authMiddleware := internalauth.AuthMiddleware(validator, internalauth.DefaultExtractor())

	// Create a handler that requires admin permissions
	adminHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Admin panel"))
	})

	// Require admin:* permission
	wrappedHandler := authMiddleware(
		internalauth.RequirePermissions("admin:*")(adminHandler),
	)

	_ = wrappedHandler
}

func ExampleRequireTeam() {
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target: "team-service",
		Secret: []byte("test-secret-key-32-bytes-long!!!"),
	})

	authMiddleware := internalauth.AuthMiddleware(validator, internalauth.DefaultExtractor())

	// Create a handler that requires specific team
	teamHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Team resource"))
	})

	// Require team-123
	wrappedHandler := authMiddleware(
		internalauth.RequireTeam("team-123")(teamHandler),
	)

	_ = wrappedHandler
}
