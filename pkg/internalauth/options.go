package internalauth

import (
	"crypto/ed25519"
	"time"
)

// GeneratorConfig holds the configuration for a Generator.
type GeneratorConfig struct {
	// Caller is the service name that will generate tokens.
	// Example: "cluster-gateway", "manager", "procd"
	Caller string

	// PrivateKey is the Ed25519 private key used for signing tokens.
	// Required.
	PrivateKey ed25519.PrivateKey

	// TTL is the token time-to-live.
	// Default: 30 seconds.
	TTL time.Duration

	// NowFunc is an optional function that returns the current time.
	// Useful for testing.
	NowFunc func() time.Time
}

// DefaultGeneratorConfig returns a GeneratorConfig with sensible defaults.
func DefaultGeneratorConfig(caller string, privateKey ed25519.PrivateKey) GeneratorConfig {
	return GeneratorConfig{
		Caller:     caller,
		PrivateKey: privateKey,
		TTL:        30 * time.Second,
	}
}

// ValidatorConfig holds the configuration for a Validator.
type ValidatorConfig struct {
	// Target is the service name that will validate tokens.
	// Tokens must have aud == Target to be valid.
	Target string

	// PublicKey is the Ed25519 public key used for verifying tokens.
	// Required.
	PublicKey ed25519.PublicKey

	// AllowedCallers is a list of allowed caller services.
	// If empty, any caller is allowed (trust all internal services).
	AllowedCallers []string

	// ClockSkewTolerance is the allowed clock skew for token expiration.
	// Default: 5 seconds.
	ClockSkewTolerance time.Duration

	// ReplayDetectionEnabled enables replay attack detection.
	// When enabled, tokens with the same JTI will be rejected.
	ReplayDetectionEnabled bool

	// NowFunc is an optional function that returns the current time.
	// Useful for testing.
	NowFunc func() time.Time
}

// DefaultValidatorConfig returns a ValidatorConfig with sensible defaults.
func DefaultValidatorConfig(target string, publicKey ed25519.PublicKey) ValidatorConfig {
	return ValidatorConfig{
		Target:                 target,
		PublicKey:              publicKey,
		AllowedCallers:         nil, // Allow all
		ClockSkewTolerance:     5 * time.Second,
		ReplayDetectionEnabled: false,
	}
}

// GenerateOptions holds optional parameters for token generation.
type GenerateOptions struct {
	// UserID is the optional user ID for audit logging.
	UserID string

	// Permissions is the list of granted permissions.
	// If empty, the token will have no specific permissions.
	Permissions []string

	// SandboxID is the optional sandbox ID for mount/volume tracking.
	SandboxID string

	// TTL is the token time-to-live.
	// If zero, the generator's default TTL is used.
	TTL time.Duration
}

// ValidateOptions holds optional parameters for token validation.
type ValidateOptions struct {
	// RequireTeamID requires the token to have a non-empty team_id.
	RequireTeamID bool

	// RequirePermissions validates that the token has all specified permissions.
	RequirePermissions []string

	// SkipTargetCheck skips the audience/target validation.
	// Useful for debugging or shared endpoints.
	SkipTargetCheck bool

	// SkipCallerCheck skips the caller validation.
	// Useful for debugging.
	SkipCallerCheck bool
}
