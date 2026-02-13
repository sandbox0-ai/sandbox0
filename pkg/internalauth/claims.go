// Package internalauth provides internal token-based authentication for
// inter-service communication within the sandbox0 infrastructure.
//
// The package implements a JWT-based authentication scheme using Ed25519
// asymmetric signing where any service acting as a caller can generate tokens
// to authenticate with target services.
//
// # Security Model
//
//   - Uses Ed25519 asymmetric signing (Private key for signing, Public key for verification)
//   - Tokens are short-lived (default 30 seconds)
//   - Tokens are bound to specific caller, target, and team
//   - Tokens cannot be reused across different services via audience and caller validation
//
// # Usage
//
//	// Generate a token
//	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
//	    Caller:     "internal-gateway",
//	    PrivateKey: privateKey,
//	})
//	token, err := generator.Generate("storage-proxy", "team-123", "user-456", internalauth.GenerateOptions{})
//
//	// Validate a token
//	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
//	    Target:    "storage-proxy",
//	    PublicKey: publicKey,
//	})
//	claims, err := validator.Validate(token)
package internalauth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrInvalidToken is returned when the token format is invalid.
	ErrInvalidToken = errors.New("invalid token format")

	// ErrTokenExpired is returned when the token has expired.
	ErrTokenExpired = errors.New("token expired")

	// ErrInvalidSignature is returned when the token signature is invalid.
	ErrInvalidSignature = errors.New("invalid token signature")

	// ErrInvalidIssuer is returned when the token issuer is not recognized.
	ErrInvalidIssuer = errors.New("invalid issuer")

	// ErrInvalidCaller is returned when the caller field is invalid.
	ErrInvalidCaller = errors.New("invalid caller")

	// ErrInvalidTarget is returned when the token audience does not match the expected target.
	ErrInvalidTarget = errors.New("invalid target")

	// ErrReplayAttack is returned when a token has been used before.
	ErrReplayAttack = errors.New("potential replay attack detected")
)

// Claims represents the JWT claims for internal authentication.
type Claims struct {
	// Issuer is the service that issued the token (always the caller).
	Issuer string `json:"iss"`

	// Subject is the team ID that this token represents.
	// For system tokens, this is "system".
	Subject string `json:"sub"`

	// Audience is the target service that should accept this token.
	Audience string `json:"aud"`

	// IssuedAt is the time when the token was issued.
	IssuedAt *jwt.NumericDate `json:"iat"`

	// ExpiresAt is the time when the token expires.
	ExpiresAt *jwt.NumericDate `json:"exp"`

	// ID is a unique identifier for this token (JWT ID).
	ID string `json:"jti"`

	// Caller is the service making the request (same as Issuer).
	Caller string `json:"caller"`

	// Target is the service being called (same as Audience).
	Target string `json:"target"`

	// TeamID is the team ID for authorization context.
	// Empty for system tokens.
	TeamID string `json:"team_id,omitempty"`

	// UserID is the optional user ID for audit logging.
	UserID string `json:"user_id,omitempty"`

	// Permissions is the list of granted permissions.
	Permissions []string `json:"permissions,omitempty"`

	// IsSystem indicates this is a system-level token for internal service communication.
	// System tokens have full access and are not bound to a specific team.
	IsSystem bool `json:"is_system,omitempty"`
}

// GetExpirationTime implements jwt.Claims interface.
func (c *Claims) GetExpirationTime() (*jwt.NumericDate, error) {
	return c.ExpiresAt, nil
}

// GetNotBefore implements jwt.Claims interface.
func (c *Claims) GetNotBefore() (*jwt.NumericDate, error) {
	return nil, nil
}

// GetIssuedAt implements jwt.Claims interface.
func (c *Claims) GetIssuedAt() (*jwt.NumericDate, error) {
	return c.IssuedAt, nil
}

// GetIssuer implements jwt.Claims interface.
func (c *Claims) GetIssuer() (string, error) {
	return c.Issuer, nil
}

// GetSubject implements jwt.Claims interface.
func (c *Claims) GetSubject() (string, error) {
	return c.Subject, nil
}

// GetAudience implements jwt.Claims interface.
func (c *Claims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings{c.Audience}, nil
}

// IsSystemToken returns true if this is a system-level token.
// System tokens are used for internal service communication and have full access.
func (c *Claims) IsSystemToken() bool {
	return c.IsSystem
}

// generateJTI generates a unique JWT ID using crypto/rand.
func generateJTI() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp if crypto/rand fails
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
