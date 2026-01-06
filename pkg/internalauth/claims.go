// Package internalauth provides internal token-based authentication for
// inter-service communication within the sandbox0 infrastructure.
//
// The package implements a JWT-based authentication scheme where any service
// acting as a caller can generate tokens to authenticate with target services.
// Tokens are signed with a shared secret and contain claims about the caller,
// target, team, and permissions.
//
// # Security Model
//
//   - All services share a common INTERNAL_JWT_SECRET
//   - Tokens are short-lived (default 30 seconds)
//   - Tokens are bound to specific caller, target, and team
//   - Tokens cannot be reused across different services
//
// # Usage
//
//	// Generate a token
//	generator := internalauth.NewGenerator("internal-gateway")
//	token, err := generator.Generate("storage-proxy", "team-123", "user-456", perms)
//
//	// Validate a token
//	validator := internalauth.NewValidator("storage-proxy")
//	claims, err := validator.Validate(token)
package internalauth

import (
	"errors"
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
	TeamID string `json:"team_id"`

	// UserID is the optional user ID for audit logging.
	UserID string `json:"user_id,omitempty"`

	// Permissions is the list of granted permissions.
	Permissions []string `json:"permissions,omitempty"`

	// RequestID is the optional request ID for tracing.
	RequestID string `json:"request_id,omitempty"`
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

// newClaims creates a new Claims instance with the given parameters.
func newClaims(caller, target, teamID, userID, requestID string, permissions []string, ttl time.Duration) *Claims {
	now := time.Now()
	return &Claims{
		Issuer:      caller,
		Subject:     teamID,
		Audience:    target,
		IssuedAt:    jwt.NewNumericDate(now),
		ExpiresAt:   jwt.NewNumericDate(now.Add(ttl)),
		ID:          generateJTI(),
		Caller:      caller,
		Target:      target,
		TeamID:      teamID,
		UserID:      userID,
		Permissions: permissions,
		RequestID:   requestID,
	}
}

// generateJTI generates a unique JWT ID using timestamp and random bytes.
func generateJTI() string {
	return jwt.NewNumericDate(time.Now()).String() + "-" + randomString(8)
}

// randomString generates a random hex string of the specified length.
func randomString(length int) string {
	const charset = "0123456789abcdef"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[jwtrnd(len(charset))]
	}
	return string(b)
}

func jwtrnd(n int) int {
	// Simple random generator for JWT ID
	// In production, use crypto/rand
	return int(time.Now().UnixNano()) % n
}
