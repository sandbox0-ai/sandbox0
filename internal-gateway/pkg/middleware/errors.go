package middleware

import "errors"

var (
	// Authentication errors
	ErrInvalidToken         = errors.New("invalid token")
	ErrExpiredToken         = errors.New("token expired")
	ErrInvalidSigningMethod = errors.New("invalid signing method")
	ErrJWTNotConfigured     = errors.New("JWT authentication not configured")

	// Rate limiting errors
	ErrRateLimitExceeded = errors.New("rate limit exceeded")

	// Quota errors
	ErrQuotaExceeded = errors.New("quota exceeded")
)
