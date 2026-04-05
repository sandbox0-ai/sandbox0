package middleware

import "errors"

var (
	// Authentication errors
	ErrInvalidToken            = errors.New("invalid token")
	ErrExpiredToken            = errors.New("token expired")
	ErrInvalidSigningMethod    = errors.New("invalid signing method")
	ErrJWTNotConfigured        = errors.New("JWT authentication not configured")
	ErrSelectedTeamWrongRegion = errors.New("selected team is not hosted in this region")
	ErrSelectedTeamForbidden   = errors.New("not a member of selected team")

	// Rate limiting errors
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)
