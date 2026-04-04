package identity

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrUserNotFound       = errors.New("user not found")
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")

	ErrTeamNotFound      = errors.New("team not found")
	ErrTeamAlreadyExists = errors.New("team already exists")
	ErrMemberNotFound    = errors.New("team member not found")
	ErrAlreadyMember     = errors.New("user is already a team member")

	ErrIdentityNotFound      = errors.New("identity not found")
	ErrIdentityAlreadyExists = errors.New("identity already exists")

	ErrTokenNotFound = errors.New("refresh token not found")
	ErrTokenRevoked  = errors.New("refresh token revoked")
	ErrTokenExpired  = errors.New("refresh token expired")

	ErrDeviceAuthSessionNotFound = errors.New("device auth session not found")
	ErrDeviceAuthSessionExpired  = errors.New("device auth session expired")
	ErrDeviceAuthSessionConsumed = errors.New("device auth session already consumed")
)

// Repository provides database access for identity and tenancy data.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new database repository.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Pool returns the underlying connection pool.
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}
