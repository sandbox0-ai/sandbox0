package identity

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrUserNotFound       = errors.New("user not found")
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")

	ErrTeamNotFound   = errors.New("team not found")
	ErrMemberNotFound = errors.New("team member not found")
	ErrAlreadyMember  = errors.New("user is already a team member")

	ErrIdentityNotFound      = errors.New("identity not found")
	ErrIdentityAlreadyExists = errors.New("identity already exists")

	ErrSSHPublicKeyNotFound      = errors.New("ssh public key not found")
	ErrSSHPublicKeyAlreadyExists = errors.New("ssh public key already exists")

	ErrTokenNotFound = errors.New("refresh token not found")
	ErrTokenRevoked  = errors.New("refresh token revoked")
	ErrTokenExpired  = errors.New("refresh token expired")

	ErrDeviceAuthSessionNotFound = errors.New("device auth session not found")
	ErrDeviceAuthSessionExpired  = errors.New("device auth session expired")
	ErrDeviceAuthSessionConsumed = errors.New("device auth session already consumed")

	ErrWebLoginCodeNotFound = errors.New("web login code not found")
)

// Repository provides database access for identity and tenancy data.
type Repository struct {
	pool             *pgxpool.Pool
	teamCreationHook TeamCreationHook
}

// RepositoryOption configures optional identity repository behavior.
type RepositoryOption func(*Repository)

// WithTeamCreationHook adds transactional and post-commit work to every team
// creation path.
func WithTeamCreationHook(hook TeamCreationHook) RepositoryOption {
	return func(repository *Repository) {
		repository.teamCreationHook = hook
	}
}

// NewRepository creates a new database repository.
func NewRepository(pool *pgxpool.Pool, opts ...RepositoryOption) *Repository {
	repository := &Repository{pool: pool}
	for _, opt := range opts {
		opt(repository)
	}
	return repository
}

// Pool returns the underlying connection pool.
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}
