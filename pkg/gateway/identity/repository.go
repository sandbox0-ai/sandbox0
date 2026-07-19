package identity

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

var (
	ErrUserNotFound       = errors.New("user not found")
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")

	ErrTeamNotFound              = errors.New("team not found")
	ErrTeamAlreadyExists         = errors.New("team already exists")
	ErrMemberNotFound            = errors.New("team member not found")
	ErrAlreadyMember             = errors.New("user is already a team member")
	ErrCannotRemoveTeamOwner     = errors.New("cannot remove team owner")
	ErrCannotRemoveLastTeamAdmin = errors.New("cannot remove the last team admin")
	ErrCannotDemoteTeamOwner     = errors.New("cannot demote team owner")
	ErrTeamOwnerChanged          = errors.New("team owner changed")
	ErrTeamDeletionInProgress    = errors.New("team deletion is in progress")
	ErrTeamDeletionNotFenced     = errors.New("team deletion is not fenced")

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

	ErrOIDCPendingStateNotFound      = errors.New("OIDC pending state not found")
	ErrOIDCPendingStateAlreadyExists = errors.New("OIDC pending state already exists")
	ErrInvalidOIDCPendingState       = errors.New("invalid OIDC pending state")
)

// Repository provides database access for identity and tenancy data.
type Repository struct {
	pool                  *pgxpool.Pool
	teamQuotaStore        teamquota.CapacityTxStore
	identityResourceGuard *IdentityResourceGuardLimits
}

// RepositoryOption customizes an identity repository.
type RepositoryOption func(*Repository)

// WithTeamQuotaStore overrides the team quota store.
func WithTeamQuotaStore(store teamquota.CapacityTxStore) RepositoryOption {
	return func(repository *Repository) {
		repository.teamQuotaStore = store
	}
}

// WithIdentityResourceGuard enables global identity cardinality enforcement.
// Region gateways must not use this as a replacement for region Team Quota.
func WithIdentityResourceGuard(limits IdentityResourceGuardLimits) RepositoryOption {
	return func(repository *Repository) {
		normalized := limits.withDefaults()
		repository.identityResourceGuard = &normalized
	}
}

// NewRepository creates a new database repository.
func NewRepository(pool *pgxpool.Pool, opts ...RepositoryOption) *Repository {
	repository := &Repository{
		pool:           pool,
		teamQuotaStore: teamquota.NewRepository(pool),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(repository)
		}
	}
	return repository
}

func (r *Repository) identityResourceLimits() (IdentityResourceGuardLimits, bool) {
	if r == nil || r.identityResourceGuard == nil {
		return IdentityResourceGuardLimits{}, false
	}
	return *r.identityResourceGuard, true
}

// Pool returns the underlying connection pool.
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}
