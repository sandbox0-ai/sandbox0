package identity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

const (
	DefaultMaxTeamsOwnedPerUser          int64 = 10
	DefaultMaxMembersPerTeam             int64 = 256
	DefaultMaxTeamMembershipsPerUser     int64 = 32
	DefaultMaxLinkedIdentitiesPerUser    int64 = 8
	DefaultMaxActiveRefreshTokensPerUser int64 = 16
	DefaultMaxActiveWebLoginCodesPerUser int64 = 3
	DefaultMaxActiveDeviceAuthSessions   int64 = 10_000
	DefaultMaxPendingOIDCStates          int64 = 10_000

	MaxIdentityRawClaimsBytes        = 256 * 1024
	MaxIdentityRequestBodyBytes      = 32 * 1024
	MaxIdentityEmailBytes            = 254
	MaxIdentityNameBytes             = 250
	MaxIdentityPasswordBytes         = 4 * 1024
	MaxIdentityTokenBytes            = 16 * 1024
	MaxIdentityLoginCodeBytes        = 1024
	MaxIdentityDeviceLoginIDBytes    = 128
	MaxIdentityTeamNameBytes         = 255
	MaxIdentityTeamSlugBytes         = 255
	MaxIdentityTeamRoleBytes         = 50
	MaxIdentityHomeRegionIDBytes     = 128
	MaxIdentityMemberSearchBytes     = 256
	MaxIdentityAvatarURLBytes        = 8 * 1024
	MaxIdentityReturnURLBytes        = 8 * 1024
	MaxIdentityDeviceCodeBytes       = 16 * 1024
	MaxIdentityVerificationURIBytes  = 8 * 1024
	MaxIdentityOIDCStateHashBytes    = 128
	MaxIdentityOIDCProviderBytes     = 256
	MaxIdentityOIDCCodeVerifierBytes = 1024
)

const (
	IdentityLimitResourceTeamsOwned       = "teams_owned"
	IdentityLimitResourceTeamMembers      = "team_members"
	IdentityLimitResourceTeamMemberships  = "team_memberships"
	IdentityLimitResourceLinkedIdentities = "linked_identities"
	IdentityLimitResourceRefreshTokens    = "active_refresh_tokens"
	IdentityLimitResourceWebLoginCodes    = "active_web_login_codes"
	IdentityLimitResourceDeviceSessions   = "active_device_auth_sessions"
	IdentityLimitResourceOIDCStates       = "pending_oidc_states"
)

// IdentityResourceGuardLimits bounds global identity state independently from
// region-owned Team Quota policies.
type IdentityResourceGuardLimits struct {
	MaxTeamsOwnedPerUser          int64
	MaxMembersPerTeam             int64
	MaxTeamMembershipsPerUser     int64
	MaxLinkedIdentitiesPerUser    int64
	MaxActiveRefreshTokensPerUser int64
	MaxActiveWebLoginCodesPerUser int64
	MaxActiveDeviceAuthSessions   int64
	MaxPendingOIDCStates          int64
}

// DefaultIdentityResourceGuardLimits returns the fail-safe global identity
// cardinality limits.
func DefaultIdentityResourceGuardLimits() IdentityResourceGuardLimits {
	return IdentityResourceGuardLimits{
		MaxTeamsOwnedPerUser:          DefaultMaxTeamsOwnedPerUser,
		MaxMembersPerTeam:             DefaultMaxMembersPerTeam,
		MaxTeamMembershipsPerUser:     DefaultMaxTeamMembershipsPerUser,
		MaxLinkedIdentitiesPerUser:    DefaultMaxLinkedIdentitiesPerUser,
		MaxActiveRefreshTokensPerUser: DefaultMaxActiveRefreshTokensPerUser,
		MaxActiveWebLoginCodesPerUser: DefaultMaxActiveWebLoginCodesPerUser,
		MaxActiveDeviceAuthSessions:   DefaultMaxActiveDeviceAuthSessions,
		MaxPendingOIDCStates:          DefaultMaxPendingOIDCStates,
	}
}

func (l IdentityResourceGuardLimits) withDefaults() IdentityResourceGuardLimits {
	defaults := DefaultIdentityResourceGuardLimits()
	if l.MaxTeamsOwnedPerUser <= 0 {
		l.MaxTeamsOwnedPerUser = defaults.MaxTeamsOwnedPerUser
	}
	if l.MaxMembersPerTeam <= 0 {
		l.MaxMembersPerTeam = defaults.MaxMembersPerTeam
	}
	if l.MaxTeamMembershipsPerUser <= 0 {
		l.MaxTeamMembershipsPerUser = defaults.MaxTeamMembershipsPerUser
	}
	if l.MaxLinkedIdentitiesPerUser <= 0 {
		l.MaxLinkedIdentitiesPerUser = defaults.MaxLinkedIdentitiesPerUser
	}
	if l.MaxActiveRefreshTokensPerUser <= 0 {
		l.MaxActiveRefreshTokensPerUser = defaults.MaxActiveRefreshTokensPerUser
	}
	if l.MaxActiveWebLoginCodesPerUser <= 0 {
		l.MaxActiveWebLoginCodesPerUser = defaults.MaxActiveWebLoginCodesPerUser
	}
	if l.MaxActiveDeviceAuthSessions <= 0 {
		l.MaxActiveDeviceAuthSessions = defaults.MaxActiveDeviceAuthSessions
	}
	if l.MaxPendingOIDCStates <= 0 {
		l.MaxPendingOIDCStates = defaults.MaxPendingOIDCStates
	}
	return l
}

// IdentityResourceLimitExceededError reports a bounded global identity resource.
// It is deliberately distinct from region Team Quota errors.
type IdentityResourceLimitExceededError struct {
	Scope    string
	ScopeID  string
	Resource string
	Limit    int64
}

func (e *IdentityResourceLimitExceededError) Error() string {
	if e == nil {
		return "global identity limit exceeded"
	}
	return fmt.Sprintf(
		"global identity limit exceeded: scope=%s scope_id=%s resource=%s limit=%d",
		e.Scope,
		e.ScopeID,
		e.Resource,
		e.Limit,
	)
}

// IsIdentityResourceLimitExceeded reports whether err is a global identity
// cardinality error.
func IsIdentityResourceLimitExceeded(err error) bool {
	var target *IdentityResourceLimitExceededError
	return errors.As(err, &target)
}

// IdentityPayloadTooLargeError reports an identity value that is unsafe to
// persist. The IdP is an upstream input and is not trusted to bound its claims.
type IdentityPayloadTooLargeError struct {
	Field       string
	ActualBytes int
	MaxBytes    int
}

func (e *IdentityPayloadTooLargeError) Error() string {
	if e == nil {
		return "identity payload is too large"
	}
	return fmt.Sprintf(
		"identity payload field %s is too large: got %d bytes, max %d",
		e.Field,
		e.ActualBytes,
		e.MaxBytes,
	)
}

// IsIdentityPayloadTooLarge reports whether err is a persisted identity size
// violation.
func IsIdentityPayloadTooLarge(err error) bool {
	var target *IdentityPayloadTooLargeError
	return errors.As(err, &target)
}

func validateIdentityFieldSize(field, value string, maxBytes int) error {
	if len(value) <= maxBytes {
		return nil
	}
	return &IdentityPayloadTooLargeError{
		Field:       field,
		ActualBytes: len(value),
		MaxBytes:    maxBytes,
	}
}

func validateIdentityBytesSize(field string, value []byte, maxBytes int) error {
	if len(value) <= maxBytes {
		return nil
	}
	return &IdentityPayloadTooLargeError{
		Field:       field,
		ActualBytes: len(value),
		MaxBytes:    maxBytes,
	}
}

func identityUserScope(userID string) string {
	return "global-identity:user:" + strings.TrimSpace(userID)
}

func identityTeamScope(teamID string) string {
	return "global-identity:team:" + strings.TrimSpace(teamID)
}

const identityDeviceSessionsScope = "global-identity:device-auth-sessions"

const identityOIDCPendingStatesScope = "global-identity:oidc-pending-states"

// lockIdentityScopesTx serializes cardinality checks across all global-gateway
// replicas. Sorting and de-duplicating the keys gives every multi-scope writer
// one lock order and prevents user/team inversion deadlocks.
func lockIdentityScopesTx(ctx context.Context, tx pgx.Tx, scopes ...string) error {
	unique := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope != "" {
			unique[scope] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(unique))
	for scope := range unique {
		ordered = append(ordered, scope)
	}
	sort.Strings(ordered)
	for _, scope := range ordered {
		if _, err := tx.Exec(
			ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			scope,
		); err != nil {
			return fmt.Errorf("lock global identity scope %q: %w", scope, err)
		}
	}
	return nil
}
