package public

import (
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
)

// IdentityResourceGuardLimits maps shared gateway configuration to the
// identity repository's topology-independent cardinality guard.
func IdentityResourceGuardLimits(cfg config.IdentityResourceGuardConfig) identity.IdentityResourceGuardLimits {
	return identity.IdentityResourceGuardLimits{
		MaxTeamsOwnedPerUser:          int64(cfg.MaxTeamsOwnedPerUser),
		MaxMembersPerTeam:             int64(cfg.MaxMembersPerTeam),
		MaxTeamMembershipsPerUser:     int64(cfg.MaxTeamMembershipsPerUser),
		MaxLinkedIdentitiesPerUser:    int64(cfg.MaxLinkedIdentitiesPerUser),
		MaxActiveRefreshTokensPerUser: int64(cfg.MaxActiveRefreshTokensPerUser),
		MaxActiveWebLoginCodesPerUser: int64(cfg.MaxActiveWebLoginCodesPerUser),
		MaxActiveDeviceAuthSessions:   int64(cfg.MaxActiveDeviceAuthSessions),
		MaxPendingOIDCStates:          int64(cfg.MaxPendingOIDCStates),
	}
}
