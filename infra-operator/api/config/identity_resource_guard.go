// +kubebuilder:object:generate=true
package config

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// IdentityResourceGuardConfig bounds global identity graph and authentication
// session state. It is independent from region Team Quota policies.
type IdentityResourceGuardConfig struct {
	MaxTeamsOwnedPerUser          int             `yaml:"max_teams_owned_per_user" json:"maxTeamsOwnedPerUser"`
	MaxMembersPerTeam             int             `yaml:"max_members_per_team" json:"maxMembersPerTeam"`
	MaxTeamMembershipsPerUser     int             `yaml:"max_team_memberships_per_user" json:"maxTeamMembershipsPerUser"`
	MaxLinkedIdentitiesPerUser    int             `yaml:"max_linked_identities_per_user" json:"maxLinkedIdentitiesPerUser"`
	MaxActiveRefreshTokensPerUser int             `yaml:"max_active_refresh_tokens_per_user" json:"maxActiveRefreshTokensPerUser"`
	MaxActiveWebLoginCodesPerUser int             `yaml:"max_active_web_login_codes_per_user" json:"maxActiveWebLoginCodesPerUser"`
	MaxActiveDeviceAuthSessions   int             `yaml:"max_active_device_auth_sessions" json:"maxActiveDeviceAuthSessions"`
	MaxPendingOIDCStates          int             `yaml:"max_pending_oidc_states" json:"maxPendingOidcStates"`
	SessionCleanupInterval        metav1.Duration `yaml:"session_cleanup_interval" json:"sessionCleanupInterval"`
	SessionCleanupBatchSize       int             `yaml:"session_cleanup_batch_size" json:"sessionCleanupBatchSize"`
}
