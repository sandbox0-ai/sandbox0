package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// IdentityResourceGuardConfig bounds global identity graph and authentication
// session state independently from region Team Quota.
type IdentityResourceGuardConfig struct {
	// +optional
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	MaxTeamsOwnedPerUser int `json:"maxTeamsOwnedPerUser,omitempty"`
	// +optional
	// +kubebuilder:default=256
	// +kubebuilder:validation:Minimum=1
	MaxMembersPerTeam int `json:"maxMembersPerTeam,omitempty"`
	// +optional
	// +kubebuilder:default=32
	// +kubebuilder:validation:Minimum=1
	MaxTeamMembershipsPerUser int `json:"maxTeamMembershipsPerUser,omitempty"`
	// +optional
	// +kubebuilder:default=8
	// +kubebuilder:validation:Minimum=1
	MaxLinkedIdentitiesPerUser int `json:"maxLinkedIdentitiesPerUser,omitempty"`
	// +optional
	// +kubebuilder:default=16
	// +kubebuilder:validation:Minimum=1
	MaxActiveRefreshTokensPerUser int `json:"maxActiveRefreshTokensPerUser,omitempty"`
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	MaxActiveWebLoginCodesPerUser int `json:"maxActiveWebLoginCodesPerUser,omitempty"`
	// +optional
	// +kubebuilder:default=10000
	// +kubebuilder:validation:Minimum=1
	MaxActiveDeviceAuthSessions int `json:"maxActiveDeviceAuthSessions,omitempty"`
	// +optional
	// +kubebuilder:default=10000
	// +kubebuilder:validation:Minimum=1
	MaxPendingOIDCStates int `json:"maxPendingOidcStates,omitempty"`
	// +optional
	// +kubebuilder:default="1m"
	SessionCleanupInterval metav1.Duration `json:"sessionCleanupInterval,omitempty"`
	// +optional
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=1
	SessionCleanupBatchSize int `json:"sessionCleanupBatchSize,omitempty"`
}
