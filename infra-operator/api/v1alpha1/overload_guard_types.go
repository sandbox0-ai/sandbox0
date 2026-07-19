package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// OverloadGuardConfig configures a platform safety valve that is independent
// from region Team Quota policies.
type OverloadGuardConfig struct {
	// +optional
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	RequestsPerSecond int `json:"requestsPerSecond,omitempty"`

	// +optional
	// +kubebuilder:default=200
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000000
	Burst int `json:"burst,omitempty"`

	// LocalRequestsPerSecond bounds attempts made by one service process before
	// the region-shared Redis guard is consulted.
	// +optional
	// +kubebuilder:default=500
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	LocalRequestsPerSecond int `json:"localRequestsPerSecond,omitempty"`

	// +optional
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000000
	LocalBurst int `json:"localBurst,omitempty"`

	// MaxInFlight bounds requests concurrently executing in one service
	// process.
	// +optional
	// +kubebuilder:default=512
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	MaxInFlight int `json:"maxInFlight,omitempty"`

	// +optional
	// +kubebuilder:default="10m"
	CleanupInterval metav1.Duration `json:"cleanupInterval,omitempty"`
}
