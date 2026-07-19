package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// TeamQuotaConfig defines region-level defaults and distributed enforcement.
// Defaults are reconciled only by the region entrypoint. Data-plane services
// consume the resulting policy state and receive only distributed runtime
// settings.
// +kubebuilder:validation:XValidation:rule="!has(self.defaults) || ['sandbox_identity_count', 'sandbox_runtime_count', 'sandbox_cpu_millicores', 'sandbox_memory_bytes', 'sandbox_ephemeral_storage_bytes', 'volume_storage_bytes', 'snapshot_storage_bytes', 'rootfs_storage_bytes', 'template_image_storage_bytes', 'storage_object_count', 'control_plane_object_count', 'active_connection_count', 'active_request_count', 'api_requests', 'sandbox_service_requests', 'sandbox_starts', 'network_operations', 'network_ingress_bytes', 'network_egress_bytes', 'storage_operations', 'observability_ingest_bytes'].all(k, self.defaults.exists(p, p.key == k))",message="when configured, defaults must contain every known team quota key exactly once"
type TeamQuotaConfig struct {
	// StateID is required on consumer-only resources and must match the region
	// owner's status.teamQuota.stateId. A region owner normally omits this
	// field so the operator generates a fresh UUID v4 and persists it in
	// status. Set it on a new owner only when recovering the same retained
	// PostgreSQL and Redis state plane; after status is initialized, a
	// different value is rejected.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
	StateID string `json:"stateId,omitempty"`

	// Defaults is the complete set of fallback policies for teams without an
	// override for the same key. It is required on the region policy owner and
	// must be omitted from consumer-only resources.
	// +optional
	// +kubebuilder:validation:MinItems=21
	// +kubebuilder:validation:MaxItems=21
	// +listType=map
	// +listMapKey=key
	Defaults []TeamQuotaPolicyConfig `json:"defaults,omitempty"`

	// DistributedEnforcement configures rate-policy lookup and live-concurrency
	// lease behavior. Redis connection details are derived from spec.redis.
	// +optional
	// +kubebuilder:default={}
	DistributedEnforcement TeamQuotaDistributedEnforcementConfig `json:"distributedEnforcement,omitempty"`
}

// TeamQuotaPolicyConfig defines one region-level default team quota policy.
// Capacity and concurrency policies use limit. Rate policies use tokens,
// interval, and burst.
// +kubebuilder:validation:XValidation:rule="self.key in ['sandbox_identity_count', 'sandbox_runtime_count', 'sandbox_cpu_millicores', 'sandbox_memory_bytes', 'sandbox_ephemeral_storage_bytes', 'volume_storage_bytes', 'snapshot_storage_bytes', 'rootfs_storage_bytes', 'template_image_storage_bytes', 'storage_object_count', 'control_plane_object_count'] ? self.kind == 'capacity' : self.key in ['active_connection_count', 'active_request_count'] ? self.kind == 'concurrency' : self.kind == 'rate'",message="quota key and kind do not match"
// +kubebuilder:validation:XValidation:rule="self.kind in ['capacity', 'concurrency'] ? has(self.limit) && !has(self.tokens) && !has(self.interval) && !has(self.burst) : !has(self.limit) && has(self.tokens) && has(self.interval) && has(self.burst)",message="capacity and concurrency policies require only limit; rate policies require tokens, interval, and burst"
// +kubebuilder:validation:XValidation:rule="self.kind != 'concurrency' || !has(self.limit) || self.limit <= 9007199254740991",message="concurrency limit exceeds the exact Redis integer range"
// +kubebuilder:validation:XValidation:rule="self.kind != 'rate' || !has(self.tokens) || !has(self.burst) || self.burst >= self.tokens",message="rate policy burst must be greater than or equal to tokens"
// Exact whole-millisecond validation remains in the operator plan and runtime
// because expressing that conversion in CRD CEL exceeds API server cost limits.
// +kubebuilder:validation:XValidation:rule="self.kind != 'rate' || !has(self.interval) || (duration(self.interval) >= duration('1ms') && duration(self.interval) <= duration('1h'))",message="rate policy interval must be between 1ms and 1h"
type TeamQuotaPolicyConfig struct {
	// Key identifies the resource protected by this policy.
	// +kubebuilder:validation:Enum=sandbox_identity_count;sandbox_runtime_count;sandbox_cpu_millicores;sandbox_memory_bytes;sandbox_ephemeral_storage_bytes;volume_storage_bytes;snapshot_storage_bytes;rootfs_storage_bytes;template_image_storage_bytes;storage_object_count;control_plane_object_count;active_connection_count;active_request_count;api_requests;sandbox_service_requests;sandbox_starts;network_operations;network_ingress_bytes;network_egress_bytes;storage_operations;observability_ingest_bytes
	Key string `json:"key"`

	// Kind selects capacity accounting, live concurrency, or token-bucket rate
	// enforcement.
	// +kubebuilder:validation:Enum=capacity;concurrency;rate
	Kind string `json:"kind"`

	// Limit is required for capacity and concurrency policies and must be
	// omitted for rate policies. Concurrency limits must fit Redis's exact
	// integer range.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Limit *int64 `json:"limit,omitempty"`

	// Tokens is the number of tokens replenished every interval. It is required
	// for rate policies.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	Tokens *int64 `json:"tokens,omitempty"`

	// Interval is the token replenishment interval. It is required for rate
	// policies, must be between 1ms and 1h, and must use whole milliseconds.
	// +optional
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	Interval *metav1.Duration `json:"interval,omitempty"`

	// Burst is the maximum number of accumulated tokens. It is required for
	// rate policies and must be at least tokens.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	Burst *int64 `json:"burst,omitempty"`
}

// TeamQuotaDistributedEnforcementConfig controls policy lookup and
// concurrency lease behavior shared by distributed enforcers.
// +kubebuilder:validation:XValidation:rule="!has(self.policyCacheTtl) || duration(self.policyCacheTtl) >= duration('0s')",message="policyCacheTtl must be non-negative"
// +kubebuilder:validation:XValidation:rule="!has(self.leaseTtl) || duration(self.leaseTtl) > duration('0s')",message="leaseTtl must be positive"
// +kubebuilder:validation:XValidation:rule="!has(self.renewInterval) || duration(self.renewInterval) > duration('0s')",message="renewInterval must be positive"
// +kubebuilder:validation:XValidation:rule="!has(self.renewInterval) || !has(self.leaseTtl) || duration(self.renewInterval) + duration(self.renewInterval) < duration(self.leaseTtl)",message="renewInterval doubled must be less than leaseTtl"
type TeamQuotaDistributedEnforcementConfig struct {
	// PolicyCacheTTL bounds how long an enforcer may cache an effective policy.
	// +optional
	// +kubebuilder:default="5s"
	PolicyCacheTTL metav1.Duration `json:"policyCacheTtl,omitempty"`

	// LeaseTTL bounds crash leakage for a concurrency member and must use
	// whole milliseconds.
	// +optional
	// +kubebuilder:default="15s"
	LeaseTTL metav1.Duration `json:"leaseTtl,omitempty"`

	// RenewInterval is the heartbeat period for an active concurrency lease
	// and must use whole milliseconds.
	// +optional
	// +kubebuilder:default="5s"
	RenewInterval metav1.Duration `json:"renewInterval,omitempty"`
}

// TeamQuotaStatus contains the create-once identity issued by a region policy
// owner for its PostgreSQL and Redis Team Quota state plane.
type TeamQuotaStatus struct {
	// StateID is generated once by the operator, or initialized from the
	// owner's explicit recovery value, and is copied to every consumer-only
	// resource in the region.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
	StateID string `json:"stateId"`
}
