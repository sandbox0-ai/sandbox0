package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SandboxNetworkPolicy defines network policy for a sandbox (instance-level)
type SandboxNetworkPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxNetworkPolicySpec   `json:"spec"`
	Status SandboxNetworkPolicyStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SandboxNetworkPolicyList contains a list of SandboxNetworkPolicy
type SandboxNetworkPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxNetworkPolicy `json:"items"`
}

// SandboxNetworkPolicySpec defines the desired state of SandboxNetworkPolicy
type SandboxNetworkPolicySpec struct {
	// SandboxID is the unique identifier of the sandbox this policy applies to
	SandboxID string `json:"sandboxId"`

	// TeamID is the team that owns this sandbox
	TeamID string `json:"teamId"`

	// Egress defines outbound traffic rules (default deny)
	Egress *EgressPolicySpec `json:"egress,omitempty"`

	// Ingress defines inbound traffic rules (default deny)
	Ingress *IngressPolicySpec `json:"ingress,omitempty"`

	// Audit defines audit configuration
	Audit *AuditSpec `json:"audit,omitempty"`
}

// EgressPolicySpec defines egress policy specification
type EgressPolicySpec struct {
	// DefaultAction is the default action for egress traffic (deny or allow)
	// +kubebuilder:validation:Enum=deny;allow
	// +kubebuilder:default=deny
	DefaultAction string `json:"defaultAction,omitempty"`

	// AllowedCIDRs is a list of allowed destination CIDRs
	AllowedCIDRs []string `json:"allowedCidrs,omitempty"`

	// DeniedCIDRs is a list of denied destination CIDRs
	DeniedCIDRs []string `json:"deniedCidrs,omitempty"`

	// AllowedDomains is a list of allowed destination domains (supports wildcards like *.example.com)
	AllowedDomains []string `json:"allowedDomains,omitempty"`

	// DeniedDomains is a list of denied destination domains
	DeniedDomains []string `json:"deniedDomains,omitempty"`

	// EnforceProxyPorts are ports that must go through the L7 proxy (e.g., 80, 443)
	EnforceProxyPorts []int32 `json:"enforceProxyPorts,omitempty"`

	// DNSPolicy defines DNS access policy
	DNSPolicy *DNSPolicySpec `json:"dnsPolicy,omitempty"`

	// AlwaysDeniedCIDRs are platform-enforced deny CIDRs (user cannot override)
	// This includes RFC1918, metadata services, cluster services, etc.
	AlwaysDeniedCIDRs []string `json:"alwaysDeniedCidrs,omitempty"`
}

// IngressPolicySpec defines ingress policy specification
type IngressPolicySpec struct {
	// DefaultAction is the default action for ingress traffic (deny or allow)
	// +kubebuilder:validation:Enum=deny;allow
	// +kubebuilder:default=deny
	DefaultAction string `json:"defaultAction,omitempty"`

	// AllowedSourceCIDRs is a list of allowed source CIDRs
	AllowedSourceCIDRs []string `json:"allowedSourceCidrs,omitempty"`

	// DeniedSourceCIDRs is a list of denied source CIDRs
	DeniedSourceCIDRs []string `json:"deniedSourceCidrs,omitempty"`

	// AllowedPorts is a list of allowed destination ports (ports on the sandbox)
	AllowedPorts []PortSpec `json:"allowedPorts,omitempty"`
}

// PortSpec defines a port specification
type PortSpec struct {
	// Port number
	Port int32 `json:"port"`

	// Protocol (tcp or udp)
	// +kubebuilder:validation:Enum=tcp;udp
	// +kubebuilder:default=tcp
	Protocol string `json:"protocol,omitempty"`

	// EndPort for port ranges (optional)
	EndPort *int32 `json:"endPort,omitempty"`
}

// DNSPolicySpec defines DNS access policy
type DNSPolicySpec struct {
	// AllowedServers is a list of allowed DNS servers
	// If empty, defaults to cluster DNS
	AllowedServers []string `json:"allowedServers,omitempty"`

	// AllowDoH allows DNS over HTTPS
	AllowDoH bool `json:"allowDoH,omitempty"`

	// AllowDoT allows DNS over TLS
	AllowDoT bool `json:"allowDoT,omitempty"`

	// AllowQUIC allows DNS over QUIC
	AllowQUIC bool `json:"allowQuic,omitempty"`
}

// AuditSpec defines audit configuration
type AuditSpec struct {
	// Level is the audit level (off, basic, full)
	// +kubebuilder:validation:Enum=off;basic;full
	// +kubebuilder:default=basic
	Level string `json:"level,omitempty"`

	// SampleRate is the sampling rate for audit logs (0.0-1.0)
	// +kubebuilder:validation:Pattern=`^(0(\.\d+)?|1(\.0+)?)$`
	// +kubebuilder:default=1.0
	SampleRate string `json:"sampleRate,omitempty"`
}

// SandboxNetworkPolicyStatus defines the observed state of SandboxNetworkPolicy
type SandboxNetworkPolicyStatus struct {
	// ObservedGeneration is the generation observed by netd
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastAppliedTime is the last time the policy was applied
	LastAppliedTime metav1.Time `json:"lastAppliedTime,omitempty"`

	// Phase is the current phase of the policy
	// +kubebuilder:validation:Enum=Pending;Applied;Failed
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the status
	Message string `json:"message,omitempty"`

	// Conditions represent the latest available observations
	Conditions []NetworkPolicyCondition `json:"conditions,omitempty"`
}

// NetworkPolicyCondition defines a condition of the network policy
type NetworkPolicyCondition struct {
	Type               string      `json:"type"`
	Status             string      `json:"status"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	Reason             string      `json:"reason,omitempty"`
	Message            string      `json:"message,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SandboxBandwidthPolicy defines bandwidth policy for a sandbox
type SandboxBandwidthPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxBandwidthPolicySpec   `json:"spec"`
	Status SandboxBandwidthPolicyStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SandboxBandwidthPolicyList contains a list of SandboxBandwidthPolicy
type SandboxBandwidthPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxBandwidthPolicy `json:"items"`
}

// SandboxBandwidthPolicySpec defines the desired state of SandboxBandwidthPolicy
type SandboxBandwidthPolicySpec struct {
	// SandboxID is the unique identifier of the sandbox this policy applies to
	SandboxID string `json:"sandboxId"`

	// TeamID is the team that owns this sandbox
	TeamID string `json:"teamId"`

	// EgressRateLimit defines egress rate limiting
	EgressRateLimit *RateLimitSpec `json:"egressRateLimit,omitempty"`

	// IngressRateLimit defines ingress rate limiting
	IngressRateLimit *RateLimitSpec `json:"ingressRateLimit,omitempty"`

	// Accounting defines traffic accounting configuration
	Accounting *AccountingSpec `json:"accounting,omitempty"`
}

// RateLimitSpec defines rate limiting specification
type RateLimitSpec struct {
	// RateBps is the rate limit in bits per second
	RateBps int64 `json:"rateBps"`

	// BurstBytes is the burst size in bytes
	BurstBytes int64 `json:"burstBytes,omitempty"`

	// CeilBps is the ceiling rate in bits per second (for HTB)
	CeilBps int64 `json:"ceilBps,omitempty"`
}

// AccountingSpec defines traffic accounting configuration
type AccountingSpec struct {
	// Enabled enables traffic accounting
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// ReportIntervalSeconds is the interval for reporting traffic statistics
	// Fixed at 10 seconds per platform policy
	// +kubebuilder:default=10
	ReportIntervalSeconds int32 `json:"reportIntervalSeconds,omitempty"`
}

// SandboxBandwidthPolicyStatus defines the observed state of SandboxBandwidthPolicy
type SandboxBandwidthPolicyStatus struct {
	// ObservedGeneration is the generation observed by netd
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastAppliedTime is the last time the policy was applied
	LastAppliedTime metav1.Time `json:"lastAppliedTime,omitempty"`

	// Phase is the current phase of the policy
	// +kubebuilder:validation:Enum=Pending;Applied;Failed
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the status
	Message string `json:"message,omitempty"`

	// CurrentStats contains current bandwidth statistics
	CurrentStats *BandwidthStats `json:"currentStats,omitempty"`
}

// BandwidthStats contains bandwidth usage statistics
type BandwidthStats struct {
	// EgressBytes is the total egress bytes
	EgressBytes int64 `json:"egressBytes,omitempty"`

	// IngressBytes is the total ingress bytes
	IngressBytes int64 `json:"ingressBytes,omitempty"`

	// EgressPackets is the total egress packets
	EgressPackets int64 `json:"egressPackets,omitempty"`

	// IngressPackets is the total ingress packets
	IngressPackets int64 `json:"ingressPackets,omitempty"`

	// ConnectionCount is the total number of connections
	ConnectionCount int64 `json:"connectionCount,omitempty"`

	// LastUpdated is the last time stats were updated
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`
}

// Default platform-enforced deny CIDRs
var PlatformDeniedCIDRs = []string{
	"10.0.0.0/8",         // RFC1918 private
	"172.16.0.0/12",      // RFC1918 private
	"192.168.0.0/16",     // RFC1918 private
	"127.0.0.0/8",        // Loopback
	"169.254.0.0/16",     // Link-local
	"169.254.169.254/32", // Cloud metadata service
	"fc00::/7",           // IPv6 unique local
	"fe80::/10",          // IPv6 link-local
}
