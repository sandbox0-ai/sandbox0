package v1alpha1

// NetworkPolicySpec defines network policy for a sandbox (stored in pod annotation)
type NetworkPolicySpec struct {
	// Version identifies the policy schema version
	Version string `json:"version,omitempty"`

	// SandboxID is the unique identifier of the sandbox this policy applies to
	SandboxID string `json:"sandboxId"`

	// TeamID is the team that owns this sandbox
	TeamID string `json:"teamId"`

	// Egress defines outbound traffic rules (default deny)
	Egress *EgressPolicySpec `json:"egress,omitempty"`

	// Ingress defines inbound traffic rules (default deny)
	Ingress *IngressPolicySpec `json:"ingress,omitempty"`
}

// EgressPolicySpec defines egress policy specification
type EgressPolicySpec struct {
	// DefaultAction is the default action for egress traffic (deny or allow)
	DefaultAction string `json:"defaultAction,omitempty"`

	// AllowedCIDRs is a list of allowed destination CIDRs
	AllowedCIDRs []string `json:"allowedCidrs,omitempty"`

	// DeniedCIDRs is a list of denied destination CIDRs
	DeniedCIDRs []string `json:"deniedCidrs,omitempty"`

	// AllowedPorts is a list of allowed destination ports (egress)
	AllowedPorts []PortSpec `json:"allowedPorts,omitempty"`

	// DeniedPorts is a list of denied destination ports (egress)
	DeniedPorts []PortSpec `json:"deniedPorts,omitempty"`

	// AllowedDomains is a list of allowed destination domains (supports wildcards like *.example.com)
	AllowedDomains []string `json:"allowedDomains,omitempty"`

	// DeniedDomains is a list of denied destination domains (supports wildcards like *.example.com)
	DeniedDomains []string `json:"deniedDomains,omitempty"`

	// AlwaysDeniedCIDRs are platform-enforced deny CIDRs (user cannot override)
	// This includes RFC1918, metadata services, cluster services, etc.
	AlwaysDeniedCIDRs []string `json:"alwaysDeniedCidrs,omitempty"`
}

// IngressPolicySpec defines ingress policy specification
type IngressPolicySpec struct {
	// DefaultAction is the default action for ingress traffic (deny or allow)
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
	Protocol string `json:"protocol,omitempty"`

	// EndPort for port ranges (optional)
	EndPort *int32 `json:"endPort,omitempty"`
}

// BandwidthPolicySpec defines bandwidth policy for a sandbox (stored in pod annotation)
type BandwidthPolicySpec struct {
	// Version identifies the policy schema version
	Version string `json:"version,omitempty"`

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
	Enabled bool `json:"enabled"`

	// ReportIntervalSeconds is the interval for reporting traffic statistics
	// Fixed at 10 seconds per platform policy
	ReportIntervalSeconds int32 `json:"reportIntervalSeconds,omitempty"`
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
