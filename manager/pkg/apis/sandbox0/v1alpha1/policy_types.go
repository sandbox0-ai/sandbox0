package v1alpha1

// NetworkPolicySpec defines network policy for a sandbox (stored in pod annotation)
type NetworkPolicySpec struct {
	// Version identifies the policy schema version
	Version string `json:"version,omitempty"`

	// SandboxID is the unique identifier of the sandbox this policy applies to
	SandboxID string `json:"sandboxId"`

	// TeamID is the team that owns this sandbox
	TeamID string `json:"teamId"`

	// Mode controls the baseline policy for egress
	Mode NetworkPolicyMode `json:"mode"`

	// Egress defines outbound traffic rules
	Egress *NetworkEgressPolicy `json:"egress,omitempty"`
}

// NetworkPolicyRequiresApply reports whether the network runtime must
// acknowledge this policy before the sandbox can be used. The default
// unrestricted policy has no enforcement state to install.
func NetworkPolicyRequiresApply(spec *NetworkPolicySpec) bool {
	if spec == nil {
		return false
	}
	mode := spec.Mode
	if mode == "" {
		mode = NetworkModeAllowAll
	}
	return mode != NetworkModeAllowAll || spec.Egress != nil
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
