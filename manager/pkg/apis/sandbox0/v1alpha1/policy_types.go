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

// NetworkPolicyRequiresSynchronousApply reports whether a claim must wait for
// the network runtime to acknowledge the policy. Unrestricted policies are
// still applied asynchronously and retain their desired and applied hashes.
func NetworkPolicyRequiresSynchronousApply(spec *NetworkPolicySpec) bool {
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
