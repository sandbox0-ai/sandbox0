// Package model defines runtime-neutral inputs to the node network policy
// compiler.
package model

// SandboxInfo is one exact runtime-slot network incarnation.
type SandboxInfo struct {
	Scope             string
	Name              string
	IncarnationID     string
	Revision          string
	SourceIP          string
	NodeID            string
	SandboxID         string
	TeamID            string
	OwnerKind         string
	NetworkPolicy     string
	NetworkPolicyHash string
}

// Key returns the stable registry key for this runtime slot.
func (s *SandboxInfo) Key() string {
	if s == nil {
		return ""
	}
	return s.Scope + "/" + s.Name
}
