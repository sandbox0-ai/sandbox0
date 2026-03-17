package egressauth

import "time"

// CredentialBinding describes how a credential reference should be resolved.
// Phase 1/2 keeps the payload generic enough for future providers while still
// supporting the current static header injection path.
type CredentialBinding struct {
	Ref        string                `json:"ref"`
	Provider   string                `json:"provider,omitempty"`
	Headers    map[string]string     `json:"headers,omitempty"`
	Config     map[string]string     `json:"config,omitempty"`
	SecretRefs []CredentialSecretRef `json:"secretRefs,omitempty"`
	SourceRef  *CredentialSourceRef  `json:"sourceRef,omitempty"`
}

// CredentialSecretRef identifies a secret-backed input used by a binding.
type CredentialSecretRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	Key       string `json:"key"`
}

// CredentialSourceRef points to an external logical source for a binding.
type CredentialSourceRef struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// BindingRecord stores the effective bindings for one sandbox in one cluster.
type BindingRecord struct {
	ClusterID string              `json:"clusterId"`
	SandboxID string              `json:"sandboxId"`
	TeamID    string              `json:"teamId,omitempty"`
	Bindings  []CredentialBinding `json:"bindings,omitempty"`
	UpdatedAt time.Time           `json:"updatedAt,omitempty"`
}
