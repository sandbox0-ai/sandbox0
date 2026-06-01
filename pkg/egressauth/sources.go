package egressauth

import "time"

// CredentialSource identifies one region-scoped credential source.
type CredentialSource struct {
	ID             int64     `json:"id"`
	TeamID         string    `json:"teamId"`
	Name           string    `json:"name"`
	ResolverKind   string    `json:"resolverKind"`
	CurrentVersion int64     `json:"currentVersion"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"createdAt,omitempty"`
	UpdatedAt      time.Time `json:"updatedAt,omitempty"`
}

// CredentialSourceVersion stores one versioned resolver config.
type CredentialSourceVersion struct {
	SourceID     int64                            `json:"sourceId"`
	TeamID       string                           `json:"teamId,omitempty"`
	Version      int64                            `json:"version"`
	ResolverKind string                           `json:"resolverKind"`
	StorageKind  string                           `json:"storageKind,omitempty"`
	Spec         CredentialSourceSecretSpec       `json:"spec"`
	ExternalRef  *CredentialSourceExternalRefSpec `json:"externalRef,omitempty"`
	CreatedAt    time.Time                        `json:"createdAt,omitempty"`
}

const (
	CredentialSourceStorageKindEncryptedPG    = "encrypted_pg"
	CredentialSourceStorageKindHashiCorpVault = "hashicorp_vault"
	CredentialSourceStorageKindExternalRef    = "external_ref"
	CredentialSourceStorageKindPlaintextPG    = "plaintext_pg"

	CredentialSourceExternalProviderHashiCorpVault = "hashicorp_vault"
)

// CredentialSourceSecretSpec is the typed source config resolved for runtime use.
type CredentialSourceSecretSpec struct {
	StaticHeaders              *StaticHeadersSourceSpec              `json:"staticHeaders,omitempty"`
	StaticTLSClientCertificate *StaticTLSClientCertificateSourceSpec `json:"staticTLSClientCertificate,omitempty"`
	StaticUsernamePassword     *StaticUsernamePasswordSourceSpec     `json:"staticUsernamePassword,omitempty"`
	StaticSSHPrivateKey        *StaticSSHPrivateKeySourceSpec        `json:"staticSSHPrivateKey,omitempty"`
}

// StaticHeadersSourceSpec stores named values used by header projections.
type StaticHeadersSourceSpec struct {
	Values map[string]string `json:"values,omitempty"`
}

// StaticTLSClientCertificateSourceSpec stores a PEM-encoded client certificate bundle.
type StaticTLSClientCertificateSourceSpec struct {
	CertificatePEM string `json:"certificatePem,omitempty"`
	PrivateKeyPEM  string `json:"privateKeyPem,omitempty"`
	CAPEM          string `json:"caPem,omitempty"`
}

// StaticUsernamePasswordSourceSpec stores one username/password pair for early protocol auth.
type StaticUsernamePasswordSourceSpec struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// StaticSSHPrivateKeySourceSpec stores one PEM-encoded SSH private key.
type StaticSSHPrivateKeySourceSpec struct {
	PrivateKeyPEM string `json:"privateKeyPem,omitempty"`
	Passphrase    string `json:"passphrase,omitempty"`
}

// CredentialSourceWriteRequest is the secret-bearing public write model.
type CredentialSourceWriteRequest struct {
	Name         string                           `json:"name"`
	ResolverKind string                           `json:"resolverKind"`
	StorageKind  string                           `json:"storageKind,omitempty"`
	Spec         CredentialSourceSecretSpec       `json:"spec"`
	ExternalRef  *CredentialSourceExternalRefSpec `json:"externalRef,omitempty"`
}

// CredentialSourceMetadata is the public metadata view of one source.
type CredentialSourceMetadata struct {
	Name           string    `json:"name"`
	ResolverKind   string    `json:"resolverKind"`
	StorageKind    string    `json:"-"`
	CurrentVersion int64     `json:"currentVersion"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"createdAt,omitempty"`
	UpdatedAt      time.Time `json:"updatedAt,omitempty"`
}

// CredentialSourceExternalRefSpec points at secret material held in a Vault-compatible backend.
type CredentialSourceExternalRefSpec struct {
	Provider   string            `json:"provider"`
	Connection string            `json:"connection,omitempty"`
	Mount      string            `json:"mount,omitempty"`
	Path       string            `json:"path"`
	Version    string            `json:"version,omitempty"`
	Fields     map[string]string `json:"fields,omitempty"`
}
