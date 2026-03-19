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
	SourceID     int64                      `json:"sourceId"`
	Version      int64                      `json:"version"`
	ResolverKind string                     `json:"resolverKind"`
	Spec         CredentialSourceSecretSpec `json:"spec"`
	CreatedAt    time.Time                  `json:"createdAt,omitempty"`
}

// CredentialSourceSecretSpec is the typed source config stored in PostgreSQL.
type CredentialSourceSecretSpec struct {
	StaticHeaders              *StaticHeadersSourceSpec              `json:"staticHeaders,omitempty"`
	StaticTLSClientCertificate *StaticTLSClientCertificateSourceSpec `json:"staticTLSClientCertificate,omitempty"`
	StaticUsernamePassword     *StaticUsernamePasswordSourceSpec     `json:"staticUsernamePassword,omitempty"`
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

// CredentialSourceWriteRequest is the secret-bearing public write model.
type CredentialSourceWriteRequest struct {
	Name         string                     `json:"name"`
	ResolverKind string                     `json:"resolverKind"`
	Spec         CredentialSourceSecretSpec `json:"spec"`
}

// CredentialSourceMetadata is the public metadata view of one source.
type CredentialSourceMetadata struct {
	Name           string    `json:"name"`
	ResolverKind   string    `json:"resolverKind"`
	CurrentVersion int64     `json:"currentVersion"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"createdAt,omitempty"`
	UpdatedAt      time.Time `json:"updatedAt,omitempty"`
}
