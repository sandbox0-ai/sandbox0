package egressauth

import "time"

// CredentialBinding stores one effective sandbox binding materialized by manager.
type CredentialBinding struct {
	Ref           string           `json:"ref"`
	SourceRef     string           `json:"sourceRef"`
	SourceID      int64            `json:"sourceId,omitempty"`
	SourceVersion int64            `json:"sourceVersion,omitempty"`
	Projection    ProjectionSpec   `json:"projection"`
	CachePolicy   *CachePolicySpec `json:"cachePolicy,omitempty"`
}

// ProjectionSpec defines how resolved source data should be projected into runtime directives.
type ProjectionSpec struct {
	Type                 CredentialProjectionType        `json:"type"`
	HTTPHeaders          *HTTPHeadersProjection          `json:"httpHeaders,omitempty"`
	TLSClientCertificate *TLSClientCertificateProjection `json:"tlsClientCertificate,omitempty"`
	UsernamePassword     *UsernamePasswordProjection     `json:"usernamePassword,omitempty"`
}

// CredentialProjectionType identifies the runtime projection shape.
type CredentialProjectionType string

const (
	CredentialProjectionTypeHTTPHeaders          CredentialProjectionType = "http_headers"
	CredentialProjectionTypeTLSClientCertificate CredentialProjectionType = "tls_client_certificate"
	CredentialProjectionTypeUsernamePassword     CredentialProjectionType = "username_password"
)

// HTTPHeadersProjection injects HTTP headers derived from source data.
type HTTPHeadersProjection struct {
	Headers []ProjectedHeader `json:"headers,omitempty"`
}

// TLSClientCertificateProjection projects one client certificate for TLS re-origination.
type TLSClientCertificateProjection struct{}

// UsernamePasswordProjection projects one username/password pair into an early auth exchange.
type UsernamePasswordProjection struct{}

// ProjectedHeader defines one projected header template.
type ProjectedHeader struct {
	Name          string `json:"name"`
	ValueTemplate string `json:"valueTemplate"`
}

// CachePolicySpec controls broker-side caching for one binding.
type CachePolicySpec struct {
	TTL string `json:"ttl,omitempty"`
}

// BindingRecord stores the effective bindings for one sandbox in one cluster.
type BindingRecord struct {
	ClusterID string              `json:"clusterId"`
	SandboxID string              `json:"sandboxId"`
	TeamID    string              `json:"teamId,omitempty"`
	Bindings  []CredentialBinding `json:"bindings,omitempty"`
	UpdatedAt time.Time           `json:"updatedAt,omitempty"`
}
