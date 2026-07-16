package egressauth

import (
	"encoding/json"
	"time"
)

// ResolveRequest describes an auth material lookup for a matched egress auth rule.
type ResolveRequest struct {
	SandboxID       string `json:"sandboxId"`
	TeamID          string `json:"teamId,omitempty"`
	AuthRef         string `json:"authRef"`
	RuleName        string `json:"ruleName,omitempty"`
	Destination     string `json:"destination,omitempty"`
	DestinationPort int    `json:"destinationPort,omitempty"`
	Transport       string `json:"transport,omitempty"`
	Protocol        string `json:"protocol,omitempty"`
}

// ResolveResponse describes the resolved outbound auth material.
type ResolveResponse struct {
	AuthRef    string             `json:"authRef"`
	Directives []ResolveDirective `json:"directives,omitempty"`
	ExpiresAt  *time.Time         `json:"expiresAt,omitempty"`
	Source     *ResolveSource     `json:"source,omitempty"`

	// Headers is an in-memory compatibility view derived from `directives`.
	// It is intentionally excluded from the wire format so the broker protocol
	// can move to typed directives before the ctld network runtime adapters are rewritten.
	Headers map[string]string `json:"-"`
}

// ResolveSource identifies the source version used to resolve runtime auth
// material. It intentionally contains no secret material.
type ResolveSource struct {
	TeamID        string `json:"teamId,omitempty"`
	SourceRef     string `json:"sourceRef,omitempty"`
	SourceID      int64  `json:"sourceId,omitempty"`
	SourceVersion int64  `json:"sourceVersion,omitempty"`
}

type ResolveDirectiveKind string

const (
	ResolveDirectiveKindHTTPHeaders             ResolveDirectiveKind = "http_headers"
	ResolveDirectiveKindPlaceholderSubstitution ResolveDirectiveKind = "placeholder_substitution"
	ResolveDirectiveKindGRPCMetadata            ResolveDirectiveKind = "grpc_metadata"
	ResolveDirectiveKindTLSClientCertificate    ResolveDirectiveKind = "tls_client_certificate"
	ResolveDirectiveKindUsernamePassword        ResolveDirectiveKind = "username_password"
	ResolveDirectiveKindSSHProxy                ResolveDirectiveKind = "ssh_proxy"
	ResolveDirectiveKindSSHAgentSign            ResolveDirectiveKind = "ssh_agent_sign"
	ResolveDirectiveKindCustom                  ResolveDirectiveKind = "custom"
)

// ResolveDirective is a typed outbound auth directive.
type ResolveDirective struct {
	Kind                    ResolveDirectiveKind              `json:"kind"`
	HTTPHeaders             *HTTPHeadersDirective             `json:"httpHeaders,omitempty"`
	PlaceholderSubstitution *PlaceholderSubstitutionDirective `json:"placeholderSubstitution,omitempty"`
	TLSClientCertificate    *TLSClientCertificateDirective    `json:"tlsClientCertificate,omitempty"`
	UsernamePassword        *UsernamePasswordDirective        `json:"usernamePassword,omitempty"`
	SSHProxy                *SSHProxyDirective                `json:"sshProxy,omitempty"`
}

// HTTPHeadersDirective injects HTTP headers into a matching request.
type HTTPHeadersDirective struct {
	Headers map[string]string `json:"headers,omitempty"`
}

// PlaceholderSubstitutionDirective replaces placeholders in outbound HTTP requests.
type PlaceholderSubstitutionDirective struct {
	Replacements []PlaceholderSubstitutionReplacement `json:"replacements,omitempty"`
}

// PlaceholderSubstitutionReplacement is one resolved placeholder replacement.
type PlaceholderSubstitutionReplacement struct {
	Placeholder string                            `json:"placeholder"`
	Value       string                            `json:"value"`
	Locations   []PlaceholderSubstitutionLocation `json:"locations,omitempty"`
}

// TLSClientCertificateDirective configures one upstream mTLS client certificate.
type TLSClientCertificateDirective struct {
	CertificatePEM string `json:"certificatePem,omitempty"`
	PrivateKeyPEM  string `json:"privateKeyPem,omitempty"`
	CAPEM          string `json:"caPem,omitempty"`
}

// UsernamePasswordDirective injects one username/password pair into a bounded auth exchange.
type UsernamePasswordDirective struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// SSHProxyDirective configures transparent SSH proxy authentication.
type SSHProxyDirective struct {
	SandboxPublicKeys []string `json:"sandboxPublicKeys,omitempty"`
	UpstreamUsername  string   `json:"upstreamUsername,omitempty"`
	PrivateKeyPEM     string   `json:"privateKeyPem,omitempty"`
	Passphrase        string   `json:"passphrase,omitempty"`
	KnownHosts        []string `json:"knownHosts,omitempty"`
}

type resolveResponseWire struct {
	AuthRef    string             `json:"authRef"`
	Directives []ResolveDirective `json:"directives,omitempty"`
	Headers    map[string]string  `json:"headers,omitempty"`
	ExpiresAt  *time.Time         `json:"expiresAt,omitempty"`
	Source     *ResolveSource     `json:"source,omitempty"`
}

// NewHTTPHeadersResolveResponse constructs the first typed directive response
// supported by the Phase 4 wire model.
func NewHTTPHeadersResolveResponse(authRef string, headers map[string]string, expiresAt *time.Time) *ResolveResponse {
	resp := &ResolveResponse{
		AuthRef: authRef,
	}
	if len(headers) > 0 {
		resp.Directives = []ResolveDirective{{
			Kind: ResolveDirectiveKindHTTPHeaders,
			HTTPHeaders: &HTTPHeadersDirective{
				Headers: cloneStringMap(headers),
			},
		}}
	}
	if expiresAt != nil {
		expiresCopy := *expiresAt
		resp.ExpiresAt = &expiresCopy
	}
	resp.EnsureCompatibilityFields()
	return resp
}

// NewPlaceholderSubstitutionResolveResponse constructs a typed placeholder substitution response.
func NewPlaceholderSubstitutionResolveResponse(authRef string, directive *PlaceholderSubstitutionDirective, expiresAt *time.Time) *ResolveResponse {
	resp := &ResolveResponse{
		AuthRef: authRef,
	}
	if directive != nil {
		resp.Directives = []ResolveDirective{{
			Kind: ResolveDirectiveKindPlaceholderSubstitution,
			PlaceholderSubstitution: &PlaceholderSubstitutionDirective{
				Replacements: clonePlaceholderSubstitutionReplacements(directive.Replacements),
			},
		}}
	}
	if expiresAt != nil {
		expiresCopy := *expiresAt
		resp.ExpiresAt = &expiresCopy
	}
	resp.EnsureCompatibilityFields()
	return resp
}

// NewTLSClientCertificateResolveResponse constructs a typed TLS client certificate response.
func NewTLSClientCertificateResolveResponse(authRef string, directive *TLSClientCertificateDirective, expiresAt *time.Time) *ResolveResponse {
	resp := &ResolveResponse{
		AuthRef: authRef,
	}
	if directive != nil {
		resp.Directives = []ResolveDirective{{
			Kind: ResolveDirectiveKindTLSClientCertificate,
			TLSClientCertificate: &TLSClientCertificateDirective{
				CertificatePEM: directive.CertificatePEM,
				PrivateKeyPEM:  directive.PrivateKeyPEM,
				CAPEM:          directive.CAPEM,
			},
		}}
	}
	if expiresAt != nil {
		expiresCopy := *expiresAt
		resp.ExpiresAt = &expiresCopy
	}
	resp.EnsureCompatibilityFields()
	return resp
}

// NewUsernamePasswordResolveResponse constructs a typed username/password response.
func NewUsernamePasswordResolveResponse(authRef string, directive *UsernamePasswordDirective, expiresAt *time.Time) *ResolveResponse {
	resp := &ResolveResponse{
		AuthRef: authRef,
	}
	if directive != nil {
		resp.Directives = []ResolveDirective{{
			Kind: ResolveDirectiveKindUsernamePassword,
			UsernamePassword: &UsernamePasswordDirective{
				Username: directive.Username,
				Password: directive.Password,
			},
		}}
	}
	if expiresAt != nil {
		expiresCopy := *expiresAt
		resp.ExpiresAt = &expiresCopy
	}
	resp.EnsureCompatibilityFields()
	return resp
}

// NewSSHProxyResolveResponse constructs a typed transparent SSH proxy response.
func NewSSHProxyResolveResponse(authRef string, directive *SSHProxyDirective, expiresAt *time.Time) *ResolveResponse {
	resp := &ResolveResponse{
		AuthRef: authRef,
	}
	if directive != nil {
		resp.Directives = []ResolveDirective{{
			Kind: ResolveDirectiveKindSSHProxy,
			SSHProxy: &SSHProxyDirective{
				SandboxPublicKeys: append([]string(nil), directive.SandboxPublicKeys...),
				UpstreamUsername:  directive.UpstreamUsername,
				PrivateKeyPEM:     directive.PrivateKeyPEM,
				Passphrase:        directive.Passphrase,
				KnownHosts:        append([]string(nil), directive.KnownHosts...),
			},
		}}
	}
	if expiresAt != nil {
		expiresCopy := *expiresAt
		resp.ExpiresAt = &expiresCopy
	}
	resp.EnsureCompatibilityFields()
	return resp
}

// EnsureCompatibilityFields keeps in-memory compatibility fields consistent.
func (r *ResolveResponse) EnsureCompatibilityFields() {
	if r == nil {
		return
	}
	if len(r.Directives) == 0 && len(r.Headers) > 0 {
		r.Directives = []ResolveDirective{{
			Kind: ResolveDirectiveKindHTTPHeaders,
			HTTPHeaders: &HTTPHeadersDirective{
				Headers: cloneStringMap(r.Headers),
			},
		}}
	}
	r.Headers = extractHTTPHeaders(r.Directives)
}

// MarshalJSON emits only the typed directive wire model.
func (r ResolveResponse) MarshalJSON() ([]byte, error) {
	clone := CloneResolveResponse(&r)
	wire := resolveResponseWire{
		AuthRef:    clone.AuthRef,
		Directives: clone.Directives,
		ExpiresAt:  clone.ExpiresAt,
		Source:     cloneResolveSource(clone.Source),
	}
	return json.Marshal(wire)
}

// UnmarshalJSON accepts typed directives and upgrades any legacy `headers`
// payload into the new directive representation.
func (r *ResolveResponse) UnmarshalJSON(data []byte) error {
	var wire resolveResponseWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	r.AuthRef = wire.AuthRef
	r.Directives = cloneDirectives(wire.Directives)
	r.Headers = cloneStringMap(wire.Headers)
	if wire.ExpiresAt != nil {
		expiresCopy := *wire.ExpiresAt
		r.ExpiresAt = &expiresCopy
	} else {
		r.ExpiresAt = nil
	}
	r.Source = cloneResolveSource(wire.Source)
	r.EnsureCompatibilityFields()
	return nil
}

// CloneResolveResponse deep-copies one resolved response.
func CloneResolveResponse(in *ResolveResponse) *ResolveResponse {
	if in == nil {
		return nil
	}
	out := &ResolveResponse{
		AuthRef:    in.AuthRef,
		Directives: cloneDirectives(in.Directives),
		Headers:    cloneStringMap(in.Headers),
		Source:     cloneResolveSource(in.Source),
	}
	if in.ExpiresAt != nil {
		expiresCopy := *in.ExpiresAt
		out.ExpiresAt = &expiresCopy
	}
	out.EnsureCompatibilityFields()
	return out
}

func cloneResolveSource(in *ResolveSource) *ResolveSource {
	if in == nil {
		return nil
	}
	return &ResolveSource{
		TeamID:        in.TeamID,
		SourceRef:     in.SourceRef,
		SourceID:      in.SourceID,
		SourceVersion: in.SourceVersion,
	}
}

func extractHTTPHeaders(directives []ResolveDirective) map[string]string {
	if len(directives) == 0 {
		return nil
	}
	merged := map[string]string{}
	for _, directive := range directives {
		if directive.Kind != ResolveDirectiveKindHTTPHeaders || directive.HTTPHeaders == nil {
			continue
		}
		for key, value := range directive.HTTPHeaders.Headers {
			merged[key] = value
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func cloneDirectives(in []ResolveDirective) []ResolveDirective {
	if len(in) == 0 {
		return nil
	}
	out := make([]ResolveDirective, 0, len(in))
	for _, directive := range in {
		cloned := ResolveDirective{
			Kind: directive.Kind,
		}
		if directive.HTTPHeaders != nil {
			cloned.HTTPHeaders = &HTTPHeadersDirective{
				Headers: cloneStringMap(directive.HTTPHeaders.Headers),
			}
		}
		if directive.PlaceholderSubstitution != nil {
			cloned.PlaceholderSubstitution = &PlaceholderSubstitutionDirective{
				Replacements: clonePlaceholderSubstitutionReplacements(directive.PlaceholderSubstitution.Replacements),
			}
		}
		if directive.TLSClientCertificate != nil {
			cloned.TLSClientCertificate = &TLSClientCertificateDirective{
				CertificatePEM: directive.TLSClientCertificate.CertificatePEM,
				PrivateKeyPEM:  directive.TLSClientCertificate.PrivateKeyPEM,
				CAPEM:          directive.TLSClientCertificate.CAPEM,
			}
		}
		if directive.UsernamePassword != nil {
			cloned.UsernamePassword = &UsernamePasswordDirective{
				Username: directive.UsernamePassword.Username,
				Password: directive.UsernamePassword.Password,
			}
		}
		if directive.SSHProxy != nil {
			cloned.SSHProxy = &SSHProxyDirective{
				SandboxPublicKeys: append([]string(nil), directive.SSHProxy.SandboxPublicKeys...),
				UpstreamUsername:  directive.SSHProxy.UpstreamUsername,
				PrivateKeyPEM:     directive.SSHProxy.PrivateKeyPEM,
				Passphrase:        directive.SSHProxy.Passphrase,
				KnownHosts:        append([]string(nil), directive.SSHProxy.KnownHosts...),
			}
		}
		out = append(out, cloned)
	}
	return out
}

func clonePlaceholderSubstitutionReplacements(in []PlaceholderSubstitutionReplacement) []PlaceholderSubstitutionReplacement {
	if len(in) == 0 {
		return nil
	}
	out := make([]PlaceholderSubstitutionReplacement, 0, len(in))
	for _, replacement := range in {
		out = append(out, PlaceholderSubstitutionReplacement{
			Placeholder: replacement.Placeholder,
			Value:       replacement.Value,
			Locations:   append([]PlaceholderSubstitutionLocation(nil), replacement.Locations...),
		})
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
