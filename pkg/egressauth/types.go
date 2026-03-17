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

	// Headers is an in-memory compatibility view derived from `directives`.
	// It is intentionally excluded from the wire format so the broker protocol
	// can move to typed directives before netd adapters are rewritten.
	Headers map[string]string `json:"-"`
}

type ResolveDirectiveKind string

const (
	ResolveDirectiveKindHTTPHeaders         ResolveDirectiveKind = "http_headers"
	ResolveDirectiveKindGRPCMetadata        ResolveDirectiveKind = "grpc_metadata"
	ResolveDirectiveKindTLSClientCertificate ResolveDirectiveKind = "tls_client_certificate"
	ResolveDirectiveKindSSHAgentSign        ResolveDirectiveKind = "ssh_agent_sign"
	ResolveDirectiveKindCustom              ResolveDirectiveKind = "custom"
)

// ResolveDirective is a typed outbound auth directive.
type ResolveDirective struct {
	Kind        ResolveDirectiveKind  `json:"kind"`
	HTTPHeaders *HTTPHeadersDirective `json:"httpHeaders,omitempty"`
}

// HTTPHeadersDirective injects HTTP headers into a matching request.
type HTTPHeadersDirective struct {
	Headers map[string]string `json:"headers,omitempty"`
}

type resolveResponseWire struct {
	AuthRef    string             `json:"authRef"`
	Directives []ResolveDirective `json:"directives,omitempty"`
	Headers    map[string]string  `json:"headers,omitempty"`
	ExpiresAt  *time.Time         `json:"expiresAt,omitempty"`
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
	}
	if in.ExpiresAt != nil {
		expiresCopy := *in.ExpiresAt
		out.ExpiresAt = &expiresCopy
	}
	out.EnsureCompatibilityFields()
	return out
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
		out = append(out, cloned)
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
