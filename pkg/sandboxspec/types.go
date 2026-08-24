// Package sandboxspec defines runtime-neutral sandbox template and network policy data.
package sandboxspec

import "time"

const DefaultSandboxEphemeralStorage = "8Gi"

// SandboxSecurityClass selects the immutable guest privilege boundary. It is
// part of warm-slot compatibility and never changes after a sandbox claim.
type SandboxSecurityClass string

const (
	SandboxSecurityClassStandard   SandboxSecurityClass = "standard"
	SandboxSecurityClassPrivileged SandboxSecurityClass = "privileged"
)

// TemplateSpec defines the reusable runtime inputs for sandbox claims.
type TemplateSpec struct {
	Description     string                `json:"description,omitempty"`
	DisplayName     string                `json:"displayName,omitempty"`
	Tags            []string              `json:"tags,omitempty"`
	MainContainer   ContainerSpec         `json:"mainContainer"`
	EphemeralMounts []EphemeralMountSpec  `json:"ephemeralMounts,omitempty"`
	Network         *SandboxNetworkPolicy `json:"network,omitempty"`
	EnvVars         map[string]string     `json:"envVars,omitempty"`
}

// SandboxTemplateSpec is retained as the public product-domain name.
type SandboxTemplateSpec = TemplateSpec

// ContainerSpec defines the sandbox image, environment, and default resources.
type ContainerSpec struct {
	Image         string               `json:"image"`
	Env           []EnvVar             `json:"env,omitempty"`
	Resources     ResourceQuota        `json:"resources"`
	SecurityClass SandboxSecurityClass `json:"securityClass,omitempty"`
}

// EphemeralMountSpec declares claim-lifetime storage that is intentionally
// excluded from pause, resume, fork, and snapshot RootFS generations.
type EphemeralMountSpec struct {
	MountPath string `json:"mountPath"`
	SizeLimit string `json:"sizeLimit"`
}

// EffectiveSandboxSecurityClass returns the canonical class and treats the
// omitted legacy value as standard.
func EffectiveSandboxSecurityClass(value SandboxSecurityClass) (SandboxSecurityClass, bool) {
	switch value {
	case "", SandboxSecurityClassStandard:
		return SandboxSecurityClassStandard, true
	case SandboxSecurityClassPrivileged:
		return SandboxSecurityClassPrivileged, true
	default:
		return "", false
	}
}

// EnvVar represents an environment variable.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ResourceQuota contains canonical runtime resource quantity strings. CPU is
// platform-derived and intentionally omitted from the public API.
type ResourceQuota struct {
	CPU              string `json:"cpu,omitempty"`
	Memory           string `json:"memory,omitempty"`
	EphemeralStorage string `json:"ephemeralStorage,omitempty"`
}

// NetworkPolicyMode defines network policy mode
type NetworkPolicyMode string

const (
	NetworkModeAllowAll NetworkPolicyMode = "allow-all"
	NetworkModeBlockAll NetworkPolicyMode = "block-all"
)

// NetworkEgressPolicy defines egress policy.
// In allow-all mode, denied* fields are enforced and allowed* fields are ignored.
// In block-all mode, allowed* fields are enforced and denied* fields are ignored.
type NetworkEgressPolicy struct {
	// Deprecated: use TrafficRules instead.
	AllowedCIDRs []string `json:"allowedCidrs,omitempty"`
	// Deprecated: use TrafficRules instead.
	AllowedDomains []string `json:"allowedDomains,omitempty"`
	// Deprecated: use TrafficRules instead.
	DeniedCIDRs []string `json:"deniedCidrs,omitempty"`
	// Deprecated: use TrafficRules instead.
	DeniedDomains []string `json:"deniedDomains,omitempty"`
	// Deprecated: use TrafficRules instead.
	AllowedPorts []PortSpec `json:"allowedPorts,omitempty"`
	// Deprecated: use TrafficRules instead.
	DeniedPorts     []PortSpec             `json:"deniedPorts,omitempty"`
	TrafficRules    []TrafficRule          `json:"trafficRules,omitempty"`
	ProtocolRules   []ProtocolRule         `json:"protocolRules,omitempty"`
	CredentialRules []EgressCredentialRule `json:"credentialRules,omitempty"`
	Proxy           *EgressProxyPolicy     `json:"proxy,omitempty"`
}

// EgressProxyType identifies the customer-managed egress proxy protocol.
type EgressProxyType string

const (
	EgressProxyTypeSOCKS5 EgressProxyType = "socks5"
)

// EgressProxyPolicy configures a transparent egress proxy for allowed TCP traffic.
type EgressProxyPolicy struct {
	// Type selects the proxy protocol. The first version supports SOCKS5 only.
	Type EgressProxyType `json:"type"`
	// Address is the proxy endpoint in host:port form.
	Address string `json:"address"`
	// CredentialRef optionally references a username_password credential binding.
	CredentialRef string `json:"credentialRef,omitempty"`
}

// TrafficRuleAction defines the enforcement action for one traffic rule.
type TrafficRuleAction string

const (
	TrafficRuleActionAllow TrafficRuleAction = "allow"
	TrafficRuleActionDeny  TrafficRuleAction = "deny"
)

// TrafficRuleAppProtocol defines the classified application protocol matched by one traffic rule.
type TrafficRuleAppProtocol string

const (
	TrafficRuleAppProtocolHTTP    TrafficRuleAppProtocol = "http"
	TrafficRuleAppProtocolTLS     TrafficRuleAppProtocol = "tls"
	TrafficRuleAppProtocolSSH     TrafficRuleAppProtocol = "ssh"
	TrafficRuleAppProtocolSOCKS5  TrafficRuleAppProtocol = "socks5"
	TrafficRuleAppProtocolMQTT    TrafficRuleAppProtocol = "mqtt"
	TrafficRuleAppProtocolRedis   TrafficRuleAppProtocol = "redis"
	TrafficRuleAppProtocolAMQP    TrafficRuleAppProtocol = "amqp"
	TrafficRuleAppProtocolDNS     TrafficRuleAppProtocol = "dns"
	TrafficRuleAppProtocolMongoDB TrafficRuleAppProtocol = "mongodb"
	TrafficRuleAppProtocolUDP     TrafficRuleAppProtocol = "udp"
)

// TrafficRule defines one ordered egress allow/deny matcher.
type TrafficRule struct {
	// Name is an optional stable identifier used for merge and replacement.
	Name string `json:"name,omitempty"`

	// Action defines whether matching traffic is allowed or denied.
	Action TrafficRuleAction `json:"action"`

	// CIDRs matches outbound destinations by IP range.
	CIDRs []string `json:"cidrs,omitempty"`

	// Domains matches outbound destinations by DNS name or wildcard suffix.
	Domains []string `json:"domains,omitempty"`

	// Ports constrains the rule to specific ports/protocols.
	Ports []PortSpec `json:"ports,omitempty"`

	// AppProtocols constrains the rule to classified application protocols.
	AppProtocols []TrafficRuleAppProtocol `json:"appProtocols,omitempty"`
}

// ProtocolRuleProtocol identifies the protocol parser used by a protocol rule.
type ProtocolRuleProtocol string

const (
	ProtocolRuleProtocolMCP  ProtocolRuleProtocol = "mcp"
	ProtocolRuleProtocolHTTP ProtocolRuleProtocol = "http"
)

// ProtocolRule defines protocol-aware controls applied after traffic is allowed.
type ProtocolRule struct {
	// Name is an optional stable identifier used for merge and replacement.
	Name string `json:"name,omitempty"`

	// Protocol selects the protocol adapter for this rule.
	Protocol ProtocolRuleProtocol `json:"protocol"`

	// Domains matches outbound destinations by DNS name or wildcard suffix.
	Domains []string `json:"domains,omitempty"`

	// Ports constrains the rule to specific ports/protocols.
	Ports []PortSpec `json:"ports,omitempty"`

	// TLSMode controls whether the ctld network runtime must terminate TLS to
	// inspect this protocol.
	TLSMode EgressTLSMode `json:"tlsMode,omitempty"`

	// HTTPMatch constrains HTTP-carried protocol rules to request attributes.
	HTTPMatch *HTTPMatch `json:"httpMatch,omitempty"`

	// HTTP configures HTTP request policy.
	HTTP *HTTPProtocolRule `json:"http,omitempty"`

	// MCP configures Model Context Protocol operation policy.
	MCP *MCPProtocolRule `json:"mcp,omitempty"`
}

// HTTPProtocolRule defines HTTP-specific request policy.
type HTTPProtocolRule struct {
	// Methods controls HTTP methods.
	Methods *HTTPMethodPolicy `json:"methods,omitempty"`

	// Paths controls URL paths.
	Paths *HTTPPathPolicy `json:"paths,omitempty"`
}

// HTTPMethodPolicy defines allow and deny lists for HTTP methods.
type HTTPMethodPolicy struct {
	// Allowed permits only listed methods when non-empty.
	Allowed []string `json:"allowed,omitempty"`

	// Denied blocks listed methods before evaluating Allowed.
	Denied []string `json:"denied,omitempty"`
}

// HTTPPathPolicy defines allow and deny lists for HTTP request paths.
type HTTPPathPolicy struct {
	// Allowed permits only listed exact paths when any allowed path list is non-empty.
	Allowed []string `json:"allowed,omitempty"`

	// Denied blocks listed exact paths before evaluating Allowed.
	Denied []string `json:"denied,omitempty"`

	// AllowedPrefixes permits only paths with listed prefixes when any allowed path list is non-empty.
	AllowedPrefixes []string `json:"allowedPrefixes,omitempty"`

	// DeniedPrefixes blocks paths with listed prefixes before evaluating Allowed.
	DeniedPrefixes []string `json:"deniedPrefixes,omitempty"`
}

// MCPProtocolRule defines MCP-specific operation policy.
type MCPProtocolRule struct {
	// Tools controls MCP tools/call requests.
	Tools *MCPToolPolicy `json:"tools,omitempty"`
}

// MCPToolPolicy defines allow and deny lists for MCP tool names.
type MCPToolPolicy struct {
	// Allowed permits only listed tools when non-empty.
	Allowed []string `json:"allowed,omitempty"`

	// Denied blocks listed tools before evaluating Allowed.
	Denied []string `json:"denied,omitempty"`
}

// SandboxNetworkPolicy defines the public network policy shape used by
// templates, sandbox claim/update requests, and runtime sandbox APIs.
type SandboxNetworkPolicy struct {
	Mode   NetworkPolicyMode    `json:"mode"`
	Egress *NetworkEgressPolicy `json:"egress,omitempty"`
	// CredentialBindings defines sandbox-scoped credential bindings that
	// EgressCredentialRule entries can resolve by CredentialRef.
	CredentialBindings []CredentialBinding `json:"credentialBindings,omitempty"`
}

// EgressCredentialRule defines a credential injection rule matched against outbound traffic.
type EgressCredentialRule struct {
	// Name is an optional stable identifier used for merge and replacement.
	Name string `json:"name,omitempty"`

	// CredentialRef identifies the binding ref resolved by the runtime egress
	// auth resolver when this traffic rule matches.
	CredentialRef string `json:"credentialRef"`

	// Rollout controls whether this rule is active. Empty defaults to enabled.
	Rollout EgressAuthRolloutMode `json:"rollout,omitempty"`

	// Protocol is the intended application protocol for the rule.
	Protocol EgressAuthProtocol `json:"protocol,omitempty"`

	// TLSMode indicates whether the ctld network runtime should intercept TLS for
	// matching flows.
	TLSMode EgressTLSMode `json:"tlsMode,omitempty"`

	// FailurePolicy controls whether the ctld network runtime should fail-open or
	// fail-closed when auth material cannot be enforced.
	FailurePolicy EgressAuthFailurePolicy `json:"failurePolicy,omitempty"`

	// Domains matches outbound destinations by DNS name or wildcard suffix.
	Domains []string `json:"domains,omitempty"`

	// Ports constrains the rule to specific ports/protocols.
	Ports []PortSpec `json:"ports,omitempty"`

	// HTTPMatch constrains HTTP-family credential injection to request attributes.
	HTTPMatch *HTTPMatch `json:"httpMatch,omitempty"`
}

// HTTPMatch defines request-level matching for HTTP-family egress auth rules.
type HTTPMatch struct {
	// Methods matches HTTP methods. Values are normalized to uppercase.
	Methods []string `json:"methods,omitempty"`

	// Paths matches exact URL paths.
	Paths []string `json:"paths,omitempty"`

	// PathPrefixes matches URL path prefixes.
	PathPrefixes []string `json:"pathPrefixes,omitempty"`

	// Query matches decoded query parameters.
	Query []HTTPValueMatch `json:"query,omitempty"`

	// Headers matches HTTP request headers.
	Headers []HTTPValueMatch `json:"headers,omitempty"`
}

// HTTPValueMatch defines one header or query parameter matcher.
type HTTPValueMatch struct {
	// Name is the header or query parameter name.
	Name string `json:"name"`

	// Values matches any one value. Empty with Present=true only requires presence.
	Values []string `json:"values,omitempty"`

	// Present controls presence-only matching when Values is empty.
	Present bool `json:"present,omitempty"`
}

// CredentialBinding defines one named credential projection that outbound auth
// rules can reference. The binding itself does not match traffic.
type CredentialBinding struct {
	// Ref is the stable identifier matched by EgressCredentialRule.CredentialRef.
	Ref string `json:"ref"`
	// SourceRef identifies the region-scoped credential source resolved by manager.
	SourceRef string `json:"sourceRef"`
	// Projection defines how resolved source material is projected into runtime auth directives.
	Projection ProjectionSpec `json:"projection"`
	// CachePolicy controls broker-side caching for resolved auth material.
	CachePolicy *CachePolicySpec `json:"cachePolicy,omitempty"`
}

// ProjectionSpec defines how resolved source data should be projected into runtime directives.
type ProjectionSpec struct {
	// Type selects the runtime projection shape.
	Type CredentialProjectionType `json:"type"`
	// HTTPHeaders projects resolved source data into outbound HTTP headers.
	HTTPHeaders *HTTPHeadersProjection `json:"httpHeaders,omitempty"`
	// PlaceholderSubstitution replaces sandbox-visible placeholders at the egress boundary.
	PlaceholderSubstitution *PlaceholderSubstitutionProjection `json:"placeholderSubstitution,omitempty"`
	// TLSClientCertificate projects one client certificate for TLS re-origination.
	TLSClientCertificate *TLSClientCertificateProjection `json:"tlsClientCertificate,omitempty"`
	// UsernamePassword projects one username/password pair into an early auth exchange.
	UsernamePassword *UsernamePasswordProjection `json:"usernamePassword,omitempty"`
	// SSHProxy projects sandbox-side fake keys and upstream identity for transparent SSH proxying.
	SSHProxy *SSHProxyProjection `json:"sshProxy,omitempty"`
}

// CredentialProjectionType identifies the runtime projection shape.
type CredentialProjectionType string

const (
	CredentialProjectionTypeHTTPHeaders             CredentialProjectionType = "http_headers"
	CredentialProjectionTypePlaceholderSubstitution CredentialProjectionType = "placeholder_substitution"
	CredentialProjectionTypeTLSClientCertificate    CredentialProjectionType = "tls_client_certificate"
	CredentialProjectionTypeUsernamePassword        CredentialProjectionType = "username_password"
	CredentialProjectionTypeSSHProxy                CredentialProjectionType = "ssh_proxy"
)

// HTTPHeadersProjection injects HTTP headers derived from source data.
type HTTPHeadersProjection struct {
	// Headers lists the outbound headers to synthesize.
	Headers []ProjectedHeader `json:"headers,omitempty"`
}

// PlaceholderSubstitutionProjection replaces placeholders in outbound HTTP traffic.
type PlaceholderSubstitutionProjection struct {
	// Replacements lists placeholder replacement templates.
	Replacements []PlaceholderReplacement `json:"replacements,omitempty"`
}

// PlaceholderReplacement defines one placeholder replacement template.
type PlaceholderReplacement struct {
	// Placeholder is the opaque sandbox-visible value to replace.
	Placeholder string `json:"placeholder"`
	// ValueTemplate is rendered against the resolved source payload.
	ValueTemplate string `json:"valueTemplate"`
	// Locations limits replacement to selected HTTP request locations.
	Locations []PlaceholderSubstitutionLocation `json:"locations,omitempty"`
}

// PlaceholderSubstitutionLocation identifies an HTTP request location.
type PlaceholderSubstitutionLocation string

const (
	PlaceholderSubstitutionLocationHeader PlaceholderSubstitutionLocation = "header"
	PlaceholderSubstitutionLocationQuery  PlaceholderSubstitutionLocation = "query"
	PlaceholderSubstitutionLocationBody   PlaceholderSubstitutionLocation = "body"
)

// TLSClientCertificateProjection projects one client certificate for TLS re-origination.
type TLSClientCertificateProjection struct{}

// UsernamePasswordProjection projects one username/password pair into an early auth exchange.
type UsernamePasswordProjection struct{}

// SSHProxyProjection configures transparent SSH re-origination.
type SSHProxyProjection struct {
	// SandboxPublicKeys are fake public keys accepted from sandbox-side SSH clients.
	SandboxPublicKeys []string `json:"sandboxPublicKeys,omitempty"`
	// UpstreamUsername is the username the ctld network runtime uses when
	// authenticating to the upstream SSH server.
	UpstreamUsername string `json:"upstreamUsername,omitempty"`
	// KnownHosts contains OpenSSH known_hosts entries used to verify upstream host keys.
	KnownHosts []string `json:"knownHosts,omitempty"`
}

// ProjectedHeader defines one projected header template.
type ProjectedHeader struct {
	// Name is the outbound header name.
	Name string `json:"name"`
	// ValueTemplate is rendered against the resolved source payload.
	ValueTemplate string `json:"valueTemplate"`
}

// CachePolicySpec controls broker-side caching for one binding.
type CachePolicySpec struct {
	// TTL overrides the default broker cache TTL for resolved auth material.
	TTL string `json:"ttl,omitempty"`
}

// EgressAuthProtocol defines the supported application protocols for egress auth rules.
type EgressAuthProtocol string

const (
	EgressAuthProtocolHTTP   EgressAuthProtocol = "http"
	EgressAuthProtocolHTTPS  EgressAuthProtocol = "https"
	EgressAuthProtocolGRPC   EgressAuthProtocol = "grpc"
	EgressAuthProtocolTLS    EgressAuthProtocol = "tls"
	EgressAuthProtocolSSH    EgressAuthProtocol = "ssh"
	EgressAuthProtocolSOCKS5 EgressAuthProtocol = "socks5"
	EgressAuthProtocolMQTT   EgressAuthProtocol = "mqtt"
	EgressAuthProtocolRedis  EgressAuthProtocol = "redis"
)

// EgressAuthRolloutMode defines whether a matched auth rule is active.
type EgressAuthRolloutMode string

const (
	EgressAuthRolloutEnabled  EgressAuthRolloutMode = "enabled"
	EgressAuthRolloutDisabled EgressAuthRolloutMode = "disabled"
)

// EgressTLSMode defines how the ctld network runtime should handle TLS for
// auth-enabled egress traffic.
type EgressTLSMode string

const (
	EgressTLSModePassthrough          EgressTLSMode = "passthrough"
	EgressTLSModeTerminateReoriginate EgressTLSMode = "terminate-reoriginate"
)

// EgressAuthFailurePolicy defines ctld network runtime behavior when auth cannot be enforced.
type EgressAuthFailurePolicy string

const (
	EgressAuthFailurePolicyFailClosed EgressAuthFailurePolicy = "fail-closed"
	EgressAuthFailurePolicyFailOpen   EgressAuthFailurePolicy = "fail-open"
)

// TemplateStatus reports asynchronous template creation.
type TemplateStatus struct {
	Creation *TemplateCreationStatus `json:"creation,omitempty"`
}

// SandboxTemplateStatus is retained as the public product-domain name.
type SandboxTemplateStatus = TemplateStatus

// TemplateCreationStatus reports asynchronous creation of a template RootFS.
type TemplateCreationStatus struct {
	State TemplateCreationState `json:"state"`
	Stage TemplateCreationStage `json:"stage"`

	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CapturedAt  *time.Time `json:"capturedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`

	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// TemplateCreationState is the externally visible template creation state.
type TemplateCreationState string

const (
	TemplateCreationStateCreating TemplateCreationState = "creating"
	TemplateCreationStateReady    TemplateCreationState = "ready"
	TemplateCreationStateFailed   TemplateCreationState = "failed"
)

// TemplateCreationStage is the current asynchronous template creation stage.
type TemplateCreationStage string

const (
	TemplateCreationStageCapturing  TemplateCreationStage = "capturing"
	TemplateCreationStagePublishing TemplateCreationStage = "publishing"
)

// DeepCopy returns an independent template spec.
func (in *TemplateSpec) DeepCopy() *TemplateSpec {
	return clone(in)
}

// DeepCopy returns an independent network policy.
func (in *SandboxNetworkPolicy) DeepCopy() *SandboxNetworkPolicy {
	return clone(in)
}

// DeepCopy returns an independent template status.
func (in *TemplateStatus) DeepCopy() *TemplateStatus {
	return clone(in)
}

// BuildEgressSpec returns an independent runtime egress policy.
func BuildEgressSpec(policy *SandboxNetworkPolicy) *NetworkEgressPolicy {
	if policy == nil || policy.Egress == nil {
		return nil
	}
	return clone(policy.Egress)
}

// NetworkPolicySpec is the authenticated policy envelope applied to a runtime.
type NetworkPolicySpec struct {
	// Version identifies the policy schema version
	Version string `json:"version,omitempty"`

	// SandboxID is the unique identifier of the sandbox this policy applies to
	SandboxID string `json:"sandboxId"`

	// TeamID is the team that owns this sandbox
	TeamID string `json:"teamId"`

	// CredentialBindingDigest binds source-version-independent projection
	// semantics into policy acknowledgement and runtime cache identity.
	CredentialBindingDigest string `json:"credentialBindingDigest,omitempty"`

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
	return mode != NetworkModeAllowAll || spec.Egress != nil || spec.CredentialBindingDigest != ""
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
