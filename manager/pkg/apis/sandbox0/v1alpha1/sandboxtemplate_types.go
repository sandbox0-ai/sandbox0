package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SandboxTemplate defines a template for creating sandboxes
type SandboxTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxTemplateSpec   `json:"spec"`
	Status SandboxTemplateStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SandboxTemplateList contains a list of SandboxTemplate
type SandboxTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxTemplate `json:"items"`
}

// SandboxTemplateSpec defines the desired state of SandboxTemplate
type SandboxTemplateSpec struct {
	// Description of the template
	Description string   `json:"description,omitempty"`
	DisplayName string   `json:"displayName,omitempty"`
	Tags        []string `json:"tags,omitempty"`

	// MainContainer configuration (required)
	MainContainer ContainerSpec `json:"mainContainer"`

	// Sidecar containers (optional)
	Sidecars []corev1.Container `json:"sidecars,omitempty"`

	// Pod-level configuration
	Pod *PodSpecOverride `json:"pod,omitempty"`

	// Template Sandbox Network policy (template-level default)
	Network *TplSandboxNetworkPolicy `json:"network,omitempty"`

	// CredentialBindings defines template-level default credential bindings.
	// Each binding declares a stable ref plus a projection from a manager-owned
	// credential source. Egress credential rules reference these bindings by ref.
	CredentialBindings []CredentialBinding `json:"credentialBindings,omitempty"`

	// Pool strategy
	Pool PoolStrategy `json:"pool"`

	// Environment variables (global, shared by all containers)
	EnvVars map[string]string `json:"envVars,omitempty"`

	// Environment configuration
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`
	ClusterId        *string `json:"clusterId,omitempty"`
}

type ContainerSpec struct {
	Image           string           `json:"image"`
	ImagePullPolicy string           `json:"imagePullPolicy,omitempty"`
	Env             []EnvVar         `json:"env,omitempty"`
	Resources       ResourceQuota    `json:"resources"`
	SecurityContext *SecurityContext `json:"securityContext,omitempty"`
}

// EnvVar represents an environment variable
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ResourceRequirements defines resource requirements for containers
type ResourceRequirements struct {
	Limits   map[string]string `json:"limits,omitempty"`   // e.g. {"cpu": "2", "memory": "4Gi"}
	Requests map[string]string `json:"requests,omitempty"` // e.g. {"cpu": "1", "memory": "2Gi"}
}

// SecurityContext defines security context for containers
type SecurityContext struct {
	Capabilities *Capabilities `json:"capabilities,omitempty"`
	RunAsUser    *int64        `json:"runAsUser,omitempty"`
	RunAsGroup   *int64        `json:"runAsGroup,omitempty"`
}

// Capabilities defines Linux capabilities
type Capabilities struct {
	// Add field is removed to prevent privilege escalation
	Drop []string `json:"drop,omitempty"` // e.g. ["NET_RAW"]
}

// PodSpecOverride allows overriding pod-level settings
type PodSpecOverride struct {
	NodeSelector       map[string]string `json:"nodeSelector,omitempty"`
	Affinity           *Affinity         `json:"affinity,omitempty"`
	Tolerations        []Toleration      `json:"tolerations,omitempty"`
	ServiceAccountName string            `json:"serviceAccountName,omitempty"`
}

// Affinity defines pod affinity rules
type Affinity struct {
	NodeAffinity *NodeAffinity `json:"nodeAffinity,omitempty"`
	PodAffinity  *PodAffinity  `json:"podAffinity,omitempty"`
}

// NodeAffinity defines node affinity rules
type NodeAffinity struct {
	RequiredDuringSchedulingIgnoredDuringExecution  *NodeSelector             `json:"requiredDuringSchedulingIgnoredDuringExecution,omitempty"`
	PreferredDuringSchedulingIgnoredDuringExecution []PreferredSchedulingTerm `json:"preferredDuringSchedulingIgnoredDuringExecution,omitempty"`
}

// NodeSelector defines node selector
type NodeSelector struct {
	NodeSelectorTerms []NodeSelectorTerm `json:"nodeSelectorTerms"`
}

// NodeSelectorTerm defines node selector term
type NodeSelectorTerm struct {
	MatchExpressions []NodeSelectorRequirement `json:"matchExpressions,omitempty"`
	MatchFields      []NodeSelectorRequirement `json:"matchFields,omitempty"`
}

// NodeSelectorRequirement defines node selector requirement
type NodeSelectorRequirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values,omitempty"`
}

// PreferredSchedulingTerm defines preferred scheduling term
type PreferredSchedulingTerm struct {
	Weight     int32            `json:"weight"`
	Preference NodeSelectorTerm `json:"preference"`
}

// PodAffinity defines pod affinity rules
type PodAffinity struct {
	RequiredDuringSchedulingIgnoredDuringExecution  []PodAffinityTerm         `json:"requiredDuringSchedulingIgnoredDuringExecution,omitempty"`
	PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm `json:"preferredDuringSchedulingIgnoredDuringExecution,omitempty"`
}

// PodAffinityTerm defines pod affinity term
type PodAffinityTerm struct {
	LabelSelector *LabelSelector `json:"labelSelector,omitempty"`
	Namespaces    []string       `json:"namespaces,omitempty"`
	TopologyKey   string         `json:"topologyKey"`
}

// LabelSelector defines label selector
type LabelSelector struct {
	MatchLabels      map[string]string          `json:"matchLabels,omitempty"`
	MatchExpressions []LabelSelectorRequirement `json:"matchExpressions,omitempty"`
}

// LabelSelectorRequirement defines label selector requirement
type LabelSelectorRequirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values,omitempty"`
}

// WeightedPodAffinityTerm defines weighted pod affinity term
type WeightedPodAffinityTerm struct {
	Weight          int32           `json:"weight"`
	PodAffinityTerm PodAffinityTerm `json:"podAffinityTerm"`
}

// Toleration defines pod toleration
type Toleration struct {
	Key      string `json:"key,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
	Effect   string `json:"effect,omitempty"`
}

// ResourceQuota defines resource quota (per template)
type ResourceQuota struct {
	CPU    resource.Quantity `json:"cpu,omitempty"`    // e.g. "2"
	Memory resource.Quantity `json:"memory,omitempty"` // e.g. "4Gi"
}

// PoolStrategy defines pool strategy
type PoolStrategy struct {
	MinIdle int32 `json:"minIdle"` // Minimum idle pods (ReplicaSet replicas)
	MaxIdle int32 `json:"maxIdle"` // Maximum idle pods (enforced by CleanupController)
}

// TplSandboxNetworkPolicy defines network policy (template-level default).
// allow-all permits traffic by default and applies denied* rules as subtractive filters.
// block-all denies traffic by default and applies allowed* rules as additive exceptions.
type TplSandboxNetworkPolicy struct {
	Mode   NetworkPolicyMode    `json:"mode"`
	Egress *NetworkEgressPolicy `json:"egress,omitempty"`
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
	CredentialRules []EgressCredentialRule `json:"credentialRules,omitempty"`
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

// SandboxNetworkPolicy defines the sandbox-scoped network policy API shape.
type SandboxNetworkPolicy struct {
	Mode               NetworkPolicyMode    `json:"mode"`
	Egress             *NetworkEgressPolicy `json:"egress,omitempty"`
	// CredentialBindings defines sandbox-scoped credential bindings that
	// EgressCredentialRule entries can resolve by CredentialRef.
	CredentialBindings []CredentialBinding  `json:"credentialBindings,omitempty"`
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

	// TLSMode indicates whether netd should intercept TLS for matching flows.
	TLSMode EgressTLSMode `json:"tlsMode,omitempty"`

	// FailurePolicy controls whether netd should fail-open or fail-closed when auth material cannot be enforced.
	FailurePolicy EgressAuthFailurePolicy `json:"failurePolicy,omitempty"`

	// Domains matches outbound destinations by DNS name or wildcard suffix.
	Domains []string `json:"domains,omitempty"`

	// Ports constrains the rule to specific ports/protocols.
	Ports []PortSpec `json:"ports,omitempty"`
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
	Type                 CredentialProjectionType        `json:"type"`
	// HTTPHeaders projects resolved source data into outbound HTTP headers.
	HTTPHeaders          *HTTPHeadersProjection          `json:"httpHeaders,omitempty"`
	// TLSClientCertificate projects one client certificate for TLS re-origination.
	TLSClientCertificate *TLSClientCertificateProjection `json:"tlsClientCertificate,omitempty"`
	// UsernamePassword projects one username/password pair into an early auth exchange.
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
	// Headers lists the outbound headers to synthesize.
	Headers []ProjectedHeader `json:"headers,omitempty"`
}

// TLSClientCertificateProjection projects one client certificate for TLS re-origination.
type TLSClientCertificateProjection struct{}

// UsernamePasswordProjection projects one username/password pair into an early auth exchange.
type UsernamePasswordProjection struct{}

// ProjectedHeader defines one projected header template.
type ProjectedHeader struct {
	// Name is the outbound header name.
	Name          string `json:"name"`
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

// EgressTLSMode defines how netd should handle TLS for auth-enabled egress traffic.
type EgressTLSMode string

const (
	EgressTLSModePassthrough          EgressTLSMode = "passthrough"
	EgressTLSModeTerminateReoriginate EgressTLSMode = "terminate-reoriginate"
)

// EgressAuthFailurePolicy defines netd behavior when auth cannot be enforced.
type EgressAuthFailurePolicy string

const (
	EgressAuthFailurePolicyFailClosed EgressAuthFailurePolicy = "fail-closed"
	EgressAuthFailurePolicyFailOpen   EgressAuthFailurePolicy = "fail-open"
)

// SandboxTemplateStatus defines the observed state of SandboxTemplate
type SandboxTemplateStatus struct {
	// Pool statistics
	IdleCount   int32 `json:"idleCount"`
	ActiveCount int32 `json:"activeCount"`

	// Conditions
	Conditions []SandboxTemplateCondition `json:"conditions,omitempty"`

	// Last updated time
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
}

// SandboxTemplateCondition defines a condition of SandboxTemplate
type SandboxTemplateCondition struct {
	Type               SandboxTemplateConditionType `json:"type"`
	Status             ConditionStatus              `json:"status"`
	LastTransitionTime metav1.Time                  `json:"lastTransitionTime,omitempty"`
	Reason             string                       `json:"reason,omitempty"`
	Message            string                       `json:"message,omitempty"`
}

// SandboxTemplateConditionType defines condition type
type SandboxTemplateConditionType string

const (
	SandboxTemplateReady       SandboxTemplateConditionType = "Ready"
	SandboxTemplatePoolHealthy SandboxTemplateConditionType = "PoolHealthy"
)

// ConditionStatus defines condition status
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)
