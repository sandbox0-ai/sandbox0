package service

import (
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
)

// NetworkPolicyServiceConfig holds configuration for NetworkPolicyService
type NetworkPolicyServiceConfig struct {
	DefaultBandwidthRateBps     int64
	DefaultBandwidthBurstBytes  int64
	BandwidthAccountingInterval int
}

// NetworkPolicyService builds network and bandwidth policy specs for pod annotations
type NetworkPolicyService struct {
	config NetworkPolicyServiceConfig
	logger *zap.Logger
}

// NewNetworkPolicyService creates a new NetworkPolicyService
func NewNetworkPolicyService(config NetworkPolicyServiceConfig, logger *zap.Logger) *NetworkPolicyService {
	return &NetworkPolicyService{
		config: config,
		logger: logger,
	}
}

// BuildNetworkPolicyRequest contains the request to build a network policy
type BuildNetworkPolicyRequest struct {
	SandboxID    string
	TeamID       string
	TemplateSpec *v1alpha1.TplSandboxNetworkPolicy // From template
	RequestSpec  *v1alpha1.TplSandboxNetworkPolicy // From claim request (overrides template)
}

// BuildNetworkPolicyAnnotation builds the network policy annotation JSON
func (s *NetworkPolicyService) BuildNetworkPolicyAnnotation(req *BuildNetworkPolicyRequest) (string, error) {
	spec := s.BuildNetworkPolicySpec(req)
	return v1alpha1.NetworkPolicyToAnnotation(spec)
}

// BuildNetworkPolicySpec builds the network policy spec without serialization.
func (s *NetworkPolicyService) BuildNetworkPolicySpec(req *BuildNetworkPolicyRequest) *v1alpha1.NetworkPolicySpec {
	// Merge template and request specs
	mergedSpec := s.mergeNetworkPolicies(req.TemplateSpec, req.RequestSpec)

	// Build the policy spec
	return &v1alpha1.NetworkPolicySpec{
		SandboxID: req.SandboxID,
		TeamID:    req.TeamID,
		Egress:    v1alpha1.BuildEgressSpec(mergedSpec),
		Ingress:   v1alpha1.BuildIngressSpec(mergedSpec),
	}
}

// BuildBandwidthPolicyRequest contains the request to build a bandwidth policy
type BuildBandwidthPolicyRequest struct {
	SandboxID         string
	TeamID            string
	EgressRateBps     int64
	IngressRateBps    int64
	BurstBytes        int64
	AccountingEnabled bool
}

// BuildBandwidthPolicyAnnotation builds the bandwidth policy annotation JSON
func (s *NetworkPolicyService) BuildBandwidthPolicyAnnotation(req *BuildBandwidthPolicyRequest) (string, error) {
	spec := s.BuildBandwidthPolicySpec(req)
	return v1alpha1.BandwidthPolicyToAnnotation(spec)
}

// BuildBandwidthPolicySpec builds the bandwidth policy spec without serialization.
func (s *NetworkPolicyService) BuildBandwidthPolicySpec(req *BuildBandwidthPolicyRequest) *v1alpha1.BandwidthPolicySpec {
	// Default values
	egressRateBps := req.EgressRateBps
	if egressRateBps == 0 {
		egressRateBps = s.config.DefaultBandwidthRateBps
	}
	ingressRateBps := req.IngressRateBps
	if ingressRateBps == 0 {
		ingressRateBps = s.config.DefaultBandwidthRateBps
	}
	burstBytes := req.BurstBytes
	if burstBytes == 0 {
		burstBytes = s.config.DefaultBandwidthBurstBytes
	}
	if burstBytes == 0 {
		burstBytes = egressRateBps / 8 // fallback if still 0
	}

	return &v1alpha1.BandwidthPolicySpec{
		SandboxID: req.SandboxID,
		TeamID:    req.TeamID,
		EgressRateLimit: &v1alpha1.RateLimitSpec{
			RateBps:    egressRateBps,
			BurstBytes: burstBytes,
		},
		IngressRateLimit: &v1alpha1.RateLimitSpec{
			RateBps:    ingressRateBps,
			BurstBytes: burstBytes,
		},
		Accounting: &v1alpha1.AccountingSpec{
			Enabled:               true,
			ReportIntervalSeconds: int32(s.config.BandwidthAccountingInterval),
		},
	}
}

// mergeNetworkPolicies merges template and request network policies
// Request values override template values
func (s *NetworkPolicyService) mergeNetworkPolicies(
	template *v1alpha1.TplSandboxNetworkPolicy,
	request *v1alpha1.TplSandboxNetworkPolicy,
) *v1alpha1.TplSandboxNetworkPolicy {
	if template == nil && request == nil {
		return &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll, // Default to block all
		}
	}

	if template == nil {
		return request
	}

	if request == nil {
		return template
	}

	// Merge: request overrides template
	merged := template.DeepCopy()

	// Mode from request takes precedence
	if request.Mode != "" {
		merged.Mode = request.Mode
	}

	// Merge egress
	if request.Egress != nil {
		if merged.Egress == nil {
			merged.Egress = request.Egress
		} else {
			// Append allowed IPs and domains
			merged.Egress.AllowedIPs = append(merged.Egress.AllowedIPs, request.Egress.AllowedIPs...)
			merged.Egress.AllowedDomains = append(merged.Egress.AllowedDomains, request.Egress.AllowedDomains...)
			merged.Egress.BlockedIPs = append(merged.Egress.BlockedIPs, request.Egress.BlockedIPs...)
			merged.Egress.BlockedDomains = append(merged.Egress.BlockedDomains, request.Egress.BlockedDomains...)
		}
	}

	// Merge ingress
	if request.Ingress != nil {
		if merged.Ingress == nil {
			merged.Ingress = request.Ingress
		} else {
			merged.Ingress.AllowedIPs = append(merged.Ingress.AllowedIPs, request.Ingress.AllowedIPs...)
			merged.Ingress.BlockedIPs = append(merged.Ingress.BlockedIPs, request.Ingress.BlockedIPs...)
		}
	}

	return merged
}
