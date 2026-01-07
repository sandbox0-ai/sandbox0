package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// NetworkPolicyService manages SandboxNetworkPolicy and SandboxBandwidthPolicy CRDs
type NetworkPolicyService struct {
	restClient *rest.RESTClient
	logger     *zap.Logger
}

// NewNetworkPolicyService creates a new NetworkPolicyService
func NewNetworkPolicyService(
	restConfig *rest.Config,
	logger *zap.Logger,
) (*NetworkPolicyService, error) {
	// Configure REST client for our CRD group
	config := *restConfig
	config.ContentConfig.GroupVersion = &v1alpha1.SchemeGroupVersion
	config.APIPath = "/apis"
	config.NegotiatedSerializer = nil
	config.ContentType = "application/json"

	restClient, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, fmt.Errorf("create REST client: %w", err)
	}

	return &NetworkPolicyService{
		restClient: restClient,
		logger:     logger,
	}, nil
}

// CreateNetworkPolicyRequest contains the request to create a network policy
type CreateNetworkPolicyRequest struct {
	SandboxID    string
	TeamID       string
	Namespace    string
	TemplateSpec *v1alpha1.NetworkPolicy // From template
	RequestSpec  *v1alpha1.NetworkPolicy // From claim request (overrides template)
}

// CreateOrUpdateNetworkPolicy creates or updates a SandboxNetworkPolicy for a sandbox
func (s *NetworkPolicyService) CreateOrUpdateNetworkPolicy(
	ctx context.Context,
	req *CreateNetworkPolicyRequest,
) error {
	policyName := fmt.Sprintf("sandbox-%s-network", req.SandboxID)

	// Merge template and request specs
	mergedSpec := s.mergeNetworkPolicies(req.TemplateSpec, req.RequestSpec)

	// Build the policy spec
	policySpec := &v1alpha1.SandboxNetworkPolicySpec{
		SandboxID: req.SandboxID,
		TeamID:    req.TeamID,
		Egress:    s.buildEgressSpec(mergedSpec),
		Ingress:   s.buildIngressSpec(mergedSpec),
		Audit: &v1alpha1.AuditSpec{
			Level:      "basic",
			SampleRate: 1.0,
		},
	}

	policy := &v1alpha1.SandboxNetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "SandboxNetworkPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      policyName,
			Namespace: req.Namespace,
			Labels: map[string]string{
				"sandbox0.ai/sandbox-id": req.SandboxID,
				"sandbox0.ai/team-id":    req.TeamID,
			},
		},
		Spec: *policySpec,
	}

	// Try to get existing policy
	existingPolicy, err := s.GetNetworkPolicy(ctx, req.Namespace, req.SandboxID)
	if err == nil {
		// Update existing policy
		policy.ResourceVersion = existingPolicy.ResourceVersion
		return s.updateNetworkPolicyCRD(ctx, req.Namespace, policy)
	}

	// Create new policy
	return s.createNetworkPolicyCRD(ctx, req.Namespace, policy)
}

// createNetworkPolicyCRD creates a new SandboxNetworkPolicy CRD
func (s *NetworkPolicyService) createNetworkPolicyCRD(ctx context.Context, namespace string, policy *v1alpha1.SandboxNetworkPolicy) error {
	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	result := s.restClient.Post().
		Namespace(namespace).
		Resource("sandboxnetworkpolicies").
		Body(bytes.NewReader(data)).
		Do(ctx)

	if err := result.Error(); err != nil {
		return fmt.Errorf("create network policy: %w", err)
	}

	s.logger.Info("Created SandboxNetworkPolicy",
		zap.String("name", policy.Name),
		zap.String("sandboxID", policy.Spec.SandboxID),
	)

	return nil
}

// updateNetworkPolicyCRD updates an existing SandboxNetworkPolicy CRD
func (s *NetworkPolicyService) updateNetworkPolicyCRD(ctx context.Context, namespace string, policy *v1alpha1.SandboxNetworkPolicy) error {
	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	result := s.restClient.Put().
		Namespace(namespace).
		Resource("sandboxnetworkpolicies").
		Name(policy.Name).
		Body(bytes.NewReader(data)).
		Do(ctx)

	if err := result.Error(); err != nil {
		return fmt.Errorf("update network policy: %w", err)
	}

	s.logger.Info("Updated SandboxNetworkPolicy",
		zap.String("name", policy.Name),
		zap.String("sandboxID", policy.Spec.SandboxID),
	)

	return nil
}

// CreateBandwidthPolicyRequest contains the request to create a bandwidth policy
type CreateBandwidthPolicyRequest struct {
	SandboxID         string
	TeamID            string
	Namespace         string
	EgressRateBps     int64
	IngressRateBps    int64
	BurstBytes        int64
	AccountingEnabled bool
}

// CreateOrUpdateBandwidthPolicy creates or updates a SandboxBandwidthPolicy for a sandbox
func (s *NetworkPolicyService) CreateOrUpdateBandwidthPolicy(
	ctx context.Context,
	req *CreateBandwidthPolicyRequest,
) error {
	policyName := fmt.Sprintf("sandbox-%s-bandwidth", req.SandboxID)

	// Default values
	if req.EgressRateBps == 0 {
		req.EgressRateBps = 100 * 1000 * 1000 // 100 Mbps default
	}
	if req.IngressRateBps == 0 {
		req.IngressRateBps = 100 * 1000 * 1000 // 100 Mbps default
	}
	if req.BurstBytes == 0 {
		req.BurstBytes = req.EgressRateBps / 8 // 1 second burst
	}

	policySpec := &v1alpha1.SandboxBandwidthPolicySpec{
		SandboxID: req.SandboxID,
		TeamID:    req.TeamID,
		EgressRateLimit: &v1alpha1.RateLimitSpec{
			RateBps:    req.EgressRateBps,
			BurstBytes: req.BurstBytes,
		},
		IngressRateLimit: &v1alpha1.RateLimitSpec{
			RateBps:    req.IngressRateBps,
			BurstBytes: req.BurstBytes,
		},
		Accounting: &v1alpha1.AccountingSpec{
			Enabled:               true,
			ReportIntervalSeconds: 10, // Fixed per platform policy
		},
	}

	policy := &v1alpha1.SandboxBandwidthPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "SandboxBandwidthPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      policyName,
			Namespace: req.Namespace,
			Labels: map[string]string{
				"sandbox0.ai/sandbox-id": req.SandboxID,
				"sandbox0.ai/team-id":    req.TeamID,
			},
		},
		Spec: *policySpec,
	}

	// Try to get existing policy
	existingPolicy, err := s.GetBandwidthPolicy(ctx, req.Namespace, req.SandboxID)
	if err == nil {
		// Update existing policy
		policy.ResourceVersion = existingPolicy.ResourceVersion
		return s.updateBandwidthPolicyCRD(ctx, req.Namespace, policy)
	}

	// Create new policy
	return s.createBandwidthPolicyCRD(ctx, req.Namespace, policy)
}

// createBandwidthPolicyCRD creates a new SandboxBandwidthPolicy CRD
func (s *NetworkPolicyService) createBandwidthPolicyCRD(ctx context.Context, namespace string, policy *v1alpha1.SandboxBandwidthPolicy) error {
	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	result := s.restClient.Post().
		Namespace(namespace).
		Resource("sandboxbandwidthpolicies").
		Body(bytes.NewReader(data)).
		Do(ctx)

	if err := result.Error(); err != nil {
		return fmt.Errorf("create bandwidth policy: %w", err)
	}

	s.logger.Info("Created SandboxBandwidthPolicy",
		zap.String("name", policy.Name),
		zap.String("sandboxID", policy.Spec.SandboxID),
	)

	return nil
}

// updateBandwidthPolicyCRD updates an existing SandboxBandwidthPolicy CRD
func (s *NetworkPolicyService) updateBandwidthPolicyCRD(ctx context.Context, namespace string, policy *v1alpha1.SandboxBandwidthPolicy) error {
	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	result := s.restClient.Put().
		Namespace(namespace).
		Resource("sandboxbandwidthpolicies").
		Name(policy.Name).
		Body(bytes.NewReader(data)).
		Do(ctx)

	if err := result.Error(); err != nil {
		return fmt.Errorf("update bandwidth policy: %w", err)
	}

	s.logger.Info("Updated SandboxBandwidthPolicy",
		zap.String("name", policy.Name),
		zap.String("sandboxID", policy.Spec.SandboxID),
	)

	return nil
}

// DeleteNetworkPolicy deletes the network policy for a sandbox
func (s *NetworkPolicyService) DeleteNetworkPolicy(ctx context.Context, namespace, sandboxID string) error {
	policyName := fmt.Sprintf("sandbox-%s-network", sandboxID)

	result := s.restClient.Delete().
		Namespace(namespace).
		Resource("sandboxnetworkpolicies").
		Name(policyName).
		Do(ctx)

	if err := result.Error(); err != nil {
		// Check if it's a 404 - that's OK
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete network policy: %w", err)
	}

	return nil
}

// DeleteBandwidthPolicy deletes the bandwidth policy for a sandbox
func (s *NetworkPolicyService) DeleteBandwidthPolicy(ctx context.Context, namespace, sandboxID string) error {
	policyName := fmt.Sprintf("sandbox-%s-bandwidth", sandboxID)

	result := s.restClient.Delete().
		Namespace(namespace).
		Resource("sandboxbandwidthpolicies").
		Name(policyName).
		Do(ctx)

	if err := result.Error(); err != nil {
		// Check if it's a 404 - that's OK
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete bandwidth policy: %w", err)
	}

	return nil
}

// GetNetworkPolicy gets the network policy for a sandbox
func (s *NetworkPolicyService) GetNetworkPolicy(ctx context.Context, namespace, sandboxID string) (*v1alpha1.SandboxNetworkPolicy, error) {
	policyName := fmt.Sprintf("sandbox-%s-network", sandboxID)

	result := s.restClient.Get().
		Namespace(namespace).
		Resource("sandboxnetworkpolicies").
		Name(policyName).
		Do(ctx)

	if err := result.Error(); err != nil {
		return nil, err
	}

	data, err := result.Raw()
	if err != nil {
		return nil, err
	}

	policy := &v1alpha1.SandboxNetworkPolicy{}
	if err := json.Unmarshal(data, policy); err != nil {
		return nil, fmt.Errorf("unmarshal policy: %w", err)
	}

	return policy, nil
}

// GetBandwidthPolicy gets the bandwidth policy for a sandbox
func (s *NetworkPolicyService) GetBandwidthPolicy(ctx context.Context, namespace, sandboxID string) (*v1alpha1.SandboxBandwidthPolicy, error) {
	policyName := fmt.Sprintf("sandbox-%s-bandwidth", sandboxID)

	result := s.restClient.Get().
		Namespace(namespace).
		Resource("sandboxbandwidthpolicies").
		Name(policyName).
		Do(ctx)

	if err := result.Error(); err != nil {
		return nil, err
	}

	data, err := result.Raw()
	if err != nil {
		return nil, err
	}

	policy := &v1alpha1.SandboxBandwidthPolicy{}
	if err := json.Unmarshal(data, policy); err != nil {
		return nil, fmt.Errorf("unmarshal policy: %w", err)
	}

	return policy, nil
}

// mergeNetworkPolicies merges template and request network policies
// Request values override template values
func (s *NetworkPolicyService) mergeNetworkPolicies(
	template *v1alpha1.NetworkPolicy,
	request *v1alpha1.NetworkPolicy,
) *v1alpha1.NetworkPolicy {
	if template == nil && request == nil {
		return &v1alpha1.NetworkPolicy{
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

// buildEgressSpec builds EgressPolicySpec from NetworkPolicy
func (s *NetworkPolicyService) buildEgressSpec(policy *v1alpha1.NetworkPolicy) *v1alpha1.EgressPolicySpec {
	if policy == nil {
		return &v1alpha1.EgressPolicySpec{
			DefaultAction:     "deny",
			AlwaysDeniedCIDRs: v1alpha1.PlatformDeniedCIDRs,
			EnforceProxyPorts: []int32{80, 443},
		}
	}

	spec := &v1alpha1.EgressPolicySpec{
		AlwaysDeniedCIDRs: v1alpha1.PlatformDeniedCIDRs,
		EnforceProxyPorts: []int32{80, 443},
	}

	switch policy.Mode {
	case v1alpha1.NetworkModeAllowAll:
		spec.DefaultAction = "allow"
	case v1alpha1.NetworkModeBlockAll:
		spec.DefaultAction = "deny"
	case v1alpha1.NetworkModeCustom:
		spec.DefaultAction = "deny" // Custom defaults to deny
	default:
		spec.DefaultAction = "deny"
	}

	if policy.Egress != nil {
		spec.AllowedCIDRs = policy.Egress.AllowedIPs
		spec.DeniedCIDRs = policy.Egress.BlockedIPs
		spec.AllowedDomains = policy.Egress.AllowedDomains
		spec.DeniedDomains = policy.Egress.BlockedDomains
	}

	return spec
}

// buildIngressSpec builds IngressPolicySpec from NetworkPolicy
func (s *NetworkPolicyService) buildIngressSpec(policy *v1alpha1.NetworkPolicy) *v1alpha1.IngressPolicySpec {
	spec := &v1alpha1.IngressPolicySpec{
		DefaultAction: "deny", // Always default deny for ingress
		// Allow procd port from internal-gateway
		AllowedPorts: []v1alpha1.PortSpec{
			{Port: 49983, Protocol: "tcp"},
		},
	}

	if policy != nil && policy.Ingress != nil {
		spec.AllowedSourceCIDRs = policy.Ingress.AllowedIPs
		spec.DeniedSourceCIDRs = policy.Ingress.BlockedIPs
	}

	return spec
}

// UpdateNetworkPolicyRequest is the request to update a network policy
type UpdateNetworkPolicyRequest struct {
	SandboxID      string
	Namespace      string
	AllowedDomains []string
	DeniedDomains  []string
	AllowedCIDRs   []string
	DeniedCIDRs    []string
}

// UpdateNetworkPolicy updates an existing network policy
func (s *NetworkPolicyService) UpdateNetworkPolicy(
	ctx context.Context,
	req *UpdateNetworkPolicyRequest,
) error {
	// Get existing policy
	policy, err := s.GetNetworkPolicy(ctx, req.Namespace, req.SandboxID)
	if err != nil {
		return fmt.Errorf("get existing policy: %w", err)
	}

	// Update spec
	if policy.Spec.Egress == nil {
		policy.Spec.Egress = &v1alpha1.EgressPolicySpec{}
	}

	if req.AllowedDomains != nil {
		policy.Spec.Egress.AllowedDomains = req.AllowedDomains
	}
	if req.DeniedDomains != nil {
		policy.Spec.Egress.DeniedDomains = req.DeniedDomains
	}
	if req.AllowedCIDRs != nil {
		policy.Spec.Egress.AllowedCIDRs = req.AllowedCIDRs
	}
	if req.DeniedCIDRs != nil {
		policy.Spec.Egress.DeniedCIDRs = req.DeniedCIDRs
	}

	return s.updateNetworkPolicyCRD(ctx, req.Namespace, policy)
}

// UpdateBandwidthPolicyRequest is the request to update a bandwidth policy
type UpdateBandwidthPolicyRequest struct {
	SandboxID      string
	Namespace      string
	EgressRateBps  *int64
	IngressRateBps *int64
	BurstBytes     *int64
}

// UpdateBandwidthPolicy updates an existing bandwidth policy
func (s *NetworkPolicyService) UpdateBandwidthPolicy(
	ctx context.Context,
	req *UpdateBandwidthPolicyRequest,
) error {
	// Get existing policy
	policy, err := s.GetBandwidthPolicy(ctx, req.Namespace, req.SandboxID)
	if err != nil {
		return fmt.Errorf("get existing policy: %w", err)
	}

	// Update spec
	if req.EgressRateBps != nil {
		if policy.Spec.EgressRateLimit == nil {
			policy.Spec.EgressRateLimit = &v1alpha1.RateLimitSpec{}
		}
		policy.Spec.EgressRateLimit.RateBps = *req.EgressRateBps
	}
	if req.IngressRateBps != nil {
		if policy.Spec.IngressRateLimit == nil {
			policy.Spec.IngressRateLimit = &v1alpha1.RateLimitSpec{}
		}
		policy.Spec.IngressRateLimit.RateBps = *req.IngressRateBps
	}
	if req.BurstBytes != nil {
		if policy.Spec.EgressRateLimit != nil {
			policy.Spec.EgressRateLimit.BurstBytes = *req.BurstBytes
		}
		if policy.Spec.IngressRateLimit != nil {
			policy.Spec.IngressRateLimit.BurstBytes = *req.BurstBytes
		}
	}

	return s.updateBandwidthPolicyCRD(ctx, req.Namespace, policy)
}
