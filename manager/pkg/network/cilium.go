package network

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// CiliumConfig defines configuration for the Cilium provider.
type CiliumConfig struct {
	PolicyNamePrefix           string
	BaselinePolicyName         string
	SandboxSelectorLabelKey    string
	CNPGroup                   string
	CNPVersion                 string
	CNPKind                    string
	FieldManager               string
	EnableBandwidthAnnotations bool
	EgressBandwidthAnnotation  string
	IngressBandwidthAnnotation string
}

// CiliumProvider applies network policies using Cilium CRDs.
type CiliumProvider struct {
	k8sClient kubernetes.Interface
	dynamic   dynamic.Interface
	logger    *zap.Logger
	config    CiliumConfig
	policyGVR schema.GroupVersionResource
}

// NewCiliumProvider creates a Cilium provider with sane defaults.
func NewCiliumProvider(
	k8sClient kubernetes.Interface,
	dynamicClient dynamic.Interface,
	cfg CiliumConfig,
	logger *zap.Logger,
) *CiliumProvider {
	if cfg.PolicyNamePrefix == "" {
		cfg.PolicyNamePrefix = "sandbox0"
	}
	if cfg.BaselinePolicyName == "" {
		cfg.BaselinePolicyName = "sandbox0-baseline"
	}
	if cfg.SandboxSelectorLabelKey == "" {
		cfg.SandboxSelectorLabelKey = "sandbox0.ai/sandbox-id"
	}
	if cfg.CNPGroup == "" {
		cfg.CNPGroup = "cilium.io"
	}
	if cfg.CNPVersion == "" {
		cfg.CNPVersion = "v2"
	}
	if cfg.CNPKind == "" {
		cfg.CNPKind = "CiliumNetworkPolicy"
	}
	if cfg.FieldManager == "" {
		cfg.FieldManager = "sandbox0-manager"
	}
	if cfg.EgressBandwidthAnnotation == "" {
		cfg.EgressBandwidthAnnotation = "kubernetes.io/egress-bandwidth"
	}
	if cfg.IngressBandwidthAnnotation == "" {
		cfg.IngressBandwidthAnnotation = "kubernetes.io/ingress-bandwidth"
	}

	return &CiliumProvider{
		k8sClient: k8sClient,
		dynamic:   dynamicClient,
		logger:    logger,
		config:    cfg,
		policyGVR: schema.GroupVersionResource{
			Group:    cfg.CNPGroup,
			Version:  cfg.CNPVersion,
			Resource: "ciliumnetworkpolicies",
		},
	}
}

func (p *CiliumProvider) Name() string { return "cilium" }

func (p *CiliumProvider) EnsureBaseline(ctx context.Context, namespace string) error {
	spec := map[string]any{
		"endpointSelector": map[string]any{
			"matchExpressions": []any{
				map[string]any{
					"key":      p.config.SandboxSelectorLabelKey,
					"operator": "Exists",
				},
			},
		},
		"ingress": []any{},
		"egress":  []any{},
	}

	return p.applyPolicy(ctx, namespace, p.config.BaselinePolicyName, spec, map[string]string{
		"sandbox0.ai/managed-by": "manager",
		"sandbox0.ai/policy":     "baseline",
	})
}

func (p *CiliumProvider) ApplySandboxPolicy(ctx context.Context, input SandboxPolicyInput) error {
	if input.NetworkPolicy != nil {
		spec := p.buildPolicySpec(input)
		name := p.policyName(input.SandboxID)
		labels := map[string]string{
			"sandbox0.ai/managed-by": "manager",
			"sandbox0.ai/sandbox-id": input.SandboxID,
		}
		if err := p.applyPolicy(ctx, input.Namespace, name, spec, labels); err != nil {
			return err
		}
	}

	if p.config.EnableBandwidthAnnotations && input.BandwidthPolicy != nil {
		return p.applyBandwidthAnnotations(ctx, input.Namespace, input.PodName, input.BandwidthPolicy)
	}

	return nil
}

func (p *CiliumProvider) RemoveSandboxPolicy(ctx context.Context, namespace, sandboxID string) error {
	name := p.policyName(sandboxID)
	err := p.dynamic.Resource(p.policyGVR).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (p *CiliumProvider) applyPolicy(ctx context.Context, namespace, name string, spec map[string]any, labels map[string]string) error {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", p.config.CNPGroup, p.config.CNPVersion),
			"kind":       p.config.CNPKind,
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
				"labels":    labels,
			},
			"spec": spec,
		},
	}

	payload, err := json.Marshal(obj.Object)
	if err != nil {
		return fmt.Errorf("marshal policy %s: %w", name, err)
	}

	force := true
	_, err = p.dynamic.Resource(p.policyGVR).Namespace(namespace).Patch(
		ctx,
		name,
		types.ApplyPatchType,
		payload,
		metav1.PatchOptions{
			FieldManager: p.config.FieldManager,
			Force:        &force,
		},
	)
	return err
}

func (p *CiliumProvider) buildPolicySpec(input SandboxPolicyInput) map[string]any {
	policy := input.NetworkPolicy
	spec := map[string]any{
		"endpointSelector": map[string]any{
			"matchLabels": map[string]any{
				p.config.SandboxSelectorLabelKey: input.SandboxID,
			},
		},
	}

	egress, egressDeny := p.buildEgress(policy)
	if egress != nil {
		spec["egress"] = egress
	}
	if egressDeny != nil {
		spec["egressDeny"] = egressDeny
	}

	ingress, ingressDeny := p.buildIngress(policy)
	if ingress != nil {
		spec["ingress"] = ingress
	}
	if ingressDeny != nil {
		spec["ingressDeny"] = ingressDeny
	}

	return spec
}

func (p *CiliumProvider) buildEgress(policy *v1alpha1.NetworkPolicySpec) ([]any, []any) {
	if policy == nil || policy.Egress == nil {
		return []any{}, nil
	}

	eg := policy.Egress
	allowedRules := []any{}
	deniedRules := []any{}

	if len(eg.AllowedCIDRs) > 0 {
		allowedRules = append(allowedRules, map[string]any{
			"toCIDR": eg.AllowedCIDRs,
		})
	}
	if len(eg.AllowedDomains) > 0 {
		allowedRules = append(allowedRules, map[string]any{
			"toFQDNs": buildFQDNRules(eg.AllowedDomains),
		})
	}

	deniedCIDRs := append([]string{}, eg.DeniedCIDRs...)
	deniedCIDRs = append(deniedCIDRs, eg.AlwaysDeniedCIDRs...)
	if len(deniedCIDRs) > 0 {
		deniedRules = append(deniedRules, map[string]any{
			"toCIDR": deniedCIDRs,
		})
	}
	if len(eg.DeniedDomains) > 0 {
		deniedRules = append(deniedRules, map[string]any{
			"toFQDNs": buildFQDNRules(eg.DeniedDomains),
		})
	}

	defaultAllow := strings.EqualFold(eg.DefaultAction, "allow")
	if defaultAllow {
		allowedRules = append(allowedRules, map[string]any{
			"toEntities": []string{"all"},
		})
	}

	if !defaultAllow && len(allowedRules) == 0 {
		allowedRules = []any{}
	}

	if len(deniedRules) == 0 {
		deniedRules = nil
	}

	return allowedRules, deniedRules
}

func (p *CiliumProvider) buildIngress(policy *v1alpha1.NetworkPolicySpec) ([]any, []any) {
	if policy == nil || policy.Ingress == nil {
		return []any{}, nil
	}

	ig := policy.Ingress
	allowedRules := []any{}
	deniedRules := []any{}

	rule := map[string]any{}
	if len(ig.AllowedSourceCIDRs) > 0 {
		rule["fromCIDR"] = ig.AllowedSourceCIDRs
	}
	toPorts := buildPortRules(ig.AllowedPorts)
	if len(toPorts) > 0 {
		rule["toPorts"] = toPorts
	}
	if len(rule) > 0 {
		allowedRules = append(allowedRules, rule)
	}

	if len(ig.DeniedSourceCIDRs) > 0 {
		deniedRules = append(deniedRules, map[string]any{
			"fromCIDR": ig.DeniedSourceCIDRs,
		})
	}

	defaultAllow := strings.EqualFold(ig.DefaultAction, "allow")
	if defaultAllow && len(allowedRules) == 0 {
		allowedRules = append(allowedRules, map[string]any{
			"fromEntities": []string{"all"},
		})
	}

	if !defaultAllow && len(allowedRules) == 0 {
		allowedRules = []any{}
	}

	if len(deniedRules) == 0 {
		deniedRules = nil
	}

	return allowedRules, deniedRules
}

func buildFQDNRules(domains []string) []any {
	rules := make([]any, 0, len(domains))
	for _, domain := range domains {
		if domain == "" {
			continue
		}
		if strings.Contains(domain, "*") {
			rules = append(rules, map[string]any{"matchPattern": domain})
			continue
		}
		rules = append(rules, map[string]any{"matchName": domain})
	}
	return rules
}

func buildPortRules(ports []v1alpha1.PortSpec) []any {
	if len(ports) == 0 {
		return nil
	}
	result := make([]any, 0, len(ports))
	for _, port := range ports {
		if port.Port == 0 {
			continue
		}
		proto := strings.ToUpper(port.Protocol)
		if proto == "" {
			proto = "TCP"
		}
		portEntry := map[string]any{
			"port":     strconv.FormatInt(int64(port.Port), 10),
			"protocol": proto,
		}
		if port.EndPort != nil && *port.EndPort > port.Port {
			portEntry["endPort"] = int(*port.EndPort)
		}
		result = append(result, portEntry)
	}
	if len(result) == 0 {
		return nil
	}
	return []any{
		map[string]any{"ports": result},
	}
}

func (p *CiliumProvider) applyBandwidthAnnotations(
	ctx context.Context,
	namespace string,
	podName string,
	policy *v1alpha1.BandwidthPolicySpec,
) error {
	if policy == nil {
		return nil
	}

	annotations := map[string]any{}
	if policy.EgressRateLimit != nil && policy.EgressRateLimit.RateBps > 0 {
		annotations[p.config.EgressBandwidthAnnotation] = formatBandwidth(policy.EgressRateLimit.RateBps)
	} else {
		annotations[p.config.EgressBandwidthAnnotation] = nil
	}
	if policy.IngressRateLimit != nil && policy.IngressRateLimit.RateBps > 0 {
		annotations[p.config.IngressBandwidthAnnotation] = formatBandwidth(policy.IngressRateLimit.RateBps)
	} else {
		annotations[p.config.IngressBandwidthAnnotation] = nil
	}

	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": annotations,
		},
	}
	payload, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal bandwidth annotations: %w", err)
	}

	_, err = p.k8sClient.CoreV1().Pods(namespace).Patch(ctx, podName, types.MergePatchType, payload, metav1.PatchOptions{})
	return err
}

func formatBandwidth(bps int64) string {
	if bps <= 0 {
		return ""
	}
	if bps >= 1_000_000_000 {
		return fmt.Sprintf("%dG", ceilDiv(bps, 1_000_000_000))
	}
	if bps >= 1_000_000 {
		return fmt.Sprintf("%dM", ceilDiv(bps, 1_000_000))
	}
	if bps >= 1_000 {
		return fmt.Sprintf("%dK", ceilDiv(bps, 1_000))
	}
	return "1K"
}

func ceilDiv(value, unit int64) int64 {
	return (value + unit - 1) / unit
}

func (p *CiliumProvider) policyName(sandboxID string) string {
	base := fmt.Sprintf("%s-%s", p.config.PolicyNamePrefix, sandboxID)
	if len(base) <= 63 {
		return base
	}
	hash := sha1.Sum([]byte(sandboxID))
	shortHash := hex.EncodeToString(hash[:])[:8]
	trim := 63 - len(p.config.PolicyNamePrefix) - 1 - 1 - len(shortHash)
	if trim < 1 {
		trim = 1
	}
	return fmt.Sprintf("%s-%s-%s", p.config.PolicyNamePrefix, sandboxID[:trim], shortHash)
}

var _ Provider = (*CiliumProvider)(nil)
