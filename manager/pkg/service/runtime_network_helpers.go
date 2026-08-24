package service

import (
	"net"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

// AppendWebhookNetworkPolicy returns an independent request policy that
// permits the configured webhook destination.
func AppendWebhookNetworkPolicy(request *v1alpha1.SandboxNetworkPolicy, webhookURL string) *v1alpha1.SandboxNetworkPolicy {
	parsed, err := url.Parse(strings.TrimSpace(webhookURL))
	if err != nil || parsed.Hostname() == "" {
		return request
	}
	if request == nil {
		request = &v1alpha1.SandboxNetworkPolicy{}
	} else {
		request = request.DeepCopy()
	}
	if request.Egress == nil {
		request.Egress = &v1alpha1.NetworkEgressPolicy{}
	}
	host := parsed.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		request.Egress.AllowedCIDRs = append(request.Egress.AllowedCIDRs, formatCIDRForIP(ip))
	} else {
		request.Egress.AllowedDomains = append(request.Egress.AllowedDomains, host)
	}
	return request
}

func formatCIDRForIP(ip net.IP) string {
	if ip.To4() != nil {
		return ip.String() + "/32"
	}
	return ip.String() + "/128"
}

func requestCredentialBindings(cfg *sandboxstore.SandboxConfig) []v1alpha1.CredentialBinding {
	if cfg == nil || cfg.Network == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), cfg.Network.CredentialBindings...)
}

func templateCredentialBindings(policy *v1alpha1.SandboxNetworkPolicy) []v1alpha1.CredentialBinding {
	if policy == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), policy.CredentialBindings...)
}

func sanitizedNetworkPolicyForPersistence(policy *v1alpha1.SandboxNetworkPolicy) *v1alpha1.SandboxNetworkPolicy {
	if policy == nil {
		return nil
	}
	cloned := policy.DeepCopy()
	cloned.CredentialBindings = nil
	return cloned
}

func networkPolicyFromSpec(spec *v1alpha1.NetworkPolicySpec) *v1alpha1.SandboxNetworkPolicy {
	if spec == nil {
		return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	policy := &v1alpha1.SandboxNetworkPolicy{Mode: spec.Mode}
	if policy.Mode == "" {
		policy.Mode = v1alpha1.NetworkModeAllowAll
	}
	if spec.Egress != nil {
		policy.Egress = &v1alpha1.NetworkEgressPolicy{
			AllowedCIDRs:    append([]string(nil), spec.Egress.AllowedCIDRs...),
			DeniedCIDRs:     append([]string(nil), spec.Egress.DeniedCIDRs...),
			AllowedDomains:  append([]string(nil), spec.Egress.AllowedDomains...),
			DeniedDomains:   append([]string(nil), spec.Egress.DeniedDomains...),
			AllowedPorts:    append([]v1alpha1.PortSpec(nil), spec.Egress.AllowedPorts...),
			DeniedPorts:     append([]v1alpha1.PortSpec(nil), spec.Egress.DeniedPorts...),
			TrafficRules:    append([]v1alpha1.TrafficRule(nil), spec.Egress.TrafficRules...),
			ProtocolRules:   append([]v1alpha1.ProtocolRule(nil), spec.Egress.ProtocolRules...),
			CredentialRules: append([]v1alpha1.EgressCredentialRule(nil), spec.Egress.CredentialRules...),
			Proxy:           networkpolicy.CloneEgressProxyPolicy(spec.Egress.Proxy),
		}
	}
	return policy
}

func sandboxNetworkPolicyFromState(state *networkpolicy.BuildNetworkPolicyResult) *v1alpha1.SandboxNetworkPolicy {
	if state == nil {
		return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	policy := networkPolicyFromSpec(state.PolicySpec)
	policy.CredentialBindings = append([]v1alpha1.CredentialBinding(nil), state.CredentialBindings...)
	return policy
}
