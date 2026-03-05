package policy

import (
	"fmt"
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

type DomainMatchType string

const (
	DomainMatchExact  DomainMatchType = "exact"
	DomainMatchSuffix DomainMatchType = "suffix"
)

type DomainRule struct {
	Pattern string
	Type    DomainMatchType
}

type PortRange struct {
	Protocol string
	Start    int32
	End      int32
}

type CompiledRuleSet struct {
	AllowedCIDRs   []*net.IPNet
	DeniedCIDRs    []*net.IPNet
	AllowedPorts   []PortRange
	DeniedPorts    []PortRange
	AllowedDomains []DomainRule
	DeniedDomains  []DomainRule
}

type CompiledPolicy struct {
	SandboxID string
	TeamID    string
	Mode      v1alpha1.NetworkPolicyMode
	Egress    CompiledRuleSet
	Platform  *PlatformPolicy
}

func CompileNetworkPolicy(spec *v1alpha1.NetworkPolicySpec) (*CompiledPolicy, error) {
	if spec == nil {
		return &CompiledPolicy{
			Mode:   v1alpha1.NetworkModeAllowAll,
			Egress: CompiledRuleSet{},
		}, nil
	}

	mode := spec.Mode
	if mode == "" {
		mode = v1alpha1.NetworkModeAllowAll
	}

	compiled := &CompiledPolicy{
		SandboxID: spec.SandboxID,
		TeamID:    spec.TeamID,
		Mode:      mode,
	}

	if spec.Egress != nil {
		egress, err := compileRuleSet(spec.Egress.AllowedCIDRs, spec.Egress.DeniedCIDRs, spec.Egress.AllowedPorts, spec.Egress.DeniedPorts, spec.Egress.AllowedDomains, spec.Egress.DeniedDomains)
		if err != nil {
			return nil, fmt.Errorf("compile egress: %w", err)
		}
		compiled.Egress = egress
	}

	return compiled, nil
}

func compileRuleSet(
	allowedCIDRs []string,
	deniedCIDRs []string,
	allowedPorts []v1alpha1.PortSpec,
	deniedPorts []v1alpha1.PortSpec,
	allowedDomains []string,
	deniedDomains []string,
) (CompiledRuleSet, error) {
	result := CompiledRuleSet{}

	var err error
	result.AllowedCIDRs, err = parseCIDRs(allowedCIDRs)
	if err != nil {
		return result, err
	}
	deniedCIDRsParsed, err := parseCIDRs(deniedCIDRs)
	if err != nil {
		return result, err
	}
	result.DeniedCIDRs = append(result.DeniedCIDRs, deniedCIDRsParsed...)

	result.AllowedPorts, err = parsePorts(allowedPorts)
	if err != nil {
		return result, err
	}
	result.DeniedPorts, err = parsePorts(deniedPorts)
	if err != nil {
		return result, err
	}

	result.AllowedDomains, err = parseDomains(allowedDomains)
	if err != nil {
		return result, err
	}
	result.DeniedDomains, err = parseDomains(deniedDomains)
	if err != nil {
		return result, err
	}

	return result, nil
}

func parseCIDRs(values []string) ([]*net.IPNet, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		netIP, netCIDR, err := net.ParseCIDR(value)
		if err != nil || netIP == nil {
			return nil, fmt.Errorf("invalid CIDR %q", value)
		}
		out = append(out, netCIDR)
	}
	return out, nil
}

func parsePorts(values []v1alpha1.PortSpec) ([]PortRange, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]PortRange, 0, len(values))
	for _, port := range values {
		if port.Port <= 0 || port.Port > 65535 {
			return nil, fmt.Errorf("invalid port %d", port.Port)
		}
		end := port.Port
		if port.EndPort != nil {
			if *port.EndPort < port.Port || *port.EndPort > 65535 {
				return nil, fmt.Errorf("invalid end port %d", *port.EndPort)
			}
			end = *port.EndPort
		}
		proto := strings.ToLower(strings.TrimSpace(port.Protocol))
		if proto == "" {
			proto = "tcp"
		}
		if proto != "tcp" && proto != "udp" {
			return nil, fmt.Errorf("unsupported protocol %q", port.Protocol)
		}
		out = append(out, PortRange{
			Protocol: proto,
			Start:    port.Port,
			End:      end,
		})
	}
	return out, nil
}

func parseDomains(values []string) ([]DomainRule, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]DomainRule, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if strings.HasPrefix(value, "*.") {
			suffix := strings.TrimPrefix(value, "*.")
			if suffix == "" {
				return nil, fmt.Errorf("invalid wildcard domain %q", value)
			}
			out = append(out, DomainRule{Pattern: suffix, Type: DomainMatchSuffix})
			continue
		}
		out = append(out, DomainRule{Pattern: value, Type: DomainMatchExact})
	}
	return out, nil
}
