package policy

import (
	"fmt"
	"net"
)

// PlatformPolicy defines platform-managed allow/deny rules that override user policy.
type PlatformPolicy struct {
	AllowedCIDRs   []*net.IPNet
	DeniedCIDRs    []*net.IPNet
	AllowedDomains []DomainRule
	DeniedDomains  []DomainRule
	SandboxPodIPs  map[string]struct{}
	SourcePodIP    string
}

// BuildPlatformPolicy compiles platform allow/deny lists into parsed rules.
func BuildPlatformPolicy(allowedCIDRs, deniedCIDRs, allowedDomains, deniedDomains []string) (*PlatformPolicy, error) {
	compiledAllowedCIDRs, err := parseCIDRs(allowedCIDRs)
	if err != nil {
		return nil, fmt.Errorf("parse platform allowed cidrs: %w", err)
	}
	compiledDeniedCIDRs, err := parseCIDRs(deniedCIDRs)
	if err != nil {
		return nil, fmt.Errorf("parse platform denied cidrs: %w", err)
	}
	compiledAllowedDomains, err := parseDomains(allowedDomains)
	if err != nil {
		return nil, fmt.Errorf("parse platform allowed domains: %w", err)
	}
	compiledDeniedDomains, err := parseDomains(deniedDomains)
	if err != nil {
		return nil, fmt.Errorf("parse platform denied domains: %w", err)
	}

	return &PlatformPolicy{
		AllowedCIDRs:   compiledAllowedCIDRs,
		DeniedCIDRs:    compiledDeniedCIDRs,
		AllowedDomains: compiledAllowedDomains,
		DeniedDomains:  compiledDeniedDomains,
	}, nil
}
