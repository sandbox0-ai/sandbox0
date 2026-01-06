// Package network provides network isolation for Procd.
package network

import (
	"net"
	"sync"
	"time"
)

// PolicyMode defines the network policy mode.
type PolicyMode string

const (
	// PolicyModeAllowAll allows all outbound traffic.
	PolicyModeAllowAll PolicyMode = "allow-all"
	// PolicyModeDenyAll denies all outbound traffic.
	PolicyModeDenyAll PolicyMode = "deny-all"
	// PolicyModeWhitelist only allows whitelisted traffic.
	PolicyModeWhitelist PolicyMode = "whitelist"
)

// NetworkPolicy defines the network policy for the sandbox.
type NetworkPolicy struct {
	Mode      PolicyMode     `json:"mode"`
	Egress    *EgressPolicy  `json:"egress,omitempty"`
	Ingress   *IngressPolicy `json:"ingress,omitempty"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// EgressPolicy defines outbound traffic rules.
type EgressPolicy struct {
	AllowCIDRs   []string `json:"allow_cidrs,omitempty"`
	AllowDomains []string `json:"allow_domains,omitempty"`
	DenyCIDRs    []string `json:"deny_cidrs,omitempty"`
	TCPProxyPort int32    `json:"tcp_proxy_port,omitempty"`
}

// IngressPolicy defines inbound traffic rules (reserved for future).
type IngressPolicy struct {
	AllowPorts   []int32  `json:"allow_ports,omitempty"`
	AllowSources []string `json:"allow_sources,omitempty"`
}

// Config holds network configuration.
type Config struct {
	SandboxID        string
	TCPProxyPort     int32
	EnableTCPProxy   bool
	DNSServers       []string
	DefaultDenyCIDRs []string
}

// IPNetSet is a set of IP networks for efficient lookups.
type IPNetSet struct {
	mu   sync.RWMutex
	nets []*net.IPNet
}

// NewIPNetSet creates a new IPNetSet.
func NewIPNetSet() *IPNetSet {
	return &IPNetSet{
		nets: make([]*net.IPNet, 0),
	}
}

// Add adds a CIDR to the set.
func (s *IPNetSet) Add(cidr string) error {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		// Try as single IP
		ip := net.ParseIP(cidr)
		if ip == nil {
			return err
		}
		// Convert single IP to /32 or /128
		if ip.To4() != nil {
			network = &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)}
		} else {
			network = &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nets = append(s.nets, network)
	return nil
}

// Contains checks if an IP is in the set.
func (s *IPNetSet) Contains(ip net.IP) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, network := range s.nets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// Clear clears the set.
func (s *IPNetSet) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nets = make([]*net.IPNet, 0)
}

// DefaultPolicy returns a default allow-all policy.
func DefaultPolicy() *NetworkPolicy {
	return &NetworkPolicy{
		Mode:      PolicyModeAllowAll,
		Egress:    &EgressPolicy{},
		UpdatedAt: time.Now(),
	}
}

// IsPrivateIP checks if an IP is a private IP address.
func IsPrivateIP(ip net.IP) bool {
	privateRanges := []string{
		"10.0.0.0/8",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fc00::/7", // IPv6 private
	}

	for _, cidr := range privateRanges {
		_, network, _ := net.ParseCIDR(cidr)
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
