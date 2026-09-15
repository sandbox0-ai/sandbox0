package policy

import (
	"net"
	"net/netip"
)

var cloudMetadataPrefixes = [...]netip.Prefix{
	netip.MustParsePrefix("100.100.100.200/32"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("fd00:ec2::254/128"),
}

// IsCloudMetadataIP protects host identity and credential endpoints before
// tenant or platform allow rules. IPv4-mapped addresses share the same boundary.
func IsCloudMetadataIP(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	for _, prefix := range cloudMetadataPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

// CloudMetadataIPv4CIDRs returns the mandatory destinations for the current
// IPv4 redirect path. Callers cannot mutate the proxy's protection set.
func CloudMetadataIPv4CIDRs() []string {
	var out []string
	for _, prefix := range cloudMetadataPrefixes {
		if prefix.Addr().Is4() {
			out = append(out, prefix.String())
		}
	}
	return out
}
