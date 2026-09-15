package policy

import (
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

func TestCloudMetadataCannotBeAllowedByTenantOrPlatform(t *testing.T) {
	_, allIPv4, _ := net.ParseCIDR("0.0.0.0/0")
	_, allIPv6, _ := net.ParseCIDR("::/0")
	allow := &CompiledPolicy{
		Mode: sandboxspec.NetworkModeAllowAll,
		Platform: &PlatformPolicy{
			AllowedCIDRs: []*net.IPNet{allIPv4, allIPv6},
		},
	}
	for _, address := range []string{
		"100.100.100.200", "169.254.169.254", "169.254.170.2",
		"::ffff:100.100.100.200", "::ffff:169.254.169.254", "fd00:ec2::254",
	} {
		t.Run(address, func(t *testing.T) {
			ip := net.ParseIP(address)
			if !IsCloudMetadataIP(ip) {
				t.Fatal("metadata address was not protected")
			}
			for _, compiled := range []*CompiledPolicy{nil, allow} {
				for _, transport := range []string{"tcp", "udp"} {
					if AllowEgressDestination(compiled, ip, 80, transport, "metadata.example", "http") {
						t.Fatalf("metadata destination allowed for %s", transport)
					}
				}
				if AllowUnknownEgressFallback(compiled, ip, "metadata.example") {
					t.Fatal("metadata destination allowed through unknown protocol fallback")
				}
			}
		})
	}
}

func TestCloudMetadataProtectionPreservesOrdinaryDestinations(t *testing.T) {
	for _, address := range []string{"100.100.2.136", "10.60.7.111", "1.1.1.1", "2001:4860:4860::8888"} {
		if IsCloudMetadataIP(net.ParseIP(address)) {
			t.Fatalf("ordinary destination %s was marked as metadata", address)
		}
	}
	if IsCloudMetadataIP(nil) {
		t.Fatal("invalid destination was classified as metadata")
	}
	cidrs := CloudMetadataIPv4CIDRs()
	if len(cidrs) != 2 {
		t.Fatalf("IPv4 metadata prefixes = %v", cidrs)
	}
	cidrs[0] = "0.0.0.0/0"
	if CloudMetadataIPv4CIDRs()[0] == cidrs[0] {
		t.Fatal("caller mutated the shared metadata protection set")
	}
}
