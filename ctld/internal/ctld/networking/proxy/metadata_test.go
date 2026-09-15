package proxy

import (
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
)

func TestCloudMetadataCannotBeAnEgressProxyEndpoint(t *testing.T) {
	_, allIPv4, _ := net.ParseCIDR("0.0.0.0/0")
	compiled := &policy.CompiledPolicy{Platform: &policy.PlatformPolicy{AllowedCIDRs: []*net.IPNet{allIPv4}}}
	for _, address := range []string{"100.100.100.200", "169.254.169.254", "::ffff:100.100.100.200", "fd00:ec2::254"} {
		for _, candidate := range []*policy.CompiledPolicy{nil, compiled} {
			if !isProtectedProxyEndpointIP(candidate, net.ParseIP(address)) {
				t.Fatalf("metadata address %s accepted as a proxy endpoint", address)
			}
		}
	}
}
