package config

import "testing"

func TestNetworkRuntimeConfigDeepCopyIsIndependent(t *testing.T) {
	preferNFT := true
	original := &NetworkRuntimeConfig{
		PreferNFT:              &preferNFT,
		PlatformAllowedCIDRs:   []string{"10.0.0.0/8"},
		PlatformDeniedCIDRs:    []string{"192.0.2.0/24"},
		PlatformAllowedDomains: []string{"example.com"},
		PlatformDeniedDomains:  []string{"invalid.example"},
	}

	copied := original.DeepCopy()
	*copied.PreferNFT = false
	copied.PlatformAllowedCIDRs[0] = "172.16.0.0/12"
	copied.PlatformDeniedCIDRs[0] = "198.51.100.0/24"
	copied.PlatformAllowedDomains[0] = "copy.example"
	copied.PlatformDeniedDomains[0] = "copy.invalid"

	if !*original.PreferNFT || original.PlatformAllowedCIDRs[0] != "10.0.0.0/8" ||
		original.PlatformDeniedCIDRs[0] != "192.0.2.0/24" ||
		original.PlatformAllowedDomains[0] != "example.com" ||
		original.PlatformDeniedDomains[0] != "invalid.example" {
		t.Fatalf("DeepCopy mutated original: %#v", original)
	}
}
