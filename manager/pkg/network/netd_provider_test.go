package network

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func TestNetdProviderNetworkPolicyHashSkipsUnrestrictedPolicy(t *testing.T) {
	provider := &NetdProvider{}
	hash, err := provider.networkPolicyHash(&v1alpha1.NetworkPolicySpec{
		Version:   "v1",
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Mode:      v1alpha1.NetworkModeAllowAll,
	})
	if err != nil {
		t.Fatalf("networkPolicyHash() error = %v", err)
	}
	if hash != "" {
		t.Fatalf("networkPolicyHash() = %q, want empty", hash)
	}
}

func TestNetdProviderNetworkPolicyHashKeepsRestrictedPolicy(t *testing.T) {
	provider := &NetdProvider{}
	hash, err := provider.networkPolicyHash(&v1alpha1.NetworkPolicySpec{
		Version:   "v1",
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Mode:      v1alpha1.NetworkModeBlockAll,
	})
	if err != nil {
		t.Fatalf("networkPolicyHash() error = %v", err)
	}
	if hash == "" {
		t.Fatal("networkPolicyHash() is empty for a restricted policy")
	}
}
