package policy

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	"go.uber.org/zap"
)

func TestStoreReconcileSandboxesRemovesStalePolicies(t *testing.T) {
	store := NewStore(zap.NewNop())
	sandboxA := testSandboxInfo("default", "sandbox-a", "10.0.0.2", "hash-a")
	sandboxB := testSandboxInfo("default", "sandbox-b", "10.0.0.3", "hash-b")

	store.ReconcileSandboxes([]*model.SandboxInfo{sandboxA, sandboxB})
	if got := store.GetByIP(sandboxB.SourceIP); got == nil {
		t.Fatalf("expected sandbox-b policy before removal")
	}

	result := store.ReconcileSandboxes([]*model.SandboxInfo{sandboxA})
	if got := store.GetByIP(sandboxB.SourceIP); got != nil {
		t.Fatalf("stale policy for removed sandbox still present: %#v", got)
	}
	if len(result.RemovedIPs) != 1 || result.RemovedIPs[0] != sandboxB.SourceIP {
		t.Fatalf("removed IPs = %#v, want [%s]", result.RemovedIPs, sandboxB.SourceIP)
	}
}

func TestStoreReconcileSandboxesRemovesOldIPOnSourceIPChange(t *testing.T) {
	store := NewStore(zap.NewNop())
	oldSandbox := testSandboxInfo("default", "sandbox-a", "10.0.0.2", "hash-a")
	newSandbox := testSandboxInfo("default", "sandbox-a", "10.0.0.4", "hash-a")

	store.ReconcileSandboxes([]*model.SandboxInfo{oldSandbox})
	result := store.ReconcileSandboxes([]*model.SandboxInfo{newSandbox})

	if got := store.GetByIP(oldSandbox.SourceIP); got != nil {
		t.Fatalf("old source IP policy still present: %#v", got)
	}
	if got := store.GetByIP(newSandbox.SourceIP); got == nil {
		t.Fatalf("new source IP policy missing")
	}
	if len(result.RemovedIPs) != 1 || result.RemovedIPs[0] != oldSandbox.SourceIP {
		t.Fatalf("removed IPs = %#v, want [%s]", result.RemovedIPs, oldSandbox.SourceIP)
	}
}

func testSandboxInfo(namespace, name, sourceIP, hash string) *model.SandboxInfo {
	return &model.SandboxInfo{
		Scope:             namespace,
		Name:              name,
		SourceIP:          sourceIP,
		NodeID:            "node-a",
		SandboxID:         "sandbox-id-" + name,
		NetworkPolicyHash: hash,
	}
}
