package policy

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	"go.uber.org/zap"
)

func TestStoreReconcileSandboxesRemovesStalePolicies(t *testing.T) {
	store := NewStore(zap.NewNop())
	sandboxA := testSandboxInfo("default", "sandbox-a", "10.0.0.2", "hash-a")
	sandboxB := testSandboxInfo("default", "sandbox-b", "10.0.0.3", "hash-b")

	store.ReconcileSandboxes([]*watcher.SandboxInfo{sandboxA, sandboxB})
	if got := store.GetByIP(sandboxB.PodIP); got == nil {
		t.Fatalf("expected sandbox-b policy before removal")
	}

	result := store.ReconcileSandboxes([]*watcher.SandboxInfo{sandboxA})
	if got := store.GetByIP(sandboxB.PodIP); got != nil {
		t.Fatalf("stale policy for removed sandbox still present: %#v", got)
	}
	if len(result.RemovedIPs) != 1 || result.RemovedIPs[0] != sandboxB.PodIP {
		t.Fatalf("removed IPs = %#v, want [%s]", result.RemovedIPs, sandboxB.PodIP)
	}
}

func TestStoreReconcileSandboxesRemovesOldIPOnPodIPChange(t *testing.T) {
	store := NewStore(zap.NewNop())
	oldSandbox := testSandboxInfo("default", "sandbox-a", "10.0.0.2", "hash-a")
	newSandbox := testSandboxInfo("default", "sandbox-a", "10.0.0.4", "hash-a")

	store.ReconcileSandboxes([]*watcher.SandboxInfo{oldSandbox})
	result := store.ReconcileSandboxes([]*watcher.SandboxInfo{newSandbox})

	if got := store.GetByIP(oldSandbox.PodIP); got != nil {
		t.Fatalf("old pod IP policy still present: %#v", got)
	}
	if got := store.GetByIP(newSandbox.PodIP); got == nil {
		t.Fatalf("new pod IP policy missing")
	}
	if len(result.RemovedIPs) != 1 || result.RemovedIPs[0] != oldSandbox.PodIP {
		t.Fatalf("removed IPs = %#v, want [%s]", result.RemovedIPs, oldSandbox.PodIP)
	}
}

func testSandboxInfo(namespace, name, podIP, hash string) *watcher.SandboxInfo {
	return &watcher.SandboxInfo{
		Namespace:         namespace,
		Name:              name,
		PodIP:             podIP,
		NodeName:          "node-a",
		SandboxID:         "sandbox-id-" + name,
		NetworkPolicyHash: hash,
	}
}
