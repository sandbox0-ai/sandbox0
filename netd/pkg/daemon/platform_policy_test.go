package daemon

import (
	"net"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	policypkg "github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	"go.uber.org/zap"
)

func TestPlatformPolicyStateTracksSandboxPodIPs(t *testing.T) {
	store := policypkg.NewStore(zap.NewNop())
	source := &watcher.SandboxInfo{
		Namespace: "default",
		Name:      "sandbox-a",
		PodIP:     "10.0.0.2",
	}
	peer := &watcher.SandboxInfo{
		Namespace: "default",
		Name:      "sandbox-b",
		PodIP:     "10.0.0.3",
	}
	if changed, _ := store.UpsertFromSandbox(source); changed {
		t.Fatalf("expected initial sandbox policy upsert to report unchanged")
	}

	state := newPlatformPolicyState(&apiconfig.NetdConfig{}, store, zap.NewNop())
	state.OnSandboxUpsert(source)
	state.OnSandboxUpsert(peer)

	compiled := store.GetByIP(source.PodIP)
	if compiled == nil || compiled.Platform == nil {
		t.Fatalf("expected platform policy to be attached")
	}
	if policypkg.AllowEgressL4(compiled, net.ParseIP(peer.PodIP), 443, "tcp") {
		t.Fatalf("expected peer sandbox pod to be denied")
	}
	if !policypkg.AllowEgressL4(compiled, net.ParseIP(source.PodIP), 443, "tcp") {
		t.Fatalf("expected self sandbox pod ip to remain allowed")
	}

	state.OnSandboxDelete(peer)

	compiled = store.GetByIP(source.PodIP)
	if compiled == nil || compiled.Platform == nil {
		t.Fatalf("expected platform policy to remain attached")
	}
	if !policypkg.AllowEgressL4(compiled, net.ParseIP(peer.PodIP), 443, "tcp") {
		t.Fatalf("expected peer ip to be allowed after sandbox delete")
	}
}

func TestPlatformPolicyStateAllowsClusterDNSService(t *testing.T) {
	store := policypkg.NewStore(zap.NewNop())
	source := &watcher.SandboxInfo{
		Namespace: "default",
		Name:      "sandbox-a",
		PodIP:     "10.0.0.2",
	}
	if changed, _ := store.UpsertFromSandbox(source); changed {
		t.Fatalf("expected initial sandbox policy upsert to report unchanged")
	}

	state := newPlatformPolicyState(&apiconfig.NetdConfig{}, store, zap.NewNop())
	state.OnSandboxUpsert(source)
	state.OnServiceUpsert(&watcher.ServiceInfo{
		Namespace: "kube-system",
		Name:      "kube-dns",
		ClusterIP: "10.96.0.10",
	})
	state.OnEndpointsUpsert(&watcher.EndpointsInfo{
		Namespace: "kube-system",
		Name:      "kube-dns",
		Addresses: []string{"10.244.0.53"},
	})

	compiled := store.GetByIP(source.PodIP)
	if compiled == nil || compiled.Platform == nil {
		t.Fatalf("expected platform policy to be attached")
	}
	if !policypkg.AllowEgressL4(compiled, net.ParseIP("10.96.0.10"), 53, "udp") {
		t.Fatalf("expected kube-dns service ip to be allowed")
	}
	if !policypkg.AllowEgressL4(compiled, net.ParseIP("10.244.0.53"), 53, "udp") {
		t.Fatalf("expected kube-dns endpoint ip to be allowed")
	}
}
