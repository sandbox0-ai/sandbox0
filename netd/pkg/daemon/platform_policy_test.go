package daemon

import (
	"bytes"
	"net"
	"strings"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	policypkg "github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

func TestPlatformPolicyStateLogsOnlyWhenEffectivePolicyChanges(t *testing.T) {
	store := policypkg.NewStore(zap.NewNop())
	var logBuffer bytes.Buffer
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&logBuffer),
		zap.InfoLevel,
	)
	logger := zap.New(core)
	state := newPlatformPolicyState(&apiconfig.NetdConfig{}, store, logger)
	const logPattern = "\"msg\":\"Platform policy updated\""
	initialLogs := strings.Count(logBuffer.String(), logPattern)

	sandbox := &watcher.SandboxInfo{
		Namespace: "default",
		Name:      "sandbox-a",
		PodIP:     "10.0.0.2",
	}
	service := &watcher.ServiceInfo{
		Namespace: "kube-system",
		Name:      "kube-dns",
		ClusterIP: "10.96.0.10",
	}
	endpoints := &watcher.EndpointsInfo{
		Namespace: "kube-system",
		Name:      "kube-dns",
		Addresses: []string{"10.244.0.53"},
	}

	state.OnSandboxUpsert(sandbox)
	if got := strings.Count(logBuffer.String(), logPattern) - initialLogs; got != 1 {
		t.Fatalf("log count after sandbox upsert = %d, want 1", got)
	}

	state.OnSandboxUpsert(sandbox)
	if got := strings.Count(logBuffer.String(), logPattern) - initialLogs; got != 1 {
		t.Fatalf("log count after duplicate sandbox upsert = %d, want 1", got)
	}

	state.OnServiceUpsert(service)
	if got := strings.Count(logBuffer.String(), logPattern) - initialLogs; got != 2 {
		t.Fatalf("log count after service upsert = %d, want 2", got)
	}

	state.OnServiceUpsert(service)
	if got := strings.Count(logBuffer.String(), logPattern) - initialLogs; got != 2 {
		t.Fatalf("log count after duplicate service upsert = %d, want 2", got)
	}

	state.OnEndpointsUpsert(endpoints)
	if got := strings.Count(logBuffer.String(), logPattern) - initialLogs; got != 3 {
		t.Fatalf("log count after endpoints upsert = %d, want 3", got)
	}

	state.OnEndpointsUpsert(endpoints)
	if got := strings.Count(logBuffer.String(), logPattern) - initialLogs; got != 3 {
		t.Fatalf("log count after duplicate endpoints upsert = %d, want 3", got)
	}
}
