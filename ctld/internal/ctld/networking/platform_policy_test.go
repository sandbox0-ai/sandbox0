package networking

import (
	"bytes"
	"net"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	policypkg "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestPlatformPolicyStateTracksSandboxSourceIPs(t *testing.T) {
	store := policypkg.NewStore(zap.NewNop())
	source := &model.SandboxInfo{Scope: "runtime-slots", Name: "slot-a", SourceIP: "10.0.0.2"}
	peer := &model.SandboxInfo{Scope: "runtime-slots", Name: "slot-b", SourceIP: "10.0.0.3"}
	if got := store.ReconcileSandboxes([]*model.SandboxInfo{source}).Upserted; got != 1 {
		t.Fatalf("initial sandbox policy upserts = %d, want 1", got)
	}

	state := newPlatformPolicyState(&config.NetworkRuntimeConfig{}, store, zap.NewNop())
	state.Reconcile([]*model.SandboxInfo{source, peer})
	compiled := store.GetByIP(source.SourceIP)
	if compiled == nil || compiled.Platform == nil {
		t.Fatal("expected platform policy to be attached")
	}
	if policypkg.AllowEgressL4(compiled, net.ParseIP(peer.SourceIP), 443, "tcp") {
		t.Fatal("expected peer sandbox to be denied")
	}
	if !policypkg.AllowEgressL4(compiled, net.ParseIP(source.SourceIP), 443, "tcp") {
		t.Fatal("expected the source sandbox address to remain allowed")
	}

	state.Reconcile([]*model.SandboxInfo{source})
	compiled = store.GetByIP(source.SourceIP)
	if !policypkg.AllowEgressL4(compiled, net.ParseIP(peer.SourceIP), 443, "tcp") {
		t.Fatal("expected retired peer address to be allowed")
	}
}

func TestPlatformPolicyStateUsesExplicitRegionalServiceConfiguration(t *testing.T) {
	store := policypkg.NewStore(zap.NewNop())
	source := &model.SandboxInfo{Scope: "runtime-slots", Name: "slot-a", SourceIP: "10.0.0.2"}
	store.ReconcileSandboxes([]*model.SandboxInfo{source})
	state := newPlatformPolicyState(&config.NetworkRuntimeConfig{
		PlatformAllowedCIDRs: []string{"10.96.0.10", "2001:db8::53"},
	}, store, zap.NewNop())
	state.Reconcile([]*model.SandboxInfo{source})

	compiled := store.GetByIP(source.SourceIP)
	for _, address := range []string{"10.96.0.10", "2001:db8::53"} {
		if !policypkg.AllowEgressL4(compiled, net.ParseIP(address), 53, "udp") {
			t.Fatalf("expected configured service address %s to be allowed", address)
		}
	}
}

func TestPlatformPolicyStateLogsOnlyWhenEffectivePolicyChanges(t *testing.T) {
	store := policypkg.NewStore(zap.NewNop())
	var logBuffer bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&logBuffer), zap.InfoLevel,
	))
	state := newPlatformPolicyState(&config.NetworkRuntimeConfig{}, store, logger)
	const pattern = `"msg":"Platform policy updated"`
	initial := strings.Count(logBuffer.String(), pattern)
	sandbox := &model.SandboxInfo{Scope: "runtime-slots", Name: "slot-a", SourceIP: "10.0.0.2"}

	state.Reconcile([]*model.SandboxInfo{sandbox})
	state.Reconcile([]*model.SandboxInfo{sandbox})
	if got := strings.Count(logBuffer.String(), pattern) - initial; got != 1 {
		t.Fatalf("effective update log count = %d, want 1", got)
	}
}
