package networking

import (
	"context"
	"net/netip"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/conntrack"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

func TestCleanupDeniedTrackedFlowsKeepsDomainAllowedTLSFlow(t *testing.T) {
	const sourceIP = "172.28.23.117"
	tracker := conntrack.NewTracker()
	tracker.Record(testFlowKey(t, sourceIP, "172.67.213.44", "app.storenoviq.com", "tls"))

	store := testPolicyStore(t, sourceIP, &v1alpha1.NetworkPolicySpec{
		SandboxID: "sbx-test",
		TeamID:    "team-test",
		Mode:      v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			AllowedDomains: []string{"app.storenoviq.com"},
		},
	})

	if killed := cleanupDeniedTrackedFlows(context.Background(), tracker, nil, store, sourceIP); killed != 0 {
		t.Fatalf("cleanupDeniedTrackedFlows killed %d flows, want 0", killed)
	}
}

func TestCleanupDeniedTrackedFlowsKillsDomainDeniedTLSFlow(t *testing.T) {
	const sourceIP = "172.28.23.117"
	tracker := conntrack.NewTracker()
	tracker.Record(testFlowKey(t, sourceIP, "172.67.213.44", "blocked.example.com", "tls"))

	store := testPolicyStore(t, sourceIP, &v1alpha1.NetworkPolicySpec{
		SandboxID: "sbx-test",
		TeamID:    "team-test",
		Mode:      v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			AllowedDomains: []string{"app.storenoviq.com"},
		},
	})

	if killed := cleanupDeniedTrackedFlows(context.Background(), tracker, nil, store, sourceIP); killed != 1 {
		t.Fatalf("cleanupDeniedTrackedFlows killed %d flows, want 1", killed)
	}
}

func testFlowKey(t *testing.T, srcIP string, dstIP string, host string, app string) conntrack.FlowKey {
	t.Helper()
	src, err := netip.ParseAddr(srcIP)
	if err != nil {
		t.Fatalf("parse src ip: %v", err)
	}
	dst, err := netip.ParseAddr(dstIP)
	if err != nil {
		t.Fatalf("parse dst ip: %v", err)
	}
	return conntrack.FlowKey{
		Proto:   6,
		SrcIP:   src,
		DstIP:   dst,
		SrcPort: 42123,
		DstPort: 443,
		Host:    host,
		App:     app,
	}
}

func testPolicyStore(t *testing.T, sourceIP string, spec *v1alpha1.NetworkPolicySpec) *policy.Store {
	t.Helper()
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(spec)
	if err != nil {
		t.Fatalf("serialize network policy: %v", err)
	}
	store := policy.NewStore(nil)
	store.ReconcileSandboxes([]*model.SandboxInfo{{
		Scope:             "default",
		Name:              "sandbox-test",
		SourceIP:          sourceIP,
		SandboxID:         spec.SandboxID,
		TeamID:            spec.TeamID,
		NetworkPolicy:     annotation,
		NetworkPolicyHash: "hash-test",
	}})
	return store
}
