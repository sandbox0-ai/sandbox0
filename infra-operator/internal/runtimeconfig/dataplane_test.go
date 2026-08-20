package runtimeconfig

import (
	"testing"
	"time"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestToManagerPreservesEgressAuthDefaultResolveTTL(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		EgressAuthDefaultResolveTTL: metav1.Duration{Duration: 90 * time.Second},
	})
	if cfg.EgressAuthDefaultResolveTTL.Duration != 90*time.Second {
		t.Fatalf("egress auth default resolve ttl = %s, want 90s", cfg.EgressAuthDefaultResolveTTL.Duration)
	}
}

func TestToManagerLeavesProcdWebhookOutboxDirUnsetWhenOmitted(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{})
	if cfg.ProcdConfig.WebhookOutboxDir != "" {
		t.Fatalf("webhook outbox dir = %q, want empty path", cfg.ProcdConfig.WebhookOutboxDir)
	}
	if cfg.ProcdConfig.SessionStateDir != "/var/lib/sandbox0/procd/sessions" {
		t.Fatalf("session state dir = %q, want persistent procd path", cfg.ProcdConfig.SessionStateDir)
	}
}

func TestToManagerPreservesProcdWebhookOutboxDir(t *testing.T) {
	outboxDir := "/custom/procd/webhook-outbox"
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		ProcdConfig: infrav1alpha1.ProcdConfig{
			WebhookOutboxDir: &outboxDir,
		},
	})
	if cfg.ProcdConfig.WebhookOutboxDir != outboxDir {
		t.Fatalf("webhook outbox dir = %q, want custom path", cfg.ProcdConfig.WebhookOutboxDir)
	}
}

func TestToManagerPreservesProcdBinImageRef(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		ProcdBinImageRef: "sandbox0ai/infra:test-procd-bin",
	})
	if cfg.ProcdBinImageRef != "sandbox0ai/infra:test-procd-bin" {
		t.Fatalf("procd bin image ref = %q, want sandbox0ai/infra:test-procd-bin", cfg.ProcdBinImageRef)
	}
}

func TestToManagerPreservesNodeAuthority(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		NodeAuthority: infrav1alpha1.NodeAuthorityConfig{
			Enabled: true, ListenHost: "172.16.100.2", Port: 8421,
			TLSSecretName: "manager-node-tls",
			Identities: []infrav1alpha1.NodeAuthorityIdentityConfig{{
				CommonName: "node-agent", ClusterID: "cluster-1", NodeID: "node-1",
				NodeUID: "node-uid-1", PodUID: "agent-1",
			}},
			WriterLeaseTTL: metav1.Duration{Duration: 20 * time.Second},
			Terminal: infrav1alpha1.RuntimeSlotTerminalConfig{
				Enabled: true, ControlSecretName: "nomad-control", ScanLimit: 64,
			},
		},
	})
	if !cfg.NodeAuthority.Enabled || cfg.NodeAuthority.ListenHost != "172.16.100.2" ||
		cfg.NodeAuthority.TLSSecretName != "manager-node-tls" ||
		cfg.NodeAuthority.WriterLeaseTTL.Duration != 20*time.Second || len(cfg.NodeAuthority.Identities) != 1 ||
		cfg.NodeAuthority.Identities[0].NodeUID != "node-uid-1" ||
		!cfg.NodeAuthority.Terminal.Enabled || cfg.NodeAuthority.Terminal.ControlSecretName != "nomad-control" ||
		cfg.NodeAuthority.Terminal.ScanLimit != 64 {
		t.Fatalf("node authority config was not preserved: %#v", cfg.NodeAuthority)
	}
}

func TestToManagerPreservesExplicitEmptyProcdWebhookOutboxDir(t *testing.T) {
	outboxDir := ""
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		ProcdConfig: infrav1alpha1.ProcdConfig{
			WebhookOutboxDir: &outboxDir,
		},
	})
	if cfg.ProcdConfig.WebhookOutboxDir != "" {
		t.Fatalf("webhook outbox dir = %q, want empty path", cfg.ProcdConfig.WebhookOutboxDir)
	}
}

func TestToManagerPreservesDefaultTeamQuotas(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		DefaultTeamQuotas: []infrav1alpha1.TeamQuotaLimitConfig{
			{Dimension: "active_sandboxes", LimitValue: 3},
			{Dimension: "sandbox_claims", LimitValue: 5, IntervalMS: 1000, BurstValue: 5},
			{Dimension: "api_requests", LimitValue: 100, IntervalMS: 1000, BurstValue: 200},
		},
	})
	if len(cfg.DefaultTeamQuotas) != 3 {
		t.Fatalf("default team quotas len = %d, want 3", len(cfg.DefaultTeamQuotas))
	}
	if cfg.DefaultTeamQuotas[0].Dimension != "active_sandboxes" || cfg.DefaultTeamQuotas[0].LimitValue != 3 {
		t.Fatalf("first default quota = %+v, want active_sandboxes=3", cfg.DefaultTeamQuotas[0])
	}
	if cfg.DefaultTeamQuotas[1].Dimension != "sandbox_claims" ||
		cfg.DefaultTeamQuotas[1].LimitValue != 5 ||
		cfg.DefaultTeamQuotas[1].IntervalMS != 1000 ||
		cfg.DefaultTeamQuotas[1].BurstValue != 5 {
		t.Fatalf("second default quota = %+v, want sandbox_claims rate policy", cfg.DefaultTeamQuotas[1])
	}
	if cfg.DefaultTeamQuotas[2].Dimension != "api_requests" ||
		cfg.DefaultTeamQuotas[2].IntervalMS != 1000 ||
		cfg.DefaultTeamQuotas[2].BurstValue != 200 {
		t.Fatalf("third default quota = %+v, want api_requests rate policy", cfg.DefaultTeamQuotas[2])
	}
}

func TestToManagerPreservesSandboxMaxMemory(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		SandboxMaxMemory: "16Gi",
	})
	if cfg.SandboxMaxMemory != "16Gi" {
		t.Fatalf("sandbox max memory = %q, want 16Gi", cfg.SandboxMaxMemory)
	}
}

func TestToNetworkRuntimeLeavesSandboxObservabilityIngestUnset(t *testing.T) {
	cfg := ToNetworkRuntime(&infrav1alpha1.NetworkRuntimeConfig{})
	if cfg.SandboxObservabilityIngestURL != "" ||
		cfg.SandboxObservabilityIngestQueueSize != 0 ||
		cfg.SandboxObservabilityIngestBatchSize != 0 ||
		cfg.SandboxObservabilityIngestFlushInterval.Duration != 0 ||
		cfg.SandboxObservabilityIngestRequestTimeout.Duration != 0 ||
		cfg.SandboxObservabilityIngestMaxRetries != 0 ||
		cfg.SandboxObservabilityIngestRetryBackoff.Duration != 0 {
		t.Fatalf("network runtime observability ingest config should be operator-derived, got %#v", cfg)
	}
}

func TestToManagerPreservesK8sClientRateLimit(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		K8sClientQPS:   25,
		K8sClientBurst: 50,
	})
	if cfg.K8sClientQPS != 25 {
		t.Fatalf("k8s client qps = %v, want 25", cfg.K8sClientQPS)
	}
	if cfg.K8sClientBurst != 50 {
		t.Fatalf("k8s client burst = %d, want 50", cfg.K8sClientBurst)
	}
}

func TestToManagerPreservesPodTeardownAndAutoscalerAnnotations(t *testing.T) {
	cfg := ToManager(&infrav1alpha1.ManagerConfig{
		AutoscalerSafeToEvictAnnotationKeys: []string{"goatscaler.io/safe-to-evict"},
		PodTeardown: infrav1alpha1.PodTeardownConfig{
			MaxConcurrentPerNode:         6,
			MaxConcurrentPerDegradedNode: 2,
			MaxConcurrentReplacements:    24,
		},
	})
	if len(cfg.AutoscalerSafeToEvictAnnotationKeys) != 1 || cfg.AutoscalerSafeToEvictAnnotationKeys[0] != "goatscaler.io/safe-to-evict" {
		t.Fatalf("autoscaler annotation keys = %#v", cfg.AutoscalerSafeToEvictAnnotationKeys)
	}
	if cfg.PodTeardown.MaxConcurrentPerNode != 6 ||
		cfg.PodTeardown.MaxConcurrentPerDegradedNode != 2 ||
		cfg.PodTeardown.MaxConcurrentReplacements != 24 {
		t.Fatalf("pod teardown config = %#v", cfg.PodTeardown)
	}
}

func TestToNetworkRuntimePreservesBandwidthLimits(t *testing.T) {
	cfg := ToNetworkRuntime(&infrav1alpha1.NetworkRuntimeConfig{
		EgressBandwidthBytesPerSecond:      1024,
		IngressBandwidthBytesPerSecond:     2048,
		BandwidthBurstBytes:                4096,
		TeamEgressBandwidthBytesPerSecond:  8192,
		TeamIngressBandwidthBytesPerSecond: 16384,
		TeamBandwidthBurstBytes:            32768,
	})
	if cfg.EgressBandwidthBytesPerSecond != 1024 ||
		cfg.IngressBandwidthBytesPerSecond != 2048 ||
		cfg.BandwidthBurstBytes != 4096 ||
		cfg.TeamEgressBandwidthBytesPerSecond != 8192 ||
		cfg.TeamIngressBandwidthBytesPerSecond != 16384 ||
		cfg.TeamBandwidthBurstBytes != 32768 {
		t.Fatalf("bandwidth limits were not preserved: %#v", cfg)
	}
}
