package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestLoadNetworkRuntimeConfigAuditDeliveryMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_observability_ingest_url: http://cluster-gateway/internal/v1/sandbox-observability/events
sandbox_observability_audit_delivery_mode: canonical_sync
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadNetworkRuntimeConfig(path)
	if err != nil {
		t.Fatalf("loadNetworkRuntimeConfig() error = %v", err)
	}
	applyNetworkRuntimeDefaults(cfg)
	if cfg.SandboxObservabilityAuditDeliveryMode != sandboxobservability.AuditDeliveryModeCanonicalSync {
		t.Fatalf("audit delivery mode = %q, want canonical_sync", cfg.SandboxObservabilityAuditDeliveryMode)
	}
}

func TestLoadNetworkRuntimeConfigFromPathAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctld-networking.yaml")
	if err := os.WriteFile(path, []byte("node_name: node-a\nhealth_port: 18081\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadNetworkRuntimeConfigFromPath(path)
	if err != nil {
		t.Fatalf("LoadNetworkRuntimeConfigFromPath() error = %v", err)
	}
	if cfg.NodeName != "node-a" {
		t.Fatalf("node name = %q, want node-a", cfg.NodeName)
	}
	if cfg.HealthPort != 18081 {
		t.Fatalf("health port = %d, want 18081", cfg.HealthPort)
	}
	if cfg.MetricsPort != 9091 {
		t.Fatalf("metrics port = %d, want default 9091", cfg.MetricsPort)
	}
}

func TestApplyNetworkRuntimeDefaultsAuditDeliveryMode(t *testing.T) {
	t.Run("empty defaults to durable async", func(t *testing.T) {
		cfg := &NetworkRuntimeConfig{}
		applyNetworkRuntimeDefaults(cfg)
		if cfg.SandboxObservabilityAuditDeliveryMode != sandboxobservability.AuditDeliveryModeDurableAsync {
			t.Fatalf("audit delivery mode = %q, want durable_async", cfg.SandboxObservabilityAuditDeliveryMode)
		}
	})

	t.Run("unknown fails closed", func(t *testing.T) {
		cfg := &NetworkRuntimeConfig{SandboxObservabilityAuditDeliveryMode: sandboxobservability.AuditDeliveryMode("typo")}
		applyNetworkRuntimeDefaults(cfg)
		if cfg.SandboxObservabilityAuditDeliveryMode != sandboxobservability.AuditDeliveryModeCanonicalSync {
			t.Fatalf("audit delivery mode = %q, want canonical_sync", cfg.SandboxObservabilityAuditDeliveryMode)
		}
	})
}

func TestNetworkRuntimeConfigValidateListenerPorts(t *testing.T) {
	cfg := &NetworkRuntimeConfig{HealthPort: 8081, MetricsPort: 9091, ProxyHTTPPort: 18080, ProxyHTTPSPort: 18443}
	if err := cfg.ValidateListenerPorts(map[int]string{8095: "ctld HTTP port"}); err != nil {
		t.Fatalf("valid ports rejected: %v", err)
	}
	cfg.HealthPort = 8095
	if err := cfg.ValidateListenerPorts(map[int]string{8095: "ctld HTTP port"}); err == nil {
		t.Fatal("reserved ctld port collision accepted")
	}
	cfg.HealthPort = 9091
	if err := cfg.ValidateListenerPorts(nil); err == nil {
		t.Fatal("network runtime listener collision accepted")
	}
}
