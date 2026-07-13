package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestLoadNetdConfigAuditDeliveryMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_observability_ingest_url: http://cluster-gateway/internal/v1/sandbox-observability/events
sandbox_observability_audit_delivery_mode: canonical_sync
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadNetdConfig(path)
	if err != nil {
		t.Fatalf("loadNetdConfig() error = %v", err)
	}
	applyNetdDefaults(cfg)
	if cfg.SandboxObservabilityAuditDeliveryMode != sandboxobservability.AuditDeliveryModeCanonicalSync {
		t.Fatalf("audit delivery mode = %q, want canonical_sync", cfg.SandboxObservabilityAuditDeliveryMode)
	}
}

func TestApplyNetdDefaultsAuditDeliveryMode(t *testing.T) {
	t.Run("empty defaults to durable async", func(t *testing.T) {
		cfg := &NetdConfig{}
		applyNetdDefaults(cfg)
		if cfg.SandboxObservabilityAuditDeliveryMode != sandboxobservability.AuditDeliveryModeDurableAsync {
			t.Fatalf("audit delivery mode = %q, want durable_async", cfg.SandboxObservabilityAuditDeliveryMode)
		}
	})

	t.Run("unknown fails closed", func(t *testing.T) {
		cfg := &NetdConfig{SandboxObservabilityAuditDeliveryMode: sandboxobservability.AuditDeliveryMode("typo")}
		applyNetdDefaults(cfg)
		if cfg.SandboxObservabilityAuditDeliveryMode != sandboxobservability.AuditDeliveryModeCanonicalSync {
			t.Fatalf("audit delivery mode = %q, want canonical_sync", cfg.SandboxObservabilityAuditDeliveryMode)
		}
	})
}
