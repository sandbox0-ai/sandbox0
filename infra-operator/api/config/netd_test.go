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

func TestLoadNetdConfigFromPathAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "netd.yaml")
	if err := os.WriteFile(path, []byte("node_name: node-a\nhealth_port: 18081\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadNetdConfigFromPath(path)
	if err != nil {
		t.Fatalf("LoadNetdConfigFromPath() error = %v", err)
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
	if cfg.ProxyMaxActiveTCPConnections != DefaultNetdProxyMaxActiveTCPConnections ||
		cfg.ProxyUDPWorkers != DefaultNetdProxyUDPWorkers ||
		cfg.ProxyUDPQueueSize != DefaultNetdProxyUDPQueueSize {
		t.Fatalf("proxy admission defaults = %#v", cfg)
	}
	limits := cfg.SandboxObservabilityAuditSpoolLimits
	if limits.MaxBytes != DefaultAuditSpoolMaxBytes ||
		limits.MaxEntries != DefaultAuditSpoolMaxEntries ||
		limits.MaxTeamBytes != DefaultAuditSpoolMaxTeamBytes ||
		limits.MaxTeamEntries != DefaultAuditSpoolMaxTeamEntries ||
		limits.MinFreeBytes != DefaultAuditSpoolMinFreeBytes ||
		limits.MaxRecordBytes != DefaultAuditSpoolMaxRecordBytes {
		t.Fatalf("audit spool limits = %#v, want defaults", limits)
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

func TestNetdProxyAdmissionLimitsRejectUnsafeConfiguration(t *testing.T) {
	tests := []struct {
		name string
		cfg  NetdConfig
	}{
		{
			name: "TCP maximum",
			cfg: NetdConfig{
				ProxyMaxActiveTCPConnections: MaxNetdProxyMaxActiveTCPConnections + 1,
			},
		},
		{
			name: "UDP worker maximum",
			cfg:  NetdConfig{ProxyUDPWorkers: MaxNetdProxyUDPWorkers + 1},
		},
		{
			name: "UDP queue maximum",
			cfg:  NetdConfig{ProxyUDPQueueSize: MaxNetdProxyUDPQueueSize + 1},
		},
		{
			name: "workers above queue",
			cfg: NetdConfig{
				ProxyUDPWorkers:   3,
				ProxyUDPQueueSize: 2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, err := tt.cfg.ProxyAdmissionLimits(); err == nil {
				t.Fatal("ProxyAdmissionLimits() error = nil, want rejection")
			}
		})
	}
}

func TestNetdConfigValidateListenerPorts(t *testing.T) {
	cfg := &NetdConfig{HealthPort: 8081, MetricsPort: 9091, ProxyHTTPPort: 18080, ProxyHTTPSPort: 18443}
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
