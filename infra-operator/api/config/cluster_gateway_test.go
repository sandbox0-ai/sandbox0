package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLoadClusterGatewayConfigSandboxObservabilityDefaultsToDisabled(t *testing.T) {
	cfg, err := loadClusterGatewayConfig("")
	if err != nil {
		t.Fatalf("loadClusterGatewayConfig() error = %v", err)
	}
	if cfg.SandboxObservability.BackendType() != SandboxObservabilityBackendDisabled {
		t.Fatalf("backend = %q, want disabled", cfg.SandboxObservability.BackendType())
	}
}

func TestLoadClusterGatewayConfigSandboxObservabilityClickHouse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_observability:
    backend: clickhouse
    audit_enabled: true
    clickhouse:
        dsn: ${TEST_CLICKHOUSE_DSN}
        database: sandbox0_obs_test
        events_table: sandbox_events_test
        retention_days: 14
        connect_timeout:
            duration: 3s
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("TEST_CLICKHOUSE_DSN", "clickhouse://default:pass@clickhouse:9000/default")

	cfg, err := loadClusterGatewayConfig(path)
	if err != nil {
		t.Fatalf("loadClusterGatewayConfig() error = %v", err)
	}
	if cfg.SandboxObservability.BackendType() != SandboxObservabilityBackendClickHouse {
		t.Fatalf("backend = %q, want clickhouse", cfg.SandboxObservability.BackendType())
	}
	if !cfg.SandboxObservability.AuditEnabled {
		t.Fatal("expected sandbox audit to be enabled")
	}
	ch := cfg.SandboxObservability.ClickHouse
	if ch.DSN != "clickhouse://default:pass@clickhouse:9000/default" ||
		ch.Database != "sandbox0_obs_test" ||
		ch.EventsTable != "sandbox_events_test" ||
		ch.RetentionDays != 14 ||
		ch.ConnectTimeout != (metav1.Duration{Duration: 3 * time.Second}) {
		t.Fatalf("clickhouse config = %+v", ch)
	}
}
