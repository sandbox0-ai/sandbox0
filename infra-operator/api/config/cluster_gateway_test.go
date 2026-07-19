package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
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
	if cfg.SandboxObservability.AuditDeliveryMode != sandboxobservability.AuditDeliveryModeDurableAsync {
		t.Fatalf("audit delivery mode = %q, want durable_async", cfg.SandboxObservability.AuditDeliveryMode)
	}
	limits := cfg.SandboxObservability.AuditSpoolLimits
	if limits.MaxBytes != DefaultAuditSpoolMaxBytes ||
		limits.MaxEntries != DefaultAuditSpoolMaxEntries ||
		limits.MaxTeamBytes != DefaultAuditSpoolMaxTeamBytes ||
		limits.MaxTeamEntries != DefaultAuditSpoolMaxTeamEntries ||
		limits.MinFreeBytes != DefaultAuditSpoolMinFreeBytes ||
		limits.MaxRecordBytes != DefaultAuditSpoolMaxRecordBytes {
		t.Fatalf("audit spool limits = %+v, want shared defaults", limits)
	}
}

func TestLoadClusterGatewayConfigSandboxObservabilityClickHouse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_observability:
    backend: clickhouse
    audit_enabled: true
    audit_delivery_mode: canonical_sync
    audit_spool_limits:
        max_bytes: 2097152
        max_entries: 200
        max_team_bytes: 1048576
        max_team_entries: 100
        min_free_bytes: 524288
        max_record_bytes: 65536
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
	if cfg.SandboxObservability.AuditDeliveryMode != sandboxobservability.AuditDeliveryModeCanonicalSync {
		t.Fatalf("audit delivery mode = %q, want canonical_sync", cfg.SandboxObservability.AuditDeliveryMode)
	}
	if got := cfg.SandboxObservability.AuditSpoolLimits; got != (AuditSpoolLimitsConfig{
		MaxBytes:       2097152,
		MaxEntries:     200,
		MaxTeamBytes:   1048576,
		MaxTeamEntries: 100,
		MinFreeBytes:   524288,
		MaxRecordBytes: 65536,
	}) {
		t.Fatalf("audit spool limits = %+v", got)
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

func TestLoadClusterGatewayConfigUnknownAuditDeliveryModeFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_observability:
    audit_enabled: true
    audit_delivery_mode: typo
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadClusterGatewayConfig(path)
	if err != nil {
		t.Fatalf("loadClusterGatewayConfig() error = %v", err)
	}
	if cfg.SandboxObservability.AuditDeliveryMode != sandboxobservability.AuditDeliveryModeCanonicalSync {
		t.Fatalf("audit delivery mode = %q, want fail-closed canonical_sync", cfg.SandboxObservability.AuditDeliveryMode)
	}
}

func TestLoadClusterGatewayConfigRequiresExplicitTeamQuotaPolicyOwner(t *testing.T) {
	tests := []struct {
		name   string
		config string
		want   bool
	}{
		{
			name: "public omitted",
			config: `
auth_mode: public
team_quota: {}
`,
		},
		{
			name: "both omitted",
			config: `
auth_mode: both
team_quota: {}
`,
		},
		{
			name: "explicit false",
			config: `
auth_mode: both
team_quota:
    policy_owner: false
`,
		},
		{
			name: "explicit true",
			config: `
auth_mode: public
team_quota:
    policy_owner: true
`,
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tt.config), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			cfg, err := loadClusterGatewayConfig(path)
			if err != nil {
				t.Fatalf("loadClusterGatewayConfig() error = %v", err)
			}
			if cfg.TeamQuota.PolicyOwner != tt.want {
				t.Fatalf("PolicyOwner = %t, want %t", cfg.TeamQuota.PolicyOwner, tt.want)
			}
		})
	}
}
