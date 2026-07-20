package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadManagerConfigPreservesDefaultTeamQuotas(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(`
default_team_quotas:
  - dimension: active_sandboxes
    limit_value: 3
  - dimension: cpu_millicpu
    limit_value: 2000
  - dimension: api_requests
    limit_value: 100
    interval_ms: 1000
    burst_value: 200
`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if len(cfg.DefaultTeamQuotas) != 3 {
		t.Fatalf("default team quotas len = %d, want 3", len(cfg.DefaultTeamQuotas))
	}
	if cfg.DefaultTeamQuotas[0].Dimension != "active_sandboxes" || cfg.DefaultTeamQuotas[0].LimitValue != 3 {
		t.Fatalf("first default quota = %+v, want active_sandboxes=3", cfg.DefaultTeamQuotas[0])
	}
	if cfg.DefaultTeamQuotas[1].Dimension != "cpu_millicpu" || cfg.DefaultTeamQuotas[1].LimitValue != 2000 {
		t.Fatalf("second default quota = %+v, want cpu_millicpu=2000", cfg.DefaultTeamQuotas[1])
	}
	if cfg.DefaultTeamQuotas[2].Dimension != "api_requests" ||
		cfg.DefaultTeamQuotas[2].IntervalMS != 1000 ||
		cfg.DefaultTeamQuotas[2].BurstValue != 200 {
		t.Fatalf("third default quota = %+v, want api_requests rate policy", cfg.DefaultTeamQuotas[2])
	}
}

func TestLoadManagerConfigPreservesSandboxMaxMemory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_max_memory: 16Gi
`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if cfg.SandboxMaxMemory != "16Gi" {
		t.Fatalf("sandbox max memory = %q, want 16Gi", cfg.SandboxMaxMemory)
	}
}
