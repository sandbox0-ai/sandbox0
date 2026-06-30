package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if len(cfg.DefaultTeamQuotas) != 2 {
		t.Fatalf("default team quotas len = %d, want 2", len(cfg.DefaultTeamQuotas))
	}
	if cfg.DefaultTeamQuotas[0].Dimension != "active_sandboxes" || cfg.DefaultTeamQuotas[0].LimitValue != 3 {
		t.Fatalf("first default quota = %+v, want active_sandboxes=3", cfg.DefaultTeamQuotas[0])
	}
	if cfg.DefaultTeamQuotas[1].Dimension != "cpu_millicpu" || cfg.DefaultTeamQuotas[1].LimitValue != 2000 {
		t.Fatalf("second default quota = %+v, want cpu_millicpu=2000", cfg.DefaultTeamQuotas[1])
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

func TestLoadManagerConfigDefaultsColdStartConcurrency(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}
	t.Setenv("CONFIG_PATH", path)

	cfg := LoadManagerConfig()
	if cfg.ColdStartConcurrency.MaxPerTemplate != 32 {
		t.Fatalf("cold start max per template = %d, want 32", cfg.ColdStartConcurrency.MaxPerTemplate)
	}
	if cfg.ColdStartConcurrency.AcquireTimeout.Duration != 30*time.Second {
		t.Fatalf("cold start acquire timeout = %s, want 30s", cfg.ColdStartConcurrency.AcquireTimeout.Duration)
	}
}

func TestLoadManagerConfigKeepsColdStartConcurrencyDisabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(`
cold_start_concurrency:
  disabled: true
`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}
	t.Setenv("CONFIG_PATH", path)

	cfg := LoadManagerConfig()
	if !cfg.ColdStartConcurrency.Disabled {
		t.Fatal("cold start concurrency disabled = false, want true")
	}
	if cfg.ColdStartConcurrency.MaxPerTemplate != 0 {
		t.Fatalf("cold start max per template = %d, want 0 when disabled", cfg.ColdStartConcurrency.MaxPerTemplate)
	}
}
