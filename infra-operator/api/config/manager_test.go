package config

import (
	"os"
	"path/filepath"
	"testing"
)

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
