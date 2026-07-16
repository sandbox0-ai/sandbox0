package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadStorageProxyConfig(t *testing.T) {
	t.Setenv("TEST_STORAGE_DATABASE_URL", "postgres://storage")
	path := filepath.Join(t.TempDir(), "storage-proxy.yaml")
	if err := os.WriteFile(path, []byte("http_port: 18081\ndatabase_url: ${TEST_STORAGE_DATABASE_URL}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := ReadStorageProxyConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != 18081 {
		t.Fatalf("HTTPPort = %d, want 18081", cfg.HTTPPort)
	}
	if cfg.DatabaseURL != "postgres://storage" {
		t.Fatalf("DatabaseURL = %q, want expanded value", cfg.DatabaseURL)
	}
}

func TestReadStorageProxyConfigReturnsReadError(t *testing.T) {
	if _, err := ReadStorageProxyConfig(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("ReadStorageProxyConfig() error = nil, want read error")
	}
}
