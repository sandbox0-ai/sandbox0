package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadGlobalDirectoryConfigExpandsEnvAndParsesDurations(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	t.Setenv("TEST_DATABASE_URL", "postgres://cloud:test@localhost:5432/cloud")
	configYAML := `database_url: ${TEST_DATABASE_URL}
region_token_ttl: 7m
shutdown_timeout: 31s
jwt_access_token_ttl: 20m
oidc_state_cleanup_interval: 2m
`
	if err := os.WriteFile(configPath, []byte(configYAML), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := loadGlobalDirectoryConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.DatabaseURL != "postgres://cloud:test@localhost:5432/cloud" {
		t.Fatalf("unexpected database url %q", cfg.DatabaseURL)
	}
	if cfg.RegionTokenTTL.Duration != 7*time.Minute {
		t.Fatalf("unexpected region token ttl %s", cfg.RegionTokenTTL.Duration)
	}
	if cfg.ShutdownTimeout.Duration != 31*time.Second {
		t.Fatalf("unexpected shutdown timeout %s", cfg.ShutdownTimeout.Duration)
	}
	if cfg.JWTAccessTokenTTL.Duration != 20*time.Minute {
		t.Fatalf("unexpected access token ttl %s", cfg.JWTAccessTokenTTL.Duration)
	}
	if cfg.OIDCStateCleanupInterval.Duration != 2*time.Minute {
		t.Fatalf("unexpected oidc cleanup interval %s", cfg.OIDCStateCleanupInterval.Duration)
	}
}

func TestLoadGlobalDirectoryConfigRejectsInvalidDuration(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	if err := os.WriteFile(configPath, []byte("region_token_ttl: nope\n"), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := loadGlobalDirectoryConfig(configPath); err == nil {
		t.Fatal("expected invalid duration error")
	}
}
