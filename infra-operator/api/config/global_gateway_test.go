package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadGlobalGatewayConfigExpandsEnvAndParsesDurations(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	t.Setenv("TEST_DATABASE_URL", "postgres://cloud:test@localhost:5432/cloud")
	configYAML := `database_url: ${TEST_DATABASE_URL}
shutdown_timeout: 31s
jwt_access_token_ttl: 20m
oidc_state_cleanup_interval: 2m
`
	if err := os.WriteFile(configPath, []byte(configYAML), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := loadGlobalGatewayConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.DatabaseURL != "postgres://cloud:test@localhost:5432/cloud" {
		t.Fatalf("unexpected database url %q", cfg.DatabaseURL)
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

func TestLoadGlobalGatewayConfigParsesStructuredDurations(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	configYAML := `database_url: postgres://cloud:test@localhost:5432/cloud
shutdown_timeout:
  duration: 31s
jwt_access_token_ttl:
  duration: 20m
oidc_state_cleanup_interval:
  duration: 2m
`
	if err := os.WriteFile(configPath, []byte(configYAML), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := loadGlobalGatewayConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
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

func TestLoadGlobalGatewayConfigRejectsInvalidDuration(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	if err := os.WriteFile(configPath, []byte("shutdown_timeout: nope\n"), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := loadGlobalGatewayConfig(configPath); err == nil {
		t.Fatal("expected invalid duration error")
	}
}
