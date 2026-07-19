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
overload_guard:
  requests_per_second: 75
  burst: 125
  local_requests_per_second: 375
  local_burst: 625
  max_in_flight: 222
  cleanup_interval: 2m
identity_resource_guard:
  max_teams_owned_per_user: 7
  max_active_refresh_tokens_per_user: 11
  session_cleanup_interval: 45s
  session_cleanup_batch_size: 321
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
	if cfg.OverloadGuard.RequestsPerSecond != 75 ||
		cfg.OverloadGuard.Burst != 125 ||
		cfg.OverloadGuard.LocalRequestsPerSecond != 375 ||
		cfg.OverloadGuard.LocalBurst != 625 ||
		cfg.OverloadGuard.MaxInFlight != 222 ||
		cfg.OverloadGuard.CleanupInterval.Duration != 2*time.Minute {
		t.Fatalf("unexpected overload guard %+v", cfg.OverloadGuard)
	}
	if cfg.IdentityResourceGuard.MaxTeamsOwnedPerUser != 7 {
		t.Fatalf("unexpected owned-team limit %d", cfg.IdentityResourceGuard.MaxTeamsOwnedPerUser)
	}
	if cfg.IdentityResourceGuard.MaxActiveRefreshTokensPerUser != 11 {
		t.Fatalf("unexpected refresh-token limit %d", cfg.IdentityResourceGuard.MaxActiveRefreshTokensPerUser)
	}
	if cfg.IdentityResourceGuard.SessionCleanupInterval.Duration != 45*time.Second {
		t.Fatalf("unexpected identity cleanup interval %s", cfg.IdentityResourceGuard.SessionCleanupInterval.Duration)
	}
	if cfg.IdentityResourceGuard.SessionCleanupBatchSize != 321 {
		t.Fatalf("unexpected identity cleanup batch size %d", cfg.IdentityResourceGuard.SessionCleanupBatchSize)
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
