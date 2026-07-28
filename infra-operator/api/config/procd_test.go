package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestProcdConfigApplyDefaultsRestoresEmptySessionStateDir(t *testing.T) {
	cfg := ProcdConfig{}
	cfg.SessionStateDir = "  "
	cfg.ApplyDefaults()
	if cfg.SessionStateDir != DefaultSessionStateDir {
		t.Fatalf("session state dir = %q, want %q", cfg.SessionStateDir, DefaultSessionStateDir)
	}
}

func TestProcdConfigApplyDefaultsPreservesCustomSessionStateDir(t *testing.T) {
	cfg := ProcdConfig{}
	cfg.SessionStateDir = "/custom/procd/sessions"
	cfg.ApplyDefaults()
	if cfg.SessionStateDir != "/custom/procd/sessions" {
		t.Fatalf("session state dir = %q, want custom path", cfg.SessionStateDir)
	}
}

func TestProcdConfigPreservesInlineYAMLAndConfiguredKeys(t *testing.T) {
	var cfg ProcdConfig
	if err := yaml.Unmarshal([]byte("http_port: 49984\ncontext_cleanup_interval: 45s\n"), &cfg); err != nil {
		t.Fatalf("unmarshal procd config: %v", err)
	}
	if cfg.HTTPPort != 49984 {
		t.Fatalf("http port = %d, want 49984", cfg.HTTPPort)
	}
	if cfg.ContextCleanupInterval.Duration != 45*time.Second {
		t.Fatalf("cleanup interval = %s, want 45s", cfg.ContextCleanupInterval.Duration)
	}
	env := cfg.EnvMap()
	if env["http_port"] != "49984" || env["context_cleanup_interval"] != "45s" {
		t.Fatalf("configured environment = %#v", env)
	}
	if _, ok := env["root_path"]; ok {
		t.Fatal("unexpected unset root_path")
	}
}
