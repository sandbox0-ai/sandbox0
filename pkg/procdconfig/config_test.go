package procdconfig

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestApplyDefaultsRestoresEmptySessionStateDir(t *testing.T) {
	cfg := Config{SessionStateDir: "  "}
	cfg.ApplyDefaults()
	if cfg.SessionStateDir != DefaultSessionStateDir {
		t.Fatalf("session state dir = %q, want %q", cfg.SessionStateDir, DefaultSessionStateDir)
	}
}

func TestApplyDefaultsPreservesCustomSessionStateDir(t *testing.T) {
	cfg := Config{SessionStateDir: "/custom/procd/sessions"}
	cfg.ApplyDefaults()
	if cfg.SessionStateDir != "/custom/procd/sessions" {
		t.Fatalf("session state dir = %q, want custom path", cfg.SessionStateDir)
	}
}

func TestConfigYAMLRoundTripAndEnvMap(t *testing.T) {
	const input = "http_port: 49984\ncontext_cleanup_interval: 45s\n"
	var cfg Config
	if err := yaml.Unmarshal([]byte(input), &cfg); err != nil {
		t.Fatalf("unmarshal YAML: %v", err)
	}
	if cfg.ContextCleanupInterval.Duration != 45*time.Second {
		t.Fatalf("cleanup interval = %s, want 45s", cfg.ContextCleanupInterval.Duration)
	}
	env := cfg.EnvMap()
	if env["http_port"] != "49984" {
		t.Fatalf("http_port = %q, want 49984", env["http_port"])
	}
	if env["context_cleanup_interval"] != "45s" {
		t.Fatalf("context_cleanup_interval = %q, want 45s", env["context_cleanup_interval"])
	}
	if _, ok := env["root_path"]; ok {
		t.Fatal("unexpected unset root_path")
	}
}

func TestDurationJSONRoundTrip(t *testing.T) {
	original := Duration{Duration: 1500 * time.Millisecond}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal duration: %v", err)
	}
	var decoded Duration
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal duration: %v", err)
	}
	if decoded != original {
		t.Fatalf("decoded duration = %s, want %s", decoded.Duration, original.Duration)
	}
}

func TestDeepCopyDoesNotShareConfiguredKeys(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte("http_port: 49984\n"), &cfg); err != nil {
		t.Fatalf("unmarshal YAML: %v", err)
	}
	copied := cfg.DeepCopy()
	copied.setKeys["root_path"] = true
	if cfg.setKeys["root_path"] {
		t.Fatal("deep copy shares configured key state")
	}
}
