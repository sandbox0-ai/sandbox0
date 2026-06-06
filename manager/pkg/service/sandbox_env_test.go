package service

import "testing"

func TestSandboxEnvVarsForInitializeClonesConfigEnvVars(t *testing.T) {
	cfg := &SandboxConfig{
		EnvVars: map[string]string{
			"APP_ENV": "test",
		},
	}

	got := sandboxEnvVarsForInitialize(cfg)
	if got["APP_ENV"] != "test" {
		t.Fatalf("APP_ENV = %q, want test", got["APP_ENV"])
	}

	got["APP_ENV"] = "mutated"
	if cfg.EnvVars["APP_ENV"] != "test" {
		t.Fatalf("config env mutated to %q, want test", cfg.EnvVars["APP_ENV"])
	}
}

func TestCloneSandboxConfigClonesEnvVars(t *testing.T) {
	cfg := &SandboxConfig{
		EnvVars: map[string]string{
			"APP_ENV": "test",
		},
	}

	got := cloneSandboxConfig(cfg)
	if got.EnvVars["APP_ENV"] != "test" {
		t.Fatalf("APP_ENV = %q, want test", got.EnvVars["APP_ENV"])
	}

	got.EnvVars["APP_ENV"] = "mutated"
	if cfg.EnvVars["APP_ENV"] != "test" {
		t.Fatalf("config env mutated to %q, want test", cfg.EnvVars["APP_ENV"])
	}
}
