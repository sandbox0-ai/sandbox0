package service

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

func TestCloneSandboxConfigClonesEnvVars(t *testing.T) {
	cfg := &sandboxstore.SandboxConfig{
		EnvVars: map[string]string{
			"APP_ENV": "test",
		},
	}

	got := CloneSandboxConfig(cfg)
	if got.EnvVars["APP_ENV"] != "test" {
		t.Fatalf("APP_ENV = %q, want test", got.EnvVars["APP_ENV"])
	}

	got.EnvVars["APP_ENV"] = "mutated"
	if cfg.EnvVars["APP_ENV"] != "test" {
		t.Fatalf("config env mutated to %q, want test", cfg.EnvVars["APP_ENV"])
	}
}
