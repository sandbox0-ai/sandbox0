package service

import "testing"

func TestSandboxEnvVarsForInitializeClonesConfigEnvVars(t *testing.T) {
	cfg := &SandboxConfig{
		EnvVars: map[string]string{
			"APP_ENV": "test",
		},
	}

	got := sandboxEnvVarsForInitialize(cfg, sandboxPlatformEnv{})
	if got["APP_ENV"] != "test" {
		t.Fatalf("APP_ENV = %q, want test", got["APP_ENV"])
	}

	got["APP_ENV"] = "mutated"
	if cfg.EnvVars["APP_ENV"] != "test" {
		t.Fatalf("config env mutated to %q, want test", cfg.EnvVars["APP_ENV"])
	}
}

func TestSandboxEnvVarsForInitializeAddsPlatformEnvVars(t *testing.T) {
	got := sandboxEnvVarsForInitialize(nil, sandboxPlatformEnv{
		SandboxID: "rs-browser-abcde",
		AppDomain: "aws-us-east-1.sandbox0.app.",
	})

	if got[SandboxEnvSandboxID] != "rs-browser-abcde" {
		t.Fatalf("%s = %q, want sandbox id", SandboxEnvSandboxID, got[SandboxEnvSandboxID])
	}
	if got[SandboxEnvAppDomain] != "aws-us-east-1.sandbox0.app" {
		t.Fatalf("%s = %q, want app domain without trailing dot", SandboxEnvAppDomain, got[SandboxEnvAppDomain])
	}
}

func TestSandboxEnvVarsForInitializePlatformEnvVarsOverrideConfig(t *testing.T) {
	cfg := &SandboxConfig{
		EnvVars: map[string]string{
			SandboxEnvSandboxID: "wrong",
			SandboxEnvAppDomain: "wrong.example.com",
		},
	}

	got := sandboxEnvVarsForInitialize(cfg, sandboxPlatformEnv{
		SandboxID: "rs-browser-abcde",
		AppDomain: "aws-us-east-1.sandbox0.app",
	})

	if got[SandboxEnvSandboxID] != "rs-browser-abcde" {
		t.Fatalf("%s = %q, want platform sandbox id", SandboxEnvSandboxID, got[SandboxEnvSandboxID])
	}
	if got[SandboxEnvAppDomain] != "aws-us-east-1.sandbox0.app" {
		t.Fatalf("%s = %q, want platform app domain", SandboxEnvAppDomain, got[SandboxEnvAppDomain])
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
