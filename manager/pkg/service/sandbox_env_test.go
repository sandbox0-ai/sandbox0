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

func TestSandboxVolumeMountPointsForInitializeNormalizesMounts(t *testing.T) {
	got := sandboxVolumeMountPointsForInitialize([]ClaimMount{
		{SandboxVolumeID: "vol-1", MountPoint: "/workspace/data"},
		{SandboxVolumeID: "vol-1", MountPoint: "/workspace/project/../data"},
		{SandboxVolumeID: "vol-2", MountPoint: "relative"},
		{SandboxVolumeID: "vol-3", MountPoint: "/"},
	})
	if len(got) != 1 || got[0] != "/workspace/data" {
		t.Fatalf("mount points = %#v, want [/workspace/data]", got)
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
