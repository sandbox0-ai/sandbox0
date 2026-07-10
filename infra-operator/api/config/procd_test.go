package config

import "testing"

func TestProcdConfigApplyDefaultsRestoresEmptySessionStateDir(t *testing.T) {
	cfg := ProcdConfig{SessionStateDir: "  "}
	cfg.applyDefaults()
	if cfg.SessionStateDir != DefaultSessionStateDir {
		t.Fatalf("session state dir = %q, want %q", cfg.SessionStateDir, DefaultSessionStateDir)
	}
}

func TestProcdConfigApplyDefaultsPreservesCustomSessionStateDir(t *testing.T) {
	cfg := ProcdConfig{SessionStateDir: "/custom/procd/sessions"}
	cfg.applyDefaults()
	if cfg.SessionStateDir != "/custom/procd/sessions" {
		t.Fatalf("session state dir = %q, want custom path", cfg.SessionStateDir)
	}
}
