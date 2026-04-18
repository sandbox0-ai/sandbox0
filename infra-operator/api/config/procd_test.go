package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProcdConfigEnvMapIncludesFuseLatencyKnobs(t *testing.T) {
	var cfg ProcdConfig
	if err := yaml.Unmarshal([]byte(`
fuse_defer_flush_to_release: true
fuse_async_release: true
fuse_writeback_cache: true
fuse_skip_access: true
`), &cfg); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	env := cfg.EnvMap()
	if env["fuse_defer_flush_to_release"] != "true" {
		t.Fatalf("fuse_defer_flush_to_release = %q, want true", env["fuse_defer_flush_to_release"])
	}
	if env["fuse_async_release"] != "true" {
		t.Fatalf("fuse_async_release = %q, want true", env["fuse_async_release"])
	}
	if env["fuse_writeback_cache"] != "true" {
		t.Fatalf("fuse_writeback_cache = %q, want true", env["fuse_writeback_cache"])
	}
	if env["fuse_skip_access"] != "true" {
		t.Fatalf("fuse_skip_access = %q, want true", env["fuse_skip_access"])
	}
}
