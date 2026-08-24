package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func TestBuildSandboxRuntimeFailsClosedBeforeLoadingAssets(t *testing.T) {
	cfg := &config.ManagerConfig{}
	if _, err := buildSandboxRuntime(cfg, sandboxRuntimeBackendDependencies{}); err == nil || !strings.Contains(err.Error(), "node authority") {
		t.Fatalf("disabled authority error = %v", err)
	}
	cfg.NodeAuthority.Enabled = true
	if _, err := buildSandboxRuntime(cfg, sandboxRuntimeBackendDependencies{}); err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("disabled terminal error = %v", err)
	}
	cfg.NodeAuthority.Terminal.Enabled = true
	cfg.NodeAuthority.Claim.ClassCatalogFile = "/tmp/classes.json"
	cfg.NodeAuthority.Claim.WriterTokenKeyFile = "/tmp/key"
	if _, err := buildSandboxRuntime(cfg, sandboxRuntimeBackendDependencies{}); err == nil || !strings.Contains(err.Error(), "deployment-pinned") {
		t.Fatalf("unpinned asset path error = %v", err)
	}
}

func TestLoadWriterTokenKeyRequiresExactBinaryLength(t *testing.T) {
	dir := t.TempDir()
	for name, payload := range map[string][]byte{
		"short":   make([]byte, 31),
		"newline": append(make([]byte, 32), '\n'),
		"long":    make([]byte, 33),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadWriterTokenKey(path); err == nil {
				t.Fatal("invalid writer token key was accepted")
			}
		})
	}

	want := []byte("0123456789abcdef0123456789abcdef")
	path := filepath.Join(dir, "valid")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadWriterTokenKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("writer token key = %q", got)
	}
}

func TestLoadWriterTokenKeyRejectsDirectories(t *testing.T) {
	if _, err := loadWriterTokenKey(t.TempDir()); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("directory error = %v", err)
	}
}
