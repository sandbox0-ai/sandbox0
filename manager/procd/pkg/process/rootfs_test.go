package process

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCommandInRootUsesRootPathPATH(t *testing.T) {
	root := t.TempDir()
	binDir := filepath.Join(root, "custom", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "python"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write executable: %v", err)
	}

	got := ResolveCommandInRoot(root, "python", []string{"PATH=/custom/bin:/usr/bin"})
	if got != "/custom/bin/python" {
		t.Fatalf("ResolveCommandInRoot() = %q, want /custom/bin/python", got)
	}
}

func TestResolveCommandInRootKeepsAbsoluteCommand(t *testing.T) {
	got := ResolveCommandInRoot(t.TempDir(), "/bin/sh", nil)
	if got != "/bin/sh" {
		t.Fatalf("ResolveCommandInRoot() = %q, want /bin/sh", got)
	}
}
