package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStoreBindSandboxRecreatesStateDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sessions")
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}

	bound, err := store.BindSandbox("sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if !bound {
		t.Fatal("BindSandbox() reported an identity conflict for a recreated store")
	}
	data, err := os.ReadFile(filepath.Join(root, sandboxIDFile))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != "sandbox-a" {
		t.Fatalf("sandbox identity = %q, want sandbox-a", data)
	}
}
