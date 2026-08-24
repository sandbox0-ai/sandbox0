package registry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCredentialValueReadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(path, []byte("file-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := credentialValue("", path, "test credential")
	if err != nil {
		t.Fatal(err)
	}
	if value != "file-token" {
		t.Fatalf("credentialValue() = %q, want file-token", value)
	}
}

func TestCredentialValueRejectsAmbiguousSources(t *testing.T) {
	if _, err := credentialValue("inline", "/tmp/credential", "test credential"); err == nil {
		t.Fatal("credentialValue() accepted both inline and file sources")
	}
}

func TestCredentialValueReturnsFileReadError(t *testing.T) {
	if _, err := credentialValue("", "/does/not/exist", "test credential"); err == nil {
		t.Fatal("credentialValue() ignored a file read error")
	}
}
