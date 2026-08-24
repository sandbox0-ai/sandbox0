package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseOptionsRequiresOneNonCLISecretSource(t *testing.T) {
	environment := map[string]string{"SANDBOX0_LEGACY_SOURCE_DSN": "postgres://source"}
	getenv := func(key string) string { return environment[key] }
	opts, err := parseOptions([]string{"-target-cluster-id", "ali-ue1-nomad"}, getenv)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if opts.targetClusterID != "ali-ue1-nomad" {
		t.Fatalf("targetClusterID = %q", opts.targetClusterID)
	}
	if _, err := parseOptions([]string{"-target-cluster-id", "ali-ue1-nomad", "-source-dsn-file", "/secret/dsn"}, getenv); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("parseOptions() conflict error = %v", err)
	}
	delete(environment, "SANDBOX0_LEGACY_SOURCE_DSN")
	if _, err := parseOptions([]string{"-target-cluster-id", "ali-ue1-nomad"}, getenv); err == nil || !strings.Contains(err.Error(), "source database DSN is required") {
		t.Fatalf("parseOptions() missing source error = %v", err)
	}
}

func TestLoadSourceDSNRequiresOwnerOnlyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source-dsn")
	if err := os.WriteFile(path, []byte("postgres://source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dsn, err := loadSourceDSN(path, "")
	if err != nil {
		t.Fatalf("loadSourceDSN() error = %v", err)
	}
	if dsn != "postgres://source" {
		t.Fatalf("DSN = %q", dsn)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSourceDSN(path, ""); err == nil || !strings.Contains(err.Error(), "owner-only") {
		t.Fatalf("loadSourceDSN() insecure file error = %v", err)
	}
}

func TestWriteAtomicOwnerOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := writeAtomicOwnerOnly(path, []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "{}\n" {
		t.Fatalf("payload = %q", payload)
	}
}
