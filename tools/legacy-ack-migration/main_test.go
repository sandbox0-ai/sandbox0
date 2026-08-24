package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
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

func TestParseOptionsAppliesModeSpecificCredentialAndSafetyContracts(t *testing.T) {
	environment := map[string]string{
		"SANDBOX0_LEGACY_SOURCE_DSN":    "postgres://source",
		"SANDBOX0_MIGRATION_TARGET_DSN": "postgres://target",
	}
	getenv := func(key string) string { return environment[key] }
	common := []string{"-target-cluster-id", "ali-ue1-nomad", "-session-id", "migration-1"}

	capture, err := parseOptions(append([]string{"-mode", modeCapture}, common...), getenv)
	if err != nil {
		t.Fatalf("capture parseOptions() error = %v", err)
	}
	if capture.timeout != defaultControlTimeout {
		t.Fatalf("capture timeout = %s", capture.timeout)
	}

	delete(environment, "SANDBOX0_LEGACY_SOURCE_DSN")
	prepareArgs := append([]string{"-mode", modePrepare, "-target-manager-config-file", "/etc/sandbox0/manager.yaml"}, common...)
	if _, err := parseOptions(prepareArgs, getenv); err != nil {
		t.Fatalf("prepare must not require source DSN: %v", err)
	}
	buildArgs := append([]string{"-mode", modeBuild, "-target-manager-config-file", "/etc/sandbox0/manager.yaml"}, common...)
	build, err := parseOptions(buildArgs, getenv)
	if err != nil {
		t.Fatalf("build parseOptions() error = %v", err)
	}
	if build.sourceManagerConfigFile != build.targetManagerConfigFile || build.timeout != defaultBuildTimeout ||
		build.buildLeaseTTL != 2*time.Minute || build.buildLeaseRenewal != 30*time.Second {
		t.Fatalf("build defaults = %#v", build)
	}

	retireArgs := append([]string{"-mode", modeRetire}, common...)
	if _, err := parseOptions(retireArgs, getenv); err == nil || !strings.Contains(err.Error(), "confirm-source-catalog-digest") {
		t.Fatalf("retire without digest error = %v", err)
	}
	retireArgs = append(retireArgs, "-confirm-source-catalog-digest", digest.FromString("catalog").String())
	if _, err := parseOptions(retireArgs, getenv); err != nil {
		t.Fatalf("retire with exact digest parseOptions() error = %v", err)
	}
}

func TestParseOptionsDoesNotRequireTargetForReadOnlyInventory(t *testing.T) {
	getenv := func(key string) string {
		if key == "SANDBOX0_LEGACY_SOURCE_DSN" {
			return "postgres://source"
		}
		return ""
	}
	if _, err := parseOptions([]string{"-mode", modeInventory, "-target-cluster-id", "ali-ue1-nomad"}, getenv); err != nil {
		t.Fatalf("inventory parseOptions() error = %v", err)
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
