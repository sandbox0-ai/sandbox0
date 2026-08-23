package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
)

type fakeSandboxClaimer struct{}

func (fakeSandboxClaimer) ClaimSandbox(context.Context, *service.ClaimRequest) (*service.ClaimResponse, error) {
	return nil, nil
}

func (fakeSandboxClaimer) TerminateSandbox(context.Context, string) error { return nil }

func (fakeSandboxClaimer) PauseSandboxAndWait(context.Context, string) (*service.PauseSandboxResponse, error) {
	return nil, nil
}

func (fakeSandboxClaimer) ResumeSandboxAndWait(context.Context, string) (*managerapi.ResumeSandboxResponse, error) {
	return nil, nil
}

func (fakeSandboxClaimer) ForkSandbox(
	context.Context,
	string,
	string,
	string,
	*service.ForkSandboxRequest,
) (*service.ForkSandboxResponse, error) {
	return nil, nil
}

func (fakeSandboxClaimer) PauseSandboxByID(context.Context, string) error { return nil }

func (fakeSandboxClaimer) CompletePausingSandboxRuntime(context.Context, string) error { return nil }

func (fakeSandboxClaimer) ResumePausedSandboxRuntime(context.Context, string) (*managerapi.Sandbox, error) {
	return nil, nil
}

func (fakeSandboxClaimer) SetPauseEnqueuer(service.SandboxPauseEnqueuer) {}

func TestBuildSandboxRuntimeBackendSelectsExplicitBackend(t *testing.T) {
	fallback := fakeSandboxClaimer{}
	claimer, err := buildSandboxRuntimeBackend(&config.ManagerConfig{
		SandboxRuntimeBackend: config.SandboxRuntimeBackendKubernetes,
	}, sandboxRuntimeBackendDependencies{kubernetes: fallback})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := claimer.(fakeSandboxClaimer); !ok {
		t.Fatalf("Kubernetes claimer type = %T", claimer)
	}

	_, err = buildSandboxRuntimeBackend(&config.ManagerConfig{SandboxRuntimeBackend: "containerd"}, sandboxRuntimeBackendDependencies{})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unknown backend error = %v", err)
	}
}

func TestBuildNomadSandboxRuntimeBackendFailsClosedBeforeLoadingAssets(t *testing.T) {
	cfg := &config.ManagerConfig{SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad}
	if _, err := buildSandboxRuntimeBackend(cfg, sandboxRuntimeBackendDependencies{}); err == nil || !strings.Contains(err.Error(), "node authority") {
		t.Fatalf("disabled authority error = %v", err)
	}
	cfg.NodeAuthority.Enabled = true
	if _, err := buildSandboxRuntimeBackend(cfg, sandboxRuntimeBackendDependencies{}); err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("disabled terminal error = %v", err)
	}
	cfg.NodeAuthority.Terminal.Enabled = true
	cfg.NodeAuthority.Claim.SecretName = "nomad-claim"
	cfg.NodeAuthority.Claim.ClassCatalogFile = "/tmp/classes.json"
	cfg.NodeAuthority.Claim.WriterTokenKeyFile = "/tmp/key"
	if _, err := buildSandboxRuntimeBackend(cfg, sandboxRuntimeBackendDependencies{}); err == nil || !strings.Contains(err.Error(), "operator-pinned") {
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

var _ service.SandboxRuntimeBackend = fakeSandboxClaimer{}
