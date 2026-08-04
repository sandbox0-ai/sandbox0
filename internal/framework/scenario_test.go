package framework

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestBuildScenarioFromManifestIncludesGlobalGatewayRolloutAndSecrets(t *testing.T) {
	cfg := Config{
		OperatorChartPath: filepath.Join("..", "..", "infra-operator", "chart"),
		InfraNamespace:    "sandbox0-system",
	}
	manifestPath := filepath.Join(cfg.OperatorChartPath, "samples", "multi-cluster", "global-service.yaml")

	scenario, err := BuildScenarioFromManifest(cfg, manifestPath)
	if err != nil {
		t.Fatalf("BuildScenarioFromManifest returned error: %v", err)
	}

	if scenario.Name != filepath.Join("multi-cluster", "global-service") {
		t.Fatalf("unexpected scenario name: %q", scenario.Name)
	}
	if len(scenario.Rollouts) != 1 {
		t.Fatalf("expected 1 rollout target, got %d", len(scenario.Rollouts))
	}
	rollout := scenario.Rollouts[0]
	if rollout.Kind != "deployment" || rollout.Name != "s0global-global-gateway" {
		t.Fatalf("unexpected rollout target: %#v", rollout)
	}
	if rollout.Namespace != "sandbox0-system" {
		t.Fatalf("unexpected rollout namespace: %q", rollout.Namespace)
	}

	if len(scenario.Secrets) != 1 {
		t.Fatalf("expected external database secret only, got %d", len(scenario.Secrets))
	}
	if scenario.Secrets[0].Name != "db-credentials" {
		t.Fatalf("unexpected secret fixture: %#v", scenario.Secrets[0])
	}
}

func TestRewriteManifestNamespace(t *testing.T) {
	manifest := `apiVersion: v1
kind: Namespace
metadata:
  name: sandbox0-system
---
apiVersion: infra.sandbox0.ai/v1alpha1
kind: Sandbox0Infra
metadata:
  name: fullmode-fixture
  namespace: sandbox0-system
spec:
  services:
    manager:
      enabled: true
`
	manifestPath := filepath.Join(t.TempDir(), "fullmode-fixture.yaml")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	rewrittenPath, cleanup, err := RewriteManifestNamespace(manifestPath, "sandbox0-system-fullmode")
	if err != nil {
		t.Fatalf("RewriteManifestNamespace returned error: %v", err)
	}
	defer cleanup()
	if rewrittenPath == manifestPath {
		t.Fatal("RewriteManifestNamespace returned original path, want rewritten manifest")
	}

	content, err := os.ReadFile(rewrittenPath)
	if err != nil {
		t.Fatalf("read rewritten manifest: %v", err)
	}
	got := string(content)
	if !strings.Contains(got, "name: sandbox0-system-fullmode") {
		t.Fatalf("rewritten manifest missing namespace resource rename:\n%s", got)
	}
	if !strings.Contains(got, "namespace: sandbox0-system-fullmode") {
		t.Fatalf("rewritten manifest missing metadata namespace rewrite:\n%s", got)
	}
	if strings.Contains(got, "sandbox0-system\n") {
		t.Fatalf("rewritten manifest still contains source namespace:\n%s", got)
	}
}

func TestBuildScenarioRolloutsUsesConsolidatedDataPlaneWorkloads(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{Runtime: &infrav1alpha1.StorageProxyConfig{}},
			Network: &infrav1alpha1.NetworkConfig{Config: &infrav1alpha1.NetdConfig{}},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	got := buildScenarioRollouts(infra, "s0", "sandbox0-system")
	want := []RolloutTarget{
		{Kind: "deployment", Name: "s0-manager", Namespace: "sandbox0-system", Timeout: "5m"},
		{Kind: "daemonset", Name: "s0-ctld-a", Namespace: "sandbox0-system", Timeout: "5m"},
		{Kind: "daemonset", Name: "s0-ctld-b", Namespace: "sandbox0-system", Timeout: "5m"},
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected rollout count: got %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected rollout %d: got %#v, want %#v", i, got[i], want[i])
		}
	}
}
