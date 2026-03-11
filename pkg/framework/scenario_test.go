package framework

import (
	"path/filepath"
	"testing"
)

func TestBuildScenarioFromManifestIncludesGlobalDirectoryRolloutAndSecrets(t *testing.T) {
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
	if rollout.Kind != "deployment" || rollout.Name != "s0global-global-directory" {
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
