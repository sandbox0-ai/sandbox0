package singlecluster

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

func TestSelectScenarioManifestPaths(t *testing.T) {
	t.Setenv(singleClusterScenariosEnvVar, "network-policy, fullmode")

	got, err := selectScenarioManifestPaths()
	if err != nil {
		t.Fatalf("selectScenarioManifestPaths returned error: %v", err)
	}

	want := []string{
		"single-cluster/fullmode.yaml",
		"single-cluster/network-policy.yaml",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectScenarioManifestPaths = %v, want %v", got, want)
	}
}

func TestSelectScenarioManifestPathsReturnsErrorForNoMatches(t *testing.T) {
	t.Setenv(singleClusterScenariosEnvVar, "does-not-exist")

	if _, err := selectScenarioManifestPaths(); err == nil {
		t.Fatal("selectScenarioManifestPaths returned nil error, want error")
	}
}

func TestLoadScenariosUsesManifestInfraNamespace(t *testing.T) {
	t.Setenv(singleClusterScenariosEnvVar, "minimal,volumes")

	cfg := framework.Config{
		OperatorChartPath: filepath.Join("..", "..", "..", "..", "infra-operator", "chart"),
		InfraNamespace:    "sandbox0-system",
	}
	scenarios, err := LoadScenarios(cfg)
	if err != nil {
		t.Fatalf("LoadScenarios returned error: %v", err)
	}
	if len(scenarios) != 2 {
		t.Fatalf("expected 2 scenarios, got %d", len(scenarios))
	}

	for i, scenario := range scenarios {
		if scenario.InfraNamespace != "sandbox0-system" {
			t.Fatalf("scenario %d namespace = %q, want sandbox0-system", i, scenario.InfraNamespace)
		}
		for _, rollout := range scenario.Rollouts {
			if rollout.Namespace != scenario.InfraNamespace {
				t.Fatalf("rollout namespace = %q, want %q", rollout.Namespace, scenario.InfraNamespace)
			}
		}
	}
}
