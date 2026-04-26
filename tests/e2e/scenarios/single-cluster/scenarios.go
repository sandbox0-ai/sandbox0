package singlecluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

const singleClusterScenariosEnvVar = "E2E_SINGLE_CLUSTER_SCENARIOS"

var scenarioManifestPaths = []string{
	"single-cluster/fullmode.yaml",
	"single-cluster/minimal.yaml",
	"single-cluster/network-policy.yaml",
	"single-cluster/volumes.yaml",
}

// LoadScenarios builds single-cluster scenarios from a fixed manifest allowlist.
func LoadScenarios(cfg framework.Config) ([]framework.Scenario, error) {
	selectedPaths, err := selectScenarioManifestPaths()
	if err != nil {
		return nil, err
	}

	var scenarios []framework.Scenario
	for _, relativePath := range selectedPaths {
		manifest, err := framework.ResolveSamplePath(cfg, relativePath)
		if err != nil {
			return nil, fmt.Errorf("resolve sample path %q: %w", relativePath, err)
		}
		scenario, err := framework.BuildScenarioFromManifest(cfg, manifest)
		if err != nil {
			return nil, err
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

func selectScenarioManifestPaths() ([]string, error) {
	raw := strings.TrimSpace(os.Getenv(singleClusterScenariosEnvVar))
	if raw == "" {
		return scenarioManifestPaths, nil
	}

	requested := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		requested[name] = struct{}{}
	}
	if len(requested) == 0 {
		return nil, fmt.Errorf("%s is set but contains no scenario names", singleClusterScenariosEnvVar)
	}

	var filtered []string
	for _, relativePath := range scenarioManifestPaths {
		base := strings.TrimSuffix(filepath.Base(relativePath), filepath.Ext(relativePath))
		if _, ok := requested[base]; ok {
			filtered = append(filtered, relativePath)
		}
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("%s=%q matched no scenarios", singleClusterScenariosEnvVar, raw)
	}
	return filtered, nil
}
