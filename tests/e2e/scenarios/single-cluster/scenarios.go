package singlecluster

import (
	"fmt"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

var scenarioManifestPaths = []string{
	"single-cluster/fullmode.yaml",
	"single-cluster/minimal.yaml",
	"single-cluster/network-policy.yaml",
	"single-cluster/volumes.yaml",
}

// LoadScenarios builds single-cluster scenarios from a fixed manifest allowlist.
func LoadScenarios(cfg framework.Config) ([]framework.Scenario, error) {
	var scenarios []framework.Scenario
	for _, relativePath := range scenarioManifestPaths {
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
