package singlecluster

import "github.com/sandbox0-ai/sandbox0/pkg/framework"

// LoadScenarios discovers and builds scenarios for single-cluster samples.
func LoadScenarios(cfg framework.Config) ([]framework.Scenario, error) {
	manifests, err := framework.DiscoverSampleManifests(cfg, "single-cluster")
	if err != nil {
		return nil, err
	}

	var scenarios []framework.Scenario
	for _, manifest := range manifests {
		scenario, err := framework.BuildScenarioFromManifest(cfg, manifest)
		if err != nil {
			return nil, err
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}
