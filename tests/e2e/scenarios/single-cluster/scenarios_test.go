package singlecluster

import (
	"reflect"
	"testing"
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
