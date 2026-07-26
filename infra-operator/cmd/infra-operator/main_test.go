package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/labels"
)

func TestManagedPodCacheSelectorExcludesRuntimeSandboxes(t *testing.T) {
	selector := managedPodCacheSelector()
	if !selector.Matches(labels.Set{
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
		"app.kubernetes.io/component":  "ctld",
	}) {
		t.Fatal("selector should include infra-operator managed Pods")
	}
	if selector.Matches(labels.Set{
		"sandbox0.ai/template-id": "default",
		"sandbox0.ai/state":       "idle",
	}) {
		t.Fatal("selector should exclude runtime sandbox Pods")
	}
}
