package main

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestConfigureSandboxClaimReconcilerIsRequiredOnlyForNomad(t *testing.T) {
	worker, err := configureSandboxClaimReconciler(&config.ManagerConfig{
		SandboxRuntimeBackend: config.SandboxRuntimeBackendKubernetes,
	}, nil)
	if err != nil || worker != nil {
		t.Fatalf("Kubernetes worker = %v, %v", worker, err)
	}
	worker, err = configureSandboxClaimReconciler(&config.ManagerConfig{
		SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad,
	}, nil)
	if err == nil || worker != nil {
		t.Fatalf("Nomad worker = %v, %v", worker, err)
	}
}
