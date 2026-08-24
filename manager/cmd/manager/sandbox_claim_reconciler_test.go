package main

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func TestConfigureSandboxClaimReconcilerRequiresStore(t *testing.T) {
	worker, err := configureSandboxClaimReconciler(&config.ManagerConfig{}, nil)
	if err == nil || worker != nil {
		t.Fatalf("worker = %v, %v", worker, err)
	}
}
