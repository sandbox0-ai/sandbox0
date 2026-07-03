package main

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"go.uber.org/zap"
)

func TestInitSandboxObservabilityDisabled(t *testing.T) {
	db, repo, err := initSandboxObservability(context.Background(), &config.ClusterGatewayConfig{}, zap.NewNop())
	if err != nil {
		t.Fatalf("initSandboxObservability() error = %v", err)
	}
	if db != nil || repo != nil {
		t.Fatalf("db=%v repo=%v, want disabled nils", db, repo)
	}
}

func TestInitSandboxObservabilityClickHouseRequiresDSN(t *testing.T) {
	_, _, err := initSandboxObservability(context.Background(), &config.ClusterGatewayConfig{
		SandboxObservability: config.SandboxObservabilityConfig{
			Backend: config.SandboxObservabilityBackendClickHouse,
		},
	}, zap.NewNop())
	if err == nil {
		t.Fatal("initSandboxObservability() error = nil, want missing DSN error")
	}
}
