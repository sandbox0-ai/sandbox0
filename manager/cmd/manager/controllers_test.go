package main

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"go.uber.org/zap"
)

func TestManagerControllerSetNomadDoesNotStartKubernetesRuntimeControllers(t *testing.T) {
	cfg := &config.ManagerConfig{SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad}
	cfg.RootFSMaintenance.Disabled = true
	controllers := &managerControllerSet{
		cfg: cfg, logger: zap.NewNop(),
		sandboxPauseController: service.NewSandboxPauseController(nil, nil, zap.NewNop()),
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	requireReturnsWithin(t, time.Second, func() {
		controllers.Start(ctx)
	})
}

func TestManagerUsesKubernetesSandboxRuntimeOnlyForLegacyBackend(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.ManagerConfig
		want bool
	}{
		{name: "nil config", want: true},
		{name: "legacy default", cfg: &config.ManagerConfig{}, want: true},
		{name: "kubernetes", cfg: &config.ManagerConfig{SandboxRuntimeBackend: config.SandboxRuntimeBackendKubernetes}, want: true},
		{name: "nomad", cfg: &config.ManagerConfig{SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad}, want: false},
		{name: "unsupported", cfg: &config.ManagerConfig{SandboxRuntimeBackend: "containerd"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := managerUsesKubernetesSandboxRuntime(test.cfg); got != test.want {
				t.Fatalf("managerUsesKubernetesSandboxRuntime() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestNomadManagerNetworkComponentsDoNotRequireKubernetesRuntime(t *testing.T) {
	components := buildNomadManagerNetworkComponents(zap.NewNop())
	if components.policyService == nil || components.provider == nil {
		t.Fatalf("Nomad network components = %+v", components)
	}
	if components.namespacePolicy != nil {
		t.Fatal("Nomad network components unexpectedly configured a Kubernetes namespace reconciler")
	}
}

func requireReturnsWithin(t *testing.T, timeout time.Duration, run func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		run()
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("operation did not return before timeout")
	}
}
