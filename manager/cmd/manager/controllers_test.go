package main

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"go.uber.org/zap"
)

func TestManagerControllerSetNomadDoesNotStartKubernetesRuntimeControllers(t *testing.T) {
	cfg := &config.ManagerConfig{}
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

func TestManagerControllerSetDoesNotStartAbsentTemplateReconciler(t *testing.T) {
	controllers := &managerControllerSet{
		cfg:    &config.ManagerConfig{},
		logger: zap.NewNop(),
	}
	controllers.cfg.RootFSMaintenance.Disabled = true

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	requireReturnsWithin(t, time.Second, func() {
		controllers.Start(ctx)
	})
}

func TestNomadManagerNetworkComponentsDoNotRequireKubernetesRuntime(t *testing.T) {
	components := buildNomadManagerNetworkComponents(zap.NewNop())
	if components.policyService == nil {
		t.Fatalf("Nomad network components = %+v", components)
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
