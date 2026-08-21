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
