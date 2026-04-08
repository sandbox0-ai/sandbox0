package service

import "context"

// SandboxPowerExecutor executes pause and resume transitions for a sandbox.
// The default implementation stays manager-local today and will be replaced by ctld later.
type SandboxPowerExecutor interface {
	Pause(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error)
	Resume(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error)
}

type localSandboxPowerExecutor struct {
	service *SandboxService
}

func newLocalSandboxPowerExecutor(service *SandboxService) SandboxPowerExecutor {
	return &localSandboxPowerExecutor{service: service}
}

func (e *localSandboxPowerExecutor) Pause(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	return e.service.pauseSandboxLocal(ctx, sandboxID)
}

func (e *localSandboxPowerExecutor) Resume(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	return e.service.resumeSandboxLocal(ctx, sandboxID)
}
