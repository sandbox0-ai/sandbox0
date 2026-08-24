package http

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
)

type staticSandboxReader struct {
	sandbox *managerapi.Sandbox
	err     error
}

func (r staticSandboxReader) ListSandboxes(context.Context, *sandboxstore.ListSandboxesRequest) (*service.ListSandboxesResponse, error) {
	return &service.ListSandboxesResponse{}, r.err
}

func (r staticSandboxReader) GetSandbox(context.Context, string) (*managerapi.Sandbox, error) {
	return r.sandbox, r.err
}

func (r staticSandboxReader) GetSandboxStatus(context.Context, string) (map[string]any, error) {
	return map[string]any{"status": r.sandbox.Status}, r.err
}
