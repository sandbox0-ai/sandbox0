package http

import "github.com/sandbox0-ai/sandbox0/manager/pkg/service"

func newHTTPTestServerWithSandboxService(sandboxService *service.SandboxService) *Server {
	return &Server{
		sandboxReader:         sandboxService,
		sandboxUpdater:        sandboxService,
		sandboxNetworkPolicy:  sandboxService,
		sandboxRootFS:         sandboxService,
		sandboxSourceResolver: sandboxService,
		sandboxClaimer:        sandboxService,
		sandboxTerminator:     sandboxService,
		sandboxPauser:         sandboxService,
		sandboxResumer:        sandboxService,
		sandboxForker:         sandboxService,
	}
}
