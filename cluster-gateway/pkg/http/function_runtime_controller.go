package http

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
)

type localRuntimeController struct {
	manager *client.ManagerClient
}

func newLocalRuntimeController(manager *client.ManagerClient) *localRuntimeController {
	return &localRuntimeController{manager: manager}
}

func (c *localRuntimeController) DeleteRuntimeSandbox(ctx context.Context, authCtx *authn.AuthContext, sandboxID string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}
	if c == nil || c.manager == nil {
		return fmt.Errorf("manager client is not configured")
	}
	if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
		return fmt.Errorf("team context is required")
	}
	principal := authCtx.Principal()
	return c.manager.TerminateSandbox(ctx, sandboxID, principal.UserID, authCtx.TeamID)
}
