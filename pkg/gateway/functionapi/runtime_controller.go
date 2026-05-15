package functionapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type HTTPRuntimeController struct {
	resolveClusterGatewayURL ClusterGatewayURLResolver
	internalAuthGen          *internalauth.Generator
	httpClient               *http.Client
}

func NewHTTPRuntimeController(resolve ClusterGatewayURLResolver, internalAuthGen *internalauth.Generator, httpClient *http.Client) *HTTPRuntimeController {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &HTTPRuntimeController{
		resolveClusterGatewayURL: resolve,
		internalAuthGen:          internalAuthGen,
		httpClient:               httpClient,
	}
}

func (c *HTTPRuntimeController) DeleteRuntimeSandbox(ctx context.Context, authCtx *authn.AuthContext, sandboxID string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}
	if c == nil || c.resolveClusterGatewayURL == nil {
		return fmt.Errorf("runtime controller is not configured")
	}
	if c.internalAuthGen == nil {
		return fmt.Errorf("internal auth generator is not configured")
	}
	clusterGatewayURL, err := c.resolveClusterGatewayURL(ctx, sandboxID)
	if err != nil {
		return err
	}
	clusterGatewayURL = strings.TrimRight(strings.TrimSpace(clusterGatewayURL), "/")
	if clusterGatewayURL == "" {
		return fmt.Errorf("cluster gateway is not configured")
	}
	teamID := ""
	userID := ""
	if authCtx != nil {
		teamID = authCtx.TeamID
		userID = principalID(authCtx)
	}
	token, err := c.internalAuthGen.Generate(internalauth.ServiceClusterGateway, teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxDelete, authn.PermSandboxRead},
	})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, clusterGatewayURL+"/api/v1/sandboxes/"+url.PathEscape(sandboxID), nil)
	if err != nil {
		return err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("delete runtime sandbox %s: %s: %s", sandboxID, resp.Status, strings.TrimSpace(string(body)))
}
