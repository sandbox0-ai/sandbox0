package http

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

// SandboxTemplateSourceClient reads source metadata from one cluster gateway.
type SandboxTemplateSourceClient interface {
	GetSandboxTemplateSource(ctx context.Context, baseURL, sandboxID, teamID, userID string, permissions []string) (*template.SandboxTemplateSource, error)
}

// SchedulerSandboxTemplateSourceResolver routes source lookup to the cluster
// encoded in the sandbox ID.
type SchedulerSandboxTemplateSourceResolver struct {
	clusters ClusterRepository
	client   SandboxTemplateSourceClient
}

// NewSchedulerSandboxTemplateSourceResolver creates a multi-cluster source resolver.
func NewSchedulerSandboxTemplateSourceResolver(clusters ClusterRepository, client SandboxTemplateSourceClient) *SchedulerSandboxTemplateSourceResolver {
	return &SchedulerSandboxTemplateSourceResolver{clusters: clusters, client: client}
}

// ResolveSandboxTemplateSource resolves and authorizes durable source metadata.
func (r *SchedulerSandboxTemplateSourceResolver) ResolveSandboxTemplateSource(ctx context.Context, sandboxID, teamID string) (*template.SandboxTemplateSource, error) {
	if r == nil || r.clusters == nil || r.client == nil {
		return nil, template.ErrTemplateSourceUnavailable
	}
	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		return nil, template.ErrTemplateSourceNotFound
	}
	cluster, err := r.clusters.GetCluster(ctx, parsed.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", template.ErrTemplateSourceUnavailable, err)
	}
	if cluster == nil {
		return nil, template.ErrTemplateSourceNotFound
	}
	if !cluster.Enabled {
		return nil, template.ErrTemplateSourceUnavailable
	}
	claims := internalauth.ClaimsFromContext(ctx)
	userID := ""
	var permissions []string
	if claims != nil {
		userID = claims.UserID
		permissions = claims.Permissions
	}
	source, err := r.client.GetSandboxTemplateSource(ctx, cluster.ClusterGatewayURL, sandboxID, teamID, userID, permissions)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, template.ErrTemplateSourceNotFound
	}
	if source.TeamID != teamID {
		return nil, template.ErrTemplateSourceForbidden
	}
	if source.ClusterID != parsed.ClusterID {
		return nil, fmt.Errorf("%w: source cluster mismatch", template.ErrTemplateSourceUnavailable)
	}
	return source, nil
}
