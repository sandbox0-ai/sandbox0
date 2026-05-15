package http

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

type schedulerCluster struct {
	ClusterID         string `json:"cluster_id"`
	ClusterGatewayURL string `json:"cluster_gateway_url"`
	Enabled           bool   `json:"enabled"`
}

type schedulerClusterListResponse struct {
	Clusters []schedulerCluster `json:"clusters"`
	Count    int                `json:"count"`
}

func (s *Server) defaultClusterGatewayURL() string {
	return strings.TrimRight(strings.TrimSpace(s.cfg.DefaultClusterGatewayURL), "/")
}

func (s *Server) schedulerURL() string {
	return strings.TrimRight(strings.TrimSpace(s.cfg.SchedulerURL), "/")
}

func (s *Server) clusterGatewayURLForSandbox(ctx context.Context, sandboxID string) (string, error) {
	defaultURL := s.defaultClusterGatewayURL()
	if s.schedulerURL() == "" {
		if defaultURL == "" {
			return "", fmt.Errorf("cluster gateway is not configured")
		}
		return defaultURL, nil
	}
	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		if defaultURL == "" {
			return "", err
		}
		return defaultURL, nil
	}
	clusters, err := s.listSchedulerClusters(ctx)
	if err != nil {
		return "", err
	}
	for _, cluster := range clusters {
		if cluster.Enabled && cluster.ClusterID == parsed.ClusterID && strings.TrimSpace(cluster.ClusterGatewayURL) != "" {
			return strings.TrimRight(strings.TrimSpace(cluster.ClusterGatewayURL), "/"), nil
		}
	}
	return "", fmt.Errorf("cluster %q not found", parsed.ClusterID)
}

func (s *Server) listSchedulerClusters(ctx context.Context) ([]schedulerCluster, error) {
	if s.internalAuthGen == nil {
		return nil, fmt.Errorf("internal auth generator is not configured")
	}
	base, err := url.Parse(s.schedulerURL())
	if err != nil {
		return nil, err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/v1/clusters"
	q := base.Query()
	q.Set("enabled", "true")
	base.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, err
	}
	token, err := s.internalAuthGen.GenerateSystem(internalauth.ServiceScheduler, internalauth.GenerateOptions{})
	if err != nil {
		return nil, fmt.Errorf("generate scheduler token: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list clusters failed: %s", resp.Status)
	}
	result, apiErr, err := spec.DecodeResponse[schedulerClusterListResponse](resp.Body)
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, fmt.Errorf("list clusters failed: %s", apiErr.Message)
	}
	if result == nil {
		return nil, nil
	}
	return result.Clusters, nil
}
