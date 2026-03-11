package http

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
)

type schedulerCluster struct {
	ClusterID          string `json:"cluster_id"`
	InternalGatewayURL string `json:"internal_gateway_url"`
	Enabled            bool   `json:"enabled"`
}

type schedulerClusterListResponse struct {
	Clusters []schedulerCluster `json:"clusters"`
	Count    int                `json:"count"`
}

func (s *Server) getInternalGatewayProxy(targetURL string) (*proxy.Router, error) {
	s.internalGatewayProxiesMu.RLock()
	p := s.internalGatewayProxies[targetURL]
	s.internalGatewayProxiesMu.RUnlock()
	if p != nil {
		return p, nil
	}

	s.internalGatewayProxiesMu.Lock()
	defer s.internalGatewayProxiesMu.Unlock()
	p = s.internalGatewayProxies[targetURL]
	if p != nil {
		return p, nil
	}

	p, err := proxy.NewRouter(targetURL, s.logger, s.cfg.ProxyTimeout.Duration)
	if err != nil {
		return nil, err
	}
	s.internalGatewayProxies[targetURL] = p
	return p, nil
}

func (s *Server) getInternalGatewayURLForCluster(ctx context.Context, clusterID string, authCtx *authn.AuthContext) (string, error) {
	if clusterID == "" {
		return "", fmt.Errorf("cluster_id is required")
	}
	if authCtx == nil {
		return "", fmt.Errorf("missing auth context")
	}
	if url := s.getClusterFromCache(clusterID); url != "" {
		return url, nil
	}
	if err := s.refreshClusterCache(ctx, authCtx); err != nil {
		return "", err
	}
	return s.getClusterFromCache(clusterID), nil
}

func (s *Server) getClusterFromCache(clusterID string) string {
	s.clusterCacheMu.RLock()
	defer s.clusterCacheMu.RUnlock()
	return s.clusterCache[clusterID]
}

func (s *Server) refreshClusterCache(ctx context.Context, authCtx *authn.AuthContext) error {
	s.clusterCacheMu.RLock()
	cacheAge := time.Since(s.clusterCacheAt)
	s.clusterCacheMu.RUnlock()
	if cacheAge <= s.cfg.ClusterCacheTTL.Duration {
		return nil
	}

	if s.cfg.SchedulerURL == "" {
		return fmt.Errorf("scheduler_url is not configured")
	}

	reqURL, err := s.buildSchedulerClustersURL()
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("create scheduler request: %w", err)
	}

	token, err := s.internalAuthGen.Generate(
		"scheduler",
		authCtx.TeamID,
		authCtx.UserID,
		internalauth.GenerateOptions{
			Permissions: authCtx.Permissions,
		},
	)
	if err != nil {
		return fmt.Errorf("generate scheduler token: %w", err)
	}

	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("X-Team-ID", authCtx.TeamID)
	if authCtx.UserID != "" {
		req.Header.Set("X-User-ID", authCtx.UserID)
	}
	req.Header.Set("X-Auth-Method", string(authCtx.AuthMethod))

	client := &http.Client{Timeout: s.cfg.ProxyTimeout.Duration}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("list clusters: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list clusters failed: %s", resp.Status)
	}

	result, apiErr, err := spec.DecodeResponse[schedulerClusterListResponse](resp.Body)
	if err != nil {
		return fmt.Errorf("decode clusters: %w", err)
	}
	if apiErr != nil {
		return fmt.Errorf("list clusters failed: %s", apiErr.Message)
	}

	cache := make(map[string]string, len(result.Clusters))
	for _, cluster := range result.Clusters {
		if !cluster.Enabled || cluster.InternalGatewayURL == "" || cluster.ClusterID == "" {
			continue
		}
		cache[cluster.ClusterID] = cluster.InternalGatewayURL
	}

	s.clusterCacheMu.Lock()
	s.clusterCache = cache
	s.clusterCacheAt = time.Now()
	s.clusterCacheMu.Unlock()
	return nil
}

func (s *Server) buildSchedulerClustersURL() (string, error) {
	base, err := url.Parse(s.cfg.SchedulerURL)
	if err != nil {
		return "", fmt.Errorf("parse scheduler_url: %w", err)
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/v1/clusters"
	q := base.Query()
	q.Set("enabled", "true")
	base.RawQuery = q.Encode()
	return base.String(), nil
}

func (s *Server) generateInternalToken(c *gin.Context, authCtx *authn.AuthContext, target string) (string, error) {
	if s.internalAuthGen == nil {
		return "", fmt.Errorf("internal auth generator not configured")
	}
	if authCtx == nil {
		return "", fmt.Errorf("missing auth context")
	}
	return s.internalAuthGen.Generate(
		target,
		authCtx.TeamID,
		authCtx.UserID,
		internalauth.GenerateOptions{
			Permissions: authCtx.Permissions,
		},
	)
}
