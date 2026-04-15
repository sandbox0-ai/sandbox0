package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
)

var errInternalTokenTeamIDRequired = errors.New("team_id is required for team-scoped internal token")

type internalTokenOptions struct {
	systemScopeForSystemAdmin bool
}

type schedulerCluster struct {
	ClusterID         string `json:"cluster_id"`
	ClusterGatewayURL string `json:"cluster_gateway_url"`
	Enabled           bool   `json:"enabled"`
}

type schedulerClusterListResponse struct {
	Clusters []schedulerCluster `json:"clusters"`
	Count    int                `json:"count"`
}

func (s *Server) getClusterGatewayProxy(targetURL string) (*proxy.Router, error) {
	s.clusterGatewayProxiesMu.RLock()
	p := s.clusterGatewayProxies[targetURL]
	s.clusterGatewayProxiesMu.RUnlock()
	if p != nil {
		return p, nil
	}

	s.clusterGatewayProxiesMu.Lock()
	defer s.clusterGatewayProxiesMu.Unlock()
	p = s.clusterGatewayProxies[targetURL]
	if p != nil {
		return p, nil
	}

	p, err := proxy.NewRouter(targetURL, s.logger, s.cfg.ProxyTimeout.Duration)
	if err != nil {
		return nil, err
	}
	s.clusterGatewayProxies[targetURL] = p
	return p, nil
}

func (s *Server) getClusterGatewayURLForCluster(ctx context.Context, clusterID string, authCtx *authn.AuthContext) (string, error) {
	if clusterID == "" {
		return "", fmt.Errorf("cluster_id is required")
	}
	if authCtx == nil {
		authCtx = &authn.AuthContext{AuthMethod: authn.AuthMethodInternal}
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
	req, cancel := proxy.ApplyRequestTimeout(req, s.cfg.ProxyTimeout.Duration)
	defer cancel()

	token, err := s.generateInternalToken(authCtx, "scheduler")
	if err != nil {
		return fmt.Errorf("generate scheduler token: %w", err)
	}

	req.Header.Set(internalauth.DefaultTokenHeader, token)
	if authCtx.TeamID != "" {
		req.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
	}
	if authCtx.UserID != "" {
		req.Header.Set(internalauth.UserIDHeader, authCtx.UserID)
	}
	req.Header.Set("X-Auth-Method", string(authCtx.AuthMethod))

	client := &http.Client{}
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
		if !cluster.Enabled || cluster.ClusterGatewayURL == "" || cluster.ClusterID == "" {
			continue
		}
		cache[cluster.ClusterID] = cluster.ClusterGatewayURL
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

func (s *Server) generateInternalToken(authCtx *authn.AuthContext, target string) (string, error) {
	return s.generateInternalTokenWithOptions(authCtx, target, internalTokenOptions{})
}

func (s *Server) generateTemplateInternalToken(authCtx *authn.AuthContext, target string) (string, error) {
	return s.generateInternalTokenWithOptions(authCtx, target, internalTokenOptions{
		systemScopeForSystemAdmin: true,
	})
}

func (s *Server) generateInternalTokenWithOptions(authCtx *authn.AuthContext, target string, opts internalTokenOptions) (string, error) {
	if s.internalAuthGen == nil {
		return "", fmt.Errorf("internal auth generator not configured")
	}
	if authCtx == nil {
		return s.internalAuthGen.GenerateSystem(target, internalauth.GenerateOptions{})
	}
	generateOpts := internalauth.GenerateOptions{
		UserID:      authCtx.UserID,
		Permissions: authCtx.Permissions,
	}
	teamID := strings.TrimSpace(authCtx.TeamID)
	if shouldGenerateSystemInternalToken(authCtx, teamID, opts) {
		return s.internalAuthGen.GenerateSystem(
			target,
			generateOpts,
		)
	}
	if teamID == "" {
		return "", errInternalTokenTeamIDRequired
	}
	return s.internalAuthGen.Generate(
		target,
		teamID,
		authCtx.UserID,
		generateOpts,
	)
}

func shouldGenerateSystemInternalToken(authCtx *authn.AuthContext, teamID string, opts internalTokenOptions) bool {
	if authCtx.AuthMethod == authn.AuthMethodInternal && teamID == "" {
		return true
	}
	if !authCtx.IsSystemAdmin {
		return false
	}
	if teamID == "" {
		return true
	}
	return opts.systemScopeForSystemAdmin && authCtx.AuthMethod == authn.AuthMethodAPIKey
}
