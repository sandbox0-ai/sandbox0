package http

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/schedulerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
)

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

	p, err := proxy.NewRouter(targetURL, s.logger, s.cfg.ProxyTimeout.Duration, proxy.WithHTTPClient(s.outboundHTTPClient()))
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

	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return fmt.Errorf("list clusters: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list clusters failed: %s", resp.Status)
	}

	result, apiErr, err := spec.DecodeResponse[schedulerapi.ListClustersResponse](resp.Body)
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
	return s.generateInternalTokenWithProof(authCtx, target, nil)
}

func (s *Server) generateForwardingInternalToken(
	authCtx *authn.AuthContext,
	target string,
	request *http.Request,
) (string, error) {
	if authCtx == nil || authCtx.TeamID == "" || request == nil {
		return s.generateInternalToken(authCtx, target)
	}
	keys := gatewayteamquota.AdmittedKeys(request.Context())
	if len(keys) == 0 {
		return s.generateInternalToken(authCtx, target)
	}
	if s.teamQuotaController == nil {
		return "", &coreteamquota.UnavailableError{
			Operation: "read forwarding admission proof version",
			Err:       fmt.Errorf("team quota controller is not configured"),
		}
	}
	version, err := s.teamQuotaController.AdmissionProofVersion(request.Context())
	if err != nil {
		return "", fmt.Errorf("read forwarding admission proof version: %w", err)
	}
	audit := delegatedAuditContext(authCtx, internalauth.ServiceRegionalGateway)
	proof, err := internalauth.NewQuotaAdmissionProof(
		internalauth.QuotaAdmissionClassEdgeAdmitted,
		request,
		authCtx.TeamID,
		audit.OperationID,
		audit.RequestID,
		internalauth.ServiceRegionalGateway,
		keys,
		version,
	)
	if err != nil {
		return "", fmt.Errorf("build quota admission proof: %w", err)
	}
	return s.generateInternalTokenWithProof(authCtx, target, proof)
}

func (s *Server) abortForwardingTokenError(
	ginCtx *gin.Context,
	target string,
	err error,
) {
	ginCtx.Abort()
	if s != nil && s.logger != nil {
		s.logger.Error(
			"Failed to generate forwarding internal token",
			zap.String("target", target),
			zap.Error(err),
		)
	}
	if coreteamquota.IsUnavailable(err) {
		ginCtx.Header("Retry-After", "1")
		spec.JSONError(
			ginCtx,
			http.StatusServiceUnavailable,
			spec.CodeUnavailable,
			"team quota admission proof unavailable",
		)
		return
	}
	spec.JSONError(
		ginCtx,
		http.StatusInternalServerError,
		spec.CodeInternal,
		"internal authentication failed",
	)
}

func (s *Server) generateInternalTokenWithProof(
	authCtx *authn.AuthContext,
	target string,
	proof *internalauth.QuotaAdmissionProof,
) (string, error) {
	if s.internalAuthGen == nil {
		return "", fmt.Errorf("internal auth generator not configured")
	}
	if authCtx == nil {
		return s.internalAuthGen.GenerateSystem(target, internalauth.GenerateOptions{})
	}
	if authCtx.TeamID == "" {
		return s.internalAuthGen.GenerateSystem(
			target,
			internalauth.GenerateOptions{
				Permissions: authCtx.Permissions,
				Audit: delegatedAuditContext(
					authCtx,
					internalauth.ServiceRegionalGateway,
				),
				QuotaAdmissionProof: proof,
			},
		)
	}
	return s.internalAuthGen.Generate(
		target,
		authCtx.TeamID,
		authCtx.UserID,
		internalauth.GenerateOptions{
			Permissions: authCtx.Permissions,
			Audit: delegatedAuditContext(
				authCtx,
				internalauth.ServiceRegionalGateway,
			),
			QuotaAdmissionProof: proof,
		},
	)
}

func delegatedAuditContext(authCtx *authn.AuthContext, origin string) *internalauth.AuditContext {
	if authCtx == nil {
		return nil
	}
	if authCtx.OperationID == "" {
		authCtx.OperationID = uuid.NewString()
	}
	if authCtx.RequestID == "" {
		authCtx.RequestID = authCtx.OperationID
	}
	principal := authCtx.Principal()
	return &internalauth.AuditContext{
		Actor: internalauth.AuditActor{
			Kind:       string(principal.Kind),
			ID:         principal.ID,
			UserID:     principal.UserID,
			APIKeyID:   principal.APIKeyID,
			AuthMethod: string(principal.AuthMethod),
		},
		OperationID: authCtx.OperationID,
		RequestID:   authCtx.RequestID,
		Origin:      origin,
	}
}

func attachAuditCorrelation() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := gatewaymiddleware.GetAuthContext(c)
		ensureAuditCorrelation(c, authCtx)
		c.Next()
	}
}

func ensureAuditCorrelation(c *gin.Context, authCtx *authn.AuthContext) {
	if c == nil || authCtx == nil {
		return
	}
	if authCtx.OperationID == "" {
		authCtx.OperationID = uuid.NewString()
	}
	requestID := strings.TrimSpace(c.GetHeader("X-Request-ID"))
	if len(requestID) > 128 {
		requestID = requestID[:128]
	}
	if requestID == "" {
		requestID = authCtx.OperationID
	}
	authCtx.RequestID = requestID
}
