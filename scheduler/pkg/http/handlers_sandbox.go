package http

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"github.com/sandbox0-ai/infra/pkg/template"
	"go.uber.org/zap"
)

type SandboxClaimRequest struct {
	Template string `json:"template"` // template id
}

// createSandbox routes and proxies sandbox claim to the selected internal-gateway.
func (s *Server) createSandbox(c *gin.Context) {
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "failed to read request body")
		return
	}

	var req SandboxClaimRequest
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
			return
		}
	}
	if req.Template == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	selected, template, selectedBy, err := s.selectClusterForTemplate(c, req.Template, claims.TeamID)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, err.Error())
		return
	}
	if selected == nil || template == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "no clusters available for template")
		return
	}

	if s.internalAuthGen == nil {
		s.logger.Error("Internal auth generator not configured")
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication not configured")
		return
	}

	token, err := s.internalAuthGen.Generate(
		"internal-gateway",
		claims.TeamID,
		claims.UserID,
		internalauth.GenerateOptions{
			Permissions: claims.Permissions,
		},
	)
	if err != nil {
		s.logger.Error("Failed to generate internal token for internal-gateway", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	c.Request.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request.Header.Set("X-Team-ID", claims.TeamID)
	if claims.UserID != "" {
		c.Request.Header.Set("X-User-ID", claims.UserID)
	}

	c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	router, err := s.getInternalGatewayProxy(selected.InternalGatewayURL)
	if err != nil {
		s.logger.Error("Failed to get internal gateway proxy", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to route sandbox")
		return
	}

	s.logger.Info("Sandbox claim routed",
		zap.String("cluster_id", selected.ClusterID),
		zap.String("selected_by", selectedBy),
	)

	router.ProxyToTarget(c)
}

// proxySandbox routes sandbox operations to the correct internal-gateway based on sandbox ID.
func (s *Server) proxySandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid sandbox_id")
		return
	}

	cluster, err := s.getClusterByID(c.Request.Context(), parsed.ClusterID)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, err.Error())
		return
	}
	if cluster == nil || !cluster.Enabled {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "cluster not found")
		return
	}

	if s.internalAuthGen == nil {
		s.logger.Error("Internal auth generator not configured")
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication not configured")
		return
	}

	token, err := s.internalAuthGen.Generate(
		"internal-gateway",
		claims.TeamID,
		claims.UserID,
		internalauth.GenerateOptions{
			Permissions: claims.Permissions,
		},
	)
	if err != nil {
		s.logger.Error("Failed to generate internal token for internal-gateway", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	c.Request.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request.Header.Set("X-Team-ID", claims.TeamID)
	if claims.UserID != "" {
		c.Request.Header.Set("X-User-ID", claims.UserID)
	}

	router, err := s.getInternalGatewayProxy(cluster.InternalGatewayURL)
	if err != nil {
		s.logger.Error("Failed to get internal gateway proxy", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to route sandbox")
		return
	}

	s.logger.Info("Sandbox request routed",
		zap.String("sandbox_id", sandboxID),
		zap.String("cluster_id", parsed.ClusterID),
	)

	router.ProxyToTarget(c)
}

func (s *Server) selectClusterForTemplate(c *gin.Context, templateID, teamID string) (*template.Cluster, *template.Template, string, error) {
	tpl, err := s.templateStore.GetTemplateForTeam(c.Request.Context(), teamID, templateID)
	if err != nil {
		s.logger.Error("Failed to get template for routing", zap.Error(err))
		return nil, nil, "", err
	}
	if tpl == nil {
		return nil, nil, "", nil
	}

	allocations, err := s.allocationStore.ListAllocationsByTemplate(c.Request.Context(), tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		s.logger.Error("Failed to list template allocations", zap.Error(err))
		return nil, tpl, "", err
	}
	if len(allocations) == 0 {
		return nil, tpl, "", nil
	}

	clusters, err := s.repo.ListEnabledClusters(c.Request.Context())
	if err != nil {
		s.logger.Error("Failed to list enabled clusters", zap.Error(err))
		return nil, tpl, "", err
	}

	clusterMap := make(map[string]*template.Cluster, len(clusters))
	for _, cluster := range clusters {
		clusterMap[cluster.ClusterID] = cluster
	}

	clusterTemplateID := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
	maxAge := s.cfg.ReconcileInterval.Duration * 2

	var selected *template.Cluster
	selectedBy := "weight"
	var selectedAlloc *template.TemplateAllocation
	var bestIdle int32 = -1

	for _, alloc := range allocations {
		cluster := clusterMap[alloc.ClusterID]
		if cluster == nil || !cluster.Enabled {
			continue
		}

		age, ok := s.reconciler.GetTemplateStatsAge(cluster.ClusterID)
		if !ok || age > maxAge {
			continue
		}

		idleCount, ok := s.reconciler.GetTemplateIdleCount(cluster.ClusterID, clusterTemplateID)
		if !ok || idleCount <= 0 {
			continue
		}

		if selected == nil ||
			idleCount > bestIdle ||
			(idleCount == bestIdle && alloc.MaxIdle > selectedAlloc.MaxIdle) ||
			(idleCount == bestIdle && alloc.MaxIdle == selectedAlloc.MaxIdle && cluster.Weight > selected.Weight) {
			selected = cluster
			selectedAlloc = alloc
			bestIdle = idleCount
			selectedBy = "idle"
		}
	}

	if selected == nil {
		selected, err = s.selectClusterByWeightWithAllocations(allocations, clusterMap)
		if err != nil {
			return nil, tpl, "", err
		}
		selectedBy = "weight"
	}

	if selected == nil {
		return nil, tpl, "", nil
	}

	s.logger.Info("Sandbox route selected",
		zap.String("template_id", tpl.TemplateID),
		zap.String("scope", tpl.Scope),
		zap.String("team_id", tpl.TeamID),
		zap.String("cluster_id", selected.ClusterID),
		zap.String("selected_by", selectedBy),
	)

	return selected, tpl, selectedBy, nil
}

func (s *Server) selectClusterByWeightWithAllocations(allocations []*template.TemplateAllocation, clusterMap map[string]*template.Cluster) (*template.Cluster, error) {
	totalWeight := 0
	for _, alloc := range allocations {
		cluster := clusterMap[alloc.ClusterID]
		if cluster == nil || !cluster.Enabled {
			continue
		}
		if cluster.Weight <= 0 {
			continue
		}
		totalWeight += cluster.Weight
	}

	if totalWeight == 0 {
		return nil, nil
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	choice := rng.Intn(totalWeight)
	running := 0
	for _, alloc := range allocations {
		cluster := clusterMap[alloc.ClusterID]
		if cluster == nil || !cluster.Enabled {
			continue
		}
		if cluster.Weight <= 0 {
			continue
		}
		running += cluster.Weight
		if choice < running {
			return cluster, nil
		}
	}

	return nil, nil
}
