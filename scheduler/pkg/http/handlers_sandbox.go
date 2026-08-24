package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/client"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/db"
	"go.uber.org/zap"
)

// createSandbox routes and proxies sandbox claim to the selected cluster-gateway.
func (s *Server) createSandbox(c *gin.Context) {
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "failed to read request body")
		return
	}

	var req apispec.ClaimRequest
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
			return
		}
	}
	if req.Template == nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(*req.Template)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.Template = &canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	selected, tpl, selectedBy, err := s.selectClusterForTemplate(c, &req, claims.TeamID)
	if err != nil {
		if errors.Is(err, template.ErrTemplateNotReady) {
			writeTemplateNotReady(c, tpl)
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, err.Error())
		return
	}
	if selected == nil || tpl == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "no clusters available for template")
		return
	}

	if s.internalAuthGen == nil {
		s.logger.Error("Internal auth generator not configured")
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication not configured")
		return
	}

	token, err := s.internalAuthGen.Generate(
		"cluster-gateway",
		claims.TeamID,
		claims.UserID,
		internalauth.GenerateOptions{
			Permissions: claims.Permissions,
			Audit:       claims.Audit,
		},
	)
	if err != nil {
		s.logger.Error("Failed to generate internal token for cluster-gateway", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	c.Request.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request.Header.Set("X-Team-ID", claims.TeamID)
	if claims.UserID != "" {
		c.Request.Header.Set("X-User-ID", claims.UserID)
	}

	c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	router, err := s.getClusterGatewayProxy(selected.ClusterGatewayURL)
	if err != nil {
		s.logger.Error("Failed to get cluster gateway proxy", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to route sandbox")
		return
	}

	s.logger.Info("Sandbox claim routed",
		zap.String("cluster_id", selected.ClusterID),
		zap.String("selected_by", selectedBy),
	)

	router.ProxyToTarget(c)
}

// proxySandbox routes sandbox operations to the correct cluster-gateway based on sandbox ID.
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
		"cluster-gateway",
		claims.TeamID,
		claims.UserID,
		internalauth.GenerateOptions{
			Permissions: claims.Permissions,
			Audit:       claims.Audit,
		},
	)
	if err != nil {
		s.logger.Error("Failed to generate internal token for cluster-gateway", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	c.Request.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request.Header.Set("X-Team-ID", claims.TeamID)
	if claims.UserID != "" {
		c.Request.Header.Set("X-User-ID", claims.UserID)
	}

	router, err := s.getClusterGatewayProxy(cluster.ClusterGatewayURL)
	if err != nil {
		s.logger.Error("Failed to get cluster gateway proxy", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to route sandbox")
		return
	}

	s.logger.Info("Sandbox request routed",
		zap.String("sandbox_id", sandboxID),
		zap.String("cluster_id", parsed.ClusterID),
	)

	router.ProxyToTarget(c)
}

func (s *Server) selectClusterForTemplate(
	c *gin.Context,
	req *apispec.ClaimRequest,
	teamID string,
) (*template.Cluster, *template.Template, string, error) {
	if req == nil || req.Template == nil {
		return nil, nil, "", fmt.Errorf("template is required")
	}
	tpl, err := s.templateStore.GetTemplateForTeam(c.Request.Context(), teamID, *req.Template)
	if err != nil {
		s.logger.Error("Failed to get template for routing", zap.Error(err))
		return nil, nil, "", err
	}
	if tpl == nil {
		return nil, nil, "", nil
	}
	if !tpl.ReadyForClaim() {
		return nil, tpl, "", template.ErrTemplateNotReady
	}

	var memoryOverride *string
	if req.Config != nil && req.Config.Resources != nil {
		value := ""
		if req.Config.Resources.Memory != nil {
			value = *req.Config.Resources.Memory
		}
		memoryOverride = &value
	}
	resources, err := template.NewResourcePolicy(
		s.cfg.TeamTemplateMemoryPerCPU,
		s.cfg.SandboxMaxMemory,
	).ResolveClaimResources(tpl.Spec, memoryOverride)
	if err != nil {
		return nil, tpl, "", fmt.Errorf("invalid claim resources: %w", err)
	}
	capacities, err := s.repo.ListSchedulableClusters(
		c.Request.Context(),
		resources.CPUMillicores,
		resources.MemoryBytes,
	)
	if err != nil {
		s.logger.Error("Failed to load live runtime capacity", zap.Error(err))
		return nil, tpl, "", err
	}
	for _, capacity := range capacities {
		s.recordClusterCapacity(capacity)
	}
	selected := selectBestCluster(capacities)
	if selected == nil {
		s.recordRoutingDecision("", "unavailable")
		return nil, tpl, "", nil
	}

	s.recordRoutingDecision(selected.Cluster.ClusterID, "resource_capacity")
	s.logger.Info("Sandbox route selected",
		zap.String("template_id", tpl.TemplateID),
		zap.String("scope", tpl.Scope),
		zap.String("team_id", tpl.TeamID),
		zap.String("cluster_id", selected.Cluster.ClusterID),
		zap.Int64("claim_capacity", selected.ClaimCapacity),
		zap.Int64("ready_slots", selected.ReadySlots),
		zap.Int64("cpu_millicores", resources.CPUMillicores),
		zap.Int64("memory_bytes", resources.MemoryBytes),
	)
	cluster := selected.Cluster
	return &cluster, tpl, "resource_capacity", nil
}

func selectBestCluster(capacities []*db.ClusterCapacity) *db.ClusterCapacity {
	var selected *db.ClusterCapacity
	for _, candidate := range capacities {
		if candidate == nil || !candidate.Cluster.Enabled ||
			candidate.ClaimCapacity <= 0 || candidate.ReadySlots <= 0 {
			continue
		}
		if selected == nil || betterClusterCapacity(candidate, selected) {
			selected = candidate
		}
	}
	return selected
}

func betterClusterCapacity(candidate, current *db.ClusterCapacity) bool {
	switch {
	case candidate.ClaimCapacity != current.ClaimCapacity:
		return candidate.ClaimCapacity > current.ClaimCapacity
	case candidate.ReadySlots != current.ReadySlots:
		return candidate.ReadySlots > current.ReadySlots
	case candidate.FreeMemoryBytes != current.FreeMemoryBytes:
		return candidate.FreeMemoryBytes > current.FreeMemoryBytes
	case candidate.FreeCPUMillicores != current.FreeCPUMillicores:
		return candidate.FreeCPUMillicores > current.FreeCPUMillicores
	case candidate.Cluster.Weight != current.Cluster.Weight:
		return candidate.Cluster.Weight > current.Cluster.Weight
	default:
		return candidate.Cluster.ClusterID < current.Cluster.ClusterID
	}
}

func writeTemplateNotReady(c *gin.Context, tpl *template.Template) {
	message := template.ErrTemplateNotReady.Error()
	if tpl != nil && tpl.Status != nil && tpl.Status.Creation != nil {
		creation := tpl.Status.Creation
		switch creation.State {
		case sandboxspec.TemplateCreationStateCreating:
			c.Header("Retry-After", "1")
			message = "template creation is still in progress"
		case sandboxspec.TemplateCreationStateFailed:
			message = "template creation failed; delete and recreate the template"
		}
	}
	spec.JSONError(c, http.StatusConflict, spec.CodeTemplateNotReady, message)
}

func (s *Server) recordRoutingDecision(clusterID, reason string) {
	if s == nil || s.metrics == nil || s.metrics.RoutingDecisions == nil {
		return
	}
	if clusterID == "" {
		clusterID = "none"
	}
	if reason == "" {
		reason = "unknown"
	}
	s.metrics.RoutingDecisions.WithLabelValues(clusterID, reason).Inc()
}

func (s *Server) recordClusterCapacity(capacity *db.ClusterCapacity) {
	if s == nil || s.metrics == nil || capacity == nil {
		return
	}
	clusterID := capacity.Cluster.ClusterID
	s.metrics.ObserveClusterCapacity(clusterID, "claim_capacity", float64(capacity.ClaimCapacity))
	s.metrics.ObserveClusterCapacity(clusterID, "ready_slots", float64(capacity.ReadySlots))
	s.metrics.ObserveClusterCapacity(clusterID, "eligible_nodes", float64(capacity.EligibleNodes))
	s.metrics.ObserveClusterCapacity(clusterID, "free_cpu_millicores", float64(capacity.FreeCPUMillicores))
	s.metrics.ObserveClusterCapacity(clusterID, "free_memory_bytes", float64(capacity.FreeMemoryBytes))
}

// listSandboxes lists all sandboxes across all enabled clusters
func (s *Server) listSandboxes(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	// Get all enabled clusters
	clusters, err := s.repo.ListEnabledClusters(c.Request.Context())
	if err != nil {
		s.logger.Error("Failed to list enabled clusters", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list clusters")
		return
	}

	if len(clusters) == 0 {
		spec.JSONSuccess(c, http.StatusOK, gin.H{
			"sandboxes": []apispec.SandboxSummary{},
			"count":     0,
			"has_more":  false,
		})
		return
	}

	// Build query string from request parameters
	queryParams := url.Values{}
	if status := c.Query("status"); status != "" {
		queryParams.Set("status", status)
	}
	if templateID := c.Query("template_id"); templateID != "" {
		queryParams.Set("template_id", templateID)
	}
	if paused := c.Query("paused"); paused != "" {
		queryParams.Set("paused", paused)
	}
	// For fan-out, we get all results and paginate after aggregation
	queryParams.Set("limit", "200")
	queryParams.Set("offset", "0")

	queryString := queryParams.Encode()

	// Fan-out to all clusters in parallel
	type clusterResult struct {
		clusterID string
		response  *apispec.SuccessSandboxListResponse
		err       error
	}

	results := make(chan clusterResult, len(clusters))
	var wg sync.WaitGroup

	clusterGatewayClient := client.NewClusterGatewayClient(s.internalAuthGen, s.logger, s.obsProvider)

	for _, cluster := range clusters {
		wg.Add(1)
		go func(clusterID, clusterGatewayURL string) {
			defer wg.Done()
			resp, err := clusterGatewayClient.ListSandboxes(
				c.Request.Context(),
				clusterGatewayURL,
				claims.TeamID,
				claims.UserID,
				queryString,
				claims.Permissions,
			)
			results <- clusterResult{
				clusterID: clusterID,
				response:  resp,
				err:       err,
			}
		}(cluster.ClusterID, cluster.ClusterGatewayURL)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and aggregate results
	var allSandboxes []apispec.SandboxSummary
	for result := range results {
		if result.err != nil {
			s.logger.Warn("Failed to list sandboxes from cluster",
				zap.String("cluster_id", result.clusterID),
				zap.Error(result.err),
			)
			continue
		}
		if result.response == nil || result.response.Data == nil {
			s.logger.Warn("Cluster sandbox list response missing data",
				zap.String("cluster_id", result.clusterID),
			)
			continue
		}

		// Add cluster_id to each sandbox
		clusterID := result.clusterID
		for i := range result.response.Data.Sandboxes {
			result.response.Data.Sandboxes[i].ClusterId = &clusterID
		}
		allSandboxes = append(allSandboxes, result.response.Data.Sandboxes...)
	}

	// Sort by created_at descending (newest first)
	sort.Slice(allSandboxes, func(i, j int) bool {
		return allSandboxes[i].CreatedAt.After(allSandboxes[j].CreatedAt)
	})

	// Parse pagination parameters
	limit := 50
	if l := c.Query("limit"); l != "" {
		if parsed, err := parsePositiveInt(l, 50); err == nil && parsed > 0 && parsed <= 200 {
			limit = parsed
		}
	}

	offset := 0
	if o := c.Query("offset"); o != "" {
		if parsed, err := parsePositiveInt(o, 0); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	// Get total count before pagination
	totalCount := len(allSandboxes)

	// Apply pagination
	hasMore := false
	if offset >= totalCount {
		allSandboxes = []apispec.SandboxSummary{}
	} else {
		end := offset + limit
		if end > totalCount {
			end = totalCount
		} else {
			hasMore = true
		}
		allSandboxes = allSandboxes[offset:end]
	}

	s.logger.Info("Listed sandboxes across clusters",
		zap.String("team_id", claims.TeamID),
		zap.Int("cluster_count", len(clusters)),
		zap.Int("total_count", totalCount),
		zap.Int("returned", len(allSandboxes)),
	)

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"sandboxes": allSandboxes,
		"count":     totalCount,
		"has_more":  hasMore,
	})
}

func parsePositiveInt(s string, defaultVal int) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	if err != nil {
		return defaultVal, err
	}
	return result, nil
}
