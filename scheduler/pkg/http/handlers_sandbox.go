package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templreconciler "github.com/sandbox0-ai/sandbox0/pkg/template/reconciler"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/client"
	"go.uber.org/zap"
)

type SandboxClaimRequest struct {
	Template string `json:"template"` // template id
}

// createSandbox routes and proxies sandbox claim to the selected cluster-gateway.
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
	canonicalTemplateID, err := naming.CanonicalTemplateID(req.Template)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.Template = canonicalTemplateID

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
		"cluster-gateway",
		claims.TeamID,
		claims.UserID,
		internalauth.GenerateOptions{
			Permissions: claims.Permissions,
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

	selected, selectedBy := s.selectClusterByIdleWithAllocations(allocations, clusterMap, clusterTemplateID, maxAge)
	if selected == nil {
		selected = s.selectClusterByHeadroomWithAllocations(allocations, clusterMap, maxAge)
		if selected != nil {
			selectedBy = "headroom"
		}
	}
	if selected == nil {
		selected, err = s.selectClusterByWeightWithAllocations(allocations, clusterMap)
		if err != nil {
			return nil, tpl, "", err
		}
		if selected != nil {
			selectedBy = "weight"
		}
	}
	if selected == nil {
		selected = s.selectClusterByFallbackWithAllocations(allocations, clusterMap)
		if selected != nil {
			selectedBy = "fallback"
		}
	}

	if selected == nil {
		s.recordRoutingDecision("", "unavailable")
		return nil, tpl, "", nil
	}

	s.recordRoutingDecision(selected.ClusterID, selectedBy)

	s.logger.Info("Sandbox route selected",
		zap.String("template_id", tpl.TemplateID),
		zap.String("scope", tpl.Scope),
		zap.String("team_id", tpl.TeamID),
		zap.String("cluster_id", selected.ClusterID),
		zap.String("selected_by", selectedBy),
	)

	return selected, tpl, selectedBy, nil
}

func (s *Server) selectClusterByIdleWithAllocations(allocations []*template.TemplateAllocation, clusterMap map[string]*template.Cluster, clusterTemplateID string, maxAge time.Duration) (*template.Cluster, string) {
	var selected *template.Cluster
	var selectedAlloc *template.TemplateAllocation
	var bestIdle int32 = -1

	for _, alloc := range allocations {
		cluster := clusterMap[alloc.ClusterID]
		if cluster == nil || !cluster.Enabled {
			continue
		}

		age, ok := s.reconciler.GetTemplateStatsAge(cluster.ClusterID)
		s.recordClusterSummaryAge(cluster.ClusterID)
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
			(idleCount == bestIdle && alloc.MaxIdle == selectedAlloc.MaxIdle && cluster.Weight > selected.Weight) ||
			(idleCount == bestIdle && alloc.MaxIdle == selectedAlloc.MaxIdle && cluster.Weight == selected.Weight && cluster.ClusterID < selected.ClusterID) {
			selected = cluster
			selectedAlloc = alloc
			bestIdle = idleCount
		}
	}

	if selected == nil {
		return nil, ""
	}
	return selected, "idle"
}

func (s *Server) selectClusterByHeadroomWithAllocations(allocations []*template.TemplateAllocation, clusterMap map[string]*template.Cluster, maxAge time.Duration) *template.Cluster {
	var selected *template.Cluster
	var selectedAlloc *template.TemplateAllocation
	var bestHeadroom int32 = -1

	for _, alloc := range allocations {
		cluster := clusterMap[alloc.ClusterID]
		if cluster == nil || !cluster.Enabled {
			continue
		}

		age, ok := s.reconciler.GetClusterSummaryAge(cluster.ClusterID)
		s.recordClusterSummaryAge(cluster.ClusterID)
		if !ok || age > maxAge {
			continue
		}

		summary, ok := s.reconciler.GetClusterSummary(cluster.ClusterID)
		if !ok || summary == nil {
			continue
		}

		headroom := clusterAvailableHeadroom(summary, s.cfg.PodsPerNode)
		if selected == nil ||
			headroom > bestHeadroom ||
			(headroom == bestHeadroom && alloc.MaxIdle > selectedAlloc.MaxIdle) ||
			(headroom == bestHeadroom && alloc.MaxIdle == selectedAlloc.MaxIdle && cluster.Weight > selected.Weight) ||
			(headroom == bestHeadroom && alloc.MaxIdle == selectedAlloc.MaxIdle && cluster.Weight == selected.Weight && cluster.ClusterID < selected.ClusterID) {
			selected = cluster
			selectedAlloc = alloc
			bestHeadroom = headroom
		}
	}

	return selected
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

func (s *Server) selectClusterByFallbackWithAllocations(allocations []*template.TemplateAllocation, clusterMap map[string]*template.Cluster) *template.Cluster {
	var selected *template.Cluster
	var selectedAlloc *template.TemplateAllocation

	for _, alloc := range allocations {
		cluster := clusterMap[alloc.ClusterID]
		if cluster == nil || !cluster.Enabled {
			continue
		}
		if selected == nil ||
			cluster.Weight > selected.Weight ||
			(cluster.Weight == selected.Weight && alloc.MaxIdle > selectedAlloc.MaxIdle) ||
			(cluster.Weight == selected.Weight && alloc.MaxIdle == selectedAlloc.MaxIdle && cluster.ClusterID < selected.ClusterID) {
			selected = cluster
			selectedAlloc = alloc
		}
	}

	return selected
}

func clusterAvailableHeadroom(summary *templreconciler.ClusterSummary, podsPerNode int) int32 {
	if summary == nil {
		return 0
	}
	if podsPerNode <= 0 {
		podsPerNode = 10
	}
	sandboxNodeCount := summary.SandboxNodeCount
	if summary.TotalNodeCount == 0 && summary.SandboxNodeCount == 0 {
		sandboxNodeCount = summary.NodeCount
	}
	estimatedCapacity := int32(sandboxNodeCount * podsPerNode)
	availableCapacity := estimatedCapacity - summary.TotalPodCount
	if availableCapacity < 0 {
		return 0
	}
	return availableCapacity
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

func (s *Server) recordClusterSummaryAge(clusterID string) {
	if s == nil || s.metrics == nil || s.metrics.ClusterSummaryAge == nil || clusterID == "" {
		return
	}
	age, ok := s.reconciler.GetClusterSummaryAge(clusterID)
	if !ok {
		return
	}
	s.metrics.ClusterSummaryAge.WithLabelValues(clusterID).Set(age.Seconds())
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
			"sandboxes": []client.SandboxSummary{},
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
		response  *client.ListSandboxesResponse
		err       error
	}

	results := make(chan clusterResult, len(clusters))
	var wg sync.WaitGroup

	clusterGatewayClient := client.NewClusterGatewayClient(s.internalAuthGen, s.logger, s.obsProvider)

	for _, cluster := range clusters {
		wg.Add(1)
		go func(clusterID, clusterGatewayURL string) {
			defer wg.Done()
			resp, err := clusterGatewayClient.ListSandboxes(c.Request.Context(), clusterGatewayURL, claims.TeamID, queryString)
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
	var allSandboxes []client.SandboxSummary
	for result := range results {
		if result.err != nil {
			s.logger.Warn("Failed to list sandboxes from cluster",
				zap.String("cluster_id", result.clusterID),
				zap.Error(result.err),
			)
			continue
		}
		// Add cluster_id to each sandbox
		for i := range result.response.Sandboxes {
			result.response.Sandboxes[i].ClusterID = result.clusterID
		}
		allSandboxes = append(allSandboxes, result.response.Sandboxes...)
	}

	// Sort by created_at descending (newest first)
	sort.Slice(allSandboxes, func(i, j int) bool {
		return allSandboxes[i].CreatedAt > allSandboxes[j].CreatedAt
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
		allSandboxes = []client.SandboxSummary{}
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
