package reconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/template/allocator"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MultiClusterReconciler handles the reconciliation of templates across clusters.
type MultiClusterReconciler struct {
	templateStore    TemplateStore
	allocationStore  AllocationStore
	clusterStore     ClusterStore
	clusterClient    ClusterClient
	logger           *zap.Logger
	interval         time.Duration
	clock            *clock.Clock
	podsPerNode      int
	allocator        *allocator.Allocator
	clusterCache     map[string]*ClusterSummary
	clusterCacheAt   map[string]time.Time
	cacheMu          sync.RWMutex
	templateStats    map[string]map[string]TemplateStat
	templateStatsAt  map[string]time.Time
	statsMu          sync.RWMutex
	lastReconcileAt  time.Time
	lastReconcileErr error
	statusMu         sync.RWMutex
	metrics          *obsmetrics.SchedulerMetrics
}

// NewMultiClusterReconciler creates a new MultiClusterReconciler.
func NewMultiClusterReconciler(
	templateStore TemplateStore,
	allocationStore AllocationStore,
	clusterStore ClusterStore,
	clusterClient ClusterClient,
	interval time.Duration,
	clk *clock.Clock,
	podsPerNode int,
	logger *zap.Logger,
	metrics *obsmetrics.SchedulerMetrics,
) *MultiClusterReconciler {
	if podsPerNode <= 0 {
		podsPerNode = 10
	}
	return &MultiClusterReconciler{
		templateStore:   templateStore,
		allocationStore: allocationStore,
		clusterStore:    clusterStore,
		clusterClient:   clusterClient,
		logger:          logger,
		interval:        interval,
		clock:           clk,
		podsPerNode:     podsPerNode,
		allocator:       allocator.NewAllocator(podsPerNode, logger, metrics),
		clusterCache:    make(map[string]*ClusterSummary),
		clusterCacheAt:  make(map[string]time.Time),
		templateStats:   make(map[string]map[string]TemplateStat),
		templateStatsAt: make(map[string]time.Time),
		metrics:         metrics,
	}
}

func (r *MultiClusterReconciler) now() time.Time {
	if r.clock != nil {
		return r.clock.Now()
	}
	return time.Now()
}

func (r *MultiClusterReconciler) since(t time.Time) time.Duration {
	if r.clock != nil {
		return r.clock.Since(t)
	}
	return time.Since(t)
}

// Start starts the reconciliation loop.
func (r *MultiClusterReconciler) Start(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Reconciler stopped")
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// TriggerReconcile triggers an immediate reconciliation (e.g., after a template is created/updated).
func (r *MultiClusterReconciler) TriggerReconcile(ctx context.Context) {
	go r.reconcile(ctx)
}

// GetStatus returns the current reconciler status.
func (r *MultiClusterReconciler) GetStatus() (lastReconcile time.Time, lastError error) {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	return r.lastReconcileAt, r.lastReconcileErr
}

// GetTemplateIdleCount returns idle count for a template in a cluster.
func (r *MultiClusterReconciler) GetTemplateIdleCount(clusterID, templateID string) (int32, bool) {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()

	statsByTemplate, ok := r.templateStats[clusterID]
	if !ok {
		return 0, false
	}

	stat, ok := statsByTemplate[templateID]
	if !ok {
		return 0, false
	}

	return stat.IdleCount, true
}

// GetTemplateStatsAge returns age since last stats update for a cluster.
func (r *MultiClusterReconciler) GetTemplateStatsAge(clusterID string) (time.Duration, bool) {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()

	updatedAt, ok := r.templateStatsAt[clusterID]
	if !ok || updatedAt.IsZero() {
		return 0, false
	}

	return r.since(updatedAt), true
}

// GetClusterSummary returns the cached cluster summary for a shard.
func (r *MultiClusterReconciler) GetClusterSummary(clusterID string) (*ClusterSummary, bool) {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	summary, ok := r.clusterCache[clusterID]
	if !ok || summary == nil {
		return nil, false
	}

	copy := *summary
	return &copy, true
}

// GetClusterSummaryAge returns age since the last summary update for a cluster.
func (r *MultiClusterReconciler) GetClusterSummaryAge(clusterID string) (time.Duration, bool) {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	updatedAt, ok := r.clusterCacheAt[clusterID]
	if !ok || updatedAt.IsZero() {
		return 0, false
	}

	return r.since(updatedAt), true
}

// UpdateTemplateStats updates stats cache for a template in a cluster.
func (r *MultiClusterReconciler) UpdateTemplateStats(clusterID, templateID string, idleCount, activeCount int32, updatedAt time.Time) {
	if clusterID == "" || templateID == "" {
		return
	}
	if updatedAt.IsZero() {
		updatedAt = r.now()
	}

	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	statsByTemplate := r.templateStats[clusterID]
	if statsByTemplate == nil {
		statsByTemplate = make(map[string]TemplateStat)
		r.templateStats[clusterID] = statsByTemplate
	}

	statsByTemplate[templateID] = TemplateStat{
		TemplateID:  templateID,
		IdleCount:   idleCount,
		ActiveCount: activeCount,
	}
	r.templateStatsAt[clusterID] = updatedAt
}

// reconcile performs one reconciliation cycle.
func (r *MultiClusterReconciler) reconcile(ctx context.Context) {
	r.logger.Debug("Starting reconciliation cycle")
	start := r.now()
	metrics := r.metrics
	defer func() {
		duration := r.since(start)
		if metrics != nil {
			metrics.ReconcileDuration.Observe(duration.Seconds())
		}

		r.statusMu.Lock()
		r.lastReconcileAt = r.now()
		r.statusMu.Unlock()
	}()

	// 1. Get all enabled clusters.
	clusters, err := r.clusterStore.ListEnabledClusters(ctx)
	if err != nil {
		r.logger.Error("Failed to list enabled clusters", zap.Error(err))
		if metrics != nil {
			metrics.ReconcileTotal.WithLabelValues("error").Inc()
		}
		r.statusMu.Lock()
		r.lastReconcileErr = err
		r.statusMu.Unlock()
		return
	}

	if len(clusters) == 0 {
		r.logger.Debug("No enabled clusters found")
		if metrics != nil {
			metrics.ReconcileTotal.WithLabelValues("success").Inc()
		}
		return
	}

	// 2. Fetch cluster summaries in parallel.
	r.fetchClusterSummaries(ctx, clusters)

	// 3. Get all templates.
	templates, err := r.templateStore.ListTemplates(ctx)
	if err != nil {
		r.logger.Error("Failed to list templates", zap.Error(err))
		return
	}

	// Build a set of valid template IDs for orphan detection.
	validTemplates := make(map[string]bool)
	for _, tpl := range templates {
		clusterTemplateID := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
		validTemplates[clusterTemplateID] = true
	}

	// 4. For each template, compute allocations and sync to clusters.
	for _, tpl := range templates {
		if err := r.reconcileTemplate(ctx, tpl, clusters); err != nil {
			r.logger.Error("Failed to reconcile template",
				zap.String("template_id", tpl.TemplateID),
				zap.String("scope", tpl.Scope),
				zap.String("team_id", tpl.TeamID),
				zap.Error(err),
			)
		}
	}

	// 5. Clean up orphaned templates (templates in clusters but not in database).
	orphansRemoved := 0
	for _, cluster := range clusters {
		removed, err := r.cleanupOrphanTemplates(ctx, cluster, validTemplates)
		if err != nil {
			r.logger.Error("Failed to cleanup orphan templates",
				zap.String("cluster_id", cluster.ClusterID),
				zap.Error(err),
			)
		}
		orphansRemoved += removed
	}

	if metrics != nil {
		metrics.ReconcileTotal.WithLabelValues("success").Inc()
		metrics.LastReconcileTimestamp.SetToCurrentTime()
	}

	r.statusMu.Lock()
	r.lastReconcileErr = nil
	r.statusMu.Unlock()

	r.logger.Info("Reconciliation cycle completed",
		zap.Duration("duration", r.since(start)),
		zap.Int("clusters", len(clusters)),
		zap.Int("templates", len(templates)),
		zap.Int("orphans_removed", orphansRemoved),
	)
}

// fetchClusterSummaries fetches summaries for all clusters in parallel.
func (r *MultiClusterReconciler) fetchClusterSummaries(ctx context.Context, clusters []*template.Cluster) {
	metrics := r.metrics
	var wg sync.WaitGroup
	summaries := make(map[string]*ClusterSummary)
	summaryTimes := make(map[string]time.Time)
	var mu sync.Mutex

	for _, cluster := range clusters {
		wg.Add(1)
		go func(c *template.Cluster) {
			defer wg.Done()

			summary, err := r.clusterClient.GetClusterSummary(ctx, c.ClusterGatewayURL)
			if err != nil {
				r.logger.Warn("Failed to get cluster summary",
					zap.String("cluster_id", c.ClusterID),
					zap.Error(err),
				)
				return
			}

			mu.Lock()
			summaries[c.ClusterID] = summary
			summaryTimes[c.ClusterID] = r.now()
			mu.Unlock()

			stats, err := r.clusterClient.GetTemplateStats(ctx, c.ClusterGatewayURL)
			if err != nil {
				r.logger.Warn("Failed to get template stats",
					zap.String("cluster_id", c.ClusterID),
					zap.Error(err),
				)
			} else {
				statsByTemplate := make(map[string]TemplateStat, len(stats.Templates))
				for _, stat := range stats.Templates {
					statsByTemplate[stat.TemplateID] = stat
				}
				r.statsMu.Lock()
				r.templateStats[c.ClusterID] = statsByTemplate
				r.templateStatsAt[c.ClusterID] = r.now()
				r.statsMu.Unlock()
			}

			if metrics != nil {
				headroom := int64(0)
				sandboxNodeCount := summary.SandboxNodeCount
				if summary.TotalNodeCount == 0 && summary.SandboxNodeCount == 0 {
					sandboxNodeCount = summary.NodeCount
				}
				headroom = int64(int32(sandboxNodeCount*r.podsPerNode) - summary.TotalPodCount)
				if headroom < 0 {
					headroom = 0
				}

				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "nodes").Set(float64(summary.NodeCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "total_nodes").Set(float64(summary.TotalNodeCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "sandbox_nodes").Set(float64(summary.SandboxNodeCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "idle_pods").Set(float64(summary.IdlePodCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "active_pods").Set(float64(summary.ActivePodCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "pending_active_pods").Set(float64(summary.PendingActivePodCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "total_pods").Set(float64(summary.TotalPodCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "available_headroom").Set(float64(headroom))
				metrics.ClusterSummaryAge.WithLabelValues(c.ClusterID).Set(0)
			}

			if err := r.clusterStore.UpdateClusterLastSeen(ctx, c.ClusterID); err != nil {
				r.logger.Warn("Failed to update cluster last seen",
					zap.String("cluster_id", c.ClusterID),
					zap.Error(err),
				)
			}
		}(cluster)
	}

	wg.Wait()

	r.cacheMu.Lock()
	r.clusterCache = summaries
	r.clusterCacheAt = summaryTimes
	r.cacheMu.Unlock()
}

// reconcileTemplate reconciles a single template across all clusters.
func (r *MultiClusterReconciler) reconcileTemplate(ctx context.Context, tpl *template.Template, clusters []*template.Cluster) error {
	allocations := r.computeAllocations(tpl, clusters)
	metrics := r.metrics

	tenantLabel := "public"
	if tpl.Scope == naming.ScopeTeam {
		tenantLabel = naming.TenantKey(tpl.TeamID)
	}

	for _, alloc := range allocations {
		cluster := r.findCluster(clusters, alloc.ClusterID)
		if cluster == nil {
			continue
		}

		clusterSpec := r.buildClusterSpec(tpl.Spec, alloc)
		clusterTemplateID := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
		crd := &v1alpha1.SandboxTemplate{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.SchemeGroupVersion.String(),
				Kind:       "SandboxTemplate",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: clusterTemplateID,
				Labels: map[string]string{
					"sandbox0.ai/template-scope":      tpl.Scope,
					"sandbox0.ai/template-logical-id": tpl.TemplateID,
				},
				Annotations: map[string]string{
					"sandbox0.ai/template-team-id": tpl.TeamID,
					"sandbox0.ai/template-user-id": tpl.UserID,
				},
			},
			Spec: clusterSpec,
		}

		if err := r.clusterClient.CreateOrUpdateTemplate(ctx, cluster.ClusterGatewayURL, crd); err != nil {
			r.logger.Error("Failed to sync template to cluster",
				zap.String("template_id", tpl.TemplateID),
				zap.String("cluster_id", alloc.ClusterID),
				zap.Error(err),
			)
			errStr := err.Error()
			if dbErr := r.allocationStore.UpdateAllocationSyncStatus(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID, alloc.ClusterID, "error", &errStr); dbErr != nil {
				r.logger.Warn("Failed to update allocation sync status", zap.Error(dbErr))
			}
			continue
		}

		alloc.SyncStatus = "synced"
		if err := r.allocationStore.UpsertAllocation(ctx, alloc); err != nil {
			r.logger.Warn("Failed to upsert allocation",
				zap.String("template_id", tpl.TemplateID),
				zap.String("cluster_id", alloc.ClusterID),
				zap.Error(err),
			)
		}
		if err := r.allocationStore.UpdateAllocationSyncStatus(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID, alloc.ClusterID, "synced", nil); err != nil {
			r.logger.Warn("Failed to update allocation sync status", zap.Error(err))
		}

		if metrics != nil {
			metrics.TemplateSyncStatus.WithLabelValues(alloc.ClusterID, tpl.TemplateID, tenantLabel).Set(1.0)
		}
	}

	return nil
}

// computeAllocations computes how to distribute minIdle/maxIdle across clusters.
func (r *MultiClusterReconciler) computeAllocations(tpl *template.Template, clusters []*template.Cluster) []*template.TemplateAllocation {
	if r.allocator == nil {
		return nil
	}

	r.cacheMu.RLock()
	summaries := make(map[string]*allocator.ClusterSummary, len(r.clusterCache))
	for clusterID, summary := range r.clusterCache {
		summaries[clusterID] = &allocator.ClusterSummary{
			NodeCount:        summary.NodeCount,
			TotalNodeCount:   summary.TotalNodeCount,
			SandboxNodeCount: summary.SandboxNodeCount,
			TotalPodCount:    summary.TotalPodCount,
		}
	}
	r.cacheMu.RUnlock()

	return r.allocator.ComputeAllocations(tpl, clusters, summaries)
}

// buildClusterSpec builds the SandboxTemplateSpec for a specific cluster.
func (r *MultiClusterReconciler) buildClusterSpec(globalSpec v1alpha1.SandboxTemplateSpec, alloc *template.TemplateAllocation) v1alpha1.SandboxTemplateSpec {
	clusterSpec := globalSpec

	clusterSpec.Pool.MinIdle = alloc.MinIdle
	clusterSpec.Pool.MaxIdle = alloc.MaxIdle

	clusterID := alloc.ClusterID
	clusterSpec.ClusterId = &clusterID

	return clusterSpec
}

// findCluster finds a cluster by ID.
func (r *MultiClusterReconciler) findCluster(clusters []*template.Cluster, clusterID string) *template.Cluster {
	for _, c := range clusters {
		if c.ClusterID == clusterID {
			return c
		}
	}
	return nil
}

// cleanupOrphanTemplates removes templates from a cluster that don't exist in the database.
func (r *MultiClusterReconciler) cleanupOrphanTemplates(ctx context.Context, cluster *template.Cluster, validTemplates map[string]bool) (int, error) {
	stats, err := r.clusterClient.GetTemplateStats(ctx, cluster.ClusterGatewayURL)
	if err != nil {
		return 0, fmt.Errorf("failed to get template stats: %w", err)
	}
	metrics := r.metrics

	orphansRemoved := 0
	for _, stat := range stats.Templates {
		if !validTemplates[stat.TemplateID] {
			r.logger.Info("Removing orphan template from cluster",
				zap.String("cluster_id", cluster.ClusterID),
				zap.String("template_id", stat.TemplateID),
			)

			err := r.clusterClient.DeleteTemplate(ctx, cluster.ClusterGatewayURL, stat.TemplateID)
			if err != nil {
				r.logger.Error("Failed to delete orphan template",
					zap.String("cluster_id", cluster.ClusterID),
					zap.String("template_id", stat.TemplateID),
					zap.Error(err),
				)
				continue
			}
			orphansRemoved++
			if metrics != nil {
				metrics.OrphansRemoved.WithLabelValues(cluster.ClusterID).Inc()
			}
		}
	}

	return orphansRemoved, nil
}
