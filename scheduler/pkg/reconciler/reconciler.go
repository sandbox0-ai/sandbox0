package reconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/clock"
	"github.com/sandbox0-ai/infra/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/infra/pkg/observability/metrics"
	"github.com/sandbox0-ai/infra/scheduler/pkg/client"
	"github.com/sandbox0-ai/infra/scheduler/pkg/db"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Reconciler handles the reconciliation of templates across clusters
type Reconciler struct {
	repo              *db.Repository
	igClient          *client.InternalGatewayClient
	logger            *zap.Logger
	interval          time.Duration
	clock             *clock.Clock
	podsPerNode       int
	clusterCache      map[string]*client.ClusterSummary
	cacheMu           sync.RWMutex
	templateStats     map[string]map[string]client.TemplateStat
	templateStatsAt   map[string]time.Time
	statsMu           sync.RWMutex
	lastReconcileTime time.Time
	lastReconcileErr  error
	statusMu          sync.RWMutex
	metrics           *obsmetrics.SchedulerMetrics
}

func (r *Reconciler) now() time.Time {
	if r.clock != nil {
		return r.clock.Now()
	}
	return time.Now()
}

func (r *Reconciler) since(t time.Time) time.Duration {
	if r.clock != nil {
		return r.clock.Since(t)
	}
	return time.Since(t)
}

// NewReconciler creates a new Reconciler
func NewReconciler(
	repo *db.Repository,
	igClient *client.InternalGatewayClient,
	interval time.Duration,
	clk *clock.Clock,
	podsPerNode int,
	logger *zap.Logger,
	metrics *obsmetrics.SchedulerMetrics,
) *Reconciler {
	if podsPerNode <= 0 {
		podsPerNode = 10 // default
	}
	return &Reconciler{
		repo:            repo,
		igClient:        igClient,
		logger:          logger,
		interval:        interval,
		clock:           clk,
		podsPerNode:     podsPerNode,
		clusterCache:    make(map[string]*client.ClusterSummary),
		templateStats:   make(map[string]map[string]client.TemplateStat),
		templateStatsAt: make(map[string]time.Time),
		metrics:         metrics,
	}
}

// Start starts the reconciliation loop
func (r *Reconciler) Start(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Run immediately on start
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

// reconcile performs one reconciliation cycle
func (r *Reconciler) reconcile(ctx context.Context) {
	r.logger.Debug("Starting reconciliation cycle")
	start := r.now()
	metrics := r.metrics
	defer func() {
		duration := r.since(start)
		if metrics != nil {
			metrics.ReconcileDuration.Observe(duration.Seconds())
		}

		r.statusMu.Lock()
		r.lastReconcileTime = r.now()
		r.statusMu.Unlock()
	}()

	// 1. Get all enabled clusters
	clusters, err := r.repo.ListEnabledClusters(ctx)
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

	// 2. Fetch cluster summaries in parallel
	r.fetchClusterSummaries(ctx, clusters)

	// 3. Get all templates
	templates, err := r.repo.ListTemplates(ctx)
	if err != nil {
		r.logger.Error("Failed to list templates", zap.Error(err))
		return
	}

	// Build a set of valid template IDs for orphan detection
	validTemplates := make(map[string]bool)
	for _, template := range templates {
		clusterTemplateID := naming.TemplateNameForCluster(template.Scope, template.TeamID, template.TemplateID)
		validTemplates[clusterTemplateID] = true
	}

	// 4. For each template, compute allocations and sync to clusters
	for _, template := range templates {
		if err := r.reconcileTemplate(ctx, template, clusters); err != nil {
			r.logger.Error("Failed to reconcile template",
				zap.String("template_id", template.TemplateID),
				zap.String("scope", template.Scope),
				zap.String("team_id", template.TeamID),
				zap.Error(err),
			)
		}
	}

	// 5. Clean up orphaned templates (templates in clusters but not in database)
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

	// Mark reconcile as successful
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

// fetchClusterSummaries fetches summaries for all clusters in parallel
func (r *Reconciler) fetchClusterSummaries(ctx context.Context, clusters []*db.Cluster) {
	metrics := r.metrics
	var wg sync.WaitGroup
	summaries := make(map[string]*client.ClusterSummary)
	var mu sync.Mutex

	for _, cluster := range clusters {
		wg.Add(1)
		go func(c *db.Cluster) {
			defer wg.Done()

			summary, err := r.igClient.GetClusterSummary(ctx, c.InternalGatewayURL)
			if err != nil {
				r.logger.Warn("Failed to get cluster summary",
					zap.String("cluster_id", c.ClusterID),
					zap.Error(err),
				)
				return
			}

			mu.Lock()
			summaries[c.ClusterID] = summary
			mu.Unlock()

			stats, err := r.igClient.GetTemplateStats(ctx, c.InternalGatewayURL)
			if err != nil {
				r.logger.Warn("Failed to get template stats",
					zap.String("cluster_id", c.ClusterID),
					zap.Error(err),
				)
			} else {
				statsByTemplate := make(map[string]client.TemplateStat, len(stats.Templates))
				for _, stat := range stats.Templates {
					statsByTemplate[stat.TemplateID] = stat
				}
				r.statsMu.Lock()
				r.templateStats[c.ClusterID] = statsByTemplate
				r.templateStatsAt[c.ClusterID] = r.now()
				r.statsMu.Unlock()
			}

			// Update metrics
			if metrics != nil {
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "nodes").Set(float64(summary.NodeCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "idle_pods").Set(float64(summary.IdlePodCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "active_pods").Set(float64(summary.ActivePodCount))
				metrics.ClusterCapacity.WithLabelValues(c.ClusterID, "total_pods").Set(float64(summary.TotalPodCount))
			}

			// Update last seen timestamp
			if err := r.repo.UpdateClusterLastSeen(ctx, c.ClusterID); err != nil {
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
	r.cacheMu.Unlock()
}

// GetTemplateIdleCount returns idle count for a template in a cluster.
func (r *Reconciler) GetTemplateIdleCount(clusterID, templateID string) (int32, bool) {
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
func (r *Reconciler) GetTemplateStatsAge(clusterID string) (time.Duration, bool) {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()

	updatedAt, ok := r.templateStatsAt[clusterID]
	if !ok || updatedAt.IsZero() {
		return 0, false
	}

	return r.since(updatedAt), true
}

// UpdateTemplateStats updates stats cache for a template in a cluster.
func (r *Reconciler) UpdateTemplateStats(clusterID, templateID string, idleCount, activeCount int32, updatedAt time.Time) {
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
		statsByTemplate = make(map[string]client.TemplateStat)
		r.templateStats[clusterID] = statsByTemplate
	}

	statsByTemplate[templateID] = client.TemplateStat{
		TemplateID:  templateID,
		IdleCount:   idleCount,
		ActiveCount: activeCount,
	}
	r.templateStatsAt[clusterID] = updatedAt
}

// reconcileTemplate reconciles a single template across all clusters
func (r *Reconciler) reconcileTemplate(ctx context.Context, template *db.Template, clusters []*db.Cluster) error {
	// Compute allocations based on weights
	allocations := r.computeAllocations(template, clusters)
	metrics := r.metrics

	tenantLabel := "public"
	if template.Scope == naming.ScopeTeam {
		tenantLabel = naming.TenantKey(template.TeamID)
	}

	// Sync each allocation to its cluster
	for _, alloc := range allocations {
		cluster := r.findCluster(clusters, alloc.ClusterID)
		if cluster == nil {
			continue
		}

		// Build the spec for this cluster
		clusterSpec := r.buildClusterSpec(template.Spec, alloc)

		clusterTemplateID := naming.TemplateNameForCluster(template.Scope, template.TeamID, template.TemplateID)
		tpl := &v1alpha1.SandboxTemplate{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.SchemeGroupVersion.String(),
				Kind:       "SandboxTemplate",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: clusterTemplateID,
				Labels: map[string]string{
					"sandbox0.ai/template-scope":      template.Scope,
					"sandbox0.ai/template-logical-id": template.TemplateID,
				},
				Annotations: map[string]string{
					"sandbox0.ai/template-team-id": template.TeamID,
					"sandbox0.ai/template-user-id": template.UserID,
				},
			},
			Spec: clusterSpec,
		}

		// Sync to the cluster
		err := r.igClient.CreateOrUpdateTemplate(ctx, cluster.InternalGatewayURL, tpl)
		if err != nil {
			r.logger.Error("Failed to sync template to cluster",
				zap.String("template_id", template.TemplateID),
				zap.String("cluster_id", alloc.ClusterID),
				zap.Error(err),
			)
			errStr := err.Error()
			if dbErr := r.repo.UpdateAllocationSyncStatus(ctx, template.Scope, template.TeamID, template.TemplateID, alloc.ClusterID, "error", &errStr); dbErr != nil {
				r.logger.Warn("Failed to update allocation sync status", zap.Error(dbErr))
			}
			continue
		}

		// Update allocation in database
		alloc.SyncStatus = "synced"
		if err := r.repo.UpsertAllocation(ctx, alloc); err != nil {
			r.logger.Warn("Failed to upsert allocation",
				zap.String("template_id", template.TemplateID),
				zap.String("cluster_id", alloc.ClusterID),
				zap.Error(err),
			)
		}
		if err := r.repo.UpdateAllocationSyncStatus(ctx, template.Scope, template.TeamID, template.TemplateID, alloc.ClusterID, "synced", nil); err != nil {
			r.logger.Warn("Failed to update allocation sync status", zap.Error(err))
		}

		// Update sync status metric
		if metrics != nil {
			metrics.TemplateSyncStatus.WithLabelValues(alloc.ClusterID, template.TemplateID, tenantLabel).Set(1.0)
		}
	}

	return nil
}

// computeAllocations computes how to distribute minIdle/maxIdle across clusters
// It uses a two-pass algorithm:
// 1. First pass: distribute based on weights
// 2. Apply capacity constraints (clamp based on cluster capacity)
func (r *Reconciler) computeAllocations(template *db.Template, clusters []*db.Cluster) []*db.TemplateAllocation {
	if len(clusters) == 0 {
		return nil
	}
	metrics := r.metrics

	// Calculate total weight for enabled clusters
	totalWeight := 0
	enabledClusters := make([]*db.Cluster, 0)
	for _, c := range clusters {
		if c.Enabled {
			totalWeight += c.Weight
			enabledClusters = append(enabledClusters, c)
		}
	}

	if totalWeight == 0 || len(enabledClusters) == 0 {
		return nil
	}

	globalMinIdle := template.Spec.Pool.MinIdle
	globalMaxIdle := template.Spec.Pool.MaxIdle
	tenantLabel := "public"
	if template.Scope == naming.ScopeTeam {
		tenantLabel = naming.TenantKey(template.TeamID)
	}

	// First pass: weight-based allocation
	var allocations []*db.TemplateAllocation
	var allocatedMinIdle int32 = 0
	var allocatedMaxIdle int32 = 0

	for i, cluster := range enabledClusters {
		// Calculate this cluster's share based on weight
		weightRatio := float64(cluster.Weight) / float64(totalWeight)

		var minIdle, maxIdle int32

		// For the last cluster, give it the remainder to avoid rounding issues
		if i == len(enabledClusters)-1 {
			minIdle = globalMinIdle - allocatedMinIdle
			maxIdle = globalMaxIdle - allocatedMaxIdle
		} else {
			minIdle = int32(float64(globalMinIdle) * weightRatio)
			maxIdle = int32(float64(globalMaxIdle) * weightRatio)
		}

		// Ensure non-negative
		if minIdle < 0 {
			minIdle = 0
		}
		if maxIdle < minIdle {
			maxIdle = minIdle
		}

		// Get cluster capacity from cache and apply clamp
		r.cacheMu.RLock()
		summary, hasSummary := r.clusterCache[cluster.ClusterID]
		r.cacheMu.RUnlock()

		originalMinIdle, originalMaxIdle := minIdle, maxIdle
		clampReason := ""

		if hasSummary {
			// Estimate cluster capacity: nodes * pods_per_node - currently_used
			estimatedCapacity := int32(summary.NodeCount * r.podsPerNode)
			availableCapacity := estimatedCapacity - summary.TotalPodCount

			if availableCapacity < 0 {
				availableCapacity = 0
			}

			// Apply capacity clamp
			if minIdle > availableCapacity {
				minIdle = availableCapacity
				clampReason = "min_idle clamped by capacity"
			}
			if maxIdle > availableCapacity {
				maxIdle = availableCapacity
				if clampReason != "" {
					clampReason = "min_idle and max_idle clamped by capacity"
				} else {
					clampReason = "max_idle clamped by capacity"
				}
			}

			// Ensure maxIdle >= minIdle after clamping
			if maxIdle < minIdle {
				maxIdle = minIdle
			}

			// Log capacity-based adjustments
			if clampReason != "" {
				if metrics != nil {
					metrics.CapacityClamps.WithLabelValues(cluster.ClusterID, template.TemplateID).Inc()
				}
				r.logger.Warn("Allocation clamped by cluster capacity",
					zap.String("cluster_id", cluster.ClusterID),
					zap.String("template_id", template.TemplateID),
					zap.Int32("original_min_idle", originalMinIdle),
					zap.Int32("original_max_idle", originalMaxIdle),
					zap.Int32("clamped_min_idle", minIdle),
					zap.Int32("clamped_max_idle", maxIdle),
					zap.Int32("available_capacity", availableCapacity),
					zap.Int32("estimated_capacity", estimatedCapacity),
					zap.Int32("current_pods", summary.TotalPodCount),
					zap.Int("nodes", summary.NodeCount),
					zap.String("reason", clampReason),
				)
			} else {
				r.logger.Debug("Allocation within capacity",
					zap.String("cluster_id", cluster.ClusterID),
					zap.String("template_id", template.TemplateID),
					zap.Int32("min_idle", minIdle),
					zap.Int32("max_idle", maxIdle),
					zap.Int32("available_capacity", availableCapacity),
					zap.Float64("weight_ratio", weightRatio),
				)
			}
		} else {
			// No capacity info available, log warning but proceed
			r.logger.Warn("No capacity info for cluster, skipping capacity clamp",
				zap.String("cluster_id", cluster.ClusterID),
				zap.String("template_id", template.TemplateID),
			)
		}

		allocatedMinIdle += minIdle
		allocatedMaxIdle += maxIdle

		// Update metrics
		if metrics != nil {
			metrics.TemplateAllocations.WithLabelValues(cluster.ClusterID, template.TemplateID, tenantLabel, "min_idle").Set(float64(minIdle))
			metrics.TemplateAllocations.WithLabelValues(cluster.ClusterID, template.TemplateID, tenantLabel, "max_idle").Set(float64(maxIdle))
		}

		allocations = append(allocations, &db.TemplateAllocation{
			TemplateID: template.TemplateID,
			Scope:      template.Scope,
			TeamID:     template.TeamID,
			ClusterID:  cluster.ClusterID,
			MinIdle:    minIdle,
			MaxIdle:    maxIdle,
			SyncStatus: "pending",
		})
	}

	// Log final allocation summary
	r.logger.Info("Template allocation computed",
		zap.String("template_id", template.TemplateID),
		zap.Int32("global_min_idle", globalMinIdle),
		zap.Int32("global_max_idle", globalMaxIdle),
		zap.Int32("allocated_min_idle", allocatedMinIdle),
		zap.Int32("allocated_max_idle", allocatedMaxIdle),
		zap.Int("num_clusters", len(enabledClusters)),
	)

	return allocations
}

// buildClusterSpec builds the SandboxTemplateSpec for a specific cluster
func (r *Reconciler) buildClusterSpec(globalSpec v1alpha1.SandboxTemplateSpec, alloc *db.TemplateAllocation) v1alpha1.SandboxTemplateSpec {
	// Copy the spec
	clusterSpec := globalSpec

	// Override pool settings with the allocation
	clusterSpec.Pool.MinIdle = alloc.MinIdle
	clusterSpec.Pool.MaxIdle = alloc.MaxIdle

	// Set the cluster ID so the ReplicaSet name is unique
	clusterID := alloc.ClusterID
	clusterSpec.ClusterId = &clusterID

	return clusterSpec
}

// findCluster finds a cluster by ID
func (r *Reconciler) findCluster(clusters []*db.Cluster, clusterID string) *db.Cluster {
	for _, c := range clusters {
		if c.ClusterID == clusterID {
			return c
		}
	}
	return nil
}

// cleanupOrphanTemplates removes templates from a cluster that don't exist in the database
func (r *Reconciler) cleanupOrphanTemplates(ctx context.Context, cluster *db.Cluster, validTemplates map[string]bool) (int, error) {
	// Get template stats from the cluster
	stats, err := r.igClient.GetTemplateStats(ctx, cluster.InternalGatewayURL)
	if err != nil {
		return 0, fmt.Errorf("failed to get template stats: %w", err)
	}
	metrics := r.metrics

	orphansRemoved := 0
	for _, stat := range stats.Templates {
		if !validTemplates[stat.TemplateID] {
			// This template exists in the cluster but not in our database - it's an orphan
			r.logger.Info("Removing orphan template from cluster",
				zap.String("cluster_id", cluster.ClusterID),
				zap.String("template_id", stat.TemplateID),
			)

			err := r.igClient.DeleteTemplate(ctx, cluster.InternalGatewayURL, stat.TemplateID)
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

// TriggerReconcile triggers an immediate reconciliation (e.g., after a template is created/updated)
func (r *Reconciler) TriggerReconcile(ctx context.Context) {
	go r.reconcile(ctx)
}

// GetStatus returns the current reconciler status
func (r *Reconciler) GetStatus() (lastReconcile time.Time, lastError error) {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	return r.lastReconcileTime, r.lastReconcileErr
}
