package allocator

import (
	"github.com/sandbox0-ai/infra/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/infra/pkg/observability/metrics"
	"github.com/sandbox0-ai/infra/pkg/template"
	"go.uber.org/zap"
)

// ClusterSummary provides capacity inputs for allocation.
type ClusterSummary struct {
	NodeCount     int
	TotalPodCount int32
}

// Allocator distributes pool sizes across clusters.
type Allocator struct {
	podsPerNode int
	logger      *zap.Logger
	metrics     *obsmetrics.SchedulerMetrics
}

// NewAllocator creates a new Allocator.
func NewAllocator(podsPerNode int, logger *zap.Logger, metrics *obsmetrics.SchedulerMetrics) *Allocator {
	if podsPerNode <= 0 {
		podsPerNode = 10
	}
	return &Allocator{
		podsPerNode: podsPerNode,
		logger:      logger,
		metrics:     metrics,
	}
}

// ComputeAllocations computes how to distribute minIdle/maxIdle across clusters.
func (a *Allocator) ComputeAllocations(tpl *template.Template, clusters []*template.Cluster, summaries map[string]*ClusterSummary) []*template.TemplateAllocation {
	if len(clusters) == 0 {
		return nil
	}
	metrics := a.metrics

	totalWeight := 0
	enabledClusters := make([]*template.Cluster, 0)
	for _, c := range clusters {
		if c.Enabled {
			totalWeight += c.Weight
			enabledClusters = append(enabledClusters, c)
		}
	}

	if totalWeight == 0 || len(enabledClusters) == 0 {
		return nil
	}

	globalMinIdle := tpl.Spec.Pool.MinIdle
	globalMaxIdle := tpl.Spec.Pool.MaxIdle
	tenantLabel := "public"
	if tpl.Scope == naming.ScopeTeam {
		tenantLabel = naming.TenantKey(tpl.TeamID)
	}

	var allocations []*template.TemplateAllocation
	var allocatedMinIdle int32
	var allocatedMaxIdle int32

	for i, cluster := range enabledClusters {
		weightRatio := float64(cluster.Weight) / float64(totalWeight)

		var minIdle, maxIdle int32
		if i == len(enabledClusters)-1 {
			minIdle = globalMinIdle - allocatedMinIdle
			maxIdle = globalMaxIdle - allocatedMaxIdle
		} else {
			minIdle = int32(float64(globalMinIdle) * weightRatio)
			maxIdle = int32(float64(globalMaxIdle) * weightRatio)
		}

		if minIdle < 0 {
			minIdle = 0
		}
		if maxIdle < minIdle {
			maxIdle = minIdle
		}

		summary, hasSummary := summaries[cluster.ClusterID]

		originalMinIdle, originalMaxIdle := minIdle, maxIdle
		clampReason := ""

		if hasSummary {
			estimatedCapacity := int32(summary.NodeCount * a.podsPerNode)
			availableCapacity := estimatedCapacity - summary.TotalPodCount

			if availableCapacity < 0 {
				availableCapacity = 0
			}

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

			if maxIdle < minIdle {
				maxIdle = minIdle
			}

			if clampReason != "" {
				if metrics != nil {
					metrics.CapacityClamps.WithLabelValues(cluster.ClusterID, tpl.TemplateID).Inc()
				}
				a.logger.Warn("Allocation clamped by cluster capacity",
					zap.String("cluster_id", cluster.ClusterID),
					zap.String("template_id", tpl.TemplateID),
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
				a.logger.Debug("Allocation within capacity",
					zap.String("cluster_id", cluster.ClusterID),
					zap.String("template_id", tpl.TemplateID),
					zap.Int32("min_idle", minIdle),
					zap.Int32("max_idle", maxIdle),
					zap.Int32("available_capacity", availableCapacity),
					zap.Float64("weight_ratio", weightRatio),
				)
			}
		} else {
			a.logger.Warn("No capacity info for cluster, skipping capacity clamp",
				zap.String("cluster_id", cluster.ClusterID),
				zap.String("template_id", tpl.TemplateID),
			)
		}

		allocatedMinIdle += minIdle
		allocatedMaxIdle += maxIdle

		if metrics != nil {
			metrics.TemplateAllocations.WithLabelValues(cluster.ClusterID, tpl.TemplateID, tenantLabel, "min_idle").Set(float64(minIdle))
			metrics.TemplateAllocations.WithLabelValues(cluster.ClusterID, tpl.TemplateID, tenantLabel, "max_idle").Set(float64(maxIdle))
		}

		allocations = append(allocations, &template.TemplateAllocation{
			TemplateID: tpl.TemplateID,
			Scope:      tpl.Scope,
			TeamID:     tpl.TeamID,
			ClusterID:  cluster.ClusterID,
			MinIdle:    minIdle,
			MaxIdle:    maxIdle,
			SyncStatus: "pending",
		})
	}

	a.logger.Info("Template allocation computed",
		zap.String("template_id", tpl.TemplateID),
		zap.Int32("global_min_idle", globalMinIdle),
		zap.Int32("global_max_idle", globalMaxIdle),
		zap.Int32("allocated_min_idle", allocatedMinIdle),
		zap.Int32("allocated_max_idle", allocatedMaxIdle),
		zap.Int("num_clusters", len(enabledClusters)),
	)

	return allocations
}
