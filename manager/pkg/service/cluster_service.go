package service

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// ClusterSummary represents cluster-level sandbox capacity and demand signals.
type ClusterSummary struct {
	ClusterID             string `json:"cluster_id"`
	NodeCount             int    `json:"node_count"`
	TotalNodeCount        int    `json:"total_node_count"`
	SandboxNodeCount      int    `json:"sandbox_node_count"`
	IdlePodCount          int32  `json:"idle_pod_count"`
	ActivePodCount        int32  `json:"active_pod_count"`
	PendingActivePodCount int32  `json:"pending_active_pod_count"`
	TotalPodCount         int32  `json:"total_pod_count"`
}

// TemplateStat represents per-template sandbox demand signals.
type TemplateStat struct {
	TemplateID         string `json:"template_id"`
	Namespace          string `json:"namespace"`
	IdleCount          int32  `json:"idle_count"`
	ActiveCount        int32  `json:"active_count"`
	PendingActiveCount int32  `json:"pending_active_count"`
	MinIdle            int32  `json:"min_idle"`
	MaxIdle            int32  `json:"max_idle"`
}

// TemplateStats represents statistics for all templates
type TemplateStats struct {
	Templates []TemplateStat `json:"templates"`
}

// ClusterService handles cluster-related operations
type ClusterService struct {
	k8sClient      kubernetes.Interface
	podLister      corelisters.PodLister
	nodeLister     corelisters.NodeLister
	templateLister controller.TemplateLister
	logger         *zap.Logger
}

// NewClusterService creates a new ClusterService
func NewClusterService(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	nodeLister corelisters.NodeLister,
	templateLister controller.TemplateLister,
	logger *zap.Logger,
) *ClusterService {
	return &ClusterService{
		k8sClient:      k8sClient,
		podLister:      podLister,
		nodeLister:     nodeLister,
		templateLister: templateLister,
		logger:         logger,
	}
}

// GetClusterSummary returns the cluster summary including capacity and pod counts
func (s *ClusterService) GetClusterSummary(ctx context.Context) (*ClusterSummary, error) {
	cfg := config.LoadManagerConfig()

	// Get node count
	nodes, err := s.nodeLister.List(labels.Everything())
	if err != nil {
		s.logger.Error("Failed to list nodes", zap.Error(err))
		return nil, err
	}
	nodeCount := len(nodes)
	sandboxNodeCount := countSandboxEligibleNodes(nodes, cfg.SandboxPodPlacement.NodeSelector)

	// Get all sandbox-related pods
	idlePods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelPoolType: controller.PoolTypeIdle,
	}))
	if err != nil {
		s.logger.Error("Failed to list idle pods", zap.Error(err))
		return nil, err
	}

	activePods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelPoolType: controller.PoolTypeActive,
	}))
	if err != nil {
		s.logger.Error("Failed to list active pods", zap.Error(err))
		return nil, err
	}

	// Count only ready idle pods as available pooled capacity.
	idleCount := int32(0)
	for _, pod := range idlePods {
		if controller.IsPodReady(pod) {
			idleCount++
		}
	}

	activeCount := int32(0)
	pendingActiveCount := int32(0)
	for _, pod := range activePods {
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			activeCount++
			if pod.Status.Phase == corev1.PodPending {
				pendingActiveCount++
			}
		}
	}

	return &ClusterSummary{
		ClusterID:             cfg.DefaultClusterId,
		NodeCount:             nodeCount,
		TotalNodeCount:        nodeCount,
		SandboxNodeCount:      sandboxNodeCount,
		IdlePodCount:          idleCount,
		ActivePodCount:        activeCount,
		PendingActivePodCount: pendingActiveCount,
		TotalPodCount:         idleCount + activeCount,
	}, nil
}

// GetTemplateStats returns statistics for all templates
func (s *ClusterService) GetTemplateStats(ctx context.Context) (*TemplateStats, error) {
	// Get all templates
	templates, err := s.templateLister.List()
	if err != nil {
		s.logger.Error("Failed to list templates", zap.Error(err))
		return nil, err
	}

	stats := &TemplateStats{
		Templates: make([]TemplateStat, 0, len(templates)),
	}

	for _, template := range templates {
		// Get idle pods for this template
		idlePods, err := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
			controller.LabelTemplateID: template.Name,
			controller.LabelPoolType:   controller.PoolTypeIdle,
		}))
		if err != nil {
			s.logger.Error("Failed to list idle pods for template",
				zap.String("template", template.Name),
				zap.Error(err),
			)
			continue
		}

		// Get active pods for this template
		activePods, err := s.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
			controller.LabelTemplateID: template.Name,
			controller.LabelPoolType:   controller.PoolTypeActive,
		}))
		if err != nil {
			s.logger.Error("Failed to list active pods for template",
				zap.String("template", template.Name),
				zap.Error(err),
			)
			continue
		}

		// Count only ready idle pods as available pooled capacity.
		idleCount := int32(0)
		for _, pod := range idlePods {
			if controller.IsPodReady(pod) {
				idleCount++
			}
		}

		activeCount := int32(0)
		pendingActiveCount := int32(0)
		for _, pod := range activePods {
			if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
				activeCount++
				if pod.Status.Phase == corev1.PodPending {
					pendingActiveCount++
				}
			}
		}

		stats.Templates = append(stats.Templates, TemplateStat{
			TemplateID:         template.Name,
			Namespace:          template.Namespace,
			IdleCount:          idleCount,
			ActiveCount:        activeCount,
			PendingActiveCount: pendingActiveCount,
			MinIdle:            template.Spec.Pool.MinIdle,
			MaxIdle:            template.Spec.Pool.MaxIdle,
		})
	}

	return stats, nil
}

func countSandboxEligibleNodes(nodes []*corev1.Node, selector map[string]string) int {
	if len(selector) == 0 {
		return len(nodes)
	}

	count := 0
	for _, node := range nodes {
		if nodeMatchesSelector(node, selector) {
			count++
		}
	}
	return count
}

func nodeMatchesSelector(node *corev1.Node, selector map[string]string) bool {
	if node == nil {
		return false
	}
	for key, value := range selector {
		if node.Labels[key] != value {
			return false
		}
	}
	return true
}
