package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodepoolautoscaler"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func configureNodePoolAutoscaler(
	cfg *config.ManagerConfig,
	store nodepoolautoscaler.Store,
) (*nodepoolautoscaler.Worker, error) {
	if cfg == nil || !cfg.NodePoolAutoscaler.Enabled {
		return nil, nil
	}
	if store == nil {
		return nil, fmt.Errorf("node pool autoscaler store is required")
	}
	nodePool := cfg.NodePoolAutoscaler
	if strings.TrimSpace(nodePool.Provider) != "aliyun" {
		return nil, fmt.Errorf("node pool autoscaler provider must be aliyun")
	}
	cloud, err := nodepoolautoscaler.NewAliyunESS(nodePool.Region, nodePool.ScalingGroupID)
	if err != nil {
		return nil, err
	}
	ownerID, err := os.Hostname()
	if err != nil || strings.TrimSpace(ownerID) == "" {
		return nil, fmt.Errorf("resolve node pool autoscaler owner identity: %w", err)
	}
	return nodepoolautoscaler.New(store, cloud, nodepoolautoscaler.Config{
		PoolID: nodePool.PoolID, ClusterID: cfg.DefaultClusterId, OwnerID: ownerID,
		FixedNodes: nodePool.FixedNodes, MinElasticNodes: nodePool.MinElasticNodes,
		MaxElasticNodes:   nodePool.MaxElasticNodes,
		NodeCPUMillicores: nodePool.NodeCPUMillicores,
		NodeMemoryBytes:   nodePool.NodeMemoryBytes, WarmSlotsPerNode: nodePool.WarmSlotsPerNode,
		HeadroomCPUMillicores: nodePool.HeadroomCPUMillicores,
		HeadroomMemoryBytes:   nodePool.HeadroomMemoryBytes, HeadroomSlots: nodePool.HeadroomSlots,
		Interval: nodePool.Interval.Duration, ControllerLeaseTTL: nodePool.ControllerLeaseTTL.Duration,
		ScaleInStabilization: nodePool.ScaleInStabilization.Duration,
		ScaleOutCooldown:     nodePool.ScaleOutCooldown.Duration,
	})
}
