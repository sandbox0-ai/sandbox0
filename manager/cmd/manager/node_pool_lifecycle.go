package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodepoollifecycle"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func configureNodePoolLifecycle(
	cfg *config.ManagerConfig,
	store nodepoollifecycle.Store,
) (*nodepoollifecycle.Worker, error) {
	if cfg == nil || !cfg.NodePoolAutoscaler.Enabled {
		return nil, nil
	}
	nodePool := cfg.NodePoolAutoscaler
	enrollment := nodePool.Enrollment
	if !enrollment.Enabled {
		return nil, fmt.Errorf("node pool lifecycle requires node enrollment")
	}
	if strings.TrimSpace(nodePool.Provider) != "aliyun" {
		return nil, fmt.Errorf("node pool lifecycle provider must be aliyun")
	}
	cloud, err := nodepoollifecycle.NewAliyunCloud(
		nodePool.Region,
		nodePool.ScalingGroupID,
		enrollment.RouteTableIDs,
	)
	if err != nil {
		return nil, err
	}
	nomad, err := nodepoollifecycle.NewNomadClient(nodepoollifecycle.NomadConfig{
		Address:        enrollment.NomadAddress,
		Region:         strings.ReplaceAll(cfg.RegionID, "-", "_"),
		CACertFile:     enrollment.NomadCACertFile,
		ClientCertFile: enrollment.NomadClientCertFile,
		ClientKeyFile:  enrollment.NomadClientKeyFile,
		TokenFile:      enrollment.NomadTokenFile,
		WarmJobID:      "sandbox0-warm-slots",
	})
	if err != nil {
		return nil, err
	}
	ownerID, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("resolve node pool lifecycle owner identity: %w", err)
	}
	if strings.TrimSpace(ownerID) == "" {
		return nil, fmt.Errorf("resolve node pool lifecycle owner identity: hostname is empty")
	}
	return nodepoollifecycle.New(store, cloud, nomad, nodepoollifecycle.Config{
		PoolID: nodePool.PoolID, ScaleOutHookID: nodePool.ScaleOutHookID,
		ScaleInHookID: nodePool.ScaleInHookID, WarmSlotsPerNode: nodePool.WarmSlotsPerNode,
		Interval:                  nodePool.LifecycleInterval.Duration,
		HeartbeatTimeout:          nodePool.LifecycleHeartbeat.Duration,
		ScaleOutEnrollmentTimeout: nodePool.ScaleOutEnrollmentTimeout.Duration,
		OwnerID:                   ownerID,
	})
}
