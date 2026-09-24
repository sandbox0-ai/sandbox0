package nodepoolautoscaler

import (
	"context"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// CPUChecker only decides whether consolidation is worth attempting. It
// neither reserves capacity nor authorizes capture or restore.
type CPUChecker interface {
	Compatible(context.Context, *sandboxstore.RuntimeNodeConsolidationCPUPlan) (bool, error)
}

type CPUPreflightNode interface {
	PreflightMigrationCPU(context.Context, protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error)
}

type nodeCPUChecker struct{ node CPUPreflightNode }

func NewCPUChecker(node CPUPreflightNode) (CPUChecker, error) {
	if node == nil {
		return nil, errors.New("consolidation CPU node channel is required")
	}
	return nodeCPUChecker{node: node}, nil
}

func (c nodeCPUChecker) Compatible(ctx context.Context, plan *sandboxstore.RuntimeNodeConsolidationCPUPlan) (bool, error) {
	if plan == nil || len(plan.Sources) == 0 || plan.FixedNodeUID == "" || plan.FixedBootID == "" {
		return false, errors.New("consolidation CPU plan is incomplete")
	}
	for _, probe := range plan.Sources {
		if probe.Source.Target.NodeUID != plan.SourceNodeUID || probe.Source.Target.ClusterID != plan.ClusterID ||
			probe.Target.NodeUID != plan.FixedNodeUID || probe.Target.NodeBootID != plan.FixedBootID {
			return false, errors.New("consolidation CPU plan changed node identity")
		}
		source, err := c.node.PreflightMigrationCPU(ctx, probe.Source)
		if err != nil {
			return false, fmt.Errorf("source CPU planning preflight: %w", err)
		}
		if source == nil || source.ValidateFor(probe.Source) != nil {
			return false, errors.New("source CPU planning evidence changed")
		}
		target := protocol.MigrationCPUPreflightRequest{Planning: true, PlanningCPUSet: probe.TargetCPUSet,
			Target: probe.Target, Destination: probe.Target, Source: probe.Source.Source,
			SourceResources: probe.Source.SourceResources, Launch: &source.Launch}
		if err := target.Validate(); err != nil {
			return false, err
		}
		result, err := c.node.PreflightMigrationCPU(ctx, target)
		if err != nil {
			return false, fmt.Errorf("fixed-node CPU planning preflight: %w", err)
		}
		if result == nil || result.ValidateFor(target) != nil {
			return false, errors.New("fixed-node CPU planning evidence changed")
		}
	}
	return true, nil
}
