package runtimeslotclaim

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationStore interface {
	AcquireNomadSandboxMigrationTarget(context.Context, runtimecontrol.MigrationAssignment) (*sandboxstore.RuntimeSlot, error)
	IssueNomadSandboxMigrationTargetWriter(context.Context, runtimecontrol.MigrationAssignment, *sandboxstore.IssueRootFSWriterGrantRequest) (*sandboxstore.IssueAndBindRuntimeSlotWriterGrantResult, error)
	AuthorizeNomadSandboxMigrationRestore(context.Context, runtimecontrol.MigrationAssignment, rootfshandoff.StageRequest) (*protocol.MigrationRestoreRequest, error)
}

var _ migrationStore = (*sandboxstore.PGSandboxStore)(nil)

// RestoreMigration attaches only the already reserved and physically fenced
// destination. It shares ordinary claim's exact network/writer machinery but
// delivers a mandatory restore command, never a fresh entrypoint. It returns
// only the restore receipt; procd handover must precede command readiness.
func (p *Planner) RestoreMigration(ctx context.Context, image protocol.MigrationImagePrepareRequest, policy string) (*protocol.MigrationRestoreObservation, error) {
	if err := image.Validate(); err != nil {
		return nil, fmt.Errorf("migration destination: %w", err)
	}
	assignment, lease := image.Publication.Assignment, image.Resources
	result, err := p.claim(ctx, Request{OperationID: assignment.OperationID, SandboxID: assignment.Target.SandboxID, TeamID: assignment.Target.TeamID,
		CompatibilityDigest: image.Publication.CompatibilityDigest, ClusterID: image.Target.ClusterID, NetworkPolicy: policy,
		Runtime: assignment.Target, Resources: protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion,
			CPUMillicores: lease.CPUMillicores, MemoryBytes: lease.MemoryBytes, PIDsLimit: lease.PIDsLimit}}, &image, nil)
	if err != nil {
		return nil, err
	}
	return result.MigrationRestore, nil
}

func migrationNodeTarget(t protocol.NodeChannelTarget) NodeTarget {
	return NodeTarget{SlotID: t.SlotID, ClusterID: t.ClusterID, AllocationID: t.AllocationID, NodeID: t.NodeID, NodeUID: t.NodeUID, NodeBootID: t.NodeBootID, ControlEndpoint: t.ControlEndpoint}
}
