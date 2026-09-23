package runtimeslotclaim

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const PhaseCheckpointPrepare = "checkpoint_prepare"

type checkpointStore interface {
	GetNomadCheckpointRestorePreparation(context.Context, protocol.CheckpointRestoreAuthority, string) (*sandboxstore.NomadCheckpointRestoreEvidence, error)
	AuthorizeNomadCheckpointRestoreCPU(context.Context, protocol.CheckpointRestoreAuthority, string) (*protocol.MigrationCPUPreflightRequest, error)
	CommitNomadCheckpointRestoreCPU(context.Context, protocol.MigrationCPUPreflightRequest, protocol.CheckpointRestoreAuthority, protocol.MigrationCPUPreflight) error
	AuthorizeNomadCheckpointRestoreImage(context.Context, protocol.CheckpointRestoreAuthority, string) (*protocol.MigrationImagePrepareRequest, error)
	CommitNomadCheckpointRestoreImage(context.Context, protocol.MigrationImagePrepareRequest, protocol.MigrationImagePrepared) error
	AuthorizeNomadCheckpointRestore(context.Context, protocol.CheckpointRestoreAuthority, string, rootfshandoff.StageRequest) (*protocol.MigrationRestoreRequest, error)
}

type checkpointNode interface {
	PreflightMigrationCPU(context.Context, protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error)
	PrepareMigrationImage(context.Context, protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error)
}

var _ checkpointStore = (*sandboxstore.PGSandboxStore)(nil)

// RestoreCheckpoint acquires an ordinary carrier for an independently admitted
// memory resume or fork. CPU compatibility and immutable image custody precede
// writer issuance. It returns execution evidence only: authenticated procd
// handover, command readiness and regional resume commit still must follow.
func (p *Planner) RestoreCheckpoint(ctx context.Context, request Request, authority protocol.CheckpointRestoreAuthority) (*Result, error) {
	if authority.Assignment.Validate() != nil || authority.LifecycleEpoch <= 0 ||
		request.OperationID != authority.Assignment.OperationID || !reflect.DeepEqual(request.Runtime, authority.Assignment.Target) {
		return nil, errors.New("checkpoint restore changed admitted runtime assignment")
	}
	if _, ok := p.store.(checkpointStore); !ok {
		return nil, errors.New("checkpoint restore authority is unavailable")
	}
	if _, ok := p.node.(checkpointNode); !ok {
		return nil, errors.New("checkpoint restore node transport is unavailable")
	}
	return p.claim(ctx, request, nil, &authority)
}

func (p *Planner) prepareCheckpoint(ctx context.Context, authority protocol.CheckpointRestoreAuthority, slot *sandboxstore.RuntimeSlot) (*protocol.MigrationImagePrepareRequest, error) {
	store, node := p.store.(checkpointStore), p.node.(checkpointNode)
	evidence, err := store.GetNomadCheckpointRestorePreparation(ctx, authority, slot.ID)
	if err != nil {
		return nil, fmt.Errorf("load checkpoint restore preparation: %w", err)
	}
	if evidence == nil {
		return nil, errors.New("checkpoint restore authority returned no evidence")
	}
	image := evidence.Image
	if image == nil {
		cpu, err := store.AuthorizeNomadCheckpointRestoreCPU(ctx, authority, slot.ID)
		if err != nil {
			return nil, err
		}
		if cpu == nil || cpu.Validate() != nil || !reflect.DeepEqual(cpu.Checkpoint, &authority.Assignment) ||
			migrationNodeTarget(cpu.Target) != nodeTarget(slot) || cpu.DestinationResources != slot.ResourceLease {
			return nil, errors.New("checkpoint CPU authorization changed acquired carrier")
		}
		receipt, err := node.PreflightMigrationCPU(ctx, *cpu)
		if err != nil {
			return nil, err
		}
		if receipt == nil || receipt.ValidateFor(*cpu) != nil {
			return nil, errors.New("checkpoint CPU observation changed request")
		}
		if err := store.CommitNomadCheckpointRestoreCPU(ctx, *cpu, authority, *receipt); err != nil {
			return nil, err
		}
		image, err = store.AuthorizeNomadCheckpointRestoreImage(ctx, authority, slot.ID)
		if err != nil {
			return nil, err
		}
	}
	if image == nil || image.Validate() != nil || !reflect.DeepEqual(image.Checkpoint, &authority) ||
		migrationNodeTarget(image.Target) != nodeTarget(slot) || image.Resources != slot.ResourceLease ||
		image.Publication.CompatibilityDigest != slot.CompatibilityDigest {
		return nil, errors.New("checkpoint image authorization changed acquired carrier")
	}
	if evidence.Prepared != nil {
		if evidence.Prepared.ValidateFor(*image) != nil {
			return nil, errors.New("checkpoint prepared receipt changed image")
		}
		return image, nil
	}
	if slot.WriterGrantID != "" || slot.State != sandboxstore.RuntimeSlotStateClaiming {
		return nil, errors.New("checkpoint writer preceded image preparation")
	}
	receipt, err := node.PrepareMigrationImage(ctx, *image)
	if err != nil {
		return nil, err
	}
	if receipt == nil || receipt.ValidateFor(*image) != nil {
		return nil, errors.New("checkpoint image preparation changed request")
	}
	if err := store.CommitNomadCheckpointRestoreImage(ctx, *image, *receipt); err != nil {
		return nil, err
	}
	return image, nil
}
