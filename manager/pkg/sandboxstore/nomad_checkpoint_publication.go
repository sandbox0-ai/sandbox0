package sandboxstore

import (
	"context"
	"encoding/hex"
	"errors"
	"reflect"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadCheckpointPublication retains the exact memory/disk cut before
// upload, using the same immutable generation catalog and source validation as
// migration. Slow capture may outlive admission/CPU freshness; it cannot select
// another source, writer, CPU profile, or filesystem generation on retry.
func (s *PGSandboxStore) AuthorizeNomadCheckpointPublication(ctx context.Context, capture protocol.MigrationCapture) (*protocol.MigrationPublicationRequest, error) {
	if err := capture.Validate(); err != nil {
		return nil, err
	}
	if capture.State != protocol.MigrationCaptureComplete || capture.RootFS == nil {
		return nil, ErrNomadCheckpointConflict
	}
	c, err := s.mutateNomadCheckpointRetirement(ctx, capture.Request.OperationID, func(tx pgx.Tx, _ *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if c.Lifecycle.Phase != SandboxLifecyclePhasePublishing || !e.CaptureAuthorized || capture.Request != e.Preflight.Source || e.CPU == nil {
			return ErrNomadCheckpointConflict
		}
		cpu, err := e.CPU.Launch.GuestCPUProfile().Digest()
		if err != nil {
			return err
		}
		request := protocol.MigrationPublicationRequest{CheckpointSource: &e.Assignment, Capture: capture,
			CompatibilityDigest: c.CompatibilityDigest, CPUFeaturesDigest: cpu, CPULaunch: &e.CPU.Launch}
		if _, err := request.Digest(); err != nil {
			return err
		}
		if e.Publication != nil {
			if !reflect.DeepEqual(*e.Publication, request) {
				return ErrNomadCheckpointConflict
			}
			return nil
		}
		if err := validateNomadCheckpointPublicationSource(ctx, tx, c, request); err != nil {
			return err
		}
		if c.Lifecycle.PreparedGenerationID != "" {
			return ErrNomadCheckpointConflict
		}
		g := capture.RootFS.Generation
		generation, err := normalizeDurableRootFSGeneration(&RootFSGeneration{
			ID: g.GenerationID, FilesystemID: g.FilesystemID, ParentGenerationID: c.Lifecycle.ExpectedGenerationID,
			SourceOCIDigest: g.SourceOCIDigest, BaseArtifactDigest: g.BaseArtifactDigest, BaseBlockRoot: g.BaseBlockRoot,
			CurrentBlockHead: g.CurrentBlockHead, WriterEpoch: g.WriterEpoch, FormatGeneration: g.FormatGeneration,
			DurabilityState: g.DurabilityState, LocatorVersion: g.LocatorVersion, Descriptor: g.Descriptor,
		}, c.Lifecycle.ExpectedGenerationID)
		if err != nil {
			return err
		}
		if err := insertPreparedRootFSGeneration(ctx, tx, generation); err != nil {
			return err
		}
		if err := publishRootFSNodeUploads(ctx, tx, c.SourceWriterGrantID, c.Lifecycle.ID, generation); err != nil {
			return err
		}
		if err := (sandboxStoreTx{tx: tx}).SetLifecycleTxnPreparedGeneration(ctx, c.Lifecycle.ID, generation.ID); err != nil {
			return err
		}
		e.Publication = &request
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c.Evidence.Publication, nil
}

func validateNomadCheckpointPublicationSource(ctx context.Context, tx pgx.Tx, c *NomadSandboxCheckpoint, request protocol.MigrationPublicationRequest) error {
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1 FOR UPDATE OF runtime_slots`, c.SourceSlotID))
	if err != nil {
		return err
	}
	source := c.Evidence.Preflight.Source
	if slot.CompatibilityDigest != c.CompatibilityDigest || slot.WriterGrantID != c.SourceWriterGrantID ||
		slot.AllocationID != source.Target.AllocationID || slot.NodeUID != source.Target.NodeUID || slot.NodeBootID != source.Target.NodeBootID ||
		slot.ClaimNetworkPolicyDigest != protocol.NetworkPolicyDigest(c.Evidence.Policy) {
		return ErrNomadCheckpointConflict
	}
	binding, err := hex.DecodeString(source.BindingDigest)
	if err != nil {
		return err
	}
	err = validateNomadExecutionPublicationSource(ctx, tx, c.Lifecycle, slot, c.SourceWriterGrantID, binding, request)
	if errors.Is(err, ErrNomadSandboxMigrationConflict) {
		return ErrNomadCheckpointConflict
	}
	return err
}

// CommitNomadCheckpointPublication atomically retains the verified image and
// pins its matching immutable RootFS generation. The returned receipt is the
// prerequisite for source cleanup, not proof that source resources are free.
func (s *PGSandboxStore) CommitNomadCheckpointPublication(ctx context.Context, request protocol.MigrationPublicationRequest, publication protocol.MigrationPublication) (*protocol.CheckpointRetained, error) {
	if request.CheckpointSource == nil {
		return nil, ErrNomadCheckpointConflict
	}
	if err := publication.ValidateFor(request); err != nil {
		return nil, err
	}
	c, err := s.mutateNomadCheckpointRetirement(ctx, request.Capture.Request.OperationID, func(tx pgx.Tx, _ *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if c.Lifecycle.Phase != SandboxLifecyclePhasePublishing || e.Publication == nil ||
			!reflect.DeepEqual(*e.Publication, request) || c.Lifecycle.PreparedGenerationID != request.Capture.RootFS.Generation.GenerationID {
			return ErrNomadCheckpointConflict
		}
		if e.Published != nil {
			if *e.Published != publication || e.Retained == nil {
				return ErrNomadCheckpointConflict
			}
			return nil
		}
		if err := validateNomadCheckpointPublicationSource(ctx, tx, c, request); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO manager.sandbox_runtime_checkpoint_refs (sandbox_id,checkpoint_id,generation_id,runtime_generation)
			VALUES ($1,$2,$3,$4)`, c.Lifecycle.SandboxID, c.Lifecycle.ID, c.Lifecycle.PreparedGenerationID, c.Lifecycle.FromGeneration)
		if err != nil {
			return err
		}
		e.Published = &publication
		e.Retained = &protocol.CheckpointRetained{CheckpointID: c.Lifecycle.ID,
			PublicationRequestDigest: publication.RequestDigest, Reference: publication.Reference}
		return e.Retained.ValidateFor(request, publication)
	})
	if err != nil {
		return nil, err
	}
	return c.Evidence.Retained, nil
}
