package nomadclaim

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templatebuild"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

type nomadTemplateCaptureStore interface {
	CreateRootFSSnapshot(context.Context, *sandboxstore.CreateRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error)
	GetRootFSSnapshot(context.Context, string, string) (*sandboxstore.RootFSSnapshot, error)
	DeleteRootFSSnapshot(context.Context, string, string) error
	DeleteTemplateBuildRootFSCapture(context.Context, string, string) error
	GetRootFSGeneration(context.Context, string) (*sandboxstore.RootFSGeneration, error)
	GetReadyRootFSBaseArtifactByDigest(context.Context, string, sandboxstore.RootFSArtifactPlatform, sandboxstore.ReadyRootFSArtifactRequirements) (*sandboxstore.RootFSBaseArtifact, error)
	RequestNomadRunningTemplateCapture(context.Context, *sandboxstore.NomadTemplateCaptureRequest) (*sandboxstore.NomadTemplateCaptureCandidate, error)
}

// EnsureTemplateBuildCapture creates or recovers a deterministic block-COW
// snapshot for a Nomad template build. Running sources use the exact-writer
// checkpoint path implemented alongside running forks; paused sources can pin
// their already durable head directly.
func (s *Service) EnsureTemplateBuildCapture(
	ctx context.Context,
	sandboxID, teamID, snapshotID string,
	desiredSpec v1alpha1.SandboxTemplateSpec,
) (*templatebuild.CaptureMetadata, error) {
	store, ok := s.store.(nomadTemplateCaptureStore)
	if !ok {
		return nil, fmt.Errorf("Nomad template capture store is unavailable")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	snapshotID = strings.TrimSpace(snapshotID)
	if sandboxID == "" || teamID == "" || !templatepkg.IsBuildSnapshotID(snapshotID) {
		return nil, fmt.Errorf("canonical sandbox, team, and internal build snapshot identities are required")
	}

	snapshot, err := store.GetRootFSSnapshot(ctx, snapshotID, teamID)
	if err != nil && !errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		return nil, err
	}
	if snapshot == nil || errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		record, loadErr := s.store.GetSandbox(ctx, sandboxID)
		if loadErr != nil {
			return nil, loadErr
		}
		if record == nil || record.TeamID != teamID ||
			!record.DeletedAt.IsZero() {
			return nil, templatepkg.ErrTemplateSourceNotFound
		}
		active, lifecycleErr := s.store.GetActiveLifecycleTxn(ctx, sandboxID)
		if lifecycleErr != nil {
			return nil, lifecycleErr
		}
		if active != nil {
			return nil, fmt.Errorf("%w: source lifecycle %s is %s",
				templatepkg.ErrTemplateSourceNotReady, active.Kind, active.Phase)
		}
		if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
			snapshot, err = s.ensureRunningTemplateCapture(ctx, store, record, snapshotID)
			if err != nil {
				return nil, err
			}
			return s.nomadTemplateCaptureMetadata(ctx, store, sandboxID, teamID, snapshot, desiredSpec)
		}
		if record.DesiredState != sandboxstore.SandboxDesiredStatePaused ||
			record.RuntimeNamespace != "" || record.RuntimeID != "" {
			return nil, templatepkg.ErrTemplateSourceNotReady
		}
		created, createErr := store.CreateRootFSSnapshot(ctx, &sandboxstore.CreateRootFSSnapshotRequest{
			SandboxID: sandboxID, SnapshotID: snapshotID,
			Name:        "Template RootFS capture",
			Description: "Internal immutable block-COW source retained by a template.",
		})
		if createErr != nil {
			// A response can be lost after the deterministic snapshot commits.
			snapshot, err = store.GetRootFSSnapshot(ctx, snapshotID, teamID)
			if err != nil {
				return nil, createErr
			}
		} else {
			snapshot = created
		}
	}
	return s.nomadTemplateCaptureMetadata(ctx, store, sandboxID, teamID, snapshot, desiredSpec)
}

func (s *Service) ensureRunningTemplateCapture(
	ctx context.Context,
	store nomadTemplateCaptureStore,
	source *sandboxstore.SandboxRecord,
	snapshotID string,
) (*sandboxstore.RootFSSnapshot, error) {
	operationID := "nomad-template-capture-" + strings.TrimPrefix(snapshotID, "template-build-")
	request := &sandboxstore.NomadTemplateCaptureRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		TeamID: source.TeamID, SnapshotID: snapshotID,
	}
	candidate, err := store.RequestNomadRunningTemplateCapture(ctx, request)
	if err != nil {
		return nil, err
	}
	if candidate == nil {
		return nil, fmt.Errorf("%w: capture authority returned no candidate", templatepkg.ErrTemplateSourceUnavailable)
	}
	if candidate.Completed {
		return candidate.Snapshot, nil
	}
	if candidate.Slot == nil || candidate.OperationID != operationID ||
		candidate.SnapshotID != snapshotID || candidate.SourceFilesystemID == "" ||
		candidate.SourceGenerationID == "" || candidate.SourceWriterGrantID == "" ||
		candidate.SourceWriterEpoch <= 0 || candidate.BindingVersion != rootfshandoff.WriterBindingVersion ||
		len(candidate.BindingDigest) != 32 || candidate.TargetFilesystemID == "" ||
		candidate.TargetGenerationID == "" {
		return nil, fmt.Errorf("%w: capture authority returned an incomplete candidate",
			templatepkg.ErrTemplateSourceUnavailable)
	}
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		TargetSandboxID:    candidate.TargetFilesystemID,
		TargetGenerationID: candidate.TargetGenerationID,
	}
	checkpoint, dispatchErr := s.runningFork.RunningFork(ctx, protocol.NodeChannelTarget{
		SlotID: candidate.Slot.ID, ClusterID: candidate.Slot.ClusterID,
		AllocationID: candidate.Slot.AllocationID, NodeID: candidate.Slot.NodeID,
		NodeUID: candidate.Slot.NodeUID, NodeBootID: candidate.Slot.NodeBootID,
	}, protocol.NodeRunningForkControlRequest{
		Fork: fork, SourceFilesystemID: candidate.SourceFilesystemID,
		SourceWriterGrantID:        candidate.SourceWriterGrantID,
		SourceWriterEpoch:          candidate.SourceWriterEpoch,
		BindingVersion:             candidate.BindingVersion,
		BindingDigest:              hex.EncodeToString(candidate.BindingDigest),
		ExpectedSourceGenerationID: candidate.SourceGenerationID,
	})
	if dispatchErr == nil {
		dispatchErr = validateNomadTemplateCaptureCheckpoint(candidate, fork, checkpoint)
	}
	// PostgreSQL remains authoritative after every dispatch, including a lost
	// node-channel response after the writer callback committed.
	completed, completionErr := store.RequestNomadRunningTemplateCapture(ctx, request)
	if completionErr == nil && completed != nil && completed.Completed && completed.Snapshot != nil {
		return completed.Snapshot, nil
	}
	if dispatchErr != nil {
		return nil, fmt.Errorf("%w: dispatch running template capture: %v",
			templatepkg.ErrTemplateSourceUnavailable, dispatchErr)
	}
	if completionErr != nil {
		return nil, completionErr
	}
	return nil, fmt.Errorf("%w: writer checkpoint was not committed",
		templatepkg.ErrTemplateSourceUnavailable)
}

func validateNomadTemplateCaptureCheckpoint(
	candidate *sandboxstore.NomadTemplateCaptureCandidate,
	fork rootfshandoff.RunningForkCheckpointRequest,
	checkpoint rootfshandoff.RunningForkCheckpointResult,
) error {
	if err := checkpoint.Validate(); err != nil {
		return err
	}
	proof := checkpoint.Proof
	if proof.OperationID != fork.OperationID || proof.SourceSandboxID != fork.SourceSandboxID ||
		proof.TargetSandboxID != fork.TargetSandboxID ||
		proof.CheckpointGenerationID != fork.TargetGenerationID ||
		proof.SourceFilesystemID != candidate.SourceFilesystemID ||
		proof.SourceWriterGrantID != candidate.SourceWriterGrantID ||
		proof.SourceWriterEpoch != candidate.SourceWriterEpoch ||
		proof.BindingVersion != candidate.BindingVersion ||
		proof.BindingDigest != hex.EncodeToString(candidate.BindingDigest) ||
		proof.ExpectedSourceGenerationID != candidate.SourceGenerationID {
		return fmt.Errorf("node checkpoint belongs to another template capture")
	}
	return nil
}

func (s *Service) nomadTemplateCaptureMetadata(
	ctx context.Context,
	store nomadTemplateCaptureStore,
	sandboxID, teamID string,
	snapshot *sandboxstore.RootFSSnapshot,
	desiredSpec v1alpha1.SandboxTemplateSpec,
) (*templatebuild.CaptureMetadata, error) {
	if snapshot == nil || snapshot.SourceSandboxID != sandboxID || snapshot.TeamID != teamID ||
		snapshot.HeadGenerationID == "" {
		return nil, fmt.Errorf("%w: template snapshot has no exact block-COW generation",
			templatebuild.ErrCaptureInvalid)
	}
	generation, err := store.GetRootFSGeneration(ctx, snapshot.HeadGenerationID)
	if err != nil {
		return nil, err
	}
	if generation == nil || generation.ID != snapshot.HeadGenerationID ||
		generation.FilesystemID != snapshot.FilesystemID ||
		generation.SourceOCIDigest != snapshot.SourceOCIDigest ||
		generation.BaseArtifactDigest != snapshot.BaseArtifactDigest ||
		generation.FormatGeneration != snapshot.FormatGeneration ||
		(generation.DurabilityState != sandboxstore.RootFSGenerationStateCompositeDurable &&
			generation.DurabilityState != sandboxstore.RootFSGenerationStateS3Materialized) {
		return nil, fmt.Errorf("%w: captured block generation attestation changed",
			templatebuild.ErrCaptureInvalid)
	}
	desiredDigest, err := digestPinnedImage(strings.TrimSpace(desiredSpec.MainContainer.Image))
	if err != nil || desiredDigest != generation.SourceOCIDigest {
		return nil, fmt.Errorf("%w: captured RootFS does not match the desired template image",
			templatebuild.ErrCaptureInvalid)
	}
	if _, err := s.effectiveResources(desiredSpec, nil); err != nil {
		return nil, err
	}
	runtimeClass, err := s.runtimeClasses.Resolve("")
	if err != nil {
		return nil, fmt.Errorf("%w: no unambiguous Nomad runtime class for captured template",
			templatepkg.ErrTemplateSourceUnavailable)
	}
	requirements, err := s.rootFSArtifactRequirements(desiredSpec)
	if err != nil {
		return nil, err
	}
	artifact, err := store.GetReadyRootFSBaseArtifactByDigest(
		ctx, generation.BaseArtifactDigest, runtimeClass.ArtifactPlatform, requirements,
	)
	if err != nil {
		return nil, err
	}
	if artifact == nil || artifact.SourceOCIDigest != generation.SourceOCIDigest ||
		artifact.FormatGeneration != generation.FormatGeneration {
		return nil, fmt.Errorf("%w: captured RootFS base artifact attestation changed",
			templatebuild.ErrCaptureInvalid)
	}
	return &templatebuild.CaptureMetadata{
		Version:    templatebuild.CaptureMetadataVersion,
		SnapshotID: snapshot.ID, StorageFormat: templatepkg.RootFSTemplateStorageFormatBlockCOWV1,
		HeadGenerationID: generation.ID, SourceOCIDigest: generation.SourceOCIDigest,
		BaseArtifactDigest: generation.BaseArtifactDigest, FormatGeneration: generation.FormatGeneration,
		Platform: ocispec.Platform{OS: runtimeClass.ArtifactPlatform.OS,
			Architecture: runtimeClass.ArtifactPlatform.Architecture, Variant: runtimeClass.ArtifactPlatform.Variant},
		CapturedAt: snapshot.CreatedAt.UTC(),
	}, nil
}

// DeleteTemplateBuildCapture idempotently releases an internal block-COW
// snapshot. Public snapshot IDs are never accepted through this worker path.
func (s *Service) DeleteTemplateBuildCapture(ctx context.Context, snapshotID, teamID string) error {
	store, ok := s.store.(nomadTemplateCaptureStore)
	if !ok {
		return fmt.Errorf("Nomad template capture store is unavailable")
	}
	snapshotID = strings.TrimSpace(snapshotID)
	teamID = strings.TrimSpace(teamID)
	if !templatepkg.IsBuildSnapshotID(snapshotID) || teamID == "" {
		return fmt.Errorf("canonical internal build snapshot and team identities are required")
	}
	err := store.DeleteTemplateBuildRootFSCapture(ctx, snapshotID, teamID)
	if errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		return nil
	}
	return err
}
