package nomadclaim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const defaultNomadRunningForkRecoveryTimeout = 10 * time.Minute

// ForkSandbox pre-creates a paused logical target, then asks the exact source
// node to freeze and publish one immutable checkpoint. The target is never
// exposed with an uncommitted RootFS binding.
func (s *Service) ForkSandbox(
	ctx context.Context,
	sourceSandboxID, teamID, userID string,
	request *service.ForkSandboxRequest,
) (*service.ForkSandboxResponse, error) {
	if request == nil {
		request = &service.ForkSandboxRequest{}
	}
	sourceSandboxID = strings.TrimSpace(sourceSandboxID)
	teamID = strings.TrimSpace(teamID)
	userID = strings.TrimSpace(userID)
	operationID := strings.TrimSpace(request.OperationID)
	if sourceSandboxID == "" || len(sourceSandboxID) > 512 || teamID == "" || len(teamID) > 512 ||
		operationID == "" || len(operationID) > 512 {
		return nil, fmt.Errorf("%w: source, team, and signed operation identities are required",
			service.ErrInvalidClaimRequest)
	}
	source, err := s.store.GetSandbox(ctx, sourceSandboxID)
	if err != nil {
		return nil, mapNomadForkError("load Nomad fork source", sourceSandboxID, err)
	}
	if source == nil || source.TeamID != teamID || source.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
		return nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sourceSandboxID)
	}
	activeSource := source.DesiredState == sandboxstore.SandboxDesiredStateActive &&
		source.RuntimeGeneration > 0 && source.CurrentPodName != "" && source.CurrentPodNamespace != ""
	pausedSource := source.DesiredState == sandboxstore.SandboxDesiredStatePaused &&
		source.RuntimeGeneration >= 0 && source.CurrentPodName == "" && source.CurrentPodNamespace == ""
	targetID, err := naming.SandboxNameForOperation(source.ClusterID, source.TemplateID, operationID)
	if err != nil {
		return nil, fmt.Errorf("derive Nomad fork target ID: %w", err)
	}
	targetConfig := service.CloneSandboxConfig(&source.Config)
	if targetConfig == nil {
		targetConfig = &sandboxstore.SandboxConfig{}
	}
	if request.Config != nil {
		if request.Config.TTL != nil {
			ttl := *request.Config.TTL
			targetConfig.TTL = &ttl
		}
		if request.Config.HardTTL != nil {
			hardTTL := *request.Config.HardTTL
			targetConfig.HardTTL = &hardTTL
		}
	}
	if err := service.NormalizeSandboxConfigForPersistence(targetConfig); err != nil {
		return nil, err
	}
	startedAt := request.StartedAt.UTC()
	if startedAt.IsZero() {
		startedAt = s.now().UTC()
	}
	expiresAt := nomadForkExpiration(startedAt, targetConfig.TTL)
	if request.Config == nil || request.Config.TTL == nil {
		expiresAt = source.ExpiresAt
	}
	hardExpiresAt := nomadForkExpiration(startedAt, targetConfig.HardTTL)
	if request.Config == nil || request.Config.HardTTL == nil {
		hardExpiresAt = source.HardExpiresAt
	}
	if !expiresAt.IsZero() && !hardExpiresAt.IsZero() && expiresAt.After(hardExpiresAt) {
		expiresAt = hardExpiresAt
	}
	target := &sandboxstore.SandboxRecord{
		ID: targetID, TeamID: teamID, UserID: userID,
		TemplateID: source.TemplateID, TemplateName: source.TemplateName,
		TemplateNamespace: source.TemplateNamespace, ClusterID: source.ClusterID,
		RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
		DesiredState:   sandboxstore.SandboxDesiredStatePaused,
		Config:         *targetConfig, TemplateSpec: source.TemplateSpec, RuntimeGeneration: 0,
		OwnerKind: source.OwnerKind, ResourceMillicpu: source.ResourceMillicpu,
		ResourceMemoryMiB: source.ResourceMemoryMiB,
		ClaimedAt:         startedAt, ExpiresAt: expiresAt, HardExpiresAt: hardExpiresAt,
		CreatedAt: startedAt, UpdatedAt: startedAt,
	}
	existingTarget, err := s.store.GetSandbox(ctx, targetID)
	if err != nil {
		return nil, mapNomadForkError("load Nomad fork target retry", sourceSandboxID, err)
	}
	if existingTarget != nil {
		if existingTarget.TeamID != teamID || existingTarget.UserID != userID ||
			existingTarget.TemplateID != source.TemplateID || existingTarget.ClusterID != source.ClusterID ||
			existingTarget.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad ||
			existingTarget.DesiredState != sandboxstore.SandboxDesiredStatePaused ||
			existingTarget.RuntimeGeneration != 0 || existingTarget.CurrentPodName != "" ||
			existingTarget.CurrentPodNamespace != "" || !existingTarget.DeletedAt.IsZero() ||
			!nomadForkExplicitTTLMatches(request.Config, &existingTarget.Config) {
			return nil, k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sourceSandboxID,
				fmt.Errorf("existing fork target does not match the signed operation"))
		}
		target = existingTarget
	}
	storeRequest := &sandboxstore.NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: sourceSandboxID,
		ExpectedTeamID: teamID, Target: target,
	}
	if existingTarget != nil {
		pausedTarget, pausedErr := s.store.ForkNomadPausedSandbox(ctx, storeRequest)
		if pausedErr == nil {
			return nomadForkResponse(sourceSandboxID, pausedTarget), nil
		}
		if errors.Is(pausedErr, sandboxstore.ErrNomadSandboxRunningForkRequired) {
			runningTarget, runningErr := s.completeNomadRunningFork(ctx, source, target, operationID)
			if runningErr != nil {
				return nil, runningErr
			}
			return nomadForkResponse(sourceSandboxID, runningTarget), nil
		}
		if pausedSource {
			return nil, mapNomadForkError("retry Nomad paused fork", sourceSandboxID, pausedErr)
		}
	}
	if (!activeSource && !pausedSource) || !source.DeletedAt.IsZero() {
		return nil, k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sourceSandboxID,
			fmt.Errorf("source is not a canonical active or paused Nomad sandbox"))
	}
	if pausedSource {
		pausedTarget, err := s.store.ForkNomadPausedSandbox(ctx, storeRequest)
		if err != nil {
			return nil, mapNomadForkError("fork paused Nomad sandbox", sourceSandboxID, err)
		}
		return nomadForkResponse(sourceSandboxID, pausedTarget), nil
	}
	target, err = s.completeNomadRunningFork(ctx, source, target, operationID)
	if err != nil {
		return nil, err
	}
	return nomadForkResponse(sourceSandboxID, target), nil
}

func (s *Service) completeNomadRunningFork(
	ctx context.Context,
	source, target *sandboxstore.SandboxRecord,
	operationID string,
) (*sandboxstore.SandboxRecord, error) {
	storeRequest := &sandboxstore.NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	}
	candidate, err := s.store.RequestNomadSandboxRunningFork(ctx, storeRequest)
	if err != nil {
		return nil, mapNomadForkError("request Nomad running fork", source.ID, err)
	}
	if candidate.Completed {
		return candidate.Target, nil
	}
	if err := validateNomadRunningForkCandidate(candidate, source, target, operationID); err != nil {
		return nil, k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, source.ID, err)
	}
	bindingDigest := hex.EncodeToString(candidate.BindingDigest)
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		TargetSandboxID: target.ID, TargetGenerationID: candidate.TargetGenerationID,
	}
	checkpoint, forkErr := s.runningFork.RunningFork(ctx, protocol.NodeChannelTarget{
		SlotID: candidate.Slot.ID, ClusterID: candidate.Slot.ClusterID,
		AllocationID: candidate.Slot.AllocationID, NodeID: candidate.Slot.NodeID,
		NodeUID: candidate.Slot.NodeUID, NodeBootID: candidate.Slot.NodeBootID,
	}, protocol.NodeRunningForkControlRequest{
		Fork: fork, SourceFilesystemID: candidate.SourceFilesystemID,
		SourceWriterGrantID: candidate.SourceWriterGrantID, SourceWriterEpoch: candidate.SourceWriterEpoch,
		BindingVersion: candidate.BindingVersion, BindingDigest: bindingDigest,
		ExpectedSourceGenerationID: candidate.SourceGenerationID,
	})
	if forkErr == nil {
		forkErr = validateNomadRunningForkCheckpoint(candidate, fork, checkpoint)
	}

	// PostgreSQL is the recovery authority. Always check it after dispatch so
	// a response lost after publication succeeds without freezing newer state.
	completed, completionErr := s.store.RequestNomadSandboxRunningFork(ctx, storeRequest)
	if completionErr == nil && completed != nil && completed.Completed {
		return completed.Target, nil
	}
	if forkErr != nil {
		return nil, mapNomadRunningForkDispatchError(source.ID, forkErr)
	}
	if completionErr != nil {
		return nil, mapNomadForkError("verify Nomad running fork publication", source.ID, completionErr)
	}
	return nil, fmt.Errorf("%w: running-fork checkpoint was not committed by writer authority",
		service.ErrSandboxLifecycleUnavailable)
}

// CompleteSandboxFork reconstructs one active running-fork dispatch entirely
// from PostgreSQL. An exact live source remains retryable so a node-side
// checkpoint journal can eventually publish; only stale operations that have
// lost their durable source, target, or slot identity are aborted.
func (s *Service) CompleteSandboxFork(ctx context.Context, sourceSandboxID string) error {
	sourceSandboxID = strings.TrimSpace(sourceSandboxID)
	if sourceSandboxID == "" || len(sourceSandboxID) > 512 {
		return fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	lifecycle, err := s.store.GetActiveLifecycleTxn(ctx, sourceSandboxID)
	if err != nil {
		return fmt.Errorf("load active Nomad fork lifecycle: %w", err)
	}
	if lifecycle == nil || lifecycle.Kind != sandboxstore.SandboxLifecycleKindFork {
		return nil
	}
	source, err := s.store.GetSandbox(ctx, sourceSandboxID)
	if err != nil {
		return fmt.Errorf("load Nomad running-fork source for recovery: %w", err)
	}
	if source == nil || source.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
		return nil
	}
	if lifecycle.ID == "" || lifecycle.SandboxID != sourceSandboxID ||
		lifecycle.Phase != sandboxstore.SandboxLifecyclePhasePublishing ||
		lifecycle.Source != sandboxstore.SandboxLifecycleSourceManual || lifecycle.Cancelable ||
		!lifecycle.CancelRequestedAt.IsZero() || lifecycle.FromPodName == "" ||
		lifecycle.FromPodNamespace == "" || lifecycle.ToPodNamespace != "" || lifecycle.ToPodName != "" ||
		lifecycle.TargetSandboxID == "" || lifecycle.TargetGenerationID == "" {
		return fmt.Errorf("active Nomad running-fork lifecycle is not canonical")
	}
	target, err := s.store.GetSandbox(ctx, lifecycle.TargetSandboxID)
	if err != nil {
		return fmt.Errorf("load Nomad running-fork target for recovery: %w", err)
	}
	if target == nil {
		return fmt.Errorf("active Nomad running-fork target is missing")
	}
	_, completionErr := s.completeNomadRunningFork(ctx, source, target, lifecycle.ID)
	if completionErr == nil {
		return nil
	}
	now := s.now().UTC()
	if lifecycle.UpdatedAt.IsZero() || now.Sub(lifecycle.UpdatedAt) < defaultNomadRunningForkRecoveryTimeout {
		return completionErr
	}
	slot, slotErr := s.store.GetRuntimeSlotBySandboxID(ctx, sourceSandboxID)
	if slotErr != nil && !errors.Is(slotErr, sandboxstore.ErrRuntimeSlotNotFound) {
		return errors.Join(completionErr, fmt.Errorf("load Nomad running-fork source slot for recovery: %w", slotErr))
	}
	recoverable, recoveryErr := nomadRunningForkRecoveryStillExact(now, lifecycle, source, target, slot)
	if recoveryErr != nil {
		return errors.Join(completionErr, recoveryErr)
	}
	if recoverable {
		return completionErr
	}
	aborted, abortErr := s.store.AbortNomadSandboxRunningFork(
		ctx, lifecycle.ID, sourceSandboxID, target.ID,
		"stale running fork no longer owns its exact source, target, or runtime slot identity",
	)
	if abortErr != nil {
		return fmt.Errorf("abort stale Nomad running fork after completion failure %v: %w", completionErr, abortErr)
	}
	if aborted {
		s.logger.Warn("Aborted stale Nomad running fork and queued target cleanup",
			zap.String("sandboxID", sourceSandboxID),
			zap.String("targetSandboxID", target.ID),
			zap.String("operationID", lifecycle.ID),
			zap.Error(completionErr),
		)
		return nil
	}
	// Publication won the abort race; the committed operation is complete.
	return nil
}

func nomadRunningForkRecoveryStillExact(
	now time.Time,
	lifecycle *sandboxstore.SandboxLifecycleTxn,
	source, target *sandboxstore.SandboxRecord,
	slot *sandboxstore.RuntimeSlot,
) (bool, error) {
	if lifecycle == nil || source == nil || target == nil || slot == nil ||
		lifecycle.ID == "" || lifecycle.SandboxID != source.ID ||
		lifecycle.Kind != sandboxstore.SandboxLifecycleKindFork ||
		lifecycle.Phase != sandboxstore.SandboxLifecyclePhasePublishing ||
		lifecycle.Source != sandboxstore.SandboxLifecycleSourceManual || lifecycle.Cancelable ||
		!lifecycle.CancelRequestedAt.IsZero() || lifecycle.FromGeneration <= 0 ||
		lifecycle.ToGeneration != lifecycle.FromGeneration || lifecycle.FromGeneration != source.RuntimeGeneration ||
		lifecycle.FromPodNamespace == "" || lifecycle.FromPodName == "" ||
		lifecycle.FromPodNamespace != source.CurrentPodNamespace || lifecycle.FromPodName != source.CurrentPodName ||
		lifecycle.ToPodNamespace != "" || lifecycle.ToPodName != "" || lifecycle.PreparedHeadLayerID != "" ||
		lifecycle.ExpectedHeadLayerID == "" || lifecycle.TargetSandboxID != target.ID ||
		lifecycle.TargetGenerationID != sandboxstore.NomadSandboxRunningForkGenerationID(lifecycle.ID, target.ID) ||
		len(lifecycle.TargetRecordDigest) != sha256.Size ||
		source.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad ||
		source.DesiredState != sandboxstore.SandboxDesiredStateActive || !source.DeletedAt.IsZero() ||
		target.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad ||
		target.DesiredState != sandboxstore.SandboxDesiredStatePaused || target.RuntimeGeneration != 0 ||
		target.CurrentPodNamespace != "" || target.CurrentPodName != "" || !target.DeletedAt.IsZero() ||
		target.TeamID != source.TeamID || target.ClusterID != source.ClusterID ||
		nomadForkHardTTLExpired(now, source.HardExpiresAt) || nomadForkHardTTLExpired(now, target.HardExpiresAt) ||
		slot.State != sandboxstore.RuntimeSlotStateActive || slot.SandboxID != source.ID ||
		slot.ClusterID != source.ClusterID || slot.AllocationNamespace != lifecycle.FromPodNamespace ||
		slot.AllocationID != lifecycle.FromPodName || slot.NodeID == "" || slot.NodeUID == "" || slot.NodeBootID == "" ||
		slot.FilesystemID == "" || slot.SourceGenerationID != lifecycle.ExpectedHeadLayerID ||
		slot.WriterGrantID == "" || slot.ProcdInstanceID == "" || len(slot.CommandReadyDigest) != sha256.Size ||
		slot.CommandReadyAt.IsZero() || !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) {
		return false, nil
	}
	targetDigest, err := sandboxstore.NomadSandboxForkTargetRecordDigest(target)
	if err != nil {
		return false, fmt.Errorf("hash Nomad running-fork target for recovery: %w", err)
	}
	return bytes.Equal(targetDigest, lifecycle.TargetRecordDigest), nil
}

func nomadForkHardTTLExpired(now, hardExpiresAt time.Time) bool {
	return !hardExpiresAt.IsZero() && !hardExpiresAt.After(now)
}

func validateNomadRunningForkCandidate(
	candidate *sandboxstore.NomadSandboxRunningForkCandidate,
	source, target *sandboxstore.SandboxRecord,
	operationID string,
) error {
	if candidate == nil || candidate.Source == nil || candidate.Target == nil || candidate.Slot == nil ||
		candidate.OperationID != operationID || candidate.TargetGenerationID == "" || candidate.Completed ||
		candidate.Source.ID != source.ID || candidate.Source.TeamID != source.TeamID ||
		candidate.Target.ID != target.ID || candidate.Target.TeamID != target.TeamID ||
		candidate.Slot.SandboxID != source.ID || candidate.SourceFilesystemID == "" ||
		candidate.SourceGenerationID == "" || candidate.SourceWriterGrantID == "" ||
		candidate.SourceWriterEpoch <= 0 || candidate.BindingVersion != rootfshandoff.WriterBindingVersion ||
		len(candidate.BindingDigest) != 32 {
		return fmt.Errorf("Nomad running-fork authority returned an incomplete or changed candidate")
	}
	return nil
}

func validateNomadRunningForkCheckpoint(
	candidate *sandboxstore.NomadSandboxRunningForkCandidate,
	fork rootfshandoff.RunningForkCheckpointRequest,
	checkpoint rootfshandoff.RunningForkCheckpointResult,
) error {
	if err := checkpoint.Validate(); err != nil {
		return fmt.Errorf("node returned an invalid running-fork checkpoint: %w", err)
	}
	proof := checkpoint.Proof
	if proof.OperationID != fork.OperationID || proof.SourceSandboxID != fork.SourceSandboxID ||
		proof.TargetSandboxID != fork.TargetSandboxID || proof.CheckpointGenerationID != fork.TargetGenerationID ||
		proof.SourceFilesystemID != candidate.SourceFilesystemID ||
		proof.SourceWriterGrantID != candidate.SourceWriterGrantID ||
		proof.SourceWriterEpoch != candidate.SourceWriterEpoch || proof.BindingVersion != candidate.BindingVersion ||
		proof.BindingDigest != hex.EncodeToString(candidate.BindingDigest) ||
		proof.ExpectedSourceGenerationID != candidate.SourceGenerationID {
		return fmt.Errorf("node running-fork checkpoint belongs to another writer or target")
	}
	return nil
}

func nomadForkResponse(sourceSandboxID string, target *sandboxstore.SandboxRecord) *service.ForkSandboxResponse {
	if target == nil {
		return nil
	}
	autoResume := true
	if target.Config.AutoResume != nil {
		autoResume = *target.Config.AutoResume
	}
	var resources *managerapi.SandboxResourceConfig
	if target.Config.Resources != nil {
		copy := *target.Config.Resources
		resources = &copy
	}
	return &service.ForkSandboxResponse{
		SourceSandboxID: sourceSandboxID,
		Sandbox: &managerapi.Sandbox{
			ID: target.ID, TemplateID: target.TemplateID, TeamID: target.TeamID, UserID: target.UserID,
			Status: managerapi.SandboxStatusPaused, Paused: true, AutoResume: autoResume,
			Resources: resources, Services: append([]managerapi.SandboxAppService(nil), target.Config.Services...),
			RuntimeGeneration: target.RuntimeGeneration,
			ExpiresAt:         optionalNomadTime(target.ExpiresAt), HardExpiresAt: optionalNomadTime(target.HardExpiresAt),
			ClaimedAt: target.ClaimedAt, CreatedAt: target.CreatedAt, UpdatedAt: target.UpdatedAt,
		},
	}
}

func nomadForkExpiration(now time.Time, ttl *int32) time.Time {
	if ttl == nil || *ttl <= 0 {
		return time.Time{}
	}
	return now.Add(time.Duration(*ttl) * time.Second)
}

func nomadForkExplicitTTLMatches(request *service.ForkSandboxConfig, stored *sandboxstore.SandboxConfig) bool {
	if stored == nil || request == nil {
		return stored != nil
	}
	if request.TTL != nil && (stored.TTL == nil || *request.TTL != *stored.TTL) {
		return false
	}
	if request.HardTTL != nil && (stored.HardTTL == nil || *request.HardTTL != *stored.HardTTL) {
		return false
	}
	return true
}

func mapNomadForkError(operation, sandboxID string, err error) error {
	switch {
	case errors.Is(err, sandboxstore.ErrSandboxRecordNotFound):
		return k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
	case errors.Is(err, sandboxstore.ErrNomadSandboxForkConflict),
		errors.Is(err, sandboxstore.ErrNomadSandboxForkNotReady),
		errors.Is(err, sandboxstore.ErrSandboxClaimReservationConflict),
		errors.Is(err, sandboxstore.ErrRootFSFilesystemConflict),
		errors.Is(err, sandboxstore.ErrRootFSWriterGrantConflict):
		return k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID, err)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}

func mapNomadRunningForkDispatchError(sandboxID string, err error) error {
	if errdefs.IsInvalidArgument(err) || errdefs.IsPermissionDenied(err) || errdefs.IsFailedPrecondition(err) {
		return k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID, err)
	}
	return fmt.Errorf("%w: dispatch running fork: %v", service.ErrSandboxLifecycleUnavailable, err)
}
