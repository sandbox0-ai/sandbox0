package nomadclaim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
)

const defaultNomadPausedRebaseRollbackTTL = 24 * time.Hour

// RebaseSandboxRootFS moves a paused filesystem onto an already-attested Base
// artifact without granting a sandbox writer. PostgreSQL binds the worker
// identity before the node may create any output object.
func (s *Service) RebaseSandboxRootFS(
	ctx context.Context,
	sandboxID, teamID string,
	request *service.RebaseSandboxRootFSRequest,
) (*service.RebaseSandboxRootFSResponse, error) {
	normalized, rollbackExpiresAt, err := s.normalizePausedRebaseAPIRequest(sandboxID, teamID, request)
	if err != nil {
		return nil, err
	}
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, mapNomadPausedRebaseError("load paused rebase sandbox", sandboxID, err)
	}
	if record == nil || record.TeamID != teamID {
		return nil, apierror.NewNotFound("sandbox", sandboxID)
	}

	target, err := s.pausedRebaseWorkerTarget(ctx, record, normalized.OperationID,
		normalized.TargetBaseArtifactDigest, rollbackExpiresAt)
	if err != nil {
		return nil, err
	}
	candidate, err := s.completeNomadPausedRebase(ctx, &sandboxstore.NomadPausedRebaseRequest{
		OperationID: normalized.OperationID, SandboxID: record.ID, ExpectedTeamID: record.TeamID,
		TargetBaseArtifactDigest: normalized.TargetBaseArtifactDigest,
		RollbackExpiresAt:        rollbackExpiresAt, WorkerClusterID: target.ClusterID,
		WorkerNodeID: target.NodeID, WorkerNodeUID: target.NodeUID,
	}, target)
	if err != nil {
		if candidate != nil && candidate.Rejected {
			return nil, apierror.NewConflict("sandbox", sandboxID,
				fmt.Errorf("sandbox termination rejected the RootFS rebase"))
		}
		if candidate != nil && candidate.Completed {
			s.logPendingPausedRebaseAck(candidate, err)
			return nomadPausedRebaseResponse(candidate), nil
		}
		return nil, err
	}
	if candidate.Rejected {
		return nil, apierror.NewConflict("sandbox", sandboxID,
			fmt.Errorf("sandbox termination rejected the RootFS rebase"))
	}
	return nomadPausedRebaseResponse(candidate), nil
}

// CompleteSandboxRootFSRebase reconstructs unpublished work or a pending
// worker acknowledgement entirely from durable PostgreSQL identity.
func (s *Service) CompleteSandboxRootFSRebase(ctx context.Context, sandboxID string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	lifecycle, err := s.store.GetPendingNomadPausedRebase(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("load pending Nomad paused rebase: %w", err)
	}
	if lifecycle == nil {
		return nil
	}
	if err := validatePendingNomadPausedRebaseLifecycle(lifecycle, sandboxID); err != nil {
		return err
	}
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("load Nomad paused-rebase sandbox for recovery: %w", err)
	}
	if record == nil ||
		record.TeamID == "" || record.ClusterID != lifecycle.WorkerClusterID {
		return fmt.Errorf("pending Nomad paused rebase lost its sandbox identity")
	}
	target, err := s.pausedRebase.ResolvePausedRebaseNode(
		ctx, lifecycle.WorkerClusterID, lifecycle.WorkerNodeID, lifecycle.WorkerNodeUID,
	)
	if err != nil {
		return mapNomadPausedRebaseDispatchError(record.ID, "resolve", err)
	}
	_, err = s.completeNomadPausedRebase(ctx, &sandboxstore.NomadPausedRebaseRequest{
		OperationID: lifecycle.ID, SandboxID: record.ID, ExpectedTeamID: record.TeamID,
		TargetBaseArtifactDigest: lifecycle.TargetBaseArtifactDigest,
		RollbackExpiresAt:        lifecycle.RollbackExpiresAt, WorkerClusterID: lifecycle.WorkerClusterID,
		WorkerNodeID: lifecycle.WorkerNodeID, WorkerNodeUID: lifecycle.WorkerNodeUID,
	}, target)
	return err
}

func (s *Service) completeNomadPausedRebase(
	ctx context.Context,
	storeRequest *sandboxstore.NomadPausedRebaseRequest,
	target protocol.NodeChannelTarget,
) (*sandboxstore.NomadPausedRebaseCandidate, error) {
	candidate, err := s.store.RequestNomadPausedRebase(ctx, storeRequest)
	if err != nil {
		return nil, mapNomadPausedRebaseError("request Nomad paused rebase", storeRequest.SandboxID, err)
	}
	if err := validateNomadPausedRebaseCandidate(candidate, storeRequest); err != nil {
		return nil, apierror.NewConflict("sandbox", storeRequest.SandboxID, err)
	}
	workerRequest, err := nomadPausedRebaseWorkerRequest(storeRequest.OperationID, candidate)
	if err != nil {
		return nil, apierror.NewConflict("sandbox", storeRequest.SandboxID, err)
	}
	if candidate.Rejected {
		if candidate.WorkerAcknowledgedAt.IsZero() {
			if ackErr := s.acknowledgeNomadPausedRebase(ctx, target, workerRequest, candidate); ackErr != nil {
				return candidate, ackErr
			}
		}
		return candidate, nil
	}
	if candidate.Completed {
		if candidate.WorkerAcknowledgedAt.IsZero() {
			if ackErr := s.acknowledgeNomadPausedRebase(ctx, target, workerRequest, candidate); ackErr != nil {
				return candidate, ackErr
			}
		}
		return candidate, nil
	}
	if candidate.Sandbox.DesiredState == sandboxstore.SandboxDesiredStateTerminating {
		return s.rejectNomadPausedRebase(ctx, storeRequest, target, workerRequest)
	}
	result, err := s.pausedRebase.PausedRebase(ctx, target, protocol.NodePausedRebaseControlRequest{
		Worker: workerRequest,
	})
	if err != nil {
		return nil, mapNomadPausedRebaseDispatchError(storeRequest.SandboxID, "execute", err)
	}
	if err := result.ValidateFor(workerRequest); err != nil {
		return nil, fmt.Errorf("%w: node returned invalid paused-rebase output: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	proofDigest, err := sha256DigestBytes(result.ProofDigest)
	if err != nil {
		return nil, fmt.Errorf("%w: decode paused-rebase worker proof: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	_, err = s.store.PublishPausedRootFSRebase(ctx, &sandboxstore.PublishPausedRootFSRebaseRequest{
		SandboxID: candidate.Sandbox.ID, TeamID: candidate.Sandbox.TeamID,
		OperationID:                storeRequest.OperationID,
		ExpectedSourceGenerationID: candidate.SourceGeneration.ID,
		ExpectedBaseArtifactDigest: candidate.SourceBaseArtifact.ArtifactDigest,
		Generation: &sandboxstore.RootFSGeneration{
			ID: result.GenerationID, FilesystemID: result.FilesystemID,
			ParentGenerationID: result.ParentGenerationID, SourceOCIDigest: result.SourceOCIDigest,
			BaseArtifactDigest: result.BaseArtifactDigest, BaseBlockRoot: result.BaseBlockRoot,
			CurrentBlockHead: result.CurrentBlockHead, WriterEpoch: result.WriterEpoch,
			FormatGeneration: result.FormatGeneration, DurabilityState: result.DurabilityState,
			LocatorVersion: result.LocatorVersion, Descriptor: append([]byte(nil), result.Descriptor...),
		},
		HealthCheckDigest: append([]byte(nil), result.HealthCheckDigest...),
		RollbackExpiresAt: candidate.RollbackExpiresAt,
		WorkerClusterID:   candidate.WorkerClusterID, WorkerNodeID: candidate.WorkerNodeID,
		WorkerNodeUID: candidate.WorkerNodeUID, WorkerProofDigest: proofDigest,
	})
	if err != nil {
		if errors.Is(err, sandboxstore.ErrNomadPausedRebaseTerminating) {
			return s.rejectNomadPausedRebase(ctx, storeRequest, target, workerRequest)
		}
		return nil, mapNomadPausedRebaseError("publish Nomad paused rebase", storeRequest.SandboxID, err)
	}

	// Re-read PostgreSQL after publication so an API response lost after the
	// commit and every exact retry use the same authoritative result.
	completed, err := s.store.RequestNomadPausedRebase(ctx, storeRequest)
	if err != nil {
		return nil, mapNomadPausedRebaseError("verify Nomad paused rebase", storeRequest.SandboxID, err)
	}
	if err := validateNomadPausedRebaseCandidate(completed, storeRequest); err != nil || !completed.Completed {
		if err == nil {
			err = errors.New("publication did not commit the lifecycle")
		}
		return nil, fmt.Errorf("%w: verify paused-rebase publication: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if ackErr := s.acknowledgeNomadPausedRebase(ctx, target, workerRequest, completed); ackErr != nil {
		return completed, ackErr
	}
	return completed, nil
}

func (s *Service) rejectNomadPausedRebase(
	ctx context.Context,
	storeRequest *sandboxstore.NomadPausedRebaseRequest,
	target protocol.NodeChannelTarget,
	workerRequest rootfsrebase.WorkerRequest,
) (*sandboxstore.NomadPausedRebaseCandidate, error) {
	rejection, err := s.pausedRebase.RejectPausedRebase(ctx, target, protocol.NodePausedRebaseControlRequest{
		Worker: workerRequest, Reject: true,
	})
	if err != nil {
		return nil, mapNomadPausedRebaseDispatchError(storeRequest.SandboxID, "reject", err)
	}
	if err := rejection.ValidateFor(workerRequest); err != nil {
		return nil, fmt.Errorf("%w: node returned invalid paused-rebase rejection: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	proofDigest, err := sha256DigestBytes(rejection.ProofDigest)
	if err != nil {
		return nil, fmt.Errorf("%w: decode paused-rebase rejection proof: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if err := s.store.RejectNomadPausedRebaseWorker(ctx, storeRequest, proofDigest); err != nil {
		return nil, mapNomadPausedRebaseError("reject Nomad paused rebase", storeRequest.SandboxID, err)
	}
	rejected, err := s.store.RequestNomadPausedRebase(ctx, storeRequest)
	if err != nil {
		return nil, mapNomadPausedRebaseError("verify Nomad paused-rebase rejection", storeRequest.SandboxID, err)
	}
	if err := validateNomadPausedRebaseCandidate(rejected, storeRequest); err != nil || !rejected.Rejected {
		if err == nil {
			err = errors.New("rejection did not abort the lifecycle")
		}
		return nil, fmt.Errorf("%w: verify paused-rebase rejection: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if ackErr := s.acknowledgeNomadPausedRebase(ctx, target, workerRequest, rejected); ackErr != nil {
		return rejected, ackErr
	}
	return rejected, nil
}

func (s *Service) acknowledgeNomadPausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	workerRequest rootfsrebase.WorkerRequest,
	candidate *sandboxstore.NomadPausedRebaseCandidate,
) error {
	if candidate == nil || len(candidate.WorkerProofDigest) != sha256.Size ||
		(!candidate.Completed && !candidate.Rejected) {
		return fmt.Errorf("%w: terminal paused rebase has no worker proof", service.ErrSandboxLifecycleUnavailable)
	}
	proofDigest := digest.NewDigestFromEncoded(digest.SHA256, hex.EncodeToString(candidate.WorkerProofDigest)).String()
	ack, err := s.pausedRebase.AcknowledgePausedRebase(ctx, target, protocol.NodePausedRebaseControlRequest{
		Worker: workerRequest, AcknowledgeProofDigest: proofDigest,
	})
	if err != nil {
		return mapNomadPausedRebaseDispatchError(candidate.Sandbox.ID, "acknowledge", err)
	}
	if err := ack.ValidateFor(workerRequest, proofDigest); err != nil {
		return fmt.Errorf("%w: invalid paused-rebase acknowledgement: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if err := s.store.AcknowledgeNomadPausedRebaseWorker(
		ctx, workerRequest.OperationID, candidate.Sandbox.ID,
		candidate.WorkerClusterID, candidate.WorkerNodeID, candidate.WorkerNodeUID,
		candidate.WorkerProofDigest,
	); err != nil {
		return mapNomadPausedRebaseError("persist Nomad paused-rebase acknowledgement", candidate.Sandbox.ID, err)
	}
	return nil
}

func (s *Service) pausedRebaseWorkerTarget(
	ctx context.Context,
	record *sandboxstore.SandboxRecord,
	operationID, targetBaseArtifactDigest string,
	rollbackExpiresAt time.Time,
) (protocol.NodeChannelTarget, error) {
	lifecycle, err := s.store.GetLifecycleTxn(ctx, operationID)
	if err != nil {
		return protocol.NodeChannelTarget{}, fmt.Errorf("%w: load paused-rebase operation: %v",
			service.ErrSandboxLifecycleUnavailable, err)
	}
	if lifecycle == nil {
		target, selectErr := s.pausedRebase.SelectPausedRebaseNode(ctx, record.ClusterID, operationID)
		if selectErr != nil {
			return protocol.NodeChannelTarget{}, mapNomadPausedRebaseDispatchError(record.ID, "select", selectErr)
		}
		return target, nil
	}
	if lifecycle.SandboxID != record.ID || lifecycle.Kind != sandboxstore.SandboxLifecycleKindRebase ||
		lifecycle.TargetBaseArtifactDigest != targetBaseArtifactDigest ||
		!lifecycle.RollbackExpiresAt.Equal(rollbackExpiresAt) ||
		lifecycle.WorkerClusterID != record.ClusterID || lifecycle.WorkerNodeID == "" || lifecycle.WorkerNodeUID == "" {
		return protocol.NodeChannelTarget{}, apierror.NewConflict(
			"sandbox", record.ID,
			fmt.Errorf("signed operation already identifies a different RootFS rebase"),
		)
	}
	if lifecycle.Phase == sandboxstore.SandboxLifecyclePhaseCommitted &&
		!lifecycle.WorkerAcknowledgedAt.IsZero() {
		return protocol.NodeChannelTarget{
			ClusterID: lifecycle.WorkerClusterID, NodeID: lifecycle.WorkerNodeID,
			NodeUID: lifecycle.WorkerNodeUID,
		}, nil
	}
	target, err := s.pausedRebase.ResolvePausedRebaseNode(
		ctx, lifecycle.WorkerClusterID, lifecycle.WorkerNodeID, lifecycle.WorkerNodeUID,
	)
	if err != nil {
		// A committed result remains successful even while its best-effort ack
		// waits for the same durable worker to reconnect.
		if lifecycle.Phase == sandboxstore.SandboxLifecyclePhaseCommitted {
			return protocol.NodeChannelTarget{
				ClusterID: lifecycle.WorkerClusterID, NodeID: lifecycle.WorkerNodeID,
				NodeUID: lifecycle.WorkerNodeUID,
			}, nil
		}
		return protocol.NodeChannelTarget{}, mapNomadPausedRebaseDispatchError(record.ID, "resolve", err)
	}
	return target, nil
}

func (s *Service) normalizePausedRebaseAPIRequest(
	sandboxID, teamID string,
	request *service.RebaseSandboxRootFSRequest,
) (*service.RebaseSandboxRootFSRequest, time.Time, error) {
	if request == nil {
		return nil, time.Time{}, fmt.Errorf("%w: request is required", service.ErrInvalidRootFSRebaseRequest)
	}
	normalized := *request
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	normalized.OperationID = strings.TrimSpace(request.OperationID)
	normalized.TargetBaseArtifactDigest = strings.TrimSpace(request.TargetBaseArtifactDigest)
	if sandboxID == "" || len(sandboxID) > 512 || teamID == "" || len(teamID) > 512 ||
		normalized.OperationID == "" || len(normalized.OperationID) > 512 || normalized.StartedAt.IsZero() {
		return nil, time.Time{}, fmt.Errorf("%w: sandbox, team, signed operation, and ingress time are required",
			service.ErrInvalidRootFSRebaseRequest)
	}
	parsed, err := digest.Parse(normalized.TargetBaseArtifactDigest)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != normalized.TargetBaseArtifactDigest {
		return nil, time.Time{}, fmt.Errorf("%w: target_base_artifact_digest must be a canonical sha256 digest",
			service.ErrInvalidRootFSRebaseRequest)
	}
	rollbackTTL := defaultNomadPausedRebaseRollbackTTL
	if normalized.RollbackTTL != nil {
		if *normalized.RollbackTTL <= 0 {
			return nil, time.Time{}, fmt.Errorf("%w: rollback_ttl must be positive",
				service.ErrInvalidRootFSRebaseRequest)
		}
		rollbackTTL = time.Duration(*normalized.RollbackTTL) * time.Second
	}
	if rollbackTTL > rootfsrebase.MaxWorkerRollbackRetention {
		return nil, time.Time{}, fmt.Errorf("%w: rollback_ttl must not exceed %s",
			service.ErrInvalidRootFSRebaseRequest, rootfsrebase.MaxWorkerRollbackRetention)
	}
	normalized.StartedAt = request.StartedAt.UTC()
	return &normalized, normalized.StartedAt.Add(rollbackTTL).UTC().Truncate(time.Microsecond), nil
}

func nomadPausedRebaseWorkerRequest(
	operationID string,
	candidate *sandboxstore.NomadPausedRebaseCandidate,
) (rootfsrebase.WorkerRequest, error) {
	if candidate == nil || candidate.Sandbox == nil || candidate.Filesystem == nil ||
		candidate.SourceGeneration == nil || candidate.SourceBaseArtifact == nil || candidate.TargetBaseArtifact == nil {
		return rootfsrebase.WorkerRequest{}, errors.New("paused-rebase candidate is incomplete")
	}
	request := rootfsrebase.WorkerRequest{
		Version: rootfsrebase.WorkerProtocolVersion, OperationID: operationID,
		SandboxID: candidate.Sandbox.ID, TeamID: candidate.Sandbox.TeamID,
		FilesystemID: candidate.Filesystem.ID, SourceGenerationID: candidate.SourceGeneration.ID,
		SourceOCIDigest:            candidate.SourceGeneration.SourceOCIDigest,
		SourceBaseArtifactDigest:   candidate.SourceBaseArtifact.ArtifactDigest,
		SourceBaseBlockRoot:        candidate.SourceBaseArtifact.BaseBlockRoot,
		SourceCurrentBlockHead:     candidate.SourceGeneration.CurrentBlockHead,
		SourceFormatGeneration:     candidate.SourceGeneration.FormatGeneration,
		SourceLocatorVersion:       candidate.SourceGeneration.LocatorVersion,
		SourceBaseDescriptor:       append([]byte(nil), candidate.SourceBaseArtifact.Descriptor...),
		SourceGenerationDescriptor: append([]byte(nil), candidate.SourceGeneration.Descriptor...),
		TargetGenerationID:         candidate.TargetGenerationID,
		TargetSourceOCIDigest:      candidate.TargetBaseArtifact.SourceOCIDigest,
		TargetBaseArtifactDigest:   candidate.TargetBaseArtifact.ArtifactDigest,
		TargetBaseBlockRoot:        candidate.TargetBaseArtifact.BaseBlockRoot,
		TargetFormatGeneration:     candidate.TargetBaseArtifact.FormatGeneration,
		TargetWriterEpoch:          candidate.TargetWriterEpoch,
		TargetBaseDescriptor:       append([]byte(nil), candidate.TargetBaseArtifact.Descriptor...),
		RollbackExpiresAt:          candidate.RollbackExpiresAt.UTC().Format(time.RFC3339Nano),
		MaxChangedBlocks:           rootfsrebase.MaxWorkerChangedBlocks,
	}
	return request, request.Validate()
}

func validateNomadPausedRebaseCandidate(
	candidate *sandboxstore.NomadPausedRebaseCandidate,
	request *sandboxstore.NomadPausedRebaseRequest,
) error {
	if candidate == nil || request == nil || candidate.Sandbox == nil || candidate.Filesystem == nil ||
		candidate.SourceGeneration == nil || candidate.SourceBaseArtifact == nil || candidate.TargetBaseArtifact == nil ||
		candidate.Sandbox.ID != request.SandboxID || candidate.Sandbox.TeamID != request.ExpectedTeamID ||
		candidate.Sandbox.ClusterID != request.WorkerClusterID ||
		candidate.TargetBaseArtifact.ArtifactDigest != request.TargetBaseArtifactDigest ||
		candidate.WorkerClusterID != request.WorkerClusterID || candidate.WorkerNodeID != request.WorkerNodeID ||
		candidate.WorkerNodeUID != request.WorkerNodeUID || !candidate.RollbackExpiresAt.Equal(request.RollbackExpiresAt) ||
		candidate.TargetGenerationID == "" || candidate.TargetWriterEpoch <= 0 {
		return errors.New("paused-rebase candidate does not match its durable request")
	}
	if candidate.Completed && candidate.Rejected {
		return errors.New("paused-rebase candidate cannot be committed and rejected")
	}
	if (candidate.Completed || candidate.Rejected) && len(candidate.WorkerProofDigest) != sha256.Size {
		return errors.New("terminal paused-rebase candidate has no exact worker proof")
	}
	return nil
}

func validatePendingNomadPausedRebaseLifecycle(lifecycle *sandboxstore.SandboxLifecycleTxn, sandboxID string) error {
	if lifecycle == nil || lifecycle.ID == "" || lifecycle.SandboxID != sandboxID ||
		lifecycle.Kind != sandboxstore.SandboxLifecycleKindRebase ||
		lifecycle.Source != sandboxstore.SandboxLifecycleSourceManual || lifecycle.Cancelable ||
		!lifecycle.CancelRequestedAt.IsZero() || lifecycle.FromGeneration != lifecycle.ToGeneration ||
		lifecycle.FromRuntimeNamespace != "" || lifecycle.FromRuntimeID != "" || lifecycle.ToRuntimeNamespace != "" ||
		lifecycle.ToRuntimeID != "" || lifecycle.TargetSandboxID != "" || len(lifecycle.TargetRecordDigest) != 0 ||
		lifecycle.TargetGenerationID == "" || lifecycle.ExpectedGenerationID == "" ||
		lifecycle.SourceBaseArtifactDigest == "" || lifecycle.TargetBaseArtifactDigest == "" ||
		lifecycle.WorkerClusterID == "" || lifecycle.WorkerNodeID == "" || lifecycle.WorkerNodeUID == "" ||
		lifecycle.RollbackExpiresAt.IsZero() {
		return errors.New("pending Nomad paused-rebase lifecycle is not canonical")
	}
	switch lifecycle.Phase {
	case sandboxstore.SandboxLifecyclePhasePreparing,
		sandboxstore.SandboxLifecyclePhaseBarriered,
		sandboxstore.SandboxLifecyclePhasePublishing,
		sandboxstore.SandboxLifecyclePhaseCommitting:
		if lifecycle.PreparedGenerationID != "" || len(lifecycle.WorkerProofDigest) != 0 {
			return errors.New("active Nomad paused-rebase lifecycle contains committed output")
		}
	case sandboxstore.SandboxLifecyclePhaseCommitted:
		if lifecycle.PreparedGenerationID != lifecycle.TargetGenerationID ||
			len(lifecycle.WorkerProofDigest) != sha256.Size || !lifecycle.WorkerAcknowledgedAt.IsZero() {
			return errors.New("committed Nomad paused-rebase lifecycle is not pending acknowledgement")
		}
	case sandboxstore.SandboxLifecyclePhaseAborted:
		if lifecycle.PreparedGenerationID != "" || len(lifecycle.WorkerProofDigest) != sha256.Size ||
			!lifecycle.WorkerAcknowledgedAt.IsZero() || lifecycle.Error != "sandbox termination requested" {
			return errors.New("rejected Nomad paused-rebase lifecycle is not pending acknowledgement")
		}
	default:
		return errors.New("Nomad paused-rebase lifecycle is not pending")
	}
	return nil
}

func nomadPausedRebaseResponse(candidate *sandboxstore.NomadPausedRebaseCandidate) *service.RebaseSandboxRootFSResponse {
	if candidate == nil || candidate.Sandbox == nil || candidate.TargetBaseArtifact == nil {
		return nil
	}
	return &service.RebaseSandboxRootFSResponse{
		SandboxID: candidate.Sandbox.ID, GenerationID: candidate.TargetGenerationID,
		BaseArtifactDigest: candidate.TargetBaseArtifact.ArtifactDigest,
		RollbackExpiresAt:  candidate.RollbackExpiresAt, Status: managerapi.SandboxStatusPaused,
	}
}

func sha256DigestBytes(value string) ([]byte, error) {
	parsed, err := digest.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return nil, errors.New("digest is not canonical sha256")
	}
	decoded, err := hex.DecodeString(parsed.Encoded())
	if err != nil || len(decoded) != sha256.Size {
		return nil, errors.New("digest payload is not 32 bytes")
	}
	return decoded, nil
}

func mapNomadPausedRebaseError(operation, sandboxID string, err error) error {
	switch {
	case errors.Is(err, sandboxstore.ErrSandboxRecordNotFound),
		errors.Is(err, sandboxstore.ErrRootFSBaseArtifactNotFound):
		return apierror.NewNotFound("sandbox", sandboxID)
	case errors.Is(err, sandboxstore.ErrNomadSandboxRebaseConflict),
		errors.Is(err, sandboxstore.ErrNomadSandboxRebaseNotReady),
		errors.Is(err, sandboxstore.ErrRootFSGenerationConflict),
		errors.Is(err, sandboxstore.ErrRootFSBaseArtifactConflict),
		errors.Is(err, sandboxstore.ErrRootFSFilesystemConflict),
		errors.Is(err, sandboxstore.ErrRootFSWriterGrantConflict):
		return apierror.NewConflict("sandbox", sandboxID, err)
	default:
		return fmt.Errorf("%w: %s: %v", service.ErrSandboxLifecycleUnavailable, operation, err)
	}
}

func mapNomadPausedRebaseDispatchError(sandboxID, action string, err error) error {
	if errdefs.IsInvalidArgument(err) || errdefs.IsPermissionDenied(err) || errdefs.IsFailedPrecondition(err) {
		return apierror.NewConflict("sandbox", sandboxID, err)
	}
	return fmt.Errorf("%w: %s paused-rebase worker: %v", service.ErrSandboxLifecycleUnavailable, action, err)
}

func (s *Service) logPendingPausedRebaseAck(candidate *sandboxstore.NomadPausedRebaseCandidate, err error) {
	if s == nil || candidate == nil || candidate.Sandbox == nil || err == nil {
		return
	}
	s.logger.Warn("Paused RootFS rebase committed with worker acknowledgement pending",
		zap.String("sandboxID", candidate.Sandbox.ID),
		zap.String("workerNodeID", candidate.WorkerNodeID), zap.Error(err))
}
