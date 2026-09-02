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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
)

const defaultNomadRunningSnapshotRecoveryTimeout = 30 * time.Second

type nomadRunningRootFSCaptureAbortStore interface {
	AbortStaleNomadRunningRootFSCapture(context.Context, string, string, string) (bool, error)
}

// CreateRunningSandboxRootFSSnapshot checkpoints the exact live writer and
// publishes a named immutable snapshot without changing the source runtime.
func (s *Service) CreateRunningSandboxRootFSSnapshot(
	ctx context.Context,
	sandboxID, teamID string,
	request *service.CreateSandboxRootFSSnapshotRequest,
) (*sandboxstore.RootFSSnapshot, error) {
	if request == nil {
		request = &service.CreateSandboxRootFSSnapshotRequest{}
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	operationID := strings.TrimSpace(request.OperationID)
	if sandboxID == "" || teamID == "" || operationID == "" {
		return nil, fmt.Errorf("%w: sandbox, team, and signed operation identities are required",
			service.ErrSandboxLifecycleUnavailable)
	}
	store, ok := s.store.(nomadTemplateCaptureStore)
	if !ok {
		return nil, service.ErrSandboxRootFSStoreUnavailable
	}
	snapshotID := runningRootFSSnapshotID(sandboxID, operationID)
	snapshot, err := s.ensureRunningRootFSCapture(ctx, store, &sandboxstore.NomadRunningRootFSCaptureRequest{
		OperationID: operationID, SourceSandboxID: sandboxID,
		TeamID: teamID, SnapshotID: snapshotID,
		CaptureKind: sandboxstore.NomadRunningRootFSCaptureKindSnapshot,
		Name:        strings.TrimSpace(request.Name), Description: strings.TrimSpace(request.Description),
		ExpiresAt: request.ExpiresAt,
	})
	if err != nil {
		return nil, mapNomadRunningRootFSSnapshotError(sandboxID, err)
	}
	return snapshot, nil
}

// CompleteSandboxRootFSSnapshot reconstructs a pending exact-writer capture
// entirely from PostgreSQL after an API response or manager process is lost.
func (s *Service) CompleteSandboxRootFSSnapshot(ctx context.Context, sandboxID string) error {
	store, ok := s.store.(nomadTemplateCaptureStore)
	if !ok {
		return service.ErrSandboxRootFSStoreUnavailable
	}
	candidate, err := store.ContinueNomadRunningRootFSCapture(ctx, sandboxID)
	if err != nil {
		return s.abortStaleRunningRootFSSnapshot(ctx, sandboxID, err)
	}
	if candidate == nil {
		return nil
	}
	if candidate.Completed {
		return nil
	}
	if candidate.Source == nil || candidate.Slot == nil {
		return fmt.Errorf("pending running rootfs capture has no exact writer authority")
	}
	dispatchErr := s.dispatchRunningRootFSCapture(ctx, candidate)
	request := runningRootFSCaptureRequestFromCandidate(candidate)
	completed, completionErr := store.RequestNomadRunningRootFSCapture(ctx, request)
	if completionErr == nil && completed != nil && completed.Completed && completed.Snapshot != nil {
		return nil
	}
	if dispatchErr != nil {
		return s.abortStaleRunningRootFSSnapshot(ctx, sandboxID,
			fmt.Errorf("dispatch recovered running rootfs capture: %w", dispatchErr))
	}
	if completionErr != nil {
		return completionErr
	}
	return fmt.Errorf("recovered writer checkpoint was not committed")
}

func (s *Service) abortStaleRunningRootFSSnapshot(
	ctx context.Context,
	sandboxID string,
	completionErr error,
) error {
	lifecycle, err := s.store.GetActiveLifecycleTxn(ctx, sandboxID)
	if err != nil {
		return errors.Join(completionErr, fmt.Errorf("load running snapshot lifecycle: %w", err))
	}
	if lifecycle == nil || lifecycle.Kind != sandboxstore.SandboxLifecycleKindSnapshot ||
		lifecycle.UpdatedAt.IsZero() || s.now().UTC().Sub(lifecycle.UpdatedAt) < defaultNomadRunningSnapshotRecoveryTimeout {
		return completionErr
	}
	aborter, ok := s.store.(nomadRunningRootFSCaptureAbortStore)
	if !ok {
		return completionErr
	}
	aborted, abortErr := aborter.AbortStaleNomadRunningRootFSCapture(
		ctx, lifecycle.ID, sandboxID,
		"stale running snapshot lost its exact source writer identity",
	)
	if abortErr != nil {
		return errors.Join(completionErr, abortErr)
	}
	if aborted {
		return nil
	}
	return completionErr
}

func runningRootFSCaptureRequestFromCandidate(
	candidate *sandboxstore.NomadTemplateCaptureCandidate,
) *sandboxstore.NomadRunningRootFSCaptureRequest {
	if candidate == nil {
		return nil
	}
	sourceSandboxID := ""
	if candidate.Source != nil {
		sourceSandboxID = candidate.Source.ID
	}
	return &sandboxstore.NomadRunningRootFSCaptureRequest{
		OperationID: candidate.OperationID, SourceSandboxID: sourceSandboxID,
		TeamID: candidate.TeamID, SnapshotID: candidate.SnapshotID,
		CaptureKind: candidate.CaptureKind, Name: candidate.Name,
		Description: candidate.Description, ExpiresAt: candidate.ExpiresAt,
	}
}

func runningRootFSSnapshotID(sandboxID, operationID string) string {
	digest := sha256.Sum256([]byte("sandbox0-running-rootfs-snapshot\x00" + sandboxID + "\x00" + operationID))
	return "rootfs-snapshot-" + hex.EncodeToString(digest[:16])
}

func mapNomadRunningRootFSSnapshotError(sandboxID string, err error) error {
	switch {
	case errors.Is(err, sandboxstore.ErrSandboxRecordNotFound):
		return apierror.NewNotFound("sandbox", sandboxID)
	case errors.Is(err, sandboxstore.ErrNomadTemplateCaptureConflict),
		errors.Is(err, sandboxstore.ErrNomadTemplateCaptureNotReady),
		errors.Is(err, sandboxstore.ErrRootFSFilesystemConflict),
		errors.Is(err, sandboxstore.ErrRootFSWriterGrantConflict),
		errdefs.IsInvalidArgument(err), errdefs.IsPermissionDenied(err), errdefs.IsFailedPrecondition(err):
		return apierror.NewConflict("sandbox", sandboxID, err)
	case errdefs.IsUnavailable(err):
		return fmt.Errorf("%w: %v", service.ErrSandboxCheckpointRequiresCtld, err)
	default:
		return fmt.Errorf("create running rootfs snapshot: %w", err)
	}
}
