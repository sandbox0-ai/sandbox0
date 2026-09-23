package nomadmigration

import (
	"context"
	"errors"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type CaptureFailureWork struct {
	Request protocol.MigrationCaptureFailureRequest
	Cleanup *protocol.MigrationCaptureFailureProof
}

type CaptureFailureStore interface {
	CaptureFailureRecoveryStore
	ListNomadMigrationCaptureFailures(context.Context, string, int) ([]string, error)
	GetNomadMigrationCaptureFailure(context.Context, string) (*CaptureFailureWork, error)
}

// CaptureFailureRecoveryStore is shared by migration and retained checkpoints;
// each operation keeps its own work discovery and durable evidence owner.
type CaptureFailureRecoveryStore interface {
	CommitNomadSandboxMigrationCaptureFailureCleanup(context.Context, protocol.MigrationCaptureFailureRequest, protocol.MigrationCaptureFailureProof) error
	AuthorizeNomadSandboxMigrationCaptureFailureFinalization(context.Context, string) (*protocol.MigrationCaptureFailureFinalizeRequest, error)
	CommitNomadSandboxMigrationCaptureFailureFinalization(context.Context, protocol.MigrationCaptureFailureFinalizeRequest, protocol.MigrationCaptureFailureFinalizeProof) error
}

// NewCaptureFailure recovers an already fenced source. Each pass advances one
// durable boundary; a lost node or commit reply reuses the original authority.
// Allocation retirement remains the existing terminal reconciler's job.
func NewCaptureFailure(store CaptureFailureStore, node protocol.NodeChannelMigrationCaptureFailureExecutor) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("failed capture recovery authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationCaptureFailures, step: func(ctx context.Context, id string) (bool, error) {
		work, err := store.GetNomadMigrationCaptureFailure(ctx, id)
		if err != nil || work == nil {
			return false, err
		}
		return advanceCaptureFailure(ctx, id, work, store, node)
	}}, nil
}

func advanceCaptureFailure(ctx context.Context, id string, work *CaptureFailureWork, store CaptureFailureRecoveryStore, node protocol.NodeChannelMigrationCaptureFailureExecutor) (bool, error) {
	want, err := work.Request.Digest()
	if err != nil || work.Request.Capture.Request.OperationID != id {
		return false, errors.New("invalid failed capture cleanup authority")
	}
	if work.Cleanup == nil {
		proof, err := node.CleanupFailedMigrationCapture(ctx, work.Request)
		if err != nil {
			return false, err
		}
		if proof == nil || proof.ValidateFor(work.Request) != nil {
			return false, errors.New("failed capture lacks physical cleanup proof")
		}
		err = store.CommitNomadSandboxMigrationCaptureFailureCleanup(ctx, work.Request, *proof)
		return err == nil, err
	}
	if work.Cleanup.ValidateFor(work.Request) != nil {
		return false, errors.New("failed capture cleanup receipt changed")
	}
	request, err := store.AuthorizeNomadSandboxMigrationCaptureFailureFinalization(ctx, id)
	if err != nil {
		return false, err
	}
	if request == nil || request.Proof != *work.Cleanup {
		return false, errors.New("failed capture finalization changed cleanup receipt")
	}
	actual, err := request.Request.Digest()
	if err != nil || actual != want {
		return false, errors.New("failed capture finalization changed writer authority")
	}
	proof, err := node.FinalizeFailedMigrationCapture(ctx, *request)
	if err != nil {
		return false, err
	}
	if proof == nil || proof.ValidateFor(*request) != nil {
		return false, errors.New("failed capture retains artifacts")
	}
	err = store.CommitNomadSandboxMigrationCaptureFailureFinalization(ctx, *request, *proof)
	return err == nil, err
}
