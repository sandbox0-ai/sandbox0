package nomadmigration

import (
	"context"
	"errors"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type FailureFinalizationStore interface {
	ListNomadMigrationFailureFinalizations(context.Context, string, int) ([]string, error)
	AuthorizeNomadSandboxMigrationFailureFinalization(context.Context, string) (*protocol.MigrationFailureFinalizeRequest, error)
	CommitNomadSandboxMigrationFailureFinalization(context.Context, protocol.MigrationFailureFinalizeRequest, protocol.MigrationFailureFinalizeProof) error
}

// NewFailureFinalization releases target artifacts only after the committed
// writer retirement and exact physical cleanup receipt. Capacity remains owned
// until the allocation controller confirms its independent physical absence.
func NewFailureFinalization(store FailureFinalizationStore, node protocol.NodeChannelMigrationFailureFinalizeExecutor) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration failure finalization authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationFailureFinalizations, step: func(ctx context.Context, id string) (bool, error) {
		request, err := store.AuthorizeNomadSandboxMigrationFailureFinalization(ctx, id)
		if err != nil || request == nil {
			return false, err
		}
		if _, err := request.Digest(); err != nil || request.Request.Failure.Request.Restore.Image.Publication.Assignment.OperationID != id {
			return false, errors.New("failure finalization changed migration authority")
		}
		proof, err := node.FinalizeFailedMigrationDestination(ctx, *request)
		if err != nil {
			return false, err
		}
		if proof == nil || proof.ValidateFor(*request) != nil {
			return false, errors.New("invalid destination artifact finalization proof")
		}
		err = store.CommitNomadSandboxMigrationFailureFinalization(ctx, *request, *proof)
		return err == nil, err
	}}, nil
}
