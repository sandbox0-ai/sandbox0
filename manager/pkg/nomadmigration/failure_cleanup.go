package nomadmigration

import (
	"context"
	"errors"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type FailureCleanupStore interface {
	ListNomadMigrationFailureCleanups(context.Context, string, int) ([]string, error)
	AuthorizeNomadSandboxMigrationFailureCleanup(context.Context, string) (*protocol.MigrationFailureCleanupRequest, error)
	CommitNomadSandboxMigrationFailureCleanup(context.Context, protocol.MigrationFailureCleanupRequest, protocol.MigrationFailureCleanupProof) error
}

// NewFailureCleanup fences the writer before node teardown and commits its
// retirement only after exact physical evidence. Allocation and image custody
// remain owned by the migration's subsequent finalization protocol.
func NewFailureCleanup(store FailureCleanupStore, node protocol.NodeChannelMigrationFailureCleanupExecutor) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration failure cleanup authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationFailureCleanups, step: func(ctx context.Context, id string) (bool, error) {
		request, err := store.AuthorizeNomadSandboxMigrationFailureCleanup(ctx, id)
		if err != nil || request == nil {
			return false, err
		}
		if _, err := request.Digest(); err != nil || request.Failure.Request.Restore.Image.OperationID() != id {
			return false, errors.New("failure cleanup changed migration authority")
		}
		proof, err := node.CleanupFailedMigrationDestination(ctx, *request)
		if err != nil {
			return false, err
		}
		if proof == nil || proof.ValidateFor(*request) != nil {
			return false, errors.New("invalid destination physical cleanup proof")
		}
		err = store.CommitNomadSandboxMigrationFailureCleanup(ctx, *request, *proof)
		return err == nil, err
	}}, nil
}
