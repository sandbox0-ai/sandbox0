package nomadmigration

import (
	"context"
	"errors"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type FailureStore interface {
	ListNomadMigrationFailures(context.Context, string, int) ([]string, error)
	AuthorizeNomadSandboxMigrationFailure(context.Context, string) (*protocol.MigrationFailureRequest, error)
}

type FailureStopStore interface {
	ListNomadMigrationFailureStops(context.Context, string, int) ([]string, error)
	GetNomadMigrationFailure(context.Context, string) (*protocol.MigrationFailureRequest, error)
	CommitNomadSandboxMigrationFailureStop(context.Context, protocol.MigrationFailureRequest, protocol.MigrationFailureStopProof) error
}

// NewFailureStop delivers committed failure intent and records the target's
// execution-absence receipt. Storage and carrier cleanup are later transitions.
func NewFailureStop(store FailureStopStore, node protocol.NodeChannelMigrationFailureExecutor) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration failure stop authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationFailureStops, step: func(ctx context.Context, id string) (bool, error) {
		request, err := store.GetNomadMigrationFailure(ctx, id)
		if err != nil || request == nil {
			return false, err
		}
		if _, err := request.Digest(); err != nil || request.Restore.Image.OperationID() != id {
			return false, errors.New("failure stop changed migration authority")
		}
		proof, err := node.StopFailedMigrationDestination(ctx, *request)
		if err != nil {
			return false, err
		}
		if proof == nil || proof.ValidateFor(*request) != nil {
			return false, errors.New("invalid destination execution stop receipt")
		}
		err = store.CommitNomadSandboxMigrationFailureStop(ctx, *request, *proof)
		return err == nil, err
	}}, nil
}

// NewFailure records due failure decisions before any destructive node work.
// Its progress counter reports regional intent, never a completed migration,
// physical absence, or released resources.
func NewFailure(store FailureStore) (*Coordinator, error) {
	if store == nil {
		return nil, errors.New("migration failure store is required")
	}
	return &Coordinator{list: store.ListNomadMigrationFailures, step: func(ctx context.Context, id string) (bool, error) {
		request, err := store.AuthorizeNomadSandboxMigrationFailure(ctx, id)
		if err != nil || request == nil {
			return false, err
		}
		if _, err := request.Digest(); err != nil || request.Restore.Image.OperationID() != id {
			return false, errors.New("failure decision changed migration authority")
		}
		return true, nil
	}}, nil
}
