package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type StagingStore interface {
	ListNomadMigrationStaging(context.Context, string, int) ([]string, error)
	GetNomadMigrationStagingAssignment(context.Context, string) (*runtimecontrol.MigrationAssignment, error)
	AuthorizeNomadSandboxMigrationStaging(context.Context, runtimecontrol.MigrationAssignment) (*protocol.MigrationStagingRequest, error)
	CommitNomadSandboxMigrationStaging(context.Context, runtimecontrol.MigrationAssignment, protocol.MigrationStagingRequest, protocol.MigrationStagingReserved) error
	ListNomadMigrationStagingReleases(context.Context, string, int) ([]string, error)
	AuthorizeNomadSandboxMigrationStagingRelease(context.Context, string) (*protocol.MigrationStagingRequest, error)
	CommitNomadSandboxMigrationStagingRelease(context.Context, protocol.MigrationStagingRequest, protocol.MigrationStagingReleased) error
}

type StagingNode interface {
	ReserveMigrationStaging(context.Context, protocol.MigrationStagingRequest) (*protocol.MigrationStagingReserved, error)
	ReleaseMigrationStaging(context.Context, protocol.MigrationStagingRequest) error
}

// NewStaging advances one durable node reservation per pass. It has no procd
// preparation or checkpoint method; both reservations precede that authority.
func NewStaging(store StagingStore, node StagingNode) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration staging authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationStaging, step: func(ctx context.Context, id string) (bool, error) {
		a, err := store.GetNomadMigrationStagingAssignment(ctx, id)
		if err != nil || a == nil {
			return false, err
		}
		if a.Validate() != nil || a.OperationID != id {
			return false, errors.New("staging work changed assignment")
		}
		request, err := store.AuthorizeNomadSandboxMigrationStaging(ctx, *a)
		if err != nil || request == nil {
			return false, err
		}
		if request.Validate() != nil || request.Source.OperationID != id || request.Source.SandboxID != a.Target.SandboxID ||
			request.Source.SourceGeneration != a.SourceGeneration || request.Source.AssignmentRevision != a.SourceRevision {
			return false, errors.New("staging command changed source authority")
		}
		receipt, err := node.ReserveMigrationStaging(ctx, *request)
		if err != nil {
			return false, err
		}
		if receipt == nil || receipt.ValidateFor(*request) != nil {
			return false, errors.New("invalid staging reservation receipt")
		}
		err = store.CommitNomadSandboxMigrationStaging(ctx, *a, *request, *receipt)
		return err == nil, err
	}}, nil
}

// Release recovery is independent of fresh admission/CPU windows. It replays
// immutable cancellation or physical-finalization authority only.
func NewStagingRelease(store StagingStore, node StagingNode) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration staging release authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationStagingReleases, step: func(ctx context.Context, id string) (bool, error) {
		request, err := store.AuthorizeNomadSandboxMigrationStagingRelease(ctx, id)
		if err != nil || request == nil {
			return false, err
		}
		if request.Source.OperationID != id || request.Validate() != nil {
			return false, errors.New("staging release changed operation")
		}
		if err := node.ReleaseMigrationStaging(ctx, *request); err != nil {
			return false, err
		}
		digest, err := request.Digest()
		if err != nil {
			return false, err
		}
		err = store.CommitNomadSandboxMigrationStagingRelease(ctx, *request, protocol.MigrationStagingReleased{RequestDigest: digest})
		return err == nil, err
	}}, nil
}
