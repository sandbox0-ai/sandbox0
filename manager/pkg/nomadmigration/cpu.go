package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// CPUPreflightWork is a projection of a reserved migration. The original
// assignment is retained at reservation, never reconstructed from live config.
type CPUPreflightWork struct {
	Assignment      runtimecontrol.MigrationAssignment
	SourceCommitted bool
}

type CPUPreflightStore interface {
	ListNomadMigrationCPUPreflights(context.Context, string, int) ([]string, error)
	GetNomadMigrationCPUPreflightWork(context.Context, string) (*CPUPreflightWork, error)
	AuthorizeNomadSandboxMigrationCPUPreflight(context.Context, runtimecontrol.MigrationAssignment) (*protocol.MigrationCPUPreflightRequest, error)
	AuthorizeNomadSandboxMigrationTargetCPUPreflight(context.Context, runtimecontrol.MigrationAssignment) (*protocol.MigrationCPUPreflightRequest, error)
	CommitNomadSandboxMigrationCPUPreflight(context.Context, runtimecontrol.MigrationAssignment, protocol.MigrationCPUPreflightRequest, protocol.MigrationCPUPreflight) error
}

type CPUPreflightNode interface {
	PreflightMigrationCPU(context.Context, protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error)
}

// NewCPUPreflight resumes read-only eligibility probes through authenticated
// node channels. Each pass commits one receipt. It never prepares or captures a
// workload, changes the selected destination, or refreshes the original TTL.
func NewCPUPreflight(store CPUPreflightStore, node CPUPreflightNode) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration CPU preflight authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationCPUPreflights, step: func(ctx context.Context, id string) (bool, error) {
		work, err := store.GetNomadMigrationCPUPreflightWork(ctx, id)
		if err != nil || work == nil {
			return false, err
		}
		a := work.Assignment
		if a.Validate() != nil || a.OperationID != id {
			return false, errors.New("migration CPU preflight changed assignment")
		}
		var request *protocol.MigrationCPUPreflightRequest
		if work.SourceCommitted {
			request, err = store.AuthorizeNomadSandboxMigrationTargetCPUPreflight(ctx, a)
		} else {
			request, err = store.AuthorizeNomadSandboxMigrationCPUPreflight(ctx, a)
		}
		if err != nil {
			return false, err
		}
		if request == nil || request.Validate() != nil || request.IsSource() == work.SourceCommitted ||
			request.Source.OperationID != id || request.Source.SandboxID != a.Target.SandboxID ||
			request.Source.SourceGeneration != a.SourceGeneration || request.Source.AssignmentRevision != a.SourceRevision {
			return false, errors.New("migration CPU command changed reserved source or role")
		}
		receipt, err := node.PreflightMigrationCPU(ctx, *request)
		if err != nil {
			return false, err
		}
		if receipt == nil || receipt.ValidateFor(*request) != nil {
			return false, errors.New("node returned invalid migration CPU preflight")
		}
		err = store.CommitNomadSandboxMigrationCPUPreflight(ctx, a, *request, *receipt)
		return err == nil, err
	}}, nil
}
