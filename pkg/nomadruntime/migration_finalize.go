package nomadruntime

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// migrationFinalizeRuntime is used only by ctld's source-finalization command,
// after it verifies the adopted destination and exact source custody. It is
// deliberately absent from the ordinary driver RootFS RPC surface.
type migrationFinalizeRuntime interface {
	FinalizeMigrationRootFS(context.Context, rootfshandoff.StageRequest, rootfshandoff.MigrationRootFSFinalizeRequest) (rootfshandoff.MigrationRootFSFinalizeProof, error)
	ForgetFinalizedMigrationRootFS(rootfshandoff.StageRequest, rootfshandoff.MigrationRootFSFinalizeRequest) error
}

func (r *rootfsRuntime) FinalizeMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSFinalizeRequest) (rootfshandoff.MigrationRootFSFinalizeProof, error) {
	if r.authority == nil {
		return rootfshandoff.MigrationRootFSFinalizeProof{}, fmt.Errorf("regional writer authority is required for migration finalization")
	}
	if err := r.authority.VerifyTerminalWriterGrant(ctx, stage); err != nil {
		return rootfshandoff.MigrationRootFSFinalizeProof{}, fmt.Errorf("verify retired migration source writer: %w", err)
	}
	return r.sessions.FinalizeMigrationRootFS(ctx, stage, request)
}

func (r *rootfsRuntime) ForgetFinalizedMigrationRootFS(stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSFinalizeRequest) error {
	return r.sessions.ForgetFinalizedMigrationRootFS(stage, request)
}
