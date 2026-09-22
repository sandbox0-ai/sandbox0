package runtimeslotnode

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationAdoptionStore interface {
	CommitNomadSandboxMigrationAdoption(context.Context, protocol.MigrationAdoptionRequest, protocol.MigrationAdoptionProof) error
}

var _ migrationAdoptionStore = (*sandboxstore.PGSandboxStore)(nil)

// commitMigrationAdoption consumes only an authenticated response to the exact
// target's command-ready command. PostgreSQL checks the immutable request
// issued by generation CAS before making source cleanup eligible. A lost DB
// acknowledgement retries this historical fact without renewing execution.
func (h *ChannelHub) commitMigrationAdoption(ctx context.Context, command protocol.NodeChannelCommand, result protocol.NodeChannelResult) error {
	if command.Kind != protocol.NodeChannelCommandCommandReady || result.ValidateFor(command) != nil {
		return fmt.Errorf("invalid migration command-ready response: %w", errdefs.ErrUnavailable)
	}
	receipt := result.ControlResponse.MigrationAdoption
	if receipt == nil {
		return nil
	}
	store, ok := h.capacityStore.(migrationAdoptionStore)
	if !ok {
		return fmt.Errorf("regional migration adoption store is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := store.CommitNomadSandboxMigrationAdoption(ctx, receipt.Request, receipt.Proof); err != nil {
		return fmt.Errorf("commit migration target adoption: %w", err)
	}
	return nil
}
