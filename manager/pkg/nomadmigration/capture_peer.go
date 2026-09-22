package nomadmigration

import (
	"context"
	"errors"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type capturePeerStore interface {
	AuthorizeNomadSandboxMigrationCapturePeer(context.Context, runtimecontrol.MigrationAssignment) (*protocol.MigrationCapturePeerRequest, error)
	CommitNomadSandboxMigrationCapturePeer(context.Context, runtimecontrol.MigrationAssignment, protocol.MigrationCapturePeerRequest, *protocol.MigrationCapturePeerPrepared) error
}

type capturePeerNode interface {
	PrepareMigrationCapturePeer(context.Context, protocol.MigrationCapturePeerRequest) (*protocol.MigrationCapturePeerPrepared, error)
}

const capturePeerPreparationTimeout = 500 * time.Millisecond

// Peer preparation is an optional part of existing staging, before procd is
// interrupted. Persisted fallback permits the ordinary published-image path;
// it never releases the receiver's staging custody or assumes an RPC had no
// effect. An invalid acknowledgement is an authority error, not a cache miss.
func prepareCapturePeer(ctx context.Context, staging StagingStore, node StagingNode, a runtimecontrol.MigrationAssignment) (bool, error) {
	store, ok := staging.(capturePeerStore)
	if !ok {
		return false, nil
	}
	request, err := store.AuthorizeNomadSandboxMigrationCapturePeer(ctx, a)
	if err != nil || request == nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	source := request.Staging.Source
	if request.Validate() != nil || source.OperationID != a.OperationID || source.SandboxID != a.Target.SandboxID ||
		source.SourceGeneration != a.SourceGeneration || source.AssignmentRevision != a.SourceRevision {
		return false, errors.New("capture peer changed reserved source authority")
	}
	var receipt *protocol.MigrationCapturePeerPrepared
	if peer, ok := node.(capturePeerNode); ok {
		attempt, cancel := context.WithTimeout(ctx, capturePeerPreparationTimeout)
		receipt, err = peer.PrepareMigrationCapturePeer(attempt, *request)
		cancel()
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if err != nil {
			receipt = nil
		} else if receipt == nil || receipt.ValidateFor(*request) != nil {
			return false, errors.New("invalid capture peer preparation acknowledgement")
		}
	}
	err = store.CommitNomadSandboxMigrationCapturePeer(ctx, a, *request, receipt)
	return err == nil, err
}
