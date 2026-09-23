package nomadmigration

import (
	"context"
	"errors"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// CheckpointCancellation is the irreversible withdrawal of capture authority.
// It reopens only an unchanged source and releases only unused staging space.
type CheckpointCancellation struct {
	OperationID       string
	Address           string
	Source            protocol.NodeChannelTarget
	Namespace         string
	Preparation       *procdapi.RuntimeCheckpointRequest
	Canceled          *procdapi.RuntimeCheckpointResponse
	SourceAbsentProof []byte
	SourceTerminal    bool
	Staging           *protocol.MigrationStagingRequest
	Released          *protocol.MigrationStagingReleased
}
type CheckpointCancellationStore interface {
	AuthorizeNomadCheckpointCancellation(context.Context, string) (*CheckpointCancellation, error)
	CommitNomadCheckpointCancellation(context.Context, procdapi.RuntimeCheckpointRequest, procdapi.RuntimeCheckpointResponse) error
	CommitNomadCheckpointSourceAbsent(context.Context, string, protocol.NodeChannelTarget, []byte) error
	CommitNomadCheckpointCanceledStaging(context.Context, protocol.MigrationStagingRequest, protocol.MigrationStagingReleased) error
	CompleteNomadCheckpointCancellation(context.Context, string) error
}

// CheckpointSourceObserver uses Nomad's exact server and direct-client view.
// A missing procd response by itself never proves that the guest has stopped.
type CheckpointSourceObserver interface {
	ObserveCheckpointSource(context.Context, protocol.NodeChannelTarget, string) (bool, []byte, error)
	RetireTerminalCheckpointSource(context.Context, string, protocol.NodeChannelTarget, string) error
}

func cancelCheckpointPreparation(ctx context.Context, id string, store CheckpointCancellationStore, node CheckpointPauseNode, observer CheckpointSourceObserver, procd CheckpointProcd, tokens CheckpointTokens) (bool, error) {
	work, err := store.AuthorizeNomadCheckpointCancellation(ctx, id)
	if err != nil || work == nil {
		return false, err
	}
	if work.OperationID != id {
		return false, errors.New("checkpoint cancellation changed operation")
	}
	if work.Preparation != nil && work.Canceled == nil && len(work.SourceAbsentProof) == 0 {
		request := *work.Preparation
		if request.Action != procdapi.MigrationCancel || request.Capture.OperationID != id || protocol.ValidateNomadProcdAddress(work.Address) != nil {
			return false, errors.New("checkpoint cancellation changed source")
		}
		if _, err := request.Digest(); err != nil {
			return false, err
		}
		token, err := tokens.GenerateCheckpointToken(request)
		if err != nil {
			return false, err
		}
		if token == "" {
			return false, errors.New("checkpoint cancellation token is empty")
		}
		receipt, err := procd.CheckpointRuntime(ctx, work.Address, request, token)
		if err != nil {
			absent, proof, observeErr := observer.ObserveCheckpointSource(ctx, work.Source, work.Namespace)
			if observeErr != nil {
				return false, err
			}
			if !absent && work.SourceTerminal {
				if retireErr := observer.RetireTerminalCheckpointSource(ctx, id, work.Source, work.Namespace); retireErr != nil {
					return false, retireErr
				}
				absent, proof, observeErr = observer.ObserveCheckpointSource(ctx, work.Source, work.Namespace)
			}
			if observeErr != nil || !absent || !work.SourceTerminal {
				return false, err
			}
			commitErr := store.CommitNomadCheckpointSourceAbsent(ctx, id, work.Source, proof)
			return commitErr == nil, commitErr
		}
		if receipt == nil || receipt.ValidateFor(request) != nil {
			return false, errors.New("checkpoint cancellation lacks exact acknowledgement")
		}
		err = store.CommitNomadCheckpointCancellation(ctx, request, *receipt)
		return err == nil, err
	}
	if work.Staging != nil && work.Released == nil {
		request := *work.Staging
		if !request.CaptureOnly || request.Source.OperationID != id || request.Validate() != nil {
			return false, errors.New("checkpoint release changed staging")
		}
		if err := node.ReleaseMigrationStaging(ctx, request); err != nil {
			return false, err
		}
		digest, err := request.Digest()
		if err != nil {
			return false, err
		}
		err = store.CommitNomadCheckpointCanceledStaging(ctx, request, protocol.MigrationStagingReleased{RequestDigest: digest})
		return err == nil, err
	}
	err = store.CompleteNomadCheckpointCancellation(ctx, id)
	return err == nil, err
}
