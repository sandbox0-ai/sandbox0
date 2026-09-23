package nomadmigration

import (
	"context"
	"errors"
	"reflect"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// CheckpointPauseWork projects existing lifecycle evidence. It contains no new
// phase or destination reservation; fenced sources enter ordinary reconciliation.
type CheckpointPauseWork struct {
	Failure                *CaptureFailureWork
	FailureFinalized       *protocol.MigrationCaptureFailureFinalizeProof
	FailureStagingReleased *protocol.MigrationStagingReleased
	Staging                *protocol.MigrationStagingRequest
	CancelDue              bool
	Address                string
	Preflight              protocol.MigrationCPUPreflightRequest
	CPU                    *protocol.MigrationCPUPreflight
	Staged                 *protocol.MigrationStagingReserved
	Preparation            *procdapi.RuntimeCheckpointRequest
	CaptureAuthorized      bool
	Publication            *protocol.MigrationPublicationRequest
	Published              *protocol.MigrationPublication
}
type CheckpointPauseStore interface {
	CheckpointCancellationStore
	CaptureFailureRecoveryStore
	AuthorizeNomadSandboxMigrationCaptureFailure(context.Context, protocol.MigrationCapture) (*protocol.MigrationCaptureFailureRequest, error)
	CommitNomadCheckpointFailedStagingRelease(context.Context, protocol.MigrationStagingRequest, protocol.MigrationStagingReleased) error
	ListNomadCheckpointPauses(context.Context, string, int) ([]string, error)
	GetNomadCheckpointPauseWork(context.Context, string) (*CheckpointPauseWork, error)
	CommitNomadCheckpointCPU(context.Context, protocol.MigrationCPUPreflightRequest, protocol.MigrationCPUPreflight) error
	AuthorizeNomadCheckpointStaging(context.Context, string) (*protocol.MigrationStagingRequest, error)
	CommitNomadCheckpointStaging(context.Context, protocol.MigrationStagingRequest, protocol.MigrationStagingReserved) error
	AuthorizeNomadCheckpointPreparation(context.Context, string) (*procdapi.RuntimeCheckpointRequest, error)
	AuthorizeNomadCheckpointCapture(context.Context, procdapi.RuntimeCheckpointRequest, procdapi.RuntimeCheckpointResponse) (*protocol.MigrationCaptureRequest, error)
	AuthorizeNomadCheckpointCaptureDispatch(context.Context, protocol.MigrationCaptureRequest) error
	AuthorizeNomadCheckpointPublication(context.Context, protocol.MigrationCapture) (*protocol.MigrationPublicationRequest, error)
	CommitNomadCheckpointPublication(context.Context, protocol.MigrationPublicationRequest, protocol.MigrationPublication) (*protocol.CheckpointRetained, error)
	AuthorizeNomadCheckpointSourceFence(context.Context, protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceRequest, error)
	CommitNomadCheckpointSourceFence(context.Context, protocol.MigrationSourceFenceRequest, protocol.MigrationSourceFenceProof) error
}
type CheckpointPauseNode interface {
	protocol.NodeChannelMigrationCaptureFailureExecutor
	ReleaseMigrationStaging(context.Context, protocol.MigrationStagingRequest) error
	CPUPreflightNode
	SourceExecutionNode
	SourceRecoveryNode
	ReserveMigrationStaging(context.Context, protocol.MigrationStagingRequest) (*protocol.MigrationStagingReserved, error)
	PublishMigration(context.Context, protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error)
	FenceMigrationSource(context.Context, protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error)
}
type CheckpointProcd interface {
	CheckpointRuntime(context.Context, string, procdapi.RuntimeCheckpointRequest, string) (*procdapi.RuntimeCheckpointResponse, error)
}
type CheckpointTokens interface {
	GenerateCheckpointToken(procdapi.RuntimeCheckpointRequest) (string, error)
}

// NewCheckpointPause shares migration's bounded worker, authenticated node
// commands, stock-runsc capture, regional publication and physical source fence.
// Uncertain capture never starts again; retirement finishes in the existing
// terminal reconciler after the region owns the immutable image.
func NewCheckpointPause(store CheckpointPauseStore, node CheckpointPauseNode, observer CheckpointSourceObserver, procd CheckpointProcd, tokens CheckpointTokens) (*Coordinator, error) {
	if store == nil || node == nil || observer == nil || procd == nil || tokens == nil {
		return nil, errors.New("checkpoint pause authorities are required")
	}
	return &Coordinator{list: store.ListNomadCheckpointPauses, step: func(ctx context.Context, id string) (bool, error) {
		w, err := store.GetNomadCheckpointPauseWork(ctx, id)
		if err != nil || w == nil {
			return false, err
		}
		if w.Failure != nil {
			if w.FailureFinalized == nil {
				return advanceCaptureFailure(ctx, id, w.Failure, store, node)
			}
			if w.Staging == nil || w.FailureStagingReleased != nil || w.Staging.Source.OperationID != id || !w.Staging.CaptureOnly {
				return false, errors.New("failed checkpoint staging changed source")
			}
			if err := node.ReleaseMigrationStaging(ctx, *w.Staging); err != nil {
				return false, err
			}
			digest, err := w.Staging.Digest()
			if err != nil {
				return false, err
			}
			err = store.CommitNomadCheckpointFailedStagingRelease(ctx, *w.Staging, protocol.MigrationStagingReleased{RequestDigest: digest})
			return err == nil, err
		}
		if w.CancelDue {
			return cancelCheckpointPreparation(ctx, id, store, node, observer, procd, tokens)
		}
		source := w.Preflight.Source
		if !w.Preflight.CaptureOnly || w.Preflight.Validate() != nil || source.OperationID != id {
			return false, errors.New("checkpoint work changed captured source")
		}
		if w.CPU == nil {
			receipt, err := node.PreflightMigrationCPU(ctx, w.Preflight)
			if err != nil {
				return false, err
			}
			if receipt == nil || receipt.ValidateFor(w.Preflight) != nil {
				return false, errors.New("checkpoint CPU response changed source")
			}
			err = store.CommitNomadCheckpointCPU(ctx, w.Preflight, *receipt)
			return err == nil, err
		}
		if w.Staged == nil {
			request, err := store.AuthorizeNomadCheckpointStaging(ctx, id)
			if err != nil {
				return false, err
			}
			if request == nil || request.Validate() != nil || !request.CaptureOnly || request.Source != source {
				return false, errors.New("checkpoint staging changed source")
			}
			receipt, err := node.ReserveMigrationStaging(ctx, *request)
			if err != nil {
				return false, err
			}
			if receipt == nil || receipt.ValidateFor(*request) != nil {
				return false, errors.New("checkpoint staging response changed command")
			}
			err = store.CommitNomadCheckpointStaging(ctx, *request, *receipt)
			return err == nil, err
		}
		if !w.CaptureAuthorized {
			command, err := store.AuthorizeNomadCheckpointPreparation(ctx, id)
			if err != nil {
				return false, err
			}
			if command == nil || command.Action != procdapi.MigrationPrepare || command.Capture.OperationID != id || command.InstanceID != source.ProcdInstanceID ||
				command.Capture.SandboxID != source.SandboxID || command.Capture.Revision != source.AssignmentRevision || command.Capture.RuntimeGeneration != source.SourceGeneration || command.CaptureEpoch != source.LifecycleEpoch || protocol.ValidateNomadProcdAddress(w.Address) != nil {
				return false, errors.New("checkpoint preparation changed source")
			}
			if _, err := command.Digest(); err != nil {
				return false, err
			}
			if w.Preparation != nil && !reflect.DeepEqual(w.Preparation, command) {
				return false, errors.New("checkpoint preparation changed on retry")
			}
			token, err := tokens.GenerateCheckpointToken(*command)
			if err != nil {
				return false, err
			}
			if token == "" {
				return false, errors.New("checkpoint preparation token is empty")
			}
			receipt, err := procd.CheckpointRuntime(ctx, w.Address, *command, token)
			if err != nil {
				return false, err
			}
			if receipt == nil || receipt.ValidateFor(*command) != nil {
				return false, errors.New("checkpoint preparation acknowledgement changed command")
			}
			capture, err := store.AuthorizeNomadCheckpointCapture(ctx, *command, *receipt)
			if err != nil {
				return false, err
			}
			if capture == nil || *capture != source {
				return false, errors.New("checkpoint capture changed prepared source")
			}
			return true, nil
		}
		if w.Publication == nil {
			// Recover before dispatch: an existing intent belongs to the driver, and
			// an uncertain outcome can only enter physical failure reconciliation.
			captured, err := node.RecoverMigrationCapture(ctx, source)
			if errdefs.IsNotFound(err) {
				if err := store.AuthorizeNomadCheckpointCaptureDispatch(ctx, source); err != nil {
					return false, err
				}
				captured, err = node.CaptureMigration(ctx, source)
			}
			if err != nil {
				return false, err
			}
			if captured == nil || captured.Validate() != nil || captured.Request != source {
				return false, errors.New("checkpoint capture observation changed source")
			}
			if captured.State == protocol.MigrationCaptureIntent {
				return false, nil
			}
			if captured.State == protocol.MigrationCaptureUncertain {
				request, err := store.AuthorizeNomadSandboxMigrationCaptureFailure(ctx, *captured)
				if err != nil {
					return false, err
				}
				if request == nil || request.Capture.Request != source {
					return false, errors.New("checkpoint failure authority changed source")
				}
				_, err = request.Digest()
				return err == nil, err
			}
			if captured.State != protocol.MigrationCaptureComplete {
				return false, errors.New("checkpoint capture outcome requires physical failure resolution")
			}
			publication, err := store.AuthorizeNomadCheckpointPublication(ctx, *captured)
			if err != nil {
				return false, err
			}
			if publication == nil || publication.CheckpointSource == nil || !reflect.DeepEqual(publication.Capture, *captured) {
				return false, errors.New("checkpoint publication changed captured cut")
			}
			_, err = publication.Digest()
			return err == nil, err
		}
		if w.Publication.CheckpointSource == nil || w.Publication.Capture.Request != source {
			return false, errors.New("checkpoint publication changed source")
		}
		if _, err := w.Publication.Digest(); err != nil {
			return false, err
		}
		if w.Published == nil {
			receipt, err := node.PublishMigration(ctx, *w.Publication)
			if err != nil {
				return false, err
			}
			if receipt == nil || receipt.ValidateFor(*w.Publication) != nil {
				return false, errors.New("checkpoint publication acknowledgement changed image")
			}
			retained, err := store.CommitNomadCheckpointPublication(ctx, *w.Publication, *receipt)
			if err != nil {
				return false, err
			}
			if retained == nil || retained.ValidateFor(*w.Publication, *receipt) != nil || retained.CheckpointID != id {
				return false, errors.New("checkpoint image custody was not retained")
			}
			return true, nil
		}
		want := protocol.MigrationSourceFenceRequest{PublicationRequest: *w.Publication, Publication: *w.Published}
		command, err := store.AuthorizeNomadCheckpointSourceFence(ctx, want)
		if err != nil {
			return false, err
		}
		if command == nil || !reflect.DeepEqual(*command, want) {
			return false, errors.New("checkpoint fence changed retained image")
		}
		if _, err := command.Digest(); err != nil {
			return false, err
		}
		proof, err := node.FenceMigrationSource(ctx, *command)
		if err != nil {
			return false, err
		}
		if proof == nil || proof.ValidateFor(*command) != nil {
			return false, errors.New("checkpoint source fence lacks physical proof")
		}
		err = store.CommitNomadCheckpointSourceFence(ctx, *command, *proof)
		return err == nil, err
	}}, nil
}
