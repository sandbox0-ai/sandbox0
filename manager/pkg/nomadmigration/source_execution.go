package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// SourceExecution projects one existing reservation. Preparation delivery uses
// its original endpoint; capture dispatch requires a separate locked authority
// check. It neither selects a destination nor reconstructs launch configuration.
type SourceExecution struct {
	Assignment  runtimecontrol.MigrationAssignment
	Policy      string
	Address     string
	Preparation *procdapi.RuntimeMigrationRequest
	Capture     *protocol.MigrationCaptureRequest
}

func (s SourceExecution) Validate() error {
	ad, err := s.Assignment.Digest()
	if err != nil {
		return err
	}
	if s.Preparation != nil {
		pd, err := s.Preparation.Assignment.Digest()
		_, pe := s.Preparation.Digest()
		if err != nil || pe != nil || pd != ad || s.Preparation.Action != procdapi.MigrationPrepare || protocol.ValidateNomadProcdAddress(s.Address) != nil {
			return errors.New("source preparation changed authority")
		}
	}
	if s.Capture != nil {
		c := s.Capture
		if s.Preparation == nil || c.Validate() != nil || c.OperationID != s.Assignment.OperationID || c.SandboxID != s.Assignment.Target.SandboxID ||
			c.SourceGeneration != s.Assignment.SourceGeneration || c.AssignmentRevision != s.Assignment.SourceRevision ||
			c.ProcdInstanceID != s.Preparation.InstanceID || c.LifecycleEpoch != s.Preparation.LifecycleEpoch {
			return errors.New("source capture changed preparation")
		}
	}
	return nil
}

type SourceExecutionStore interface {
	ListNomadMigrationSourceExecutions(context.Context, string, int) ([]string, error)
	GetNomadMigrationSourceExecution(context.Context, string) (*SourceExecution, error)
	AuthorizeNomadSandboxMigrationPreparation(context.Context, runtimecontrol.MigrationAssignment, string) (*procdapi.RuntimeMigrationRequest, error)
	AuthorizeNomadSandboxMigrationCapture(context.Context, procdapi.RuntimeMigrationRequest, procdapi.RuntimeMigrationResponse) (*protocol.MigrationCaptureRequest, error)
	AuthorizeNomadMigrationSourceCaptureDispatch(context.Context, protocol.MigrationCaptureRequest) error
}

type SourceExecutionNode interface {
	CaptureMigration(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error)
}

// NewSourceExecution advances only reservations with committed CPU and staging
// evidence. Each pass authorizes preparation, acknowledges it, or dispatches
// the exact capture. The independent recovery worker seals and publishes the
// stopped source. Unknown capture outcomes never fall back to a fresh start.
func NewSourceExecution(store SourceExecutionStore, procd Procd, tokens HandoverTokens, node SourceExecutionNode) (*Coordinator, error) {
	if store == nil || procd == nil || tokens == nil || node == nil {
		return nil, errors.New("migration source execution authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationSourceExecutions, step: func(ctx context.Context, id string) (bool, error) {
		s, err := store.GetNomadMigrationSourceExecution(ctx, id)
		if err != nil || s == nil {
			return false, err
		}
		if s.Validate() != nil || s.Assignment.OperationID != id {
			return false, errors.New("invalid migration source execution work")
		}
		if s.Preparation == nil {
			command, err := store.AuthorizeNomadSandboxMigrationPreparation(ctx, s.Assignment, s.Policy)
			if err != nil {
				return false, err
			}
			if command == nil {
				return false, errors.New("missing migration preparation command")
			}
			want, _ := s.Assignment.Digest()
			got, err := command.Assignment.Digest()
			_, ce := command.Digest()
			if err != nil || ce != nil || got != want || command.Action != procdapi.MigrationPrepare {
				return false, errors.New("preparation changed source assignment")
			}
			return true, nil
		}
		if s.Capture == nil {
			// Authorization is rechecked after the queue read. Cancellation may win
			// immediately afterward; procd's retained cancellation tombstone rejects
			// a delayed prepare, and the capture transaction serializes both decisions.
			command, err := store.AuthorizeNomadSandboxMigrationPreparation(ctx, s.Assignment, s.Policy)
			if err != nil {
				return false, err
			}
			want, _ := s.Preparation.Digest()
			if command == nil {
				return false, errors.New("missing authorized preparation")
			}
			got, err := command.Digest()
			if err != nil || got != want {
				return false, errors.New("preparation retry changed command")
			}
			token, err := tokens.GenerateMigrationToken(*command)
			if err != nil {
				return false, err
			}
			if token == "" {
				return false, errors.New("empty migration preparation token")
			}
			receipt, err := procd.MigrateRuntime(ctx, s.Address, *command, token)
			if err != nil {
				return false, err
			}
			if receipt == nil || receipt.ValidateFor(*command) != nil {
				return false, errors.New("invalid source preparation acknowledgement")
			}
			capture, err := store.AuthorizeNomadSandboxMigrationCapture(ctx, *command, *receipt)
			if err != nil {
				return false, err
			}
			s.Capture = capture
			if capture == nil || s.Validate() != nil {
				return false, errors.New("capture changed source preparation")
			}
			return true, nil
		}
		if err := store.AuthorizeNomadMigrationSourceCaptureDispatch(ctx, *s.Capture); err != nil {
			return false, err
		}
		receipt, err := node.CaptureMigration(ctx, *s.Capture)
		if err != nil {
			return false, err
		}
		if receipt == nil || receipt.Validate() != nil || receipt.Request != *s.Capture {
			return false, errors.New("invalid source capture observation")
		}
		if receipt.State != protocol.MigrationCaptureIntent && receipt.State != protocol.MigrationCaptureComplete {
			return false, errors.New("source capture outcome requires reconciliation")
		}
		// Both pending and complete receipts are observations, not new regional
		// commits. Recovery independently seals the cut and authorizes its
		// publication. Reporting every completed observation as progress spins
		// RPCs while recovery is waiting for a busy node or retry backoff.
		return false, nil
	}}, nil
}
