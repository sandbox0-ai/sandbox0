package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// SourceRecovery contains historical authority for an already authorized
// capture. Reading it grants neither initial capture nor target execution.
type SourceRecovery struct {
	Assignment runtimecontrol.MigrationAssignment
	Capture    protocol.MigrationCaptureRequest
	Launch     protocol.MigrationCPULaunch
}

func (s SourceRecovery) Validate() error {
	if s.Assignment.Validate() != nil || s.Capture.Validate() != nil ||
		s.Assignment.OperationID != s.Capture.OperationID || s.Assignment.SourceGeneration != s.Capture.SourceGeneration ||
		s.Assignment.SourceRevision != s.Capture.AssignmentRevision || s.Assignment.Target.SandboxID != s.Capture.SandboxID {
		return errors.New("migration source recovery changed assignment")
	}
	return s.Launch.ValidateCapture(s.Capture, s.Launch.LaunchAttempt, s.Launch.Resources)
}

type SourceRecoveryStore interface {
	ListNomadMigrationSourceRecoveries(context.Context, string, int) ([]string, error)
	GetNomadMigrationSourceRecovery(context.Context, string) (*SourceRecovery, error)
	AuthorizeNomadSandboxMigrationPublication(context.Context, runtimecontrol.MigrationAssignment, protocol.MigrationCapture, string) (*protocol.MigrationPublicationRequest, error)
	AuthorizeNomadSandboxMigrationCaptureFailure(context.Context, protocol.MigrationCapture) (*protocol.MigrationCaptureFailureRequest, error)
}

type SourceRecoveryNode interface {
	RecoverMigrationCapture(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error)
}

// NewSourceRecovery finishes captured-source cuts and persists publication
// commands for the existing transfer worker. It has no capture dispatch API.
func NewSourceRecovery(store SourceRecoveryStore, node SourceRecoveryNode) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration source recovery authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationSourceRecoveries, step: func(ctx context.Context, id string) (bool, error) {
		source, err := store.GetNomadMigrationSourceRecovery(ctx, id)
		if err != nil || source == nil {
			return false, err
		}
		if source.Validate() != nil || source.Assignment.OperationID != id {
			return false, errors.New("invalid migration source recovery work")
		}
		captured, err := node.RecoverMigrationCapture(ctx, source.Capture)
		if err != nil {
			return false, err
		}
		if captured == nil || captured.Validate() != nil || captured.Request != source.Capture {
			return false, errors.New("source recovery changed captured runtime")
		}
		if captured.State == protocol.MigrationCaptureIntent {
			return false, nil
		}
		if captured.State == protocol.MigrationCaptureUncertain {
			command, err := store.AuthorizeNomadSandboxMigrationCaptureFailure(ctx, *captured)
			if err != nil {
				return false, err
			}
			if command == nil || command.Capture.Request != source.Capture || command.Capture.RequestDigest != captured.RequestDigest {
				return false, errors.New("failed capture changed source recovery authority")
			}
			_, err = command.Digest()
			return err == nil, err
		}
		if captured.State != protocol.MigrationCaptureComplete || captured.RootFS == nil {
			return false, errors.New("source recovery lacks a completed consistent cut")
		}
		cpu, err := source.Launch.GuestCPUProfile().Digest()
		if err != nil {
			return false, err
		}
		command, err := store.AuthorizeNomadSandboxMigrationPublication(ctx, source.Assignment, *captured, cpu)
		if err != nil {
			return false, err
		}
		if command == nil || command.CPULaunch == nil || command.Capture.Request != source.Capture {
			return false, errors.New("publication changed source recovery authority")
		}
		expected := protocol.MigrationPublicationRequest{Assignment: source.Assignment, Capture: *captured, CPUFeaturesDigest: cpu, CPULaunch: &source.Launch, CompatibilityDigest: command.CompatibilityDigest}
		want, err := expected.Digest()
		got, actualErr := command.Digest()
		if err != nil || actualErr != nil || got != want {
			return false, errors.New("publication changed recovered cut or CPU history")
		}
		return true, nil
	}}, nil
}
