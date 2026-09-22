package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

// Destination contains only regional immutable input. Policy is the exact
// source-applied payload captured with preparation, not rebuilt configuration.
type Destination struct {
	Image        protocol.MigrationImagePrepareRequest
	Policy       string
	PolicyDigest string
}

// ValidateSourcePolicy bounds and binds the captured payload before it can
// authorize a destination network; exact byte identity is checked separately.
func ValidateSourcePolicy(assignment runtimecontrol.MigrationAssignment, policy string) error {
	if len(policy) == 0 || len(policy) > protocol.MaxNetworkPolicyBytes {
		return errors.New("migration source policy exceeds bounds")
	}
	spec, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(policy)
	if err != nil || spec == nil || spec.Version != "v1" || spec.SandboxID != assignment.Target.SandboxID || spec.TeamID != assignment.Target.TeamID ||
		(spec.Mode != v1alpha1.NetworkModeAllowAll && spec.Mode != v1alpha1.NetworkModeBlockAll) {
		return errors.New("migration source policy changed sandbox identity")
	}
	return nil
}

func (d Destination) Validate() error {
	if err := d.Image.Validate(); err != nil {
		return err
	}
	if err := ValidateSourcePolicy(d.Image.Publication.Assignment, d.Policy); err != nil {
		return err
	}
	if protocol.NetworkPolicyDigest(d.Policy) != d.PolicyDigest {
		return errors.New("migration source policy digest changed")
	}
	return nil
}

type DestinationStore interface {
	ListNomadMigrationDestinations(context.Context, string, int) ([]string, error)
	GetNomadMigrationDestination(context.Context, string) (*Destination, error)
	AuthorizeNomadSandboxMigrationHandover(context.Context, protocol.MigrationRestoreObservation) (*procdapi.RuntimeMigrationRequest, error)
}

// Restorer must use migration-specific writer and execution authorization.
// Ordinary claim is intentionally absent from this interface.
type Restorer interface {
	RestoreMigration(context.Context, protocol.MigrationImagePrepareRequest, string) (*protocol.MigrationRestoreObservation, error)
}

// NewDestination shares the bounded cursor/worker with transfer recovery but
// runs in a separate lane. It stops after durable procd handover authorization;
// that command must still execute before any probe or routing publication.
func NewDestination(store DestinationStore, restorer Restorer) (*Coordinator, error) {
	if store == nil || restorer == nil {
		return nil, errors.New("migration destination authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationDestinations, step: func(ctx context.Context, id string) (bool, error) {
		d, err := store.GetNomadMigrationDestination(ctx, id)
		if err != nil || d == nil {
			return false, err
		}
		if err := d.Validate(); err != nil {
			return false, err
		}
		if d.Image.Publication.Assignment.OperationID != id {
			return false, errors.New("migration destination changed operation")
		}
		restored, err := restorer.RestoreMigration(ctx, d.Image, d.Policy)
		if err != nil {
			return false, err
		}
		if restored == nil || restored.Validate() != nil || restored.State != protocol.MigrationRestoreComplete {
			return false, errors.New("migration lacks completed restore evidence")
		}
		want, _ := d.Image.Digest()
		actual, err := restored.Request.Image.Digest()
		if err != nil || actual != want || restored.Request.Stage.ExpectedPolicyToken.PolicyDigest != d.PolicyDigest {
			return false, errors.New("restored destination changed image or network policy")
		}
		command, err := store.AuthorizeNomadSandboxMigrationHandover(ctx, *restored)
		if err != nil {
			return false, err
		}
		if command == nil {
			return false, errors.New("migration handover authority returned no command")
		}
		wantAssignment, _ := d.Image.Publication.Assignment.Digest()
		actualAssignment, err := command.Assignment.Digest()
		if err != nil || actualAssignment != wantAssignment || command.Action != procdapi.MigrationRestore || command.InstanceID != d.Image.Publication.Capture.Request.ProcdInstanceID || command.LifecycleEpoch != d.Image.Publication.Capture.Request.LifecycleEpoch {
			return false, errors.New("migration handover command changed restored execution")
		}
		if _, err := command.Digest(); err != nil {
			return false, err
		}
		return true, nil
	}}, nil
}
