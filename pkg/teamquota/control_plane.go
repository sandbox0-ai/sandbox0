package teamquota

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Control-plane owner kinds keep region-owned PostgreSQL objects distinct in
// the shared allocation ledger.
const (
	ControlPlaneOwnerKindAPIKey                = "api_key"
	ControlPlaneOwnerKindSSHPublicKey          = "ssh_public_key"
	ControlPlaneOwnerKindCredentialSource      = "credential_source"
	ControlPlaneOwnerKindSandboxEgressBindings = "sandbox_egress_bindings"
	ControlPlaneOwnerKindTemplate              = "template"
	ControlPlaneOwnerKindTemplateBuild         = "template_build"
)

// ControlPlaneObjectOwner returns a region-scoped owner for team-created
// PostgreSQL control-plane state.
func ControlPlaneObjectOwner(teamID, kind, id string) Owner {
	return Owner{
		TeamID: strings.TrimSpace(teamID),
		Kind:   strings.TrimSpace(kind),
		ID:     strings.TrimSpace(id),
	}
}

// ReserveControlPlaneObjectTargetTx reserves a complete object-count target in
// the caller's business transaction.
func ReserveControlPlaneObjectTargetTx(
	ctx context.Context,
	store CapacityTxStore,
	tx pgx.Tx,
	owner Owner,
	operationKind string,
	target int64,
) (OperationRef, error) {
	if store == nil {
		return OperationRef{}, &UnavailableError{
			Operation: "reserve control-plane object quota",
			Err:       fmt.Errorf("capacity store is not configured"),
		}
	}
	operation := Operation{
		ID:   uuid.NewString(),
		Kind: strings.TrimSpace(operationKind),
	}
	reservation, err := store.ReserveTargetTx(ctx, tx, ReserveRequest{
		Owner:     owner,
		Operation: operation,
		Target: Values{
			KeyControlPlaneObjectCount: target,
		},
	})
	if err != nil {
		return OperationRef{}, err
	}
	return Ref(reservation.Owner, reservation.Operation), nil
}

// CommitControlPlaneObjectTargetTx commits a previously reserved object-count
// target in the caller's business transaction.
func CommitControlPlaneObjectTargetTx(
	ctx context.Context,
	store CapacityTxStore,
	tx pgx.Tx,
	ref OperationRef,
) error {
	if store == nil {
		return &UnavailableError{
			Operation: "commit control-plane object quota",
			Err:       fmt.Errorf("capacity store is not configured"),
		}
	}
	return store.CommitTx(ctx, tx, ref)
}

// BeginControlPlaneObjectReleaseTx records a lower object-count target without
// releasing committed usage before the business rows are removed.
func BeginControlPlaneObjectReleaseTx(
	ctx context.Context,
	store CapacityTxStore,
	tx pgx.Tx,
	owner Owner,
	operationKind string,
	target int64,
) (OperationRef, error) {
	if store == nil {
		return OperationRef{}, &UnavailableError{
			Operation: "begin control-plane object quota release",
			Err:       fmt.Errorf("capacity store is not configured"),
		}
	}
	operation := Operation{
		ID:   uuid.NewString(),
		Kind: strings.TrimSpace(operationKind),
	}
	reservation, err := store.BeginReleaseTx(ctx, tx, ReleaseRequest{
		Owner:     owner,
		Operation: operation,
		Target: Values{
			KeyControlPlaneObjectCount: target,
		},
	})
	if err != nil {
		return OperationRef{}, err
	}
	return Ref(reservation.Owner, reservation.Operation), nil
}

// ConfirmControlPlaneObjectReleaseTx commits a lower object-count target after
// the business rows have been removed in the same transaction.
func ConfirmControlPlaneObjectReleaseTx(
	ctx context.Context,
	store CapacityTxStore,
	tx pgx.Tx,
	ref OperationRef,
) error {
	if store == nil {
		return &UnavailableError{
			Operation: "confirm control-plane object quota release",
			Err:       fmt.Errorf("capacity store is not configured"),
		}
	}
	return store.ConfirmReleaseTx(ctx, tx, ref, RuntimeRef{})
}
