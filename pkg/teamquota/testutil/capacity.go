// Package testutil provides explicit Team Quota fakes for tests that exercise
// callers without a PostgreSQL quota ledger.
package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

// PermissiveCapacityStore accepts every capacity transition without retaining
// state. It must only be used by tests that are not validating quota behavior.
type PermissiveCapacityStore struct{}

var (
	_ teamquota.CapacityStore   = (*PermissiveCapacityStore)(nil)
	_ teamquota.CapacityTxStore = (*PermissiveCapacityStore)(nil)
)

// NewPermissiveCapacityStore creates a stateless test-only capacity store.
func NewPermissiveCapacityStore() *PermissiveCapacityStore {
	return &PermissiveCapacityStore{}
}

func (*PermissiveCapacityStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	return reservation(request.Owner, request.Operation, request.Target), nil
}

func (*PermissiveCapacityStore) ReserveDelta(
	_ context.Context,
	request teamquota.DeltaRequest,
) (*teamquota.Reservation, error) {
	return reservation(request.Owner, request.Operation, request.Delta), nil
}

func (*PermissiveCapacityStore) AttachRuntime(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	return nil
}

func (*PermissiveCapacityStore) Commit(context.Context, teamquota.OperationRef) error {
	return nil
}

func (*PermissiveCapacityStore) Abort(context.Context, teamquota.OperationRef, string) error {
	return nil
}

func (*PermissiveCapacityStore) BeginRelease(
	_ context.Context,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	return reservation(request.Owner, request.Operation, request.Target), nil
}

func (*PermissiveCapacityStore) ConfirmRelease(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	return nil
}

func (*PermissiveCapacityStore) ReconcileTarget(
	context.Context,
	teamquota.Owner,
	teamquota.Values,
	teamquota.RuntimeRef,
) error {
	return nil
}

func (*PermissiveCapacityStore) PrepareTransfer(
	_ context.Context,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return reservation(request.Destination, request.Operation, request.DestinationTarget), nil
}

func (*PermissiveCapacityStore) CommitTransfer(context.Context, teamquota.OperationRef) error {
	return nil
}

func (*PermissiveCapacityStore) CommitTransferObservedSource(
	context.Context,
	teamquota.OperationRef,
	teamquota.Values,
) error {
	return nil
}

func (*PermissiveCapacityStore) AbortTransfer(context.Context, teamquota.OperationRef, string) error {
	return nil
}

func (*PermissiveCapacityStore) TransferTarget(
	_ context.Context,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return reservation(request.Destination, request.Operation, request.DestinationTarget), nil
}

func (s *PermissiveCapacityStore) ReserveTargetTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	return s.ReserveTarget(ctx, request)
}

func (s *PermissiveCapacityStore) ReserveDeltaTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.DeltaRequest,
) (*teamquota.Reservation, error) {
	return s.ReserveDelta(ctx, request)
}

func (s *PermissiveCapacityStore) AttachRuntimeTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	runtime teamquota.RuntimeRef,
) error {
	return s.AttachRuntime(ctx, operation, runtime)
}

func (s *PermissiveCapacityStore) CommitTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
) error {
	return s.Commit(ctx, operation)
}

func (s *PermissiveCapacityStore) AbortTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	reason string,
) error {
	return s.Abort(ctx, operation, reason)
}

func (s *PermissiveCapacityStore) BeginReleaseTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	return s.BeginRelease(ctx, request)
}

func (s *PermissiveCapacityStore) ConfirmReleaseTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	runtime teamquota.RuntimeRef,
) error {
	return s.ConfirmRelease(ctx, operation, runtime)
}

func (s *PermissiveCapacityStore) ReconcileTargetTx(
	ctx context.Context,
	_ pgx.Tx,
	owner teamquota.Owner,
	target teamquota.Values,
	runtime teamquota.RuntimeRef,
) error {
	return s.ReconcileTarget(ctx, owner, target, runtime)
}

func (s *PermissiveCapacityStore) PrepareTransferTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return s.PrepareTransfer(ctx, request)
}

func (s *PermissiveCapacityStore) CommitTransferTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
) error {
	return s.CommitTransfer(ctx, operation)
}

func (s *PermissiveCapacityStore) AbortTransferTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	reason string,
) error {
	return s.AbortTransfer(ctx, operation, reason)
}

func (s *PermissiveCapacityStore) TransferTargetTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return s.TransferTarget(ctx, request)
}

func reservation(
	owner teamquota.Owner,
	operation teamquota.Operation,
	target teamquota.Values,
) *teamquota.Reservation {
	return &teamquota.Reservation{
		Owner:     owner,
		Operation: operation,
		Target:    target.Clone(),
	}
}
