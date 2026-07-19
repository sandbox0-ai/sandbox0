package http

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

type permissiveTeamQuotaCapacityStore struct{}

func (*permissiveTeamQuotaCapacityStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	return quotaReservation(request.Owner, request.Operation, request.Target), nil
}

func (*permissiveTeamQuotaCapacityStore) ReserveDelta(
	_ context.Context,
	request teamquota.DeltaRequest,
) (*teamquota.Reservation, error) {
	return quotaReservation(request.Owner, request.Operation, request.Delta), nil
}

func (*permissiveTeamQuotaCapacityStore) AttachRuntime(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) Commit(context.Context, teamquota.OperationRef) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) Abort(context.Context, teamquota.OperationRef, string) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) BeginRelease(
	_ context.Context,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	return quotaReservation(request.Owner, request.Operation, request.Target), nil
}

func (*permissiveTeamQuotaCapacityStore) ConfirmRelease(
	context.Context,
	teamquota.OperationRef,
	teamquota.RuntimeRef,
) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) ReconcileTarget(
	context.Context,
	teamquota.Owner,
	teamquota.Values,
	teamquota.RuntimeRef,
) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) PrepareTransfer(
	_ context.Context,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return quotaReservation(request.Destination, request.Operation, request.DestinationTarget), nil
}

func (*permissiveTeamQuotaCapacityStore) CommitTransfer(
	context.Context,
	teamquota.OperationRef,
) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) AbortTransfer(
	context.Context,
	teamquota.OperationRef,
	string,
) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) TransferTarget(
	_ context.Context,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return quotaReservation(request.Destination, request.Operation, request.DestinationTarget), nil
}

func quotaReservation(
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

type permissiveTeamQuotaRateLimiter struct{}

func (permissiveTeamQuotaRateLimiter) Take(
	context.Context,
	string,
	teamquota.Key,
	int64,
) (tokenbucket.Decision, error) {
	return tokenbucket.Decision{Allowed: true}, nil
}
