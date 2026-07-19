package service

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func ptrTo[T any](value T) *T {
	return &value
}

func testWarmPoolTeamQuotaTarget(
	template *v1alpha1.SandboxTemplate,
	replicas int32,
) (teamquota.Values, error) {
	if template == nil {
		return nil, fmt.Errorf("template is required")
	}
	if replicas < 0 {
		return nil, fmt.Errorf("warm pool replicas must be non-negative")
	}
	perPod := PodSpecTeamQuotaResources(ptrTo(v1alpha1.BuildIdlePodSpec(template)))
	target := teamquota.Values{
		teamquota.KeySandboxRuntimeCount:          int64(replicas),
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
	for _, key := range []teamquota.Key{
		teamquota.KeySandboxCPUMillicores,
		teamquota.KeySandboxMemoryBytes,
		teamquota.KeySandboxEphemeralStorageBytes,
	} {
		value, err := multiplyQuotaValue(perPod[key], int64(replicas))
		if err != nil {
			return nil, fmt.Errorf("compute warm pool %s quota target: %w", key, err)
		}
		target[key] = value
	}
	return target, nil
}

type permissiveTeamQuotaCapacityStore struct{}

func (*permissiveTeamQuotaCapacityStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	return testQuotaReservation(request.Owner, request.Operation, request.Target), nil
}

func (*permissiveTeamQuotaCapacityStore) ReserveDelta(
	_ context.Context,
	request teamquota.DeltaRequest,
) (*teamquota.Reservation, error) {
	return testQuotaReservation(request.Owner, request.Operation, request.Delta), nil
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

func (*permissiveTeamQuotaCapacityStore) CommitObservedExact(
	context.Context,
	teamquota.OperationRef,
	teamquota.Values,
) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) Abort(context.Context, teamquota.OperationRef, string) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) BeginRelease(
	_ context.Context,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	return testQuotaReservation(request.Owner, request.Operation, request.Target), nil
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
	return testQuotaReservation(
		request.Destination,
		request.Operation,
		request.DestinationTarget,
	), nil
}

func (*permissiveTeamQuotaCapacityStore) CommitTransfer(
	context.Context,
	teamquota.OperationRef,
) error {
	return nil
}

func (*permissiveTeamQuotaCapacityStore) CommitTransferObservedSource(
	context.Context,
	teamquota.OperationRef,
	teamquota.Values,
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
	return testQuotaReservation(
		request.Destination,
		request.Operation,
		request.DestinationTarget,
	), nil
}

func (s *permissiveTeamQuotaCapacityStore) ReserveTargetTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	return s.ReserveTarget(ctx, request)
}

func (s *permissiveTeamQuotaCapacityStore) ReserveDeltaTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.DeltaRequest,
) (*teamquota.Reservation, error) {
	return s.ReserveDelta(ctx, request)
}

func (s *permissiveTeamQuotaCapacityStore) AttachRuntimeTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	runtime teamquota.RuntimeRef,
) error {
	return s.AttachRuntime(ctx, operation, runtime)
}

func (s *permissiveTeamQuotaCapacityStore) CommitTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
) error {
	return s.Commit(ctx, operation)
}

func (s *permissiveTeamQuotaCapacityStore) AbortTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	reason string,
) error {
	return s.Abort(ctx, operation, reason)
}

func (s *permissiveTeamQuotaCapacityStore) BeginReleaseTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	return s.BeginRelease(ctx, request)
}

func (s *permissiveTeamQuotaCapacityStore) ConfirmReleaseTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	runtime teamquota.RuntimeRef,
) error {
	return s.ConfirmRelease(ctx, operation, runtime)
}

func (s *permissiveTeamQuotaCapacityStore) ReconcileTargetTx(
	ctx context.Context,
	_ pgx.Tx,
	owner teamquota.Owner,
	target teamquota.Values,
	runtime teamquota.RuntimeRef,
) error {
	return s.ReconcileTarget(ctx, owner, target, runtime)
}

func (s *permissiveTeamQuotaCapacityStore) PrepareTransferTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return s.PrepareTransfer(ctx, request)
}

func (s *permissiveTeamQuotaCapacityStore) CommitTransferTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
) error {
	return s.CommitTransfer(ctx, operation)
}

func (s *permissiveTeamQuotaCapacityStore) AbortTransferTx(
	ctx context.Context,
	_ pgx.Tx,
	operation teamquota.OperationRef,
	reason string,
) error {
	return s.AbortTransfer(ctx, operation, reason)
}

func (s *permissiveTeamQuotaCapacityStore) TransferTargetTx(
	ctx context.Context,
	_ pgx.Tx,
	request teamquota.TransferRequest,
) (*teamquota.Reservation, error) {
	return s.TransferTarget(ctx, request)
}

func (s *memorySandboxStore) PrepareRootFSObjectPublish(
	ctx context.Context,
	stageID string,
	state *SandboxRootFSState,
	_ time.Time,
	quotaStore teamquota.CapacityTxStore,
) error {
	owner, err := rootFSObjectQuotaOwner(state.TeamID, state.DiffObjectKey)
	if err != nil {
		return err
	}
	stageOwner, err := rootFSPublishStageQuotaOwner(state.TeamID, stageID)
	if err != nil {
		return err
	}
	operation := rootFSPublishStageQuotaOperation(rootFSPublishTransferOperationKind, stageID)
	if _, err := quotaStore.TransferTargetTx(ctx, nil, teamquota.TransferRequest{
		Source:      stageOwner,
		Destination: owner,
		Operation:   operation,
		SourceDecrease: teamquota.Values{
			teamquota.KeyStorageObjectCount: 1,
		},
		DestinationTarget: teamquota.Values{
			teamquota.KeyRootFSStorageBytes: state.DiffSize,
			teamquota.KeyStorageObjectCount: 1,
		},
		Runtime: teamquota.RuntimeRef{Namespace: "manager", Name: state.SandboxID, UID: stageID},
	}); err != nil {
		return err
	}
	return nil
}

func (*memorySandboxStore) PrepareRootFSPublishStage(
	ctx context.Context,
	stage RootFSPublishStage,
	quotaStore teamquota.CapacityTxStore,
) error {
	owner, err := rootFSPublishStageQuotaOwner(stage.TeamID, stage.StageID)
	if err != nil {
		return err
	}
	operation := rootFSPublishStageQuotaOperation(rootFSPublishStageOperationKind, stage.StageID)
	if _, err := quotaStore.ReserveTargetTx(ctx, nil, teamquota.ReserveRequest{
		Owner:     owner,
		Operation: operation,
		Target: teamquota.Values{
			teamquota.KeyStorageObjectCount: 1,
		},
	}); err != nil {
		return err
	}
	return quotaStore.CommitTx(ctx, nil, teamquota.Ref(owner, operation))
}

func (*memorySandboxStore) ListDueRootFSPublishStages(context.Context, int) ([]RootFSPublishStage, error) {
	return nil, nil
}

func (*memorySandboxStore) ReleaseRootFSPublishStage(
	context.Context,
	string,
	teamquota.CapacityTxStore,
) (bool, error) {
	return false, nil
}

func testQuotaReservation(
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

func (memorySandboxStoreTx) TeamQuotaTx() pgx.Tx {
	return nil
}

type rejectingForkTeamQuotaCapacityStore struct {
	permissiveTeamQuotaCapacityStore
	requests []teamquota.ReserveRequest
}

func (s *rejectingForkTeamQuotaCapacityStore) ReserveTargetTx(
	_ context.Context,
	_ pgx.Tx,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	s.requests = append(s.requests, request)
	return nil, &teamquota.ExceededError{
		TeamID:    request.Owner.TeamID,
		Key:       teamquota.KeySandboxIdentityCount,
		Limit:     1,
		Committed: 1,
		Requested: request.Target[teamquota.KeySandboxIdentityCount],
	}
}
