package teamquota

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// PolicyReader resolves effective policies and their current usage without
// granting authority to mutate the distributed enforcement state.
type PolicyReader interface {
	ListStatus(ctx context.Context, teamID string) ([]Status, error)
	EffectivePolicy(ctx context.Context, teamID string, key Key) (*Policy, error)
}

// PolicyManager is the region policy-owner contract. Implementations must
// serialize PostgreSQL policy changes with the distributed guard, enforcement
// epoch, Redis generation, and local-credit quarantine.
type PolicyManager interface {
	PolicyReader
	PutTeamPolicy(ctx context.Context, teamID string, policy Policy) error
	DeleteTeamPolicy(ctx context.Context, teamID string, key Key) error
	ReplaceDefaultPoliciesVersioned(
		ctx context.Context,
		policies []Policy,
		version DefaultPolicyVersion,
	) error
}

// TeamLifecycleStore owns the durable quota tombstone used by coordinated team
// deletion. Retention cleanup may prune it only after token expiry and identity
// absence are independently established.
type TeamLifecycleStore interface {
	DisableTeamAdmission(ctx context.Context, teamID string) error
	DisableTeamAdmissionWithFinalCheck(
		ctx context.Context,
		teamID string,
		finalCheck func(context.Context) error,
	) error
	FinalizeTeamDeletion(ctx context.Context, teamID string) error
}

// TeamAdmissionStateResolver reads the durable PostgreSQL tombstone used to
// recover distributed rate markers after Redis state loss.
type TeamAdmissionStateResolver interface {
	TeamAdmissionDisabled(ctx context.Context, teamID string) (bool, error)
}

// CapacityStore atomically reserves and transitions region-wide allocations.
type CapacityStore interface {
	ReserveTarget(ctx context.Context, request ReserveRequest) (*Reservation, error)
	ReserveDelta(ctx context.Context, request DeltaRequest) (*Reservation, error)
	AttachRuntime(ctx context.Context, operation OperationRef, runtime RuntimeRef) error
	Commit(ctx context.Context, operation OperationRef) error
	Abort(ctx context.Context, operation OperationRef, reason string) error
	BeginRelease(ctx context.Context, request ReleaseRequest) (*Reservation, error)
	ConfirmRelease(ctx context.Context, operation OperationRef, runtime RuntimeRef) error
	ReconcileTarget(ctx context.Context, owner Owner, target Values, runtime RuntimeRef) error
	PrepareTransfer(ctx context.Context, request TransferRequest) (*Reservation, error)
	CommitTransfer(ctx context.Context, operation OperationRef) error
	AbortTransfer(ctx context.Context, operation OperationRef, reason string) error
	TransferTarget(ctx context.Context, request TransferRequest) (*Reservation, error)
}

// ExactCapacityStore atomically commits a measured target within a prepared
// operation's admitted upper bound.
type ExactCapacityStore interface {
	CommitExact(ctx context.Context, operation OperationRef, exact Values) error
}

// ObservedExactCapacityStore atomically adopts a measured physical target while
// finalizing a prepared operation. Unlike CommitExact, it may exceed the
// admitted pending target because the resource already exists; callers must use
// it only for recovery or post-mutation observation, never for admission.
type ObservedExactCapacityStore interface {
	CommitObservedExact(ctx context.Context, operation OperationRef, exact Values) error
}

// ObservedTransferStore commits a prepared transfer while atomically adopting
// any observed external source capacity above its nominal post-transfer target.
type ObservedTransferStore interface {
	CommitTransferObservedSource(
		ctx context.Context,
		operation OperationRef,
		observedSource Values,
	) error
}

// TransferStateStore resolves durable transfer terminality for external marker
// garbage collection. A missing operation is terminal because runtime markers
// are written only after PrepareTransfer succeeds.
type TransferStateStore interface {
	TransferStates(
		ctx context.Context,
		teamID string,
		operationIDs []string,
	) (map[string]string, error)
}

// RevisionReconcileStore conditionally applies exact inventory observations.
// It prevents a stale reconciler snapshot from overwriting a newer allocation
// mutation. Revision zero represents an allocation that was absent.
type RevisionReconcileStore interface {
	ReconcileTargetIfRevision(
		ctx context.Context,
		owner Owner,
		target Values,
		runtime RuntimeRef,
		expectedRevision int64,
	) (bool, error)
}

// CapacityTxStore exposes the same transitions inside a caller-owned business
// transaction, allowing sandbox lifecycle state and quota state to commit
// together.
type CapacityTxStore interface {
	ReserveTargetTx(ctx context.Context, tx pgx.Tx, request ReserveRequest) (*Reservation, error)
	ReserveDeltaTx(ctx context.Context, tx pgx.Tx, request DeltaRequest) (*Reservation, error)
	AttachRuntimeTx(ctx context.Context, tx pgx.Tx, operation OperationRef, runtime RuntimeRef) error
	CommitTx(ctx context.Context, tx pgx.Tx, operation OperationRef) error
	AbortTx(ctx context.Context, tx pgx.Tx, operation OperationRef, reason string) error
	BeginReleaseTx(ctx context.Context, tx pgx.Tx, request ReleaseRequest) (*Reservation, error)
	ConfirmReleaseTx(ctx context.Context, tx pgx.Tx, operation OperationRef, runtime RuntimeRef) error
	ReconcileTargetTx(ctx context.Context, tx pgx.Tx, owner Owner, target Values, runtime RuntimeRef) error
	PrepareTransferTx(ctx context.Context, tx pgx.Tx, request TransferRequest) (*Reservation, error)
	CommitTransferTx(ctx context.Context, tx pgx.Tx, operation OperationRef) error
	AbortTransferTx(ctx context.Context, tx pgx.Tx, operation OperationRef, reason string) error
	TransferTargetTx(ctx context.Context, tx pgx.Tx, request TransferRequest) (*Reservation, error)
}

// ExactCapacityTxStore exposes exact finalization inside a caller-owned
// transaction.
type ExactCapacityTxStore interface {
	CommitExactTx(ctx context.Context, tx pgx.Tx, operation OperationRef, exact Values) error
}

// ObservedExactCapacityTxStore exposes observed physical finalization inside a
// caller-owned transaction.
type ObservedExactCapacityTxStore interface {
	CommitObservedExactTx(ctx context.Context, tx pgx.Tx, operation OperationRef, exact Values) error
}
