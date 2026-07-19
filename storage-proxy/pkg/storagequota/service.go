// Package storagequota coordinates storage mutations with region-scoped team
// capacity allocations.
package storagequota

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

const (
	OwnerKindVolume   = "volume"
	OwnerKindSnapshot = "snapshot"

	maxMutationObservationAttempts = 4

	// CatalogObjectCount charges the PostgreSQL catalog row owned by a storage
	// allocation in addition to its S0FS state records.
	CatalogObjectCount int64 = 1

	regionRecoveryScopePrefix = "region-storage:"
)

// Store is the PostgreSQL-backed TeamQuota contract used by storage-proxy.
type Store interface {
	EffectivePolicy(context.Context, string, teamquota.Key) (*teamquota.Policy, error)
	ReserveDelta(context.Context, teamquota.DeltaRequest) (*teamquota.Reservation, error)
	Commit(context.Context, teamquota.OperationRef) error
	CommitExact(context.Context, teamquota.OperationRef, teamquota.Values) error
	Abort(context.Context, teamquota.OperationRef, string) error
	BeginRelease(context.Context, teamquota.ReleaseRequest) (*teamquota.Reservation, error)
	ConfirmRelease(context.Context, teamquota.OperationRef, teamquota.RuntimeRef) error
	ConfirmReleaseTx(context.Context, pgx.Tx, teamquota.OperationRef, teamquota.RuntimeRef) error
	ReconcileTargetIfRevision(
		context.Context,
		teamquota.Owner,
		teamquota.Values,
		teamquota.RuntimeRef,
		int64,
	) (bool, error)
	GetRecoveryAllocation(context.Context, teamquota.Owner) (*teamquota.RecoveryAllocation, error)
}

type recoveryAllocationLister interface {
	ListRecoveryAllocations(
		context.Context,
		teamquota.RecoveryAllocationFilter,
	) ([]teamquota.RecoveryAllocation, error)
}

// Service serializes local mutations for an owner and applies fail-closed
// capacity admission before resource-increasing physical changes.
type Service struct {
	store         Store
	recoveryScope string

	locksMu sync.Mutex
	locks   map[string]*ownerLock
}

type ownerLock struct {
	mu   sync.Mutex
	refs int
}

// New returns a storage quota coordinator. recoveryScope must be stable for
// every storage-proxy instance that shares the same regional catalog. A nil
// store remains fail closed.
func New(store Store, recoveryScope string) *Service {
	return &Service{
		store:         store,
		recoveryScope: strings.TrimSpace(recoveryScope),
		locks:         make(map[string]*ownerLock),
	}
}

// RegionRecoveryScope returns the stable quota recovery scope shared by every
// storage-proxy instance in one region.
func RegionRecoveryScope(regionID string) string {
	regionID = strings.TrimSpace(regionID)
	if regionID == "" {
		return ""
	}
	return regionRecoveryScopePrefix + regionID
}

// VolumeOwner identifies one volume's complete storage allocation.
func (s *Service) VolumeOwner(teamID, volumeID string) teamquota.Owner {
	return teamquota.Owner{
		TeamID:    strings.TrimSpace(teamID),
		Kind:      OwnerKindVolume,
		ID:        strings.TrimSpace(volumeID),
		ClusterID: s.recoveryScope,
	}
}

// SnapshotOwner identifies one immutable snapshot's complete storage allocation.
func (s *Service) SnapshotOwner(teamID, snapshotID string) teamquota.Owner {
	return teamquota.Owner{
		TeamID:    strings.TrimSpace(teamID),
		Kind:      OwnerKindSnapshot,
		ID:        strings.TrimSpace(snapshotID),
		ClusterID: s.recoveryScope,
	}
}

// Measure returns one owner's complete storage quota target.
type Measure func() (teamquota.Values, error)

// Bound returns a conservative complete target from an observed physical
// target. Mutate converts it to non-negative growth before admission.
type Bound func(before teamquota.Values) (teamquota.Values, error)

// VolumeTarget returns the complete quota target for a volume owner.
func VolumeTarget(storageBytes, objectCount int64) teamquota.Values {
	return teamquota.Values{
		teamquota.KeyVolumeStorageBytes: storageBytes,
		teamquota.KeyStorageObjectCount: objectCount,
	}
}

// SnapshotTarget returns the complete quota target for a snapshot owner.
func SnapshotTarget(storageBytes, objectCount int64) teamquota.Values {
	return teamquota.Values{
		teamquota.KeySnapshotStorageBytes: storageBytes,
		teamquota.KeyStorageObjectCount:   objectCount,
	}
}

// Mutate applies one physical storage mutation. The local owner lock avoids
// redundant work within one process. Cross-replica correctness comes from a
// durable PostgreSQL operation fence: after reserving non-negative worst-case
// growth against the current committed allocation, Mutate measures the physical
// baseline again and retries if it changed before the fence was acquired. Even
// an all-zero growth request creates the fence. maximum must include every key
// owned by the resource. A measurable result is finalized atomically within
// the admitted upper bound.
func (s *Service) Mutate(
	ctx context.Context,
	owner teamquota.Owner,
	operationKind string,
	before Measure,
	maximum Bound,
	mutate func() error,
	exact Measure,
) error {
	if before == nil || maximum == nil || mutate == nil || exact == nil {
		return unavailable("prepare storage mutation", fmt.Errorf("before, maximum, mutation, and exact callbacks are required"))
	}

	unlock := s.lock(owner)
	defer unlock()

	if err := s.requirePolicies(ctx, owner); err != nil {
		return err
	}
	operation, physicalBefore, admittedMaximum, err := s.reserveStableMutation(
		ctx,
		owner,
		operationKind,
		before,
		maximum,
	)
	if err != nil {
		return err
	}

	if mutationErr := mutate(); mutationErr != nil {
		exactTarget, exactErr := exact()
		if exactErr == nil {
			exactErr = validateTarget(owner, exactTarget)
		}
		if exactErr != nil {
			// A failed external mutation can still have consumed resources. If
			// its result cannot be observed, retain the admitted maximum.
			commitErr := s.store.Commit(ctx, teamquota.Ref(owner, operation))
			return errors.Join(
				mutationErr,
				unavailable("measure failed storage mutation", exactErr),
				normalize("commit conservative failed storage mutation", commitErr),
			)
		}
		if targetsEqual(exactTarget, physicalBefore) {
			abortErr := s.store.Abort(ctx, teamquota.Ref(owner, operation), mutationErr.Error())
			if abortErr == nil {
				return mutationErr
			}
			return errors.Join(
				mutationErr,
				normalize("abort unchanged storage mutation", abortErr),
			)
		}

		// The failed call partially changed physical state. Finalize the exact
		// result and release unused reservation in one PostgreSQL transaction.
		boundErr := normalizeBoundError("validate failed storage mutation bound", exactTarget, admittedMaximum)
		finalizeErr := s.commitMeasuredTarget(
			ctx,
			teamquota.Ref(owner, operation),
			exactTarget,
			boundErr != nil,
		)
		return errors.Join(
			mutationErr,
			normalize("finalize partially failed storage mutation", finalizeErr),
			boundErr,
		)
	}

	exactTarget, exactErr := exact()
	if exactErr == nil {
		exactErr = validateTarget(owner, exactTarget)
	}
	if exactErr != nil {
		// The physical mutation succeeded. Keep the conservative reservation
		// committed so a failed observation can never undercount usage.
		if commitErr := s.store.Commit(ctx, teamquota.Ref(owner, operation)); commitErr != nil {
			return errors.Join(
				unavailable("measure storage mutation", exactErr),
				normalize("commit conservative storage capacity", commitErr),
			)
		}
		return unavailable("measure storage mutation", exactErr)
	}

	boundErr := normalizeBoundError("validate storage mutation bound", exactTarget, admittedMaximum)
	finalizeErr := s.commitMeasuredTarget(
		ctx,
		teamquota.Ref(owner, operation),
		exactTarget,
		boundErr != nil,
	)
	return errors.Join(
		boundErr,
		normalize("finalize exact storage capacity", finalizeErr),
	)
}

func (s *Service) reserveStableMutation(
	ctx context.Context,
	owner teamquota.Owner,
	operationKind string,
	before Measure,
	maximum Bound,
) (teamquota.Operation, teamquota.Values, teamquota.Values, error) {
	var lastObserved teamquota.Values
	for attempt := 0; attempt < maxMutationObservationAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return teamquota.Operation{}, nil, nil, unavailable("prepare storage mutation", err)
		}
		beforeTarget, err := before()
		if err != nil {
			return teamquota.Operation{}, nil, nil, unavailable("measure storage before mutation", err)
		}
		if err := validateTarget(owner, beforeTarget); err != nil {
			return teamquota.Operation{}, nil, nil, unavailable("validate storage target before mutation", err)
		}
		maximumTarget, err := maximum(beforeTarget.Clone())
		if err != nil {
			return teamquota.Operation{}, nil, nil, unavailable("calculate storage mutation bound", err)
		}
		if err := validateTarget(owner, maximumTarget); err != nil {
			return teamquota.Operation{}, nil, nil, unavailable("validate storage mutation bound", err)
		}
		if err := requireComponentwiseAtLeast(maximumTarget, beforeTarget); err != nil {
			return teamquota.Operation{}, nil, nil, unavailable("validate storage mutation bound", err)
		}
		growth, err := nonNegativeGrowth(beforeTarget, maximumTarget)
		if err != nil {
			return teamquota.Operation{}, nil, nil, unavailable("calculate storage mutation growth", err)
		}

		operation := newOperation(operationKind)
		reservation, err := s.store.ReserveDelta(ctx, teamquota.DeltaRequest{
			Owner:     owner,
			Operation: operation,
			Delta:     growth,
			Observed:  beforeTarget.Clone(),
		})
		if err != nil {
			// A stale observation can overstate the remaining growth for an
			// absolute mutation such as truncate. Retry an admission failure
			// only when a fresh physical observation proves the baseline moved.
			if teamquota.IsExceeded(err) {
				afterFailure, measureErr := before()
				if measureErr == nil &&
					validateTarget(owner, afterFailure) == nil &&
					!targetsEqual(afterFailure, beforeTarget) {
					lastObserved = afterFailure.Clone()
					continue
				}
			}
			return teamquota.Operation{}, nil, nil, normalize("reserve storage capacity", err)
		}
		admittedBefore, admittedMaximum, err := validateGrowthReservation(owner, growth, reservation)
		if err != nil {
			abortErr := s.store.Abort(
				ctx,
				teamquota.Ref(owner, operation),
				"invalid storage growth reservation",
			)
			return teamquota.Operation{}, nil, nil, errors.Join(
				unavailable("validate storage growth reservation", err),
				normalize("abort invalid storage growth reservation", abortErr),
			)
		}

		fencedTarget, measureErr := before()
		if measureErr == nil {
			measureErr = validateTarget(owner, fencedTarget)
		}
		if measureErr != nil {
			abortErr := s.store.Abort(
				ctx,
				teamquota.Ref(owner, operation),
				"storage baseline could not be verified after reservation",
			)
			return teamquota.Operation{}, nil, nil, errors.Join(
				unavailable("verify fenced storage baseline", measureErr),
				normalize("abort unverifiable storage growth reservation", abortErr),
			)
		}
		if !targetsEqual(fencedTarget, beforeTarget) {
			abortErr := s.store.Abort(
				ctx,
				teamquota.Ref(owner, operation),
				"storage baseline changed before reservation",
			)
			if abortErr != nil {
				return teamquota.Operation{}, nil, nil, normalize(
					"abort stale storage growth reservation",
					abortErr,
				)
			}
			lastObserved = fencedTarget.Clone()
			continue
		}
		if err := requireComponentwiseAtLeast(admittedBefore, fencedTarget); err != nil {
			abortErr := s.store.Abort(
				ctx,
				teamquota.Ref(owner, operation),
				"storage quota ledger undercounts physical baseline",
			)
			return teamquota.Operation{}, nil, nil, errors.Join(
				unavailable("validate fenced storage baseline", err),
				normalize("abort undercounted storage growth reservation", abortErr),
			)
		}
		return operation, fencedTarget.Clone(), admittedMaximum, nil
	}
	return teamquota.Operation{}, nil, nil, unavailable(
		"stabilize storage mutation baseline",
		fmt.Errorf(
			"physical target kept changing while acquiring the quota fence (last observed %v)",
			lastObserved,
		),
	)
}

// Reconcile records an exact already-existing complete storage target. It
// intentionally bypasses admission because this call performs no physical
// resource increase. The allocation revision is captured before measurement,
// so a stale observation can only be skipped, never overwrite a concurrent
// mutation.
func (s *Service) Reconcile(ctx context.Context, owner teamquota.Owner, exact Measure) error {
	if exact == nil {
		return unavailable("reconcile storage capacity", fmt.Errorf("exact callback is required"))
	}
	unlock := s.lock(owner)
	defer unlock()
	if err := s.requirePolicies(ctx, owner); err != nil {
		return err
	}
	allocation, err := s.recoveryAllocation(ctx, owner)
	if err != nil {
		return err
	}
	if allocation != nil && allocation.Operation != nil {
		return nil
	}
	expectedRevision := allocationRevision(allocation)
	target, err := exact()
	if err != nil {
		return unavailable("measure storage capacity for reconciliation", err)
	}
	if err := validateTarget(owner, target); err != nil {
		return unavailable("reconcile storage capacity", err)
	}
	return s.reconcileObservedIfRevision(ctx, owner, target, expectedRevision)
}

// AdoptExisting records an exact catalog-backed storage bundle at startup.
// It reads the durable revision before measuring physical state. A concurrent
// mutation therefore fences a stale catalog observation instead of allowing it
// to overwrite newer committed usage.
// Prepared operations are recovery work only after PostgreSQL reports their
// reconcile_after deadline is due. Until then another replica may still be
// executing the fenced physical mutation, so startup must leave it untouched.
// An interrupted deterministic delete is left pending when its committed value
// already equals the physical value, so startup never releases or undercounts a
// still-existing object.
func (s *Service) AdoptExisting(
	ctx context.Context,
	owner teamquota.Owner,
	exact Measure,
) error {
	if exact == nil {
		return unavailable("adopt existing storage capacity", fmt.Errorf("exact callback is required"))
	}
	unlock := s.lock(owner)
	defer unlock()
	if err := s.requirePolicies(ctx, owner); err != nil {
		return err
	}
	allocation, err := s.recoveryAllocation(ctx, owner)
	if err != nil {
		return err
	}
	expectedRevision := allocationRevision(allocation)
	exactTarget, err := exact()
	if err != nil {
		return unavailable("measure existing storage capacity", err)
	}
	if err := validateTarget(owner, exactTarget); err != nil {
		return unavailable("adopt existing storage capacity", err)
	}
	if allocation == nil {
		return s.reconcileObservedIfRevision(ctx, owner, exactTarget, expectedRevision)
	}
	if allocation.Operation != nil && !allocation.ReconcileDue {
		return nil
	}

	// Preserve the region-scoped recovery owner already stored in PostgreSQL;
	// startup on another data-plane cluster must never steal it.
	storedOwner := allocation.Owner
	committed := allocation.Committed
	if allocation.Operation == nil {
		if targetsEqual(committed, exactTarget) {
			return nil
		}
		return s.reconcileObservedIfRevision(
			ctx,
			storedOwner,
			exactTarget,
			expectedRevision,
		)
	}

	if isStorageDeleteOperationID(owner, allocation.Operation.ID) {
		if !targetsEqual(committed, exactTarget) {
			return unavailable(
				"adopt existing storage capacity",
				fmt.Errorf("pending delete committed target %v does not match physical target %v", committed, exactTarget),
			)
		}
		// The physical object still exists, so committed capacity must remain
		// intact and the delete stays resumable.
		return nil
	}

	operation := teamquota.Ref(storedOwner, *allocation.Operation)
	if err := validateTarget(storedOwner, allocation.Pending); err != nil {
		return unavailable(
			"adopt recovered storage mutation",
			fmt.Errorf("prepared storage target is invalid: %w", err),
		)
	}
	if targetsEqual(exactTarget, committed) {
		if err := s.store.Abort(ctx, operation, "startup physical storage reconciliation"); err != nil {
			return normalize("abort recovered storage mutation", err)
		}
		return nil
	}

	// The physical size changed. Finalize the measured value under the expired
	// operation fence so a concurrent replica cannot interleave a new mutation.
	boundErr := normalizeBoundError(
		"validate recovered storage mutation bound",
		exactTarget,
		allocation.Pending,
	)
	finalizeErr := normalize(
		"finalize recovered storage mutation",
		s.commitMeasuredTarget(ctx, operation, exactTarget, boundErr != nil),
	)
	return errors.Join(
		boundErr,
		finalizeErr,
	)
}

// ListDueRecoveryAllocations returns one stable page of expired storage
// operations owned by this regional storage catalog.
func (s *Service) ListDueRecoveryAllocations(
	ctx context.Context,
	ownerKind string,
	afterAllocationID string,
	limit int,
) ([]teamquota.RecoveryAllocation, error) {
	if s == nil || s.store == nil {
		return nil, unavailable(
			"list storage quota recovery allocations",
			fmt.Errorf("PostgreSQL TeamQuota store is not configured"),
		)
	}
	ownerKind = strings.TrimSpace(ownerKind)
	if ownerKind != OwnerKindVolume && ownerKind != OwnerKindSnapshot {
		return nil, unavailable(
			"list storage quota recovery allocations",
			fmt.Errorf("unsupported storage owner kind %q", ownerKind),
		)
	}
	if s.recoveryScope == "" {
		return nil, unavailable(
			"list storage quota recovery allocations",
			fmt.Errorf("storage recovery scope is not configured"),
		)
	}
	store, ok := s.store.(recoveryAllocationLister)
	if !ok || store == nil {
		return nil, unavailable(
			"list storage quota recovery allocations",
			fmt.Errorf("TeamQuota recovery allocation listing is not configured"),
		)
	}
	allocations, err := store.ListRecoveryAllocations(
		ctx,
		teamquota.RecoveryAllocationFilter{
			ClusterID:         s.recoveryScope,
			OwnerKind:         ownerKind,
			AfterAllocationID: strings.TrimSpace(afterAllocationID),
			OnlyDue:           true,
			Limit:             limit,
		},
	)
	if err != nil {
		return nil, normalize("list storage quota recovery allocations", err)
	}
	return allocations, nil
}

// AbortAbsentCatalogVolumeCreate clears an expired zero-base volume create
// only after the catalog owner has confirmed that no volume row exists.
// Volume create paths persist the catalog row before any materialized state, so
// this transition cannot hide physical storage. Snapshot create is
// deliberately excluded because it persists snapshot state before its row.
func (s *Service) AbortAbsentCatalogVolumeCreate(
	ctx context.Context,
	owner teamquota.Owner,
) (bool, error) {
	if s == nil || s.store == nil {
		return false, unavailable(
			"abort absent catalog volume create",
			fmt.Errorf("PostgreSQL TeamQuota store is not configured"),
		)
	}
	owner = teamquota.Owner{
		TeamID:    strings.TrimSpace(owner.TeamID),
		Kind:      strings.TrimSpace(owner.Kind),
		ID:        strings.TrimSpace(owner.ID),
		ClusterID: strings.TrimSpace(owner.ClusterID),
	}
	if err := owner.Validate(); err != nil {
		return false, unavailable("abort absent catalog volume create", err)
	}
	if owner.Kind != OwnerKindVolume {
		return false, nil
	}

	unlock := s.lock(owner)
	defer unlock()
	allocation, err := s.recoveryAllocation(ctx, owner)
	if err != nil {
		return false, err
	}
	if allocation == nil ||
		allocation.Operation == nil ||
		!allocation.ReconcileDue ||
		allocation.State != "reserved" {
		return false, nil
	}
	if allocation.Owner.TeamID != owner.TeamID ||
		allocation.Owner.Kind != owner.Kind ||
		allocation.Owner.ID != owner.ID ||
		allocation.Owner.ClusterID != s.recoveryScope {
		return false, unavailable(
			"abort absent catalog volume create",
			fmt.Errorf("recovery allocation ownership does not match this regional catalog"),
		)
	}
	switch allocation.Operation.Kind {
	case "volume_create", "volume_create_from_snapshot":
	default:
		return false, nil
	}
	if !targetsEqual(allocation.Committed, VolumeTarget(0, 0)) {
		return false, nil
	}
	if err := s.store.Abort(
		ctx,
		teamquota.Ref(allocation.Owner, *allocation.Operation),
		"recovery found no catalog row for expired volume create",
	); err != nil {
		return false, normalize("abort absent catalog volume create", err)
	}
	return true, nil
}

func (s *Service) commitMeasuredTarget(
	ctx context.Context,
	operation teamquota.OperationRef,
	exact teamquota.Values,
	exceededBound bool,
) error {
	if !exceededBound {
		return s.store.CommitExact(ctx, operation, exact)
	}
	store, ok := s.store.(teamquota.ObservedExactCapacityStore)
	if !ok || store == nil {
		return &teamquota.UnavailableError{
			Operation: "commit observed storage capacity",
			Err:       fmt.Errorf("observed exact capacity store is not configured"),
		}
	}
	return store.CommitObservedExact(ctx, operation, exact)
}

func (s *Service) reconcileObservedIfRevision(
	ctx context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	expectedRevision int64,
) error {
	applied, err := s.store.ReconcileTargetIfRevision(
		ctx,
		owner,
		target,
		teamquota.RuntimeRef{},
		expectedRevision,
	)
	if err != nil {
		return normalize("conditionally reconcile exact storage capacity", err)
	}
	if !applied {
		// A newer mutation owns the durable value. The observed target is stale
		// and must never be retried without taking another revision snapshot.
		return nil
	}
	return nil
}

func allocationRevision(allocation *teamquota.RecoveryAllocation) int64 {
	if allocation == nil {
		return 0
	}
	return allocation.Revision
}

// PendingRelease reports whether an existing catalog owner has the
// deterministic storage delete operation in progress. It is used only after
// the physical object was confirmed absent so startup can leave the release
// pending for the normal idempotent delete retry.
func (s *Service) PendingRelease(
	ctx context.Context,
	owner teamquota.Owner,
) (bool, error) {
	unlock := s.lock(owner)
	defer unlock()
	if err := s.requirePolicies(ctx, owner); err != nil {
		return false, err
	}
	allocation, err := s.recoveryAllocation(ctx, owner)
	if err != nil {
		return false, err
	}
	return allocation != nil &&
		allocation.Operation != nil &&
		isStorageDeleteOperationID(owner, allocation.Operation.ID), nil
}

// Release is a conservative delete reservation. Committed usage is not reduced
// until Confirm is called after physical deletion and catalog removal succeed.
type Release struct {
	service   *Service
	owner     teamquota.Owner
	operation teamquota.Operation
	unlock    func()
	once      sync.Once
}

// BeginRelease starts a retry-safe delete operation. The stable owner-specific
// prefix resumes an interrupted release; a unique suffix is used only after
// PostgreSQL reports that the previous exact operation was aborted.
func (s *Service) BeginRelease(
	ctx context.Context,
	owner teamquota.Owner,
	operationKind string,
) (*Release, error) {
	unlock := s.lock(owner)
	if err := s.requirePolicies(ctx, owner); err != nil {
		unlock()
		return nil, err
	}
	zeroTarget, err := zeroTarget(owner)
	if err != nil {
		unlock()
		return nil, unavailable("prepare storage capacity release", err)
	}
	operation := teamquota.Operation{
		ID:   deterministicDeleteOperationID(owner),
		Kind: normalizedOperationKind(operationKind),
	}
	begin := func() error {
		_, err := s.store.BeginRelease(ctx, teamquota.ReleaseRequest{
			Owner:     owner,
			Operation: operation,
			Target:    zeroTarget,
		})
		return err
	}
	err = begin()
	var aborted *teamquota.OperationAbortedError
	if errors.As(err, &aborted) {
		operation.ID = deterministicDeleteOperationID(owner) + "/" + uuid.NewString()
		err = begin()
	}
	if err != nil {
		var conflict *teamquota.OperationConflictError
		if errors.As(err, &conflict) && isStorageDeleteOperationID(owner, conflict.OperationID) {
			allocation, recoveryErr := s.recoveryAllocation(ctx, owner)
			if recoveryErr != nil {
				unlock()
				return nil, recoveryErr
			}
			if allocation != nil &&
				allocation.Operation != nil &&
				allocation.Operation.ID == conflict.OperationID &&
				targetsEqual(allocation.Pending, zeroTarget) {
				operation = *allocation.Operation
				err = nil
			}
		}
	}
	if err != nil {
		unlock()
		return nil, normalize("begin storage capacity release", err)
	}
	return &Release{service: s, owner: owner, operation: operation, unlock: unlock}, nil
}

// Confirm releases committed capacity after deletion has succeeded.
func (r *Release) Confirm(ctx context.Context) error {
	if r == nil || r.service == nil {
		return unavailable("confirm storage capacity release", fmt.Errorf("release is not configured"))
	}
	defer r.close()
	if err := r.service.store.ConfirmRelease(ctx, teamquota.Ref(r.owner, r.operation), teamquota.RuntimeRef{}); err != nil {
		return normalize("confirm storage capacity release", err)
	}
	return nil
}

// ConfirmTx confirms the release inside the caller's catalog transaction.
// Keeping catalog deletion and quota release in one PostgreSQL commit removes
// the catalog-missing/pending-allocation crash window.
func (r *Release) ConfirmTx(ctx context.Context, tx pgx.Tx) error {
	if r == nil || r.service == nil {
		return unavailable("confirm storage capacity release", fmt.Errorf("release is not configured"))
	}
	defer r.close()
	if err := r.service.store.ConfirmReleaseTx(
		ctx,
		tx,
		teamquota.Ref(r.owner, r.operation),
		teamquota.RuntimeRef{},
	); err != nil {
		return normalize("confirm storage capacity release", err)
	}
	return nil
}

// Abort leaves the committed allocation intact and clears the pending delete.
func (r *Release) Abort(ctx context.Context, reason error) error {
	if r == nil || r.service == nil {
		return unavailable("abort storage capacity release", fmt.Errorf("release is not configured"))
	}
	defer r.close()
	message := "storage deletion failed"
	if reason != nil {
		message = reason.Error()
	}
	if err := r.service.store.Abort(ctx, teamquota.Ref(r.owner, r.operation), message); err != nil {
		return normalize("abort storage capacity release", err)
	}
	return nil
}

// KeepPending unlocks the local owner while preserving a successful physical
// deletion's pending release for an idempotent catalog retry.
func (r *Release) KeepPending() {
	if r != nil {
		r.close()
	}
}

func (r *Release) close() {
	r.once.Do(func() {
		if r.unlock != nil {
			r.unlock()
		}
	})
}

func (s *Service) requirePolicies(ctx context.Context, owner teamquota.Owner) error {
	if s == nil || s.store == nil {
		return unavailable("resolve storage quota policy", fmt.Errorf("PostgreSQL TeamQuota store is not configured"))
	}
	if err := owner.Validate(); err != nil {
		return unavailable("resolve storage quota policy", err)
	}
	keys, err := ownerKeys(owner)
	if err != nil {
		return unavailable("resolve storage quota policy", err)
	}
	for _, key := range keys {
		kind, ok := teamquota.KindForKey(key)
		if !ok || kind != teamquota.KindCapacity {
			return unavailable("resolve storage quota policy", fmt.Errorf("%q is not a capacity quota key", key))
		}
		policy, err := s.store.EffectivePolicy(ctx, owner.TeamID, key)
		if err != nil {
			return normalize("resolve storage quota policy", err)
		}
		if policy == nil {
			return unavailable("resolve storage quota policy", fmt.Errorf("no effective policy for %s", key))
		}
		if policy.Kind != teamquota.KindCapacity {
			return unavailable("resolve storage quota policy", fmt.Errorf("effective policy for %s is not capacity", key))
		}
	}
	return nil
}

func (s *Service) recoveryAllocation(
	ctx context.Context,
	owner teamquota.Owner,
) (*teamquota.RecoveryAllocation, error) {
	allocation, err := s.store.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		return nil, normalize("inspect storage quota recovery state", err)
	}
	return allocation, nil
}

func (s *Service) lock(owner teamquota.Owner) func() {
	if s == nil {
		return func() {}
	}
	key := owner.TeamID + "\x00" + owner.Kind + "\x00" + owner.ID
	s.locksMu.Lock()
	entry := s.locks[key]
	if entry == nil {
		entry = &ownerLock{}
		s.locks[key] = entry
	}
	entry.refs++
	s.locksMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.locksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(s.locks, key)
		}
		s.locksMu.Unlock()
	}
}

func ownerKeys(owner teamquota.Owner) ([]teamquota.Key, error) {
	switch owner.Kind {
	case OwnerKindVolume:
		return []teamquota.Key{
			teamquota.KeyVolumeStorageBytes,
			teamquota.KeyStorageObjectCount,
		}, nil
	case OwnerKindSnapshot:
		return []teamquota.Key{
			teamquota.KeySnapshotStorageBytes,
			teamquota.KeyStorageObjectCount,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported storage owner kind %q", owner.Kind)
	}
}

func validateTarget(owner teamquota.Owner, target teamquota.Values) error {
	keys, err := ownerKeys(owner)
	if err != nil {
		return err
	}
	if len(target) != len(keys) {
		return fmt.Errorf("storage target for %s must contain exactly %v", owner.Kind, keys)
	}
	for _, key := range keys {
		value, ok := target[key]
		if !ok {
			return fmt.Errorf("storage target for %s is missing %s", owner.Kind, key)
		}
		if value < 0 {
			return fmt.Errorf("storage target for %s has negative %s value %d", owner.Kind, key, value)
		}
	}
	return nil
}

func zeroTarget(owner teamquota.Owner) (teamquota.Values, error) {
	keys, err := ownerKeys(owner)
	if err != nil {
		return nil, err
	}
	target := make(teamquota.Values, len(keys))
	for _, key := range keys {
		target[key] = 0
	}
	return target, nil
}

func requireComponentwiseAtLeast(target, minimum teamquota.Values) error {
	for key, minimumValue := range minimum {
		if targetValue, ok := target[key]; !ok || targetValue < minimumValue {
			return fmt.Errorf("%s target %d is below current value %d", key, targetValue, minimumValue)
		}
	}
	return nil
}

func nonNegativeGrowth(
	before teamquota.Values,
	maximum teamquota.Values,
) (teamquota.Values, error) {
	growth := make(teamquota.Values, len(maximum))
	for key, maximumValue := range maximum {
		beforeValue, ok := before[key]
		if !ok {
			return nil, fmt.Errorf("storage mutation baseline is missing %s", key)
		}
		if maximumValue < beforeValue {
			return nil, fmt.Errorf(
				"%s target %d is below current value %d",
				key,
				maximumValue,
				beforeValue,
			)
		}
		growth[key] = maximumValue - beforeValue
	}
	return growth, nil
}

func validateGrowthReservation(
	owner teamquota.Owner,
	growth teamquota.Values,
	reservation *teamquota.Reservation,
) (teamquota.Values, teamquota.Values, error) {
	if reservation == nil {
		return nil, nil, fmt.Errorf("storage growth reservation is missing")
	}
	if err := validateTarget(owner, reservation.Committed); err != nil {
		return nil, nil, fmt.Errorf("committed target: %w", err)
	}
	if err := validateTarget(owner, reservation.Target); err != nil {
		return nil, nil, fmt.Errorf("admitted target: %w", err)
	}
	for key, delta := range growth {
		committed := reservation.Committed[key]
		if delta < 0 || committed > math.MaxInt64-delta {
			return nil, nil, fmt.Errorf("admitted %s target overflows int64", key)
		}
		expected := committed + delta
		if reservation.Target[key] != expected {
			return nil, nil, fmt.Errorf(
				"admitted %s target %d does not equal committed %d plus growth %d",
				key,
				reservation.Target[key],
				committed,
				delta,
			)
		}
	}
	return reservation.Committed.Clone(), reservation.Target.Clone(), nil
}

func normalizeBoundError(operation string, exact, maximum teamquota.Values) error {
	for key, exactValue := range exact {
		maximumValue, ok := maximum[key]
		if !ok || exactValue > maximumValue {
			return unavailable(
				operation,
				fmt.Errorf("exact %s value %d exceeds reserved maximum %d", key, exactValue, maximumValue),
			)
		}
	}
	return nil
}

func targetsEqual(left, right teamquota.Values) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, ok := right[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func newOperation(kind string) teamquota.Operation {
	return teamquota.Operation{
		ID:   uuid.NewString(),
		Kind: normalizedOperationKind(kind),
	}
}

func normalizedOperationKind(kind string) string {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return "storage_mutation"
	}
	return kind
}

func deterministicDeleteOperationID(owner teamquota.Owner) string {
	return "storage-delete/" + owner.Kind + "/" + owner.ID
}

func isStorageDeleteOperationID(owner teamquota.Owner, operationID string) bool {
	base := deterministicDeleteOperationID(owner)
	return operationID == base || strings.HasPrefix(operationID, base+"/")
}

func unavailable(operation string, err error) error {
	return &teamquota.UnavailableError{Operation: operation, Err: err}
}

func normalize(operation string, err error) error {
	if err == nil || teamquota.IsExceeded(err) || teamquota.IsUnavailable(err) {
		return err
	}
	return unavailable(operation, err)
}
