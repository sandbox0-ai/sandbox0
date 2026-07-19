package teamquota

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const defaultReconcileDelay = 5 * time.Minute

type reserveMode int

const (
	reserveTarget reserveMode = iota
	reserveDelta
	reserveRelease
)

type allocationRecord struct {
	ID                  string
	Revision            int64
	Owner               Owner
	State               string
	OperationID         string
	OperationKind       string
	OperationGeneration int64
	OperationFence      int64
	OperationBaseState  string
	LastOperationID     string
	LastOperationGen    int64
	LastOperationResult string
	Runtime             RuntimeRef
}

type allocationItem struct {
	Committed int64
	Pending   *int64
}

type allocationOperationRecord struct {
	ID          string
	Kind        string
	Generation  int64
	Fingerprint string
	State       string
}

const internalTransferOperationKind = "team_quota_transfer"

type transferRecord struct {
	TeamID                  string
	Operation               Operation
	Fingerprint             string
	State                   string
	SourceAllocationID      string
	DestinationAllocationID string
	Runtime                 RuntimeRef
}

type transferItem struct {
	SourceDecrease       int64
	DestinationCommitted int64
	DestinationTarget    int64
	Reserved             int64
}

// ReserveTarget atomically reserves an owner's complete target bundle.
func (r *Repository) ReserveTarget(ctx context.Context, request ReserveRequest) (*Reservation, error) {
	var reservation *Reservation
	err := r.inTx(ctx, "reserve team capacity target", func(tx pgx.Tx) error {
		var err error
		reservation, err = r.ReserveTargetTx(ctx, tx, request)
		return err
	})
	return reservation, err
}

// ReserveTargetTx reserves a complete target inside a caller-owned transaction.
func (r *Repository) ReserveTargetTx(ctx context.Context, tx pgx.Tx, request ReserveRequest) (*Reservation, error) {
	if tx == nil {
		return nil, fmt.Errorf("team quota transaction is required")
	}
	request = normalizeReserveRequest(request)
	if err := request.validate(); err != nil {
		return nil, err
	}
	if err := lockTeam(ctx, tx, request.Owner.TeamID); err != nil {
		return nil, err
	}
	return r.reserveLocked(ctx, tx, request.Owner, request.Operation, request.Target, reserveTarget, RuntimeRef{})
}

// ReserveDelta atomically adds non-negative values to an owner's committed
// target. Zero growth still creates an operation fence.
func (r *Repository) ReserveDelta(ctx context.Context, request DeltaRequest) (*Reservation, error) {
	var reservation *Reservation
	err := r.inTx(ctx, "reserve team capacity delta", func(tx pgx.Tx) error {
		var err error
		reservation, err = r.ReserveDeltaTx(ctx, tx, request)
		return err
	})
	return reservation, err
}

// ReserveDeltaTx adds non-negative values inside a caller-owned transaction.
func (r *Repository) ReserveDeltaTx(ctx context.Context, tx pgx.Tx, request DeltaRequest) (*Reservation, error) {
	if tx == nil {
		return nil, fmt.Errorf("team quota transaction is required")
	}
	request = normalizeDeltaRequest(request)
	if err := request.validate(); err != nil {
		return nil, err
	}
	if err := lockTeam(ctx, tx, request.Owner.TeamID); err != nil {
		return nil, err
	}
	return r.reserveLocked(ctx, tx, request.Owner, request.Operation, request.Delta, reserveDelta, RuntimeRef{})
}

// BeginRelease records a conservative lower target without releasing committed
// usage. ConfirmRelease performs the actual decrement after external deletion.
func (r *Repository) BeginRelease(ctx context.Context, request ReleaseRequest) (*Reservation, error) {
	var reservation *Reservation
	err := r.inTx(ctx, "begin team capacity release", func(tx pgx.Tx) error {
		var err error
		reservation, err = r.BeginReleaseTx(ctx, tx, request)
		return err
	})
	return reservation, err
}

// BeginReleaseTx begins a release inside a caller-owned transaction.
func (r *Repository) BeginReleaseTx(ctx context.Context, tx pgx.Tx, request ReleaseRequest) (*Reservation, error) {
	if tx == nil {
		return nil, fmt.Errorf("team quota transaction is required")
	}
	request = normalizeReleaseRequest(request)
	if err := request.validate(); err != nil {
		return nil, err
	}
	if err := lockTeam(ctx, tx, request.Owner.TeamID); err != nil {
		return nil, err
	}
	return r.reserveLocked(ctx, tx, request.Owner, request.Operation, request.Target, reserveRelease, request.Runtime)
}

func normalizeReserveRequest(request ReserveRequest) ReserveRequest {
	request.Owner = normalizeOwner(request.Owner)
	request.Operation = normalizeOperation(request.Operation)
	return request
}

func normalizeDeltaRequest(request DeltaRequest) DeltaRequest {
	request.Owner = normalizeOwner(request.Owner)
	request.Operation = normalizeOperation(request.Operation)
	return request
}

func normalizeReleaseRequest(request ReleaseRequest) ReleaseRequest {
	request.Owner = normalizeOwner(request.Owner)
	request.Operation = normalizeOperation(request.Operation)
	request.Runtime = normalizeRuntime(request.Runtime)
	return request
}

func normalizeTransferRequest(request TransferRequest) TransferRequest {
	request.Source = normalizeOwner(request.Source)
	request.Destination = normalizeOwner(request.Destination)
	request.Operation = normalizeOperation(request.Operation)
	request.Runtime = normalizeRuntime(request.Runtime)
	if request.Source.ClusterID == "" {
		request.Source.ClusterID = request.Destination.ClusterID
	}
	if request.Destination.ClusterID == "" {
		request.Destination.ClusterID = request.Source.ClusterID
	}
	return request
}

func normalizeOwner(owner Owner) Owner {
	owner.TeamID = strings.TrimSpace(owner.TeamID)
	owner.Kind = strings.TrimSpace(owner.Kind)
	owner.ID = strings.TrimSpace(owner.ID)
	owner.ClusterID = strings.TrimSpace(owner.ClusterID)
	return owner
}

func normalizeOperation(operation Operation) Operation {
	operation.ID = strings.TrimSpace(operation.ID)
	operation.Kind = strings.TrimSpace(operation.Kind)
	return operation
}

func normalizeRuntime(runtime RuntimeRef) RuntimeRef {
	runtime.Namespace = strings.TrimSpace(runtime.Namespace)
	runtime.Name = strings.TrimSpace(runtime.Name)
	runtime.UID = strings.TrimSpace(runtime.UID)
	return runtime
}

func allocationOperationFingerprint(
	owner Owner,
	operation Operation,
	values Values,
	mode reserveMode,
	runtime RuntimeRef,
) (string, error) {
	modeName := ""
	switch mode {
	case reserveTarget:
		modeName = "target"
	case reserveDelta:
		modeName = "delta"
	case reserveRelease:
		modeName = "release"
	default:
		return "", fmt.Errorf("unknown team quota reserve mode %d", mode)
	}
	payload := struct {
		Owner     Owner      `json:"owner"`
		Operation Operation  `json:"operation"`
		Mode      string     `json:"mode"`
		Values    Values     `json:"values"`
		Runtime   RuntimeRef `json:"runtime"`
	}{
		Owner:     owner,
		Operation: operation,
		Mode:      modeName,
		Values:    values,
		Runtime:   runtime,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode team quota allocation operation: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (r *Repository) reserveLocked(
	ctx context.Context,
	tx pgx.Tx,
	owner Owner,
	operation Operation,
	values Values,
	mode reserveMode,
	runtime RuntimeRef,
) (*Reservation, error) {
	fingerprint, err := allocationOperationFingerprint(owner, operation, values, mode, runtime)
	if err != nil {
		return nil, err
	}
	allocation, err := loadAllocationForUpdate(ctx, tx, owner)
	if err != nil {
		return nil, err
	}
	preserveRuntime := false
	items := make(map[Key]allocationItem)
	if allocation != nil {
		if err := requireReserveClusterTransition(allocation, owner, operation); err != nil {
			return nil, err
		}
		if operationID, err := preparedTransferForAllocation(ctx, tx, allocation.ID, true, true); err != nil {
			return nil, err
		} else if operationID != "" {
			return nil, &OperationConflictError{Owner: owner, OperationID: operationID}
		}
		items, err = loadAllocationItems(ctx, tx, allocation.ID)
		if err != nil {
			return nil, err
		}
		history, err := loadAllocationOperationForUpdate(ctx, tx, allocation.ID, operation.ID)
		if err != nil {
			return nil, err
		}
		if history != nil {
			if history.Generation != operation.Generation ||
				history.Kind != operation.Kind ||
				history.Fingerprint != fingerprint {
				return nil, &OperationConflictError{Owner: owner, OperationID: operation.ID}
			}
			switch history.State {
			case "prepared":
				if allocation.OperationID != operation.ID ||
					allocation.OperationGeneration != operation.Generation ||
					allocation.OperationKind != operation.Kind {
					return nil, &OperationConflictError{Owner: owner, OperationID: operation.ID}
				}
				return reservationFrom(allocation, items), nil
			case "committed":
				if allocation.OperationID == "" &&
					allocation.LastOperationID == operation.ID &&
					allocation.LastOperationGen == operation.Generation &&
					allocation.LastOperationResult == "committed" {
					reservation := reservationFrom(allocation, items)
					reservation.Operation = operation
					return reservation, nil
				}
				return nil, &OperationConflictError{Owner: owner, OperationID: operation.ID}
			case "aborted":
				return nil, &OperationAbortedError{OperationID: operation.ID}
			default:
				return nil, &UnavailableError{
					Operation: "resume team quota operation",
					Err:       fmt.Errorf("unknown allocation operation state %q", history.State),
				}
			}
		}
		if allocation.OperationID != "" {
			if allocation.OperationKind == internalTransferOperationKind {
				return nil, &OperationConflictError{Owner: owner, OperationID: allocation.OperationID}
			}
			return nil, &OperationConflictError{Owner: owner, OperationID: allocation.OperationID}
		}
		if allocation.LastOperationID == operation.ID {
			return nil, &OperationConflictError{Owner: owner, OperationID: operation.ID}
		}
		if operation.Generation < allocation.OperationFence {
			return nil, &OperationConflictError{Owner: owner, OperationID: operation.ID}
		}
		if mode == reserveRelease && hasRuntimeRef(allocation.Runtime) {
			if allocation.Runtime != runtime {
				return nil, fmt.Errorf(
					"release runtime %+v does not match committed runtime %+v",
					runtime,
					allocation.Runtime,
				)
			}
			preserveRuntime = true
		}
	}

	current := committedValues(items)
	var target Values
	switch mode {
	case reserveDelta:
		target = current.Clone()
		if target == nil {
			target = make(Values)
		}
		for key, delta := range values {
			next, ok := addInt64(target[key], delta)
			if !ok {
				return nil, fmt.Errorf("quota target for %q overflows int64", key)
			}
			target[key] = next
		}
	default:
		target = values.Clone()
		for key := range current {
			if _, ok := target[key]; !ok {
				target[key] = 0
			}
		}
	}
	if err := target.validateCapacity(true); err != nil {
		return nil, err
	}
	for key := range target {
		if _, ok := current[key]; !ok {
			current[key] = 0
		}
	}
	if mode == reserveRelease {
		for key, value := range target {
			if value > current[key] {
				return nil, fmt.Errorf("release target for %q cannot exceed committed value", key)
			}
		}
	}

	positive := make(Values)
	usageByKey := make(map[Key]usageValues)
	for _, key := range target.Keys() {
		delta := target[key] - current[key]
		if delta <= 0 {
			continue
		}
		positive[key] = delta
		if err := ensureUsageRow(ctx, tx, owner.TeamID, key); err != nil {
			return nil, err
		}
		usage, err := loadUsageForUpdate(ctx, tx, owner.TeamID, key)
		if err != nil {
			return nil, err
		}
		usageByKey[key] = usage
		policy, err := r.EffectivePolicyTx(ctx, tx, owner.TeamID, key)
		if err != nil {
			return nil, err
		}
		if policy == nil {
			return nil, &UnavailableError{
				Operation: "reserve team capacity",
				Err:       fmt.Errorf("no effective policy for %s", key),
			}
		}
		if policy.Kind != KindCapacity {
			return nil, &UnavailableError{
				Operation: "reserve team capacity",
				Err:       fmt.Errorf("effective policy for %s is not capacity", key),
			}
		}
		used, ok := addInt64(usage.committed, usage.reserved)
		if !ok {
			return nil, &UnavailableError{Operation: "reserve team capacity", Err: fmt.Errorf("usage for %s overflows int64", key)}
		}
		next, ok := addInt64(used, delta)
		if !ok || next > policy.Limit {
			return nil, &ExceededError{
				TeamID:    owner.TeamID,
				Key:       key,
				Limit:     policy.Limit,
				Committed: usage.committed,
				Reserved:  usage.reserved,
				Requested: delta,
			}
		}
	}

	state := "reserved"
	if mode == reserveRelease {
		state = "releasing"
	}
	baseState := "released"
	allocationID := uuid.NewString()
	createdAllocation := allocation == nil
	if allocation == nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.allocations (
				allocation_id, team_id, owner_kind, owner_id, cluster_id,
				state, operation_id, operation_kind, operation_generation,
				operation_base_state, pod_namespace, pod_name, pod_uid,
				runtime_generation, reconcile_after, operation_fence_generation
			) VALUES (
				$1, $2, $3, $4, $5,
				$6, $7, $8, $9,
				$10, $11, $12, $13,
				$14, NOW() + $15::interval, $16
			)
		`, allocationID, owner.TeamID, owner.Kind, owner.ID, owner.ClusterID,
			state, operation.ID, operation.Kind, operation.Generation,
			baseState, runtime.Namespace, runtime.Name, runtime.UID,
			runtime.Generation, postgresInterval(defaultReconcileDelay),
			operation.Generation); err != nil {
			return nil, &UnavailableError{Operation: "create team quota allocation", Err: err}
		}
		allocation = &allocationRecord{ID: allocationID, Owner: owner}
	} else {
		allocationID = allocation.ID
		baseState = allocation.State
		if _, err := tx.Exec(ctx, `
			UPDATE quota.allocations
			SET cluster_id = CASE WHEN $2 = '' THEN cluster_id ELSE $2 END,
				state = $3,
				operation_id = $4,
					operation_kind = $5,
					operation_generation = $6,
					operation_base_state = $7,
					operation_fence_generation =
						GREATEST(operation_fence_generation, $8),
					pod_namespace = CASE
						WHEN $9 OR $10 = '' THEN pod_namespace
						ELSE $10
					END,
					pod_name = CASE
						WHEN $9 OR $11 = '' THEN pod_name
						ELSE $11
					END,
					pod_uid = CASE
						WHEN $9 OR $12 = '' THEN pod_uid
						ELSE $12
					END,
					runtime_generation = CASE
						WHEN $9 THEN runtime_generation
						ELSE GREATEST(runtime_generation, $13)
					END,
					reconcile_after = NOW() + $14::interval,
					last_error = '',
					updated_at = NOW()
				WHERE allocation_id = $1
			`, allocationID, owner.ClusterID, state, operation.ID, operation.Kind,
			operation.Generation, baseState, operation.Generation,
			preserveRuntime, runtime.Namespace, runtime.Name,
			runtime.UID, runtime.Generation,
			postgresInterval(defaultReconcileDelay)); err != nil {
			return nil, &UnavailableError{Operation: "update team quota allocation", Err: err}
		}
	}
	if err := insertAllocationOperation(
		ctx,
		tx,
		allocationID,
		operation,
		fingerprint,
	); err != nil {
		return nil, err
	}

	for _, key := range target.Keys() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.allocation_items (
				allocation_id, quota_key, committed_value, pending_value
			) VALUES ($1, $2, $3, $4)
			ON CONFLICT (allocation_id, quota_key) DO UPDATE
			SET pending_value = EXCLUDED.pending_value
		`, allocationID, string(key), current[key], target[key]); err != nil {
			return nil, &UnavailableError{Operation: "write team quota allocation item", Err: err}
		}
	}
	for key, delta := range positive {
		usage := usageByKey[key]
		if _, err := tx.Exec(ctx, `
			UPDATE quota.team_usage
			SET reserved_value = $3,
				updated_at = NOW()
			WHERE team_id = $1 AND quota_key = $2
		`, owner.TeamID, string(key), usage.reserved+delta); err != nil {
			return nil, &UnavailableError{Operation: "reserve team quota usage", Err: err}
		}
	}
	if err := bumpTeamRevision(ctx, tx, owner.TeamID); err != nil {
		return nil, err
	}
	allocation.State = state
	allocation.OperationID = operation.ID
	allocation.OperationKind = operation.Kind
	allocation.OperationGeneration = operation.Generation
	if operation.Generation > allocation.OperationFence {
		allocation.OperationFence = operation.Generation
	}
	allocation.OperationBaseState = baseState
	if createdAllocation || (!preserveRuntime && hasRuntimeRef(runtime)) {
		allocation.Runtime = runtime
	}
	return &Reservation{
		AllocationID: allocationID,
		Owner:        owner,
		Operation:    operation,
		State:        state,
		Committed:    current.Clone(),
		Target:       target.Clone(),
		Reserved:     positive.Clone(),
	}, nil
}

// AttachRuntime records the exact external runtime protected by a reservation.
func (r *Repository) AttachRuntime(ctx context.Context, operation OperationRef, runtime RuntimeRef) error {
	return r.inTx(ctx, "attach team quota runtime", func(tx pgx.Tx) error {
		return r.AttachRuntimeTx(ctx, tx, operation, runtime)
	})
}

// AttachRuntimeTx records a runtime inside a caller-owned transaction.
func (r *Repository) AttachRuntimeTx(ctx context.Context, tx pgx.Tx, operation OperationRef, runtime RuntimeRef) error {
	operation.Owner = normalizeOwner(operation.Owner)
	runtime = normalizeRuntime(runtime)
	if err := operation.validate(); err != nil {
		return err
	}
	if runtime.Namespace == "" || runtime.Name == "" {
		return fmt.Errorf("runtime namespace and name are required")
	}
	if runtime.Generation < 0 {
		return fmt.Errorf("runtime generation must be non-negative")
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	allocation, err := loadAllocationForUpdate(ctx, tx, operation.Owner)
	if err != nil {
		return err
	}
	if allocation == nil {
		return &UnavailableError{Operation: "attach team quota runtime", Err: fmt.Errorf("allocation not found")}
	}
	if err := requireAllocationCluster(allocation, operation.Owner); err != nil {
		return err
	}
	if allocation.OperationKind == internalTransferOperationKind {
		return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
	}
	if allocation.OperationID == "" {
		history, err := loadAllocationOperationForUpdate(ctx, tx, allocation.ID, operation.ID)
		if err != nil {
			return err
		}
		if history != nil &&
			history.Generation == operation.Generation &&
			history.State == "committed" &&
			allocation.LastOperationID == operation.ID &&
			allocation.LastOperationGen == operation.Generation &&
			allocation.LastOperationResult == "committed" {
			return runtimeMatches(allocation.Runtime, runtime)
		}
		return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
	}
	if err := requireCurrentOperation(allocation, operation); err != nil {
		return err
	}
	history, err := loadAllocationOperationForUpdate(ctx, tx, allocation.ID, operation.ID)
	if err != nil {
		return err
	}
	if history == nil ||
		history.Generation != operation.Generation ||
		history.Kind != allocation.OperationKind ||
		history.State != "prepared" {
		return &UnavailableError{
			Operation: "attach team quota runtime",
			Err:       fmt.Errorf("prepared operation history is missing or inconsistent"),
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE quota.allocations
		SET pod_namespace = $2,
			pod_name = $3,
			pod_uid = $4,
			runtime_generation = $5,
			operation_fence_generation =
				GREATEST(operation_fence_generation, $5),
			updated_at = NOW()
		WHERE allocation_id = $1
	`, allocation.ID, runtime.Namespace, runtime.Name, runtime.UID, runtime.Generation); err != nil {
		return &UnavailableError{Operation: "attach team quota runtime", Err: err}
	}
	return nil
}

// Commit moves a pending target into committed usage.
func (r *Repository) Commit(ctx context.Context, operation OperationRef) error {
	return r.inTx(ctx, "commit team quota operation", func(tx pgx.Tx) error {
		return r.CommitTx(ctx, tx, operation)
	})
}

// CommitTx commits a target inside a caller-owned transaction.
func (r *Repository) CommitTx(ctx context.Context, tx pgx.Tx, operation OperationRef) error {
	operation.Owner = normalizeOwner(operation.Owner)
	if err := operation.validate(); err != nil {
		return err
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	return r.commitLocked(ctx, tx, operation, nil)
}

// CommitExact commits a measured complete target without exposing an
// intermediate conservative commit to another owner mutation.
func (r *Repository) CommitExact(
	ctx context.Context,
	operation OperationRef,
	exact Values,
) error {
	return r.inTx(ctx, "commit exact team quota operation", func(tx pgx.Tx) error {
		return r.CommitExactTx(ctx, tx, operation, exact)
	})
}

// CommitExactTx commits a measured target inside a caller-owned transaction.
// Every exact value must be at or below the operation's admitted pending
// target.
func (r *Repository) CommitExactTx(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	exact Values,
) error {
	if tx == nil {
		return fmt.Errorf("team quota transaction is required")
	}
	operation.Owner = normalizeOwner(operation.Owner)
	if err := operation.validate(); err != nil {
		return err
	}
	if err := exact.validateCapacity(true); err != nil {
		return err
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	return r.commitExactLocked(ctx, tx, operation, exact)
}

// CommitObservedExact atomically adopts a measured physical target while
// finalizing a prepared operation. It intentionally bypasses the prepared
// upper bound because the measured resource already exists; normal admission
// paths must use CommitExact.
func (r *Repository) CommitObservedExact(
	ctx context.Context,
	operation OperationRef,
	exact Values,
) error {
	return r.inTx(ctx, "commit observed team quota operation", func(tx pgx.Tx) error {
		return r.CommitObservedExactTx(ctx, tx, operation, exact)
	})
}

// CommitObservedExactTx adopts a measured physical target inside a
// caller-owned transaction.
func (r *Repository) CommitObservedExactTx(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	exact Values,
) error {
	if tx == nil {
		return fmt.Errorf("team quota transaction is required")
	}
	operation.Owner = normalizeOwner(operation.Owner)
	if err := operation.validate(); err != nil {
		return err
	}
	if err := exact.validateCapacity(true); err != nil {
		return err
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	return r.commitObservedExactLocked(ctx, tx, operation, exact)
}

// Abort releases only positive reservation deltas and restores committed state.
func (r *Repository) Abort(ctx context.Context, operation OperationRef, reason string) error {
	return r.inTx(ctx, "abort team quota operation", func(tx pgx.Tx) error {
		return r.AbortTx(ctx, tx, operation, reason)
	})
}

// AbortTx aborts an operation inside a caller-owned transaction.
func (r *Repository) AbortTx(ctx context.Context, tx pgx.Tx, operation OperationRef, reason string) error {
	operation.Owner = normalizeOwner(operation.Owner)
	if err := operation.validate(); err != nil {
		return err
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	allocation, err := loadAllocationForUpdate(ctx, tx, operation.Owner)
	if err != nil {
		return err
	}
	if allocation == nil {
		return &UnavailableError{Operation: "abort team quota operation", Err: fmt.Errorf("allocation not found")}
	}
	if err := requireAllocationCluster(allocation, operation.Owner); err != nil {
		return err
	}
	if allocation.OperationKind == internalTransferOperationKind {
		return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
	}
	if allocation.OperationID == "" {
		history, err := loadAllocationOperationForUpdate(ctx, tx, allocation.ID, operation.ID)
		if err != nil {
			return err
		}
		if history != nil &&
			history.Generation == operation.Generation &&
			history.State == "aborted" {
			return nil
		}
		return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
	}
	if err := requireCurrentOperation(allocation, operation); err != nil {
		return err
	}
	items, err := loadAllocationItems(ctx, tx, allocation.ID)
	if err != nil {
		return err
	}
	for key, item := range items {
		if item.Pending == nil {
			continue
		}
		positive := *item.Pending - item.Committed
		if positive > 0 {
			if err := subtractReservedUsage(ctx, tx, operation.Owner.TeamID, key, positive); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE quota.allocation_items
			SET pending_value = NULL
			WHERE allocation_id = $1 AND quota_key = $2
		`, allocation.ID, string(key)); err != nil {
			return &UnavailableError{Operation: "abort team quota allocation item", Err: err}
		}
	}
	state := allocation.OperationBaseState
	if state == "" {
		state = stateForAllocation(operation.Owner, committedValues(items))
	}
	if err := completeAllocationOperation(
		ctx,
		tx,
		allocation,
		operation,
		"aborted",
		reason,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE quota.allocations
		SET state = $2,
			operation_id = NULL,
			operation_kind = '',
			operation_generation = 0,
			operation_base_state = '',
			last_operation_id = $3,
			last_operation_generation = $4,
			last_operation_result = 'aborted',
			reconcile_after = NULL,
			last_error = $5,
			updated_at = NOW()
		WHERE allocation_id = $1
	`, allocation.ID, state, operation.ID, operation.Generation,
		strings.TrimSpace(reason)); err != nil {
		return &UnavailableError{Operation: "abort team quota operation", Err: err}
	}
	return finishTeamCapacityMutation(ctx, tx, operation.Owner.TeamID)
}

// ConfirmRelease verifies runtime identity before committing a lower target.
func (r *Repository) ConfirmRelease(ctx context.Context, operation OperationRef, runtime RuntimeRef) error {
	return r.inTx(ctx, "confirm team quota release", func(tx pgx.Tx) error {
		return r.ConfirmReleaseTx(ctx, tx, operation, runtime)
	})
}

// ConfirmReleaseTx confirms a release inside a caller-owned transaction.
func (r *Repository) ConfirmReleaseTx(ctx context.Context, tx pgx.Tx, operation OperationRef, runtime RuntimeRef) error {
	operation.Owner = normalizeOwner(operation.Owner)
	runtime = normalizeRuntime(runtime)
	if err := operation.validate(); err != nil {
		return err
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	allocation, err := loadAllocationForUpdate(ctx, tx, operation.Owner)
	if err != nil {
		return err
	}
	if allocation == nil {
		return &UnavailableError{Operation: "confirm team quota release", Err: fmt.Errorf("allocation not found")}
	}
	if err := requireAllocationCluster(allocation, operation.Owner); err != nil {
		return err
	}
	if allocation.OperationID == "" {
		return r.commitLocked(ctx, tx, operation, allocation)
	}
	if err := requireCurrentOperation(allocation, operation); err != nil {
		return err
	}
	if allocation.State != "releasing" {
		return fmt.Errorf("team quota allocation is %q, not releasing", allocation.State)
	}
	if err := runtimeMatches(allocation.Runtime, runtime); err != nil {
		return err
	}
	return r.commitLoadedAllocation(ctx, tx, operation, allocation)
}

// ReconcileTarget adopts already existing capacity without applying admission
// limits. It is restricted to bootstrap/reconciliation callers: subsequent
// reservations still observe the reconciled usage and fail when it is over the
// effective policy.
func (r *Repository) ReconcileTarget(ctx context.Context, owner Owner, target Values, runtime RuntimeRef) error {
	return r.inTx(ctx, "reconcile team quota target", func(tx pgx.Tx) error {
		return r.ReconcileTargetTx(ctx, tx, owner, target, runtime)
	})
}

// ReconcileTargetIfRevision applies an observed exact target only when the
// allocation has not changed since the caller captured its inventory. Revision
// zero means the allocation was absent in that snapshot.
func (r *Repository) ReconcileTargetIfRevision(
	ctx context.Context,
	owner Owner,
	target Values,
	runtime RuntimeRef,
	expectedRevision int64,
) (bool, error) {
	if expectedRevision < 0 {
		return false, fmt.Errorf("expected allocation revision must be non-negative")
	}
	applied := false
	err := r.inTx(ctx, "conditionally reconcile team quota target", func(tx pgx.Tx) error {
		normalizedOwner := normalizeOwner(owner)
		if err := normalizedOwner.Validate(); err != nil {
			return err
		}
		if err := lockTeam(ctx, tx, normalizedOwner.TeamID); err != nil {
			return err
		}
		allocation, err := loadAllocationForUpdate(ctx, tx, normalizedOwner)
		if err != nil {
			return err
		}
		currentRevision := int64(0)
		if allocation != nil {
			currentRevision = allocation.Revision
		}
		if currentRevision != expectedRevision {
			return nil
		}
		if err := r.ReconcileTargetTx(ctx, tx, normalizedOwner, target, runtime); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

// ReconcileTargetTx adopts existing capacity inside a caller-owned transaction.
func (r *Repository) ReconcileTargetTx(ctx context.Context, tx pgx.Tx, owner Owner, target Values, runtime RuntimeRef) error {
	if tx == nil {
		return fmt.Errorf("team quota transaction is required")
	}
	owner = normalizeOwner(owner)
	runtime = normalizeRuntime(runtime)
	if err := owner.Validate(); err != nil {
		return err
	}
	if err := target.validateCapacity(true); err != nil {
		return err
	}
	if runtime.Generation < 0 {
		return fmt.Errorf("runtime generation must be non-negative")
	}
	if err := lockTeam(ctx, tx, owner.TeamID); err != nil {
		return err
	}
	allocation, err := loadAllocationForUpdate(ctx, tx, owner)
	if err != nil {
		return err
	}
	items := make(map[Key]allocationItem)
	if allocation != nil {
		if err := requireAllocationCluster(allocation, owner); err != nil {
			return err
		}
		if operationID, err := preparedTransferForAllocation(ctx, tx, allocation.ID, true, true); err != nil {
			return err
		} else if operationID != "" {
			return &OperationConflictError{Owner: owner, OperationID: operationID}
		}
		if allocation.OperationID != "" {
			return &OperationConflictError{Owner: owner, OperationID: allocation.OperationID}
		}
		items, err = loadAllocationItems(ctx, tx, allocation.ID)
		if err != nil {
			return err
		}
	}
	current := committedValues(items)
	target = target.Clone()
	for key := range current {
		if _, ok := target[key]; !ok {
			target[key] = 0
		}
	}
	for key := range target {
		if _, ok := current[key]; !ok {
			current[key] = 0
		}
	}
	for _, key := range target.Keys() {
		policy, err := r.EffectivePolicyTx(ctx, tx, owner.TeamID, key)
		if err != nil {
			return err
		}
		if policy == nil {
			return &UnavailableError{
				Operation: "reconcile team quota target",
				Err:       fmt.Errorf("no effective policy for %s", key),
			}
		}
		if policy.Kind != KindCapacity {
			return &UnavailableError{
				Operation: "reconcile team quota target",
				Err:       fmt.Errorf("effective policy for %s is not capacity", key),
			}
		}
	}

	allocationID := uuid.NewString()
	state := stateForAllocation(owner, target)
	if allocation == nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.allocations (
				allocation_id, team_id, owner_kind, owner_id, cluster_id,
				state, pod_namespace, pod_name, pod_uid, runtime_generation,
				operation_fence_generation
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, allocationID, owner.TeamID, owner.Kind, owner.ID, owner.ClusterID,
			state, runtime.Namespace, runtime.Name, runtime.UID,
			runtime.Generation, runtime.Generation); err != nil {
			return &UnavailableError{Operation: "create reconciled team quota allocation", Err: err}
		}
	} else {
		allocationID = allocation.ID
		clearRuntime := state == "paused" || state == "released"
		if _, err := tx.Exec(ctx, `
			UPDATE quota.allocations
			SET cluster_id = CASE WHEN $2 = '' THEN cluster_id ELSE $2 END,
				state = $3,
					pod_namespace = $4,
					pod_name = $5,
					pod_uid = $6,
					runtime_generation = CASE
						WHEN $8 THEN 0
						ELSE GREATEST(runtime_generation, $7)
					END,
					operation_fence_generation =
						GREATEST(operation_fence_generation, $7),
					last_operation_id = '',
					last_operation_generation = 0,
					last_operation_result = '',
					reconcile_after = NULL,
					last_error = '',
				updated_at = NOW()
			WHERE allocation_id = $1
			`, allocationID, owner.ClusterID, state, runtime.Namespace, runtime.Name,
			runtime.UID, runtime.Generation, clearRuntime); err != nil {
			return &UnavailableError{Operation: "update reconciled team quota allocation", Err: err}
		}
	}
	for _, key := range target.Keys() {
		delta := target[key] - current[key]
		if err := applyCommittedUsage(ctx, tx, owner.TeamID, key, delta, 0); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.allocation_items (
				allocation_id, quota_key, committed_value, pending_value
			) VALUES ($1, $2, $3, NULL)
			ON CONFLICT (allocation_id, quota_key) DO UPDATE
			SET committed_value = EXCLUDED.committed_value,
				pending_value = NULL
		`, allocationID, string(key), target[key]); err != nil {
			return &UnavailableError{Operation: "write reconciled team quota allocation item", Err: err}
		}
	}
	return finishTeamCapacityMutation(ctx, tx, owner.TeamID)
}

// PrepareTransfer reserves the positive net team increase and any explicit
// transition overlap while leaving both owners' committed usage unchanged.
// The returned operation must be committed after the external runtime mutation
// succeeds, or aborted if it fails.
func (r *Repository) PrepareTransfer(ctx context.Context, request TransferRequest) (*Reservation, error) {
	var reservation *Reservation
	err := r.inTx(ctx, "prepare team quota transfer", func(tx pgx.Tx) error {
		var err error
		reservation, err = r.PrepareTransferTx(ctx, tx, request)
		return err
	})
	return reservation, err
}

// PrepareTransferTx prepares a transfer inside a caller-owned transaction.
func (r *Repository) PrepareTransferTx(
	ctx context.Context,
	tx pgx.Tx,
	request TransferRequest,
) (*Reservation, error) {
	if tx == nil {
		return nil, fmt.Errorf("team quota transaction is required")
	}
	request = normalizeTransferRequest(request)
	if err := request.validate(); err != nil {
		return nil, err
	}
	fingerprint, err := transferRequestFingerprint(request)
	if err != nil {
		return nil, err
	}
	if err := lockTeam(ctx, tx, request.Source.TeamID); err != nil {
		return nil, err
	}
	existing, err := loadTransferForUpdate(ctx, tx, request.Source.TeamID, request.Operation.ID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Fingerprint != fingerprint {
			return nil, &OperationConflictError{
				Owner:       request.Destination,
				OperationID: request.Operation.ID,
			}
		}
		if existing.State == "aborted" {
			return nil, &OperationAbortedError{OperationID: request.Operation.ID}
		}
		items, err := loadTransferItems(ctx, tx, existing.TeamID, existing.Operation.ID)
		if err != nil {
			return nil, err
		}
		return reservationForTransfer(existing, request.Destination, items), nil
	}

	source, err := loadAllocationForUpdate(ctx, tx, request.Source)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, &UnavailableError{
			Operation: "prepare team quota transfer",
			Err:       fmt.Errorf("source allocation not found"),
		}
	}
	if err := requireAllocationCluster(source, request.Source); err != nil {
		return nil, err
	}
	if source.OperationID != "" {
		return nil, &OperationConflictError{Owner: request.Source, OperationID: source.OperationID}
	}
	if source.LastOperationID == request.Operation.ID {
		return nil, &OperationConflictError{Owner: request.Source, OperationID: request.Operation.ID}
	}
	if operationID, err := preparedTransferForAllocation(ctx, tx, source.ID, false, true); err != nil {
		return nil, err
	} else if operationID != "" {
		return nil, &OperationConflictError{Owner: request.Source, OperationID: operationID}
	}
	if hasRuntimeRef(source.Runtime) {
		if err := runtimeMatches(source.Runtime, request.Runtime); err != nil {
			return nil, err
		}
	}
	sourceItems, err := loadAllocationItems(ctx, tx, source.ID)
	if err != nil {
		return nil, err
	}
	sourceCurrent := committedValues(sourceItems)
	alreadyPrepared, err := preparedSourceDecrease(ctx, tx, source.ID)
	if err != nil {
		return nil, err
	}
	for key, decrease := range request.SourceDecrease {
		claimed, ok := addInt64(alreadyPrepared[key], decrease)
		if !ok || claimed > sourceCurrent[key] {
			return nil, fmt.Errorf(
				"source committed value for %q is %d, smaller than prepared transfer decrease %d",
				key,
				sourceCurrent[key],
				claimed,
			)
		}
	}

	destination, err := loadAllocationForUpdate(ctx, tx, request.Destination)
	if err != nil {
		return nil, err
	}
	destinationItems := make(map[Key]allocationItem)
	if destination != nil {
		if err := requireReserveClusterTransition(destination, request.Destination, request.Operation); err != nil {
			return nil, err
		}
		if destination.OperationID != "" {
			return nil, &OperationConflictError{
				Owner:       request.Destination,
				OperationID: destination.OperationID,
			}
		}
		if request.Operation.Generation < destination.OperationFence {
			return nil, &OperationConflictError{
				Owner:       request.Destination,
				OperationID: request.Operation.ID,
			}
		}
		if destination.LastOperationID == request.Operation.ID {
			return nil, &OperationConflictError{
				Owner:       request.Destination,
				OperationID: request.Operation.ID,
			}
		}
		if operationID, err := preparedTransferForAllocation(ctx, tx, destination.ID, true, true); err != nil {
			return nil, err
		} else if operationID != "" {
			return nil, &OperationConflictError{
				Owner:       request.Destination,
				OperationID: operationID,
			}
		}
		if hasRuntimeRef(destination.Runtime) {
			if err := runtimeMatches(destination.Runtime, request.Runtime); err != nil {
				return nil, err
			}
		}
		destinationItems, err = loadAllocationItems(ctx, tx, destination.ID)
		if err != nil {
			return nil, err
		}
	}
	destinationCurrent := committedValues(destinationItems)
	destinationTarget := request.DestinationTarget.Clone()
	for key := range destinationCurrent {
		if _, ok := destinationTarget[key]; !ok {
			destinationTarget[key] = 0
		}
	}
	for key := range destinationTarget {
		if _, ok := destinationCurrent[key]; !ok {
			destinationCurrent[key] = 0
		}
	}
	for key := range request.SourceDecrease {
		if _, ok := destinationTarget[key]; !ok {
			destinationTarget[key] = destinationCurrent[key]
		}
		if _, ok := destinationCurrent[key]; !ok {
			destinationCurrent[key] = 0
		}
	}
	for key := range request.TransitionReserve {
		if _, ok := destinationTarget[key]; !ok {
			destinationTarget[key] = destinationCurrent[key]
		}
		if _, ok := destinationCurrent[key]; !ok {
			destinationCurrent[key] = 0
		}
	}
	for key, target := range destinationTarget {
		if target < destinationCurrent[key] {
			return nil, fmt.Errorf(
				"destination target for %q cannot reduce committed value from %d to %d",
				key,
				destinationCurrent[key],
				target,
			)
		}
	}

	transferItems := make(map[Key]transferItem)
	for key, target := range destinationTarget {
		transferItems[key] = transferItem{
			DestinationCommitted: destinationCurrent[key],
			DestinationTarget:    target,
		}
	}
	for key, decrease := range request.SourceDecrease {
		item := transferItems[key]
		item.SourceDecrease = decrease
		transferItems[key] = item
	}
	for key := range request.TransitionReserve {
		if _, ok := transferItems[key]; !ok {
			transferItems[key] = transferItem{}
		}
	}
	for key, item := range transferItems {
		net := item.DestinationTarget - item.DestinationCommitted - item.SourceDecrease
		reserved := int64(0)
		if net > 0 {
			reserved = net
		}
		var ok bool
		item.Reserved, ok = addInt64(reserved, request.TransitionReserve[key])
		if !ok {
			return nil, fmt.Errorf("prepared transfer reservation for %q overflows int64", key)
		}
		transferItems[key] = item
	}
	for _, key := range transferItemKeys(transferItems) {
		reserved := transferItems[key].Reserved
		if reserved == 0 {
			continue
		}
		if err := ensureUsageRow(ctx, tx, request.Source.TeamID, key); err != nil {
			return nil, err
		}
		usage, err := loadUsageForUpdate(ctx, tx, request.Source.TeamID, key)
		if err != nil {
			return nil, err
		}
		policy, err := r.EffectivePolicyTx(ctx, tx, request.Source.TeamID, key)
		if err != nil {
			return nil, err
		}
		if policy == nil || policy.Kind != KindCapacity {
			return nil, &UnavailableError{
				Operation: "prepare team quota transfer",
				Err:       fmt.Errorf("no capacity policy for %s", key),
			}
		}
		used, ok := addInt64(usage.committed, usage.reserved)
		if !ok {
			return nil, &UnavailableError{
				Operation: "prepare team quota transfer",
				Err:       fmt.Errorf("usage for %s overflows int64", key),
			}
		}
		next, ok := addInt64(used, reserved)
		if !ok || next > policy.Limit {
			return nil, &ExceededError{
				TeamID:    request.Source.TeamID,
				Key:       key,
				Limit:     policy.Limit,
				Committed: usage.committed,
				Reserved:  usage.reserved,
				Requested: reserved,
			}
		}
	}

	destinationAllocationID := uuid.NewString()
	if destination == nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.allocations (
				allocation_id, team_id, owner_kind, owner_id, cluster_id,
				state, operation_id, operation_kind, operation_generation,
				operation_base_state, operation_fence_generation
			) VALUES (
				$1, $2, $3, $4, $5,
				'reserved', $6, $7, $8,
				'released', $8
			)
		`, destinationAllocationID, request.Destination.TeamID,
			request.Destination.Kind, request.Destination.ID,
			request.Destination.ClusterID, request.Operation.ID,
			internalTransferOperationKind, request.Operation.Generation); err != nil {
			return nil, &UnavailableError{Operation: "create prepared transfer destination", Err: err}
		}
	} else {
		destinationAllocationID = destination.ID
		destinationBaseState := destination.State
		if _, err := tx.Exec(ctx, `
			UPDATE quota.allocations
			SET cluster_id = CASE WHEN $2 = '' THEN cluster_id ELSE $2 END,
				state = 'reserved',
				operation_id = $3,
					operation_kind = $4,
					operation_generation = $5,
					operation_base_state = $6,
					operation_fence_generation =
						GREATEST(operation_fence_generation, $5),
					reconcile_after = NULL,
				last_error = '',
				updated_at = NOW()
			WHERE allocation_id = $1
		`, destinationAllocationID, request.Destination.ClusterID,
			request.Operation.ID, internalTransferOperationKind,
			request.Operation.Generation, destinationBaseState); err != nil {
			return nil, &UnavailableError{Operation: "lock prepared transfer destination", Err: err}
		}
	}
	for _, key := range destinationTarget.Keys() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.allocation_items (
				allocation_id, quota_key, committed_value, pending_value
			) VALUES ($1, $2, $3, NULL)
			ON CONFLICT (allocation_id, quota_key) DO NOTHING
		`, destinationAllocationID, string(key), destinationCurrent[key]); err != nil {
			return nil, &UnavailableError{Operation: "initialize prepared transfer destination item", Err: err}
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO quota.transfer_operations (
			team_id, operation_id, operation_kind, operation_generation,
			request_fingerprint, state,
			source_allocation_id, destination_allocation_id,
			pod_namespace, pod_name, pod_uid, runtime_generation
		) VALUES (
			$1, $2, $3, $4,
			$5, 'prepared',
			$6, $7,
			$8, $9, $10, $11
		)
	`, request.Source.TeamID, request.Operation.ID, request.Operation.Kind,
		request.Operation.Generation, fingerprint, source.ID,
		destinationAllocationID, request.Runtime.Namespace, request.Runtime.Name,
		request.Runtime.UID, request.Runtime.Generation); err != nil {
		return nil, &UnavailableError{Operation: "record prepared team quota transfer", Err: err}
	}
	for _, key := range transferItemKeys(transferItems) {
		item := transferItems[key]
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.transfer_items (
				team_id, operation_id, quota_key,
				source_decrease, destination_committed,
				destination_target, reserved_value
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, request.Source.TeamID, request.Operation.ID, string(key),
			item.SourceDecrease, item.DestinationCommitted,
			item.DestinationTarget, item.Reserved); err != nil {
			return nil, &UnavailableError{Operation: "record prepared team quota transfer item", Err: err}
		}
		if item.Reserved > 0 {
			if err := addReservedUsage(ctx, tx, request.Source.TeamID, key, item.Reserved); err != nil {
				return nil, err
			}
		}
	}
	if err := bumpTeamRevision(ctx, tx, request.Source.TeamID); err != nil {
		return nil, err
	}
	record := &transferRecord{
		TeamID:                  request.Source.TeamID,
		Operation:               request.Operation,
		Fingerprint:             fingerprint,
		State:                   "prepared",
		SourceAllocationID:      source.ID,
		DestinationAllocationID: destinationAllocationID,
		Runtime:                 request.Runtime,
	}
	return reservationForTransfer(record, request.Destination, transferItems), nil
}

// CommitTransfer atomically completes a prepared ownership transfer after the
// external runtime mutation succeeds.
func (r *Repository) CommitTransfer(ctx context.Context, operation OperationRef) error {
	return r.inTx(ctx, "commit team quota transfer", func(tx pgx.Tx) error {
		return r.CommitTransferTx(ctx, tx, operation)
	})
}

// CommitTransferObservedSource commits a prepared transfer while atomically
// adopting the complete observed external source. This closes the observation
// race between concurrent transfers and keeps mutated physical overage counted.
func (r *Repository) CommitTransferObservedSource(
	ctx context.Context,
	operation OperationRef,
	observedSource Values,
) error {
	if err := observedSource.validateCapacity(true); err != nil {
		return fmt.Errorf("observed transfer source: %w", err)
	}
	return r.inTx(ctx, "commit observed team quota transfer", func(tx pgx.Tx) error {
		return r.commitTransferTx(ctx, tx, operation, observedSource)
	})
}

// CommitTransferTx commits a prepared transfer inside a caller-owned transaction.
func (r *Repository) CommitTransferTx(ctx context.Context, tx pgx.Tx, operation OperationRef) error {
	return r.commitTransferTx(ctx, tx, operation, nil)
}

func (r *Repository) commitTransferTx(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	observedSource Values,
) error {
	if tx == nil {
		return fmt.Errorf("team quota transaction is required")
	}
	operation.Owner = normalizeOwner(operation.Owner)
	if err := operation.validate(); err != nil {
		return err
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	record, err := loadTransferForUpdate(ctx, tx, operation.Owner.TeamID, operation.ID)
	if err != nil {
		return err
	}
	if record == nil {
		return &UnavailableError{
			Operation: "commit team quota transfer",
			Err:       fmt.Errorf("prepared transfer not found"),
		}
	}
	if record.Operation.Generation != operation.Generation {
		return &OperationConflictError{Owner: operation.Owner, OperationID: operation.ID}
	}
	destination, err := loadAllocationByIDForUpdate(ctx, tx, record.DestinationAllocationID)
	if err != nil {
		return err
	}
	if destination == nil || !sameOwner(destination.Owner, operation.Owner) {
		return &OperationConflictError{Owner: operation.Owner, OperationID: operation.ID}
	}
	switch record.State {
	case "committed":
		return nil
	case "aborted":
		return &OperationAbortedError{OperationID: operation.ID}
	case "prepared":
	default:
		return &UnavailableError{
			Operation: "commit team quota transfer",
			Err:       fmt.Errorf("unknown transfer state %q", record.State),
		}
	}
	if destination.OperationID != operation.ID ||
		destination.OperationGeneration != operation.Generation ||
		destination.OperationKind != internalTransferOperationKind {
		return &OperationConflictError{Owner: operation.Owner, OperationID: destination.OperationID}
	}
	source, err := loadAllocationByIDForUpdate(ctx, tx, record.SourceAllocationID)
	if err != nil {
		return err
	}
	if source == nil || source.OperationID != "" {
		return &OperationConflictError{
			Owner:       sourceOwnerOrZero(source, record.TeamID),
			OperationID: operation.ID,
		}
	}
	items, err := loadTransferItems(ctx, tx, record.TeamID, record.Operation.ID)
	if err != nil {
		return err
	}
	sourceItems, err := loadAllocationItems(ctx, tx, source.ID)
	if err != nil {
		return err
	}
	destinationItems, err := loadAllocationItems(ctx, tx, destination.ID)
	if err != nil {
		return err
	}
	sourceTarget := committedValues(sourceItems)
	sourceCommitted := sourceTarget.Clone()
	destinationTarget := make(Values, len(items))
	for _, key := range transferItemKeys(items) {
		item := items[key]
		sourceItem, ok := sourceItems[key]
		if item.SourceDecrease > 0 && (!ok || sourceItem.Committed < item.SourceDecrease) {
			return &UnavailableError{
				Operation: "commit team quota transfer",
				Err:       fmt.Errorf("source allocation invariant violated for %s", key),
			}
		}
		sourceTarget[key] = sourceItem.Committed - item.SourceDecrease
		destinationItem := destinationItems[key]
		if destinationItem.Committed != item.DestinationCommitted || destinationItem.Pending != nil {
			return &UnavailableError{
				Operation: "commit team quota transfer",
				Err:       fmt.Errorf("destination allocation invariant violated for %s", key),
			}
		}
		destinationTarget[key] = item.DestinationTarget
	}
	if len(observedSource) > 0 {
		for _, key := range observedSource.Keys() {
			if observedSource[key] > sourceTarget[key] {
				sourceTarget[key] = observedSource[key]
			}
		}
	}
	for _, key := range transferItemKeys(items) {
		item := items[key]
		if sourceTarget[key] != sourceCommitted[key] {
			tag, err := tx.Exec(ctx, `
				UPDATE quota.allocation_items
				SET committed_value = $3
				WHERE allocation_id = $1 AND quota_key = $2
			`, source.ID, string(key), sourceTarget[key])
			if err != nil {
				return &UnavailableError{Operation: "commit transfer source item", Err: err}
			}
			if tag.RowsAffected() != 1 {
				return &UnavailableError{
					Operation: "commit transfer source item",
					Err:       fmt.Errorf("source allocation item for %s is missing", key),
				}
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.allocation_items (
				allocation_id, quota_key, committed_value, pending_value
			) VALUES ($1, $2, $3, NULL)
			ON CONFLICT (allocation_id, quota_key) DO UPDATE
			SET committed_value = EXCLUDED.committed_value,
				pending_value = NULL
		`, destination.ID, string(key), item.DestinationTarget); err != nil {
			return &UnavailableError{Operation: "commit transfer destination item", Err: err}
		}
		net, ok := addInt64(
			item.DestinationTarget-item.DestinationCommitted,
			sourceTarget[key]-sourceCommitted[key],
		)
		if !ok {
			return &UnavailableError{
				Operation: "commit team quota transfer",
				Err:       fmt.Errorf("committed usage delta for %s overflows int64", key),
			}
		}
		if net != 0 || item.Reserved != 0 {
			if err := applyCommittedUsage(ctx, tx, record.TeamID, key, net, item.Reserved); err != nil {
				return err
			}
		}
	}
	sourceState := stateForAllocation(source.Owner, sourceTarget)
	clearSourceRuntime := sourceState == "paused" || sourceState == "released"
	if _, err := tx.Exec(ctx, `
		UPDATE quota.allocations
		SET state = $2,
			last_operation_id = $3,
			last_operation_generation = $4,
			last_operation_result = 'committed',
			pod_namespace = CASE WHEN $5 THEN '' ELSE pod_namespace END,
			pod_name = CASE WHEN $5 THEN '' ELSE pod_name END,
			pod_uid = CASE WHEN $5 THEN '' ELSE pod_uid END,
			runtime_generation = CASE WHEN $5 THEN 0 ELSE runtime_generation END,
			reconcile_after = NULL,
			last_error = '',
			updated_at = NOW()
		WHERE allocation_id = $1
	`, source.ID, sourceState, operation.ID, operation.Generation,
		clearSourceRuntime); err != nil {
		return &UnavailableError{Operation: "commit transfer source", Err: err}
	}
	destinationState := stateForAllocation(destination.Owner, destinationTarget)
	if _, err := tx.Exec(ctx, `
		UPDATE quota.allocations
		SET state = $2,
			operation_id = NULL,
			operation_kind = '',
			operation_generation = 0,
			operation_base_state = '',
			last_operation_id = $3,
			last_operation_generation = $4,
			last_operation_result = 'committed',
			operation_fence_generation =
				GREATEST(operation_fence_generation, $4, $8),
			pod_namespace = $5,
			pod_name = $6,
			pod_uid = $7,
			runtime_generation = $8,
			reconcile_after = NULL,
			last_error = '',
			updated_at = NOW()
		WHERE allocation_id = $1
	`, destination.ID, destinationState, operation.ID, operation.Generation,
		record.Runtime.Namespace, record.Runtime.Name, record.Runtime.UID,
		record.Runtime.Generation); err != nil {
		return &UnavailableError{Operation: "commit transfer destination", Err: err}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE quota.transfer_operations
		SET state = 'committed',
			completed_at = NOW(),
			last_error = ''
		WHERE team_id = $1 AND operation_id = $2
	`, record.TeamID, record.Operation.ID); err != nil {
		return &UnavailableError{Operation: "complete team quota transfer", Err: err}
	}
	return finishTeamCapacityMutation(ctx, tx, record.TeamID)
}

// AbortTransfer releases a prepared net reservation without changing source
// or destination committed usage.
func (r *Repository) AbortTransfer(ctx context.Context, operation OperationRef, reason string) error {
	return r.inTx(ctx, "abort team quota transfer", func(tx pgx.Tx) error {
		return r.AbortTransferTx(ctx, tx, operation, reason)
	})
}

// AbortTransferTx aborts a prepared transfer inside a caller-owned transaction.
func (r *Repository) AbortTransferTx(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	reason string,
) error {
	if tx == nil {
		return fmt.Errorf("team quota transaction is required")
	}
	operation.Owner = normalizeOwner(operation.Owner)
	if err := operation.validate(); err != nil {
		return err
	}
	if err := lockTeam(ctx, tx, operation.Owner.TeamID); err != nil {
		return err
	}
	record, err := loadTransferForUpdate(ctx, tx, operation.Owner.TeamID, operation.ID)
	if err != nil {
		return err
	}
	if record == nil {
		return &UnavailableError{
			Operation: "abort team quota transfer",
			Err:       fmt.Errorf("prepared transfer not found"),
		}
	}
	if record.Operation.Generation != operation.Generation {
		return &OperationConflictError{Owner: operation.Owner, OperationID: operation.ID}
	}
	destination, err := loadAllocationByIDForUpdate(ctx, tx, record.DestinationAllocationID)
	if err != nil {
		return err
	}
	if destination == nil || !sameOwner(destination.Owner, operation.Owner) {
		return &OperationConflictError{Owner: operation.Owner, OperationID: operation.ID}
	}
	switch record.State {
	case "aborted":
		return nil
	case "committed":
		return &OperationConflictError{Owner: operation.Owner, OperationID: operation.ID}
	case "prepared":
	default:
		return &UnavailableError{
			Operation: "abort team quota transfer",
			Err:       fmt.Errorf("unknown transfer state %q", record.State),
		}
	}
	if destination.OperationID != operation.ID ||
		destination.OperationGeneration != operation.Generation ||
		destination.OperationKind != internalTransferOperationKind {
		return &OperationConflictError{Owner: operation.Owner, OperationID: destination.OperationID}
	}
	items, err := loadTransferItems(ctx, tx, record.TeamID, record.Operation.ID)
	if err != nil {
		return err
	}
	for key, item := range items {
		if item.Reserved > 0 {
			if err := subtractReservedUsage(ctx, tx, record.TeamID, key, item.Reserved); err != nil {
				return err
			}
		}
	}
	baseState := destination.OperationBaseState
	if baseState == "" {
		destinationItems, err := loadAllocationItems(ctx, tx, destination.ID)
		if err != nil {
			return err
		}
		baseState = stateForAllocation(destination.Owner, committedValues(destinationItems))
	}
	if _, err := tx.Exec(ctx, `
		UPDATE quota.allocations
		SET state = $2,
			operation_id = NULL,
			operation_kind = '',
			operation_generation = 0,
			operation_base_state = '',
			last_operation_id = $3,
			last_operation_generation = $4,
			last_operation_result = 'aborted',
			operation_fence_generation =
				GREATEST(operation_fence_generation, $4),
			reconcile_after = NULL,
			last_error = $5,
			updated_at = NOW()
		WHERE allocation_id = $1
	`, destination.ID, baseState, operation.ID, operation.Generation,
		strings.TrimSpace(reason)); err != nil {
		return &UnavailableError{Operation: "abort transfer destination", Err: err}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE quota.transfer_operations
		SET state = 'aborted',
			completed_at = NOW(),
			last_error = $3
		WHERE team_id = $1 AND operation_id = $2
	`, record.TeamID, record.Operation.ID, strings.TrimSpace(reason)); err != nil {
		return &UnavailableError{Operation: "abort team quota transfer", Err: err}
	}
	return finishTeamCapacityMutation(ctx, tx, record.TeamID)
}

// TransferTarget is the single-transaction convenience form of the transfer
// saga. Callers that mutate an external runtime must use PrepareTransfer
// followed by CommitTransfer or AbortTransfer instead.
func (r *Repository) TransferTarget(ctx context.Context, request TransferRequest) (*Reservation, error) {
	var reservation *Reservation
	err := r.inTx(ctx, "transfer team quota target", func(tx pgx.Tx) error {
		var err error
		reservation, err = r.TransferTargetTx(ctx, tx, request)
		return err
	})
	return reservation, err
}

// TransferTargetTx prepares and commits a transfer in one database transaction.
func (r *Repository) TransferTargetTx(
	ctx context.Context,
	tx pgx.Tx,
	request TransferRequest,
) (*Reservation, error) {
	request = normalizeTransferRequest(request)
	prepared, err := r.PrepareTransferTx(ctx, tx, request)
	if err != nil {
		return nil, err
	}
	if err := r.CommitTransferTx(ctx, tx, Ref(request.Destination, request.Operation)); err != nil {
		return nil, err
	}
	return transferredReservation(request, prepared.AllocationID), nil
}

func transferRequestFingerprint(request TransferRequest) (string, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("encode team quota transfer request: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func transferredReservation(request TransferRequest, allocationID string) *Reservation {
	target := request.DestinationTarget.Clone()
	return &Reservation{
		AllocationID: allocationID,
		Owner:        request.Destination,
		Operation:    request.Operation,
		State:        stateForAllocation(request.Destination, target),
		Committed:    target.Clone(),
		Target:       target,
		Reserved:     make(Values),
	}
}

func reservationForTransfer(record *transferRecord, destination Owner, items map[Key]transferItem) *Reservation {
	committed := make(Values, len(items))
	target := make(Values, len(items))
	reserved := make(Values)
	for key, item := range items {
		committed[key] = item.DestinationCommitted
		target[key] = item.DestinationTarget
		if item.Reserved > 0 {
			reserved[key] = item.Reserved
		}
	}
	state := "reserved"
	if record.State == "committed" {
		state = stateForAllocation(destination, target)
		committed = target.Clone()
		reserved = make(Values)
	}
	return &Reservation{
		AllocationID: record.DestinationAllocationID,
		Owner:        destination,
		Operation:    record.Operation,
		State:        state,
		Committed:    committed,
		Target:       target,
		Reserved:     reserved,
	}
}

func loadTransferForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	teamID string,
	operationID string,
) (*transferRecord, error) {
	record := &transferRecord{}
	err := tx.QueryRow(ctx, `
		SELECT team_id, operation_id, operation_kind, operation_generation,
			request_fingerprint, state,
			source_allocation_id, destination_allocation_id,
			pod_namespace, pod_name, pod_uid, runtime_generation
		FROM quota.transfer_operations
		WHERE team_id = $1 AND operation_id = $2
		FOR UPDATE
	`, teamID, operationID).Scan(
		&record.TeamID, &record.Operation.ID, &record.Operation.Kind,
		&record.Operation.Generation, &record.Fingerprint, &record.State,
		&record.SourceAllocationID, &record.DestinationAllocationID,
		&record.Runtime.Namespace, &record.Runtime.Name, &record.Runtime.UID,
		&record.Runtime.Generation,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, &UnavailableError{Operation: "load team quota transfer", Err: err}
	}
	return record, nil
}

func loadTransferItems(
	ctx context.Context,
	tx pgx.Tx,
	teamID string,
	operationID string,
) (map[Key]transferItem, error) {
	rows, err := tx.Query(ctx, `
		SELECT quota_key, source_decrease, destination_committed,
			destination_target, reserved_value
		FROM quota.transfer_items
		WHERE team_id = $1 AND operation_id = $2
		ORDER BY quota_key
	`, teamID, operationID)
	if err != nil {
		return nil, &UnavailableError{Operation: "load team quota transfer items", Err: err}
	}
	defer rows.Close()
	items := make(map[Key]transferItem)
	for rows.Next() {
		var key Key
		var item transferItem
		if err := rows.Scan(
			&key, &item.SourceDecrease, &item.DestinationCommitted,
			&item.DestinationTarget, &item.Reserved,
		); err != nil {
			return nil, &UnavailableError{Operation: "scan team quota transfer item", Err: err}
		}
		items[key] = item
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "load team quota transfer items", Err: err}
	}
	return items, nil
}

func preparedSourceDecrease(ctx context.Context, tx pgx.Tx, allocationID string) (Values, error) {
	rows, err := tx.Query(ctx, `
		SELECT i.quota_key, SUM(i.source_decrease)::BIGINT
		FROM quota.transfer_operations o
		JOIN quota.transfer_items i
			ON i.team_id = o.team_id AND i.operation_id = o.operation_id
		WHERE o.source_allocation_id = $1 AND o.state = 'prepared'
		GROUP BY i.quota_key
	`, allocationID)
	if err != nil {
		return nil, &UnavailableError{Operation: "load prepared source decreases", Err: err}
	}
	defer rows.Close()
	values := make(Values)
	for rows.Next() {
		var key Key
		var value int64
		if err := rows.Scan(&key, &value); err != nil {
			return nil, &UnavailableError{Operation: "scan prepared source decrease", Err: err}
		}
		values[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "load prepared source decreases", Err: err}
	}
	return values, nil
}

func preparedTransferForAllocation(
	ctx context.Context,
	tx pgx.Tx,
	allocationID string,
	includeSource bool,
	includeDestination bool,
) (string, error) {
	conditions := make([]string, 0, 2)
	if includeSource {
		conditions = append(conditions, "source_allocation_id = $1")
	}
	if includeDestination {
		conditions = append(conditions, "destination_allocation_id = $1")
	}
	if len(conditions) == 0 {
		return "", nil
	}
	var operationID string
	err := tx.QueryRow(ctx, `
		SELECT operation_id
		FROM quota.transfer_operations
		WHERE state = 'prepared'
			AND (`+strings.Join(conditions, " OR ")+`)
		ORDER BY operation_id
		LIMIT 1
	`, allocationID).Scan(&operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", &UnavailableError{Operation: "load prepared allocation transfer", Err: err}
	}
	return operationID, nil
}

func transferItemKeys(items map[Key]transferItem) []Key {
	keys := make([]Key, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func sameOwner(left, right Owner) bool {
	return left.TeamID == right.TeamID &&
		left.Kind == right.Kind &&
		left.ID == right.ID &&
		(right.ClusterID == "" || left.ClusterID == right.ClusterID)
}

func requireAllocationCluster(allocation *allocationRecord, requested Owner) error {
	if allocation == nil ||
		requested.ClusterID == "" ||
		allocation.Owner.ClusterID == "" ||
		allocation.Owner.ClusterID == requested.ClusterID {
		return nil
	}
	return &OperationConflictError{
		Owner:       requested,
		OperationID: allocation.OperationID,
	}
}

func requireReserveClusterTransition(
	allocation *allocationRecord,
	requested Owner,
	operation Operation,
) error {
	if err := requireAllocationCluster(allocation, requested); err == nil {
		return nil
	}
	if allocation.OperationID == "" &&
		(allocation.State == "paused" || allocation.State == "released") &&
		!hasRuntimeRef(allocation.Runtime) &&
		operation.Generation > allocation.OperationFence {
		return nil
	}
	return &OperationConflictError{
		Owner:       requested,
		OperationID: allocation.OperationID,
	}
}

func sourceOwnerOrZero(source *allocationRecord, teamID string) Owner {
	if source == nil {
		return Owner{TeamID: teamID, Kind: "transfer_source", ID: "missing"}
	}
	return source.Owner
}

func hasRuntimeRef(runtime RuntimeRef) bool {
	return runtime.Namespace != "" ||
		runtime.Name != "" ||
		runtime.UID != "" ||
		runtime.Generation != 0
}

func loadAllocationOperationForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	allocationID string,
	operationID string,
) (*allocationOperationRecord, error) {
	record := &allocationOperationRecord{}
	err := tx.QueryRow(ctx, `
		SELECT operation_id, operation_kind, operation_generation,
			request_fingerprint, state
		FROM quota.allocation_operations
		WHERE allocation_id = $1 AND operation_id = $2
		FOR UPDATE
	`, allocationID, operationID).Scan(
		&record.ID,
		&record.Kind,
		&record.Generation,
		&record.Fingerprint,
		&record.State,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, &UnavailableError{
			Operation: "load team quota allocation operation",
			Err:       err,
		}
	}
	return record, nil
}

func insertAllocationOperation(
	ctx context.Context,
	tx pgx.Tx,
	allocationID string,
	operation Operation,
	fingerprint string,
) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO quota.allocation_operations (
			allocation_id, operation_id, operation_kind,
			operation_generation, request_fingerprint, state
		) VALUES ($1, $2, $3, $4, $5, 'prepared')
	`, allocationID, operation.ID, operation.Kind, operation.Generation, fingerprint); err != nil {
		return &UnavailableError{
			Operation: "record team quota allocation operation",
			Err:       err,
		}
	}
	return nil
}

func completeAllocationOperation(
	ctx context.Context,
	tx pgx.Tx,
	allocation *allocationRecord,
	operation OperationRef,
	state string,
	reason string,
) error {
	if allocation == nil {
		return &UnavailableError{
			Operation: "complete team quota allocation operation",
			Err:       fmt.Errorf("allocation is required"),
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE quota.allocation_operations
		SET state = $5,
			last_error = $6,
			completed_at = NOW()
		WHERE allocation_id = $1
			AND operation_id = $2
			AND operation_generation = $3
			AND operation_kind = $4
			AND state = 'prepared'
	`, allocation.ID, operation.ID, operation.Generation,
		allocation.OperationKind, state, strings.TrimSpace(reason))
	if err != nil {
		return &UnavailableError{
			Operation: "complete team quota allocation operation",
			Err:       err,
		}
	}
	if tag.RowsAffected() != 1 {
		return &UnavailableError{
			Operation: "complete team quota allocation operation",
			Err:       fmt.Errorf("prepared operation history is missing or inconsistent"),
		}
	}
	return nil
}

func (r *Repository) commitLocked(ctx context.Context, tx pgx.Tx, operation OperationRef, loaded *allocationRecord) error {
	allocation := loaded
	var err error
	if allocation == nil {
		allocation, err = loadAllocationForUpdate(ctx, tx, operation.Owner)
		if err != nil {
			return err
		}
	}
	if allocation == nil {
		return &UnavailableError{Operation: "commit team quota operation", Err: fmt.Errorf("allocation not found")}
	}
	if err := requireAllocationCluster(allocation, operation.Owner); err != nil {
		return err
	}
	if allocation.OperationKind == internalTransferOperationKind {
		return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
	}
	if allocation.OperationID == "" {
		history, err := loadAllocationOperationForUpdate(ctx, tx, allocation.ID, operation.ID)
		if err != nil {
			return err
		}
		if history == nil || history.Generation != operation.Generation {
			return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
		}
		if history.State == "committed" &&
			allocation.LastOperationID == operation.ID &&
			allocation.LastOperationGen == operation.Generation &&
			allocation.LastOperationResult == "committed" {
			return nil
		}
		if history.State == "aborted" {
			return &OperationAbortedError{OperationID: operation.ID}
		}
		return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
	}
	if err := requireCurrentOperation(allocation, operation); err != nil {
		return err
	}
	return r.commitLoadedAllocation(ctx, tx, operation, allocation)
}

func (r *Repository) commitExactLocked(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	exact Values,
) error {
	allocation, err := loadAllocationForUpdate(ctx, tx, operation.Owner)
	if err != nil {
		return err
	}
	if allocation == nil {
		return &UnavailableError{
			Operation: "commit exact team quota operation",
			Err:       fmt.Errorf("allocation not found"),
		}
	}
	if err := requireAllocationCluster(allocation, operation.Owner); err != nil {
		return err
	}
	if allocation.OperationKind == internalTransferOperationKind {
		return &OperationConflictError{
			Owner:       operation.Owner,
			OperationID: allocation.OperationID,
		}
	}
	if allocation.OperationID == "" {
		history, err := loadAllocationOperationForUpdate(
			ctx,
			tx,
			allocation.ID,
			operation.ID,
		)
		if err != nil {
			return err
		}
		if history == nil || history.Generation != operation.Generation {
			return &OperationConflictError{
				Owner:       operation.Owner,
				OperationID: allocation.OperationID,
			}
		}
		if history.State == "committed" &&
			allocation.LastOperationID == operation.ID &&
			allocation.LastOperationGen == operation.Generation &&
			allocation.LastOperationResult == "committed" {
			items, err := loadAllocationItems(ctx, tx, allocation.ID)
			if err != nil {
				return err
			}
			if exactMatchesCommitted(items, exact) {
				return nil
			}
			return &OperationConflictError{
				Owner:       operation.Owner,
				OperationID: operation.ID,
			}
		}
		if history.State == "aborted" {
			return &OperationAbortedError{OperationID: operation.ID}
		}
		return &OperationConflictError{
			Owner:       operation.Owner,
			OperationID: allocation.OperationID,
		}
	}
	if err := requireCurrentOperation(allocation, operation); err != nil {
		return err
	}
	return r.commitExactLoadedAllocation(ctx, tx, operation, allocation, exact)
}

func (r *Repository) commitObservedExactLocked(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	exact Values,
) error {
	allocation, err := loadAllocationForUpdate(ctx, tx, operation.Owner)
	if err != nil {
		return err
	}
	if allocation == nil {
		return &UnavailableError{
			Operation: "commit observed team quota operation",
			Err:       fmt.Errorf("allocation not found"),
		}
	}
	if err := requireAllocationCluster(allocation, operation.Owner); err != nil {
		return err
	}
	if allocation.OperationKind == internalTransferOperationKind {
		return &OperationConflictError{
			Owner:       operation.Owner,
			OperationID: allocation.OperationID,
		}
	}
	if allocation.OperationID == "" {
		history, err := loadAllocationOperationForUpdate(
			ctx,
			tx,
			allocation.ID,
			operation.ID,
		)
		if err != nil {
			return err
		}
		if history == nil || history.Generation != operation.Generation {
			return &OperationConflictError{
				Owner:       operation.Owner,
				OperationID: allocation.OperationID,
			}
		}
		if history.State == "committed" &&
			allocation.LastOperationID == operation.ID &&
			allocation.LastOperationGen == operation.Generation &&
			allocation.LastOperationResult == "committed" {
			items, err := loadAllocationItems(ctx, tx, allocation.ID)
			if err != nil {
				return err
			}
			if exactMatchesCommitted(items, exact) {
				return nil
			}
			return &OperationConflictError{
				Owner:       operation.Owner,
				OperationID: operation.ID,
			}
		}
		if history.State == "aborted" {
			return &OperationAbortedError{OperationID: operation.ID}
		}
		return &OperationConflictError{
			Owner:       operation.Owner,
			OperationID: allocation.OperationID,
		}
	}
	if err := requireCurrentOperation(allocation, operation); err != nil {
		return err
	}
	items, err := loadAllocationItems(ctx, tx, allocation.ID)
	if err != nil {
		return err
	}
	target, err := completeExactTarget(operation.Owner, items, exact)
	if err != nil {
		return err
	}
	return r.commitLoadedAllocationTarget(ctx, tx, operation, allocation, items, target)
}

func (r *Repository) commitLoadedAllocation(ctx context.Context, tx pgx.Tx, operation OperationRef, allocation *allocationRecord) error {
	items, err := loadAllocationItems(ctx, tx, allocation.ID)
	if err != nil {
		return err
	}
	target := make(Values)
	for key, item := range items {
		if item.Pending == nil {
			target[key] = item.Committed
			continue
		}
		target[key] = *item.Pending
	}
	return r.commitLoadedAllocationTarget(ctx, tx, operation, allocation, items, target)
}

func (r *Repository) commitExactLoadedAllocation(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	allocation *allocationRecord,
	exact Values,
) error {
	items, err := loadAllocationItems(ctx, tx, allocation.ID)
	if err != nil {
		return err
	}
	target, err := exactTargetWithinPending(operation.Owner, items, exact)
	if err != nil {
		return err
	}
	return r.commitLoadedAllocationTarget(ctx, tx, operation, allocation, items, target)
}

func (r *Repository) commitLoadedAllocationTarget(
	ctx context.Context,
	tx pgx.Tx,
	operation OperationRef,
	allocation *allocationRecord,
	items map[Key]allocationItem,
	target Values,
) error {
	for key, item := range items {
		targetValue := target[key]
		committedDelta := targetValue - item.Committed
		reservedRelease := int64(0)
		if item.Pending != nil && *item.Pending > item.Committed {
			reservedRelease = *item.Pending - item.Committed
		}
		if committedDelta != 0 || reservedRelease != 0 {
			if err := applyCommittedUsage(
				ctx,
				tx,
				operation.Owner.TeamID,
				key,
				committedDelta,
				reservedRelease,
			); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE quota.allocation_items
			SET committed_value = $3,
				pending_value = NULL
			WHERE allocation_id = $1 AND quota_key = $2
		`, allocation.ID, string(key), targetValue); err != nil {
			return &UnavailableError{
				Operation: "commit team quota allocation item",
				Err:       err,
			}
		}
	}
	if err := completeAllocationOperation(
		ctx,
		tx,
		allocation,
		operation,
		"committed",
		"",
	); err != nil {
		return err
	}
	state := stateForAllocation(operation.Owner, target)
	clearRuntime := state == "paused" || state == "released"
	if _, err := tx.Exec(ctx, `
		UPDATE quota.allocations
		SET state = $2,
			operation_id = NULL,
			operation_kind = '',
			operation_generation = 0,
			operation_base_state = '',
			last_operation_id = $3,
			last_operation_generation = $4,
			last_operation_result = 'committed',
			pod_namespace = CASE WHEN $5 THEN '' ELSE pod_namespace END,
			pod_name = CASE WHEN $5 THEN '' ELSE pod_name END,
			pod_uid = CASE WHEN $5 THEN '' ELSE pod_uid END,
			runtime_generation = CASE WHEN $5 THEN 0 ELSE runtime_generation END,
			reconcile_after = NULL,
			last_error = '',
			updated_at = NOW()
		WHERE allocation_id = $1
	`, allocation.ID, state, operation.ID, operation.Generation, clearRuntime); err != nil {
		return &UnavailableError{Operation: "commit team quota operation", Err: err}
	}
	return finishTeamCapacityMutation(ctx, tx, operation.Owner.TeamID)
}

func exactTargetWithinPending(
	owner Owner,
	items map[Key]allocationItem,
	exact Values,
) (Values, error) {
	target, err := completeExactTarget(owner, items, exact)
	if err != nil {
		return nil, err
	}
	for key, exactValue := range target {
		item, ok := items[key]
		maximum := int64(0)
		if ok {
			maximum = item.Committed
			if item.Pending != nil {
				maximum = *item.Pending
			}
		}
		if exactValue > maximum {
			return nil, fmt.Errorf(
				"exact target for %s/%s key %q is %d, exceeding admitted pending value %d",
				owner.Kind,
				owner.ID,
				key,
				exactValue,
				maximum,
			)
		}
	}
	return target, nil
}

func completeExactTarget(
	owner Owner,
	items map[Key]allocationItem,
	exact Values,
) (Values, error) {
	target := exact.Clone()
	for key := range items {
		if _, ok := target[key]; !ok {
			target[key] = 0
		}
	}
	for key, value := range target {
		if _, ok := items[key]; !ok && value != 0 {
			return nil, fmt.Errorf(
				"exact target for %s/%s contains unprepared key %q",
				owner.Kind,
				owner.ID,
				key,
			)
		}
	}
	return target, nil
}

func exactMatchesCommitted(items map[Key]allocationItem, exact Values) bool {
	for key, item := range items {
		if exact[key] != item.Committed {
			return false
		}
	}
	for key, value := range exact {
		if _, ok := items[key]; !ok && value != 0 {
			return false
		}
	}
	return true
}

func loadAllocationForUpdate(ctx context.Context, tx pgx.Tx, owner Owner) (*allocationRecord, error) {
	record := &allocationRecord{}
	var operationID sql.NullString
	err := tx.QueryRow(ctx, `
		SELECT allocation_id, revision, team_id, owner_kind, owner_id, cluster_id,
			state, operation_id, operation_kind, operation_generation,
			operation_fence_generation, operation_base_state,
			last_operation_id, last_operation_generation, last_operation_result,
			pod_namespace, pod_name, pod_uid, runtime_generation
		FROM quota.allocations
		WHERE team_id = $1 AND owner_kind = $2 AND owner_id = $3
		FOR UPDATE
	`, owner.TeamID, owner.Kind, owner.ID).Scan(
		&record.ID, &record.Revision, &record.Owner.TeamID, &record.Owner.Kind, &record.Owner.ID, &record.Owner.ClusterID,
		&record.State, &operationID, &record.OperationKind, &record.OperationGeneration,
		&record.OperationFence, &record.OperationBaseState,
		&record.LastOperationID, &record.LastOperationGen, &record.LastOperationResult,
		&record.Runtime.Namespace, &record.Runtime.Name, &record.Runtime.UID, &record.Runtime.Generation,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, &UnavailableError{Operation: "load team quota allocation", Err: err}
	}
	if operationID.Valid {
		record.OperationID = operationID.String
	}
	return record, nil
}

func loadAllocationByIDForUpdate(ctx context.Context, tx pgx.Tx, allocationID string) (*allocationRecord, error) {
	record := &allocationRecord{}
	var operationID sql.NullString
	err := tx.QueryRow(ctx, `
		SELECT allocation_id, revision, team_id, owner_kind, owner_id, cluster_id,
			state, operation_id, operation_kind, operation_generation,
			operation_fence_generation, operation_base_state,
			last_operation_id, last_operation_generation, last_operation_result,
			pod_namespace, pod_name, pod_uid, runtime_generation
		FROM quota.allocations
		WHERE allocation_id = $1
		FOR UPDATE
	`, allocationID).Scan(
		&record.ID, &record.Revision, &record.Owner.TeamID, &record.Owner.Kind, &record.Owner.ID, &record.Owner.ClusterID,
		&record.State, &operationID, &record.OperationKind, &record.OperationGeneration,
		&record.OperationFence, &record.OperationBaseState,
		&record.LastOperationID, &record.LastOperationGen, &record.LastOperationResult,
		&record.Runtime.Namespace, &record.Runtime.Name, &record.Runtime.UID, &record.Runtime.Generation,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, &UnavailableError{Operation: "load team quota allocation", Err: err}
	}
	if operationID.Valid {
		record.OperationID = operationID.String
	}
	return record, nil
}

func loadAllocationItems(ctx context.Context, tx pgx.Tx, allocationID string) (map[Key]allocationItem, error) {
	rows, err := tx.Query(ctx, `
		SELECT quota_key, committed_value, pending_value
		FROM quota.allocation_items
		WHERE allocation_id = $1
		ORDER BY quota_key
	`, allocationID)
	if err != nil {
		return nil, &UnavailableError{Operation: "load team quota allocation items", Err: err}
	}
	defer rows.Close()
	items := make(map[Key]allocationItem)
	for rows.Next() {
		var key Key
		var committed int64
		var pending sql.NullInt64
		if err := rows.Scan(&key, &committed, &pending); err != nil {
			return nil, &UnavailableError{Operation: "scan team quota allocation item", Err: err}
		}
		item := allocationItem{Committed: committed}
		if pending.Valid {
			value := pending.Int64
			item.Pending = &value
		}
		items[key] = item
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "load team quota allocation items", Err: err}
	}
	return items, nil
}

func committedValues(items map[Key]allocationItem) Values {
	values := make(Values, len(items))
	for key, item := range items {
		values[key] = item.Committed
	}
	return values
}

func reservationFrom(allocation *allocationRecord, items map[Key]allocationItem) *Reservation {
	committed := committedValues(items)
	target := make(Values, len(items))
	reserved := make(Values)
	for key, item := range items {
		target[key] = item.Committed
		if item.Pending == nil {
			continue
		}
		target[key] = *item.Pending
		if delta := *item.Pending - item.Committed; delta > 0 {
			reserved[key] = delta
		}
	}
	return &Reservation{
		AllocationID: allocation.ID,
		Owner:        allocation.Owner,
		Operation: Operation{
			ID:         allocation.OperationID,
			Kind:       allocation.OperationKind,
			Generation: allocation.OperationGeneration,
		},
		State:     allocation.State,
		Committed: committed,
		Target:    target,
		Reserved:  reserved,
	}
}

func ensureUsageRow(ctx context.Context, tx pgx.Tx, teamID string, key Key) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO quota.team_usage (team_id, quota_key)
		VALUES ($1, $2)
		ON CONFLICT (team_id, quota_key) DO NOTHING
	`, teamID, string(key)); err != nil {
		return &UnavailableError{Operation: "initialize team quota usage", Err: err}
	}
	return nil
}

func loadUsageForUpdate(ctx context.Context, tx pgx.Tx, teamID string, key Key) (usageValues, error) {
	var usage usageValues
	if err := tx.QueryRow(ctx, `
		SELECT committed_value, reserved_value
		FROM quota.team_usage
		WHERE team_id = $1 AND quota_key = $2
		FOR UPDATE
	`, teamID, string(key)).Scan(&usage.committed, &usage.reserved); err != nil {
		return usage, &UnavailableError{Operation: "load team quota usage", Err: err}
	}
	return usage, nil
}

func addReservedUsage(ctx context.Context, tx pgx.Tx, teamID string, key Key, amount int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE quota.team_usage
		SET reserved_value = reserved_value + $3,
			updated_at = NOW()
		WHERE team_id = $1 AND quota_key = $2
			AND reserved_value <= $4
	`, teamID, string(key), amount, math.MaxInt64-amount)
	if err != nil {
		return &UnavailableError{Operation: "reserve team quota usage", Err: err}
	}
	if tag.RowsAffected() != 1 {
		return &UnavailableError{
			Operation: "reserve team quota usage",
			Err:       fmt.Errorf("usage invariant violated for %s", key),
		}
	}
	return nil
}

func subtractReservedUsage(ctx context.Context, tx pgx.Tx, teamID string, key Key, amount int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE quota.team_usage
		SET reserved_value = reserved_value - $3,
			updated_at = NOW()
		WHERE team_id = $1 AND quota_key = $2
			AND reserved_value >= $3
	`, teamID, string(key), amount)
	if err != nil {
		return &UnavailableError{Operation: "release reserved team quota usage", Err: err}
	}
	if tag.RowsAffected() != 1 {
		return &UnavailableError{Operation: "release reserved team quota usage", Err: fmt.Errorf("usage invariant violated for %s", key)}
	}
	return nil
}

func applyCommittedUsage(ctx context.Context, tx pgx.Tx, teamID string, key Key, committedDelta, reservedRelease int64) error {
	if err := ensureUsageRow(ctx, tx, teamID, key); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE quota.team_usage
		SET committed_value = committed_value + $3,
			reserved_value = reserved_value - $4,
			updated_at = NOW()
		WHERE team_id = $1 AND quota_key = $2
			AND committed_value + $3 >= 0
			AND reserved_value >= $4
	`, teamID, string(key), committedDelta, reservedRelease)
	if err != nil {
		return &UnavailableError{Operation: "commit team quota usage", Err: err}
	}
	if tag.RowsAffected() != 1 {
		return &UnavailableError{Operation: "commit team quota usage", Err: fmt.Errorf("usage invariant violated for %s", key)}
	}
	return nil
}

func bumpTeamRevision(ctx context.Context, tx pgx.Tx, teamID string) error {
	_, err := advanceTeamRevision(ctx, tx, teamID)
	return err
}

func advanceTeamRevision(ctx context.Context, tx pgx.Tx, teamID string) (int64, error) {
	var revision int64
	if err := tx.QueryRow(ctx, `
		UPDATE quota.team_states
		SET revision = revision + 1,
			updated_at = NOW()
		WHERE team_id = $1
		RETURNING revision
	`, teamID).Scan(&revision); err != nil {
		return 0, &UnavailableError{Operation: "advance team quota revision", Err: err}
	}
	return revision, nil
}

func finishTeamCapacityMutation(ctx context.Context, tx pgx.Tx, teamID string) error {
	revision, err := advanceTeamRevision(ctx, tx, teamID)
	if err != nil {
		return err
	}
	// Terminal rows grow more slowly than the team revision because preparing
	// the same operation also advances it. Periodic pruning keeps latency off
	// the hot path while bounding overshoot to less than one revision gap.
	if revision%capacityHistoryPruneRevisionGap != 0 {
		return nil
	}
	return pruneTeamCapacityHistory(ctx, tx, teamID)
}

func requireCurrentOperation(allocation *allocationRecord, operation OperationRef) error {
	if allocation.OperationID != operation.ID || allocation.OperationGeneration != operation.Generation {
		return &OperationConflictError{Owner: operation.Owner, OperationID: allocation.OperationID}
	}
	return nil
}

func runtimeMatches(expected, actual RuntimeRef) error {
	if expected.Namespace != "" && expected.Namespace != actual.Namespace {
		return fmt.Errorf("runtime namespace %q does not match allocation namespace %q", actual.Namespace, expected.Namespace)
	}
	if expected.Name != "" && expected.Name != actual.Name {
		return fmt.Errorf("runtime name %q does not match allocation name %q", actual.Name, expected.Name)
	}
	if expected.UID != "" && expected.UID != actual.UID {
		return fmt.Errorf("runtime uid %q does not match allocation uid %q", actual.UID, expected.UID)
	}
	if expected.Generation != 0 && expected.Generation != actual.Generation {
		return fmt.Errorf("runtime generation %d does not match allocation generation %d", actual.Generation, expected.Generation)
	}
	return nil
}

func stateForAllocation(owner Owner, values Values) string {
	hasValue := false
	for _, value := range values {
		if value > 0 {
			hasValue = true
			break
		}
	}
	if !hasValue {
		return "released"
	}
	if owner.Kind == "sandbox" && values[KeySandboxRuntimeCount] == 0 && values[KeySandboxIdentityCount] > 0 {
		return "paused"
	}
	return "active"
}

func addInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func postgresInterval(duration time.Duration) string {
	return fmt.Sprintf("%d milliseconds", duration.Milliseconds())
}

// ValidateUsageInvariant verifies that materialized team usage exactly matches
// the allocation ledger. It is intended for reconciliation and tests.
func (r *Repository) ValidateUsageInvariant(ctx context.Context, teamID string) error {
	return r.inTx(ctx, "validate team quota usage", func(tx pgx.Tx) error {
		teamID = strings.TrimSpace(teamID)
		if teamID == "" {
			return fmt.Errorf("team_id is required")
		}
		rows, err := tx.Query(ctx, `
			WITH allocation_usage AS (
				SELECT a.team_id, i.quota_key,
					COALESCE(SUM(i.committed_value), 0)::BIGINT AS committed_value,
					COALESCE(SUM(
						CASE
							WHEN i.pending_value > i.committed_value
							THEN i.pending_value - i.committed_value
							ELSE 0
						END
					), 0)::BIGINT AS reserved_value
				FROM quota.allocations a
				JOIN quota.allocation_items i ON i.allocation_id = a.allocation_id
				WHERE a.team_id = $1
				GROUP BY a.team_id, i.quota_key
			),
			transfer_usage AS (
				SELECT i.quota_key,
					COALESCE(SUM(i.reserved_value), 0)::BIGINT AS reserved_value
				FROM quota.transfer_operations o
				JOIN quota.transfer_items i
					ON i.team_id = o.team_id AND i.operation_id = o.operation_id
				WHERE o.team_id = $1 AND o.state = 'prepared'
				GROUP BY i.quota_key
			),
			ledger_usage AS (
				SELECT quota_key,
					SUM(committed_value)::BIGINT AS committed_value,
					SUM(reserved_value)::BIGINT AS reserved_value
				FROM (
					SELECT quota_key, committed_value, reserved_value
					FROM allocation_usage
					UNION ALL
					SELECT quota_key, 0::BIGINT, reserved_value
					FROM transfer_usage
				) combined
				GROUP BY quota_key
			),
			all_keys AS (
				SELECT quota_key FROM quota.team_usage WHERE team_id = $1
				UNION
				SELECT quota_key FROM ledger_usage
			)
			SELECT k.quota_key,
				COALESCE(u.committed_value, 0), COALESCE(l.committed_value, 0),
				COALESCE(u.reserved_value, 0), COALESCE(l.reserved_value, 0)
			FROM all_keys k
			LEFT JOIN quota.team_usage u
				ON u.team_id = $1 AND u.quota_key = k.quota_key
			LEFT JOIN ledger_usage l ON l.quota_key = k.quota_key
		`, teamID)
		if err != nil {
			return &UnavailableError{Operation: "validate team quota usage", Err: err}
		}
		defer rows.Close()
		for rows.Next() {
			var key Key
			var usageCommitted, itemCommitted, usageReserved, itemReserved int64
			if err := rows.Scan(&key, &usageCommitted, &itemCommitted, &usageReserved, &itemReserved); err != nil {
				return &UnavailableError{Operation: "validate team quota usage", Err: err}
			}
			if usageCommitted != itemCommitted || usageReserved != itemReserved {
				return fmt.Errorf(
					"team quota usage invariant violated for %s: usage=(%d,%d) allocations=(%d,%d)",
					key, usageCommitted, usageReserved, itemCommitted, itemReserved,
				)
			}
		}
		return rows.Err()
	})
}
