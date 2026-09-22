package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// NomadMigrationCPUPreflightTTL matches the database execution-authority guard.
// The database starts this window before source dispatch, never at receipt time.
const NomadMigrationCPUPreflightTTL = 2 * time.Minute

type nomadMigrationCPUPreflight struct {
	request     protocol.MigrationCPUPreflightRequest
	source      *protocol.MigrationCPUPreflight
	destination *protocol.MigrationCPUPreflight
	fresh       bool
}

// AuthorizeNomadSandboxMigrationCPUPreflight retains the exact read-only source
// probe. It does not change lifecycle phase or authorize guest execution. An
// expired probe cannot be refreshed within the same operation; an untouched
// reservation may be abandoned through the existing reservation recovery path.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationCPUPreflight(ctx context.Context, assignment runtimecontrol.MigrationAssignment) (*protocol.MigrationCPUPreflightRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, assignment)
	if err != nil {
		return nil, err
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePreparing {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationSourceAndTarget(ctx, tx, reservation, assignment); err != nil {
		return nil, err
	}
	var unexpired bool
	if err := tx.QueryRow(ctx, `SELECT created_at + ($2 * INTERVAL '1 second') > clock_timestamp()
		FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, assignment.OperationID,
		int64(NomadMigrationReservationTTL.Seconds())).Scan(&unexpired); err != nil {
		return nil, err
	}
	if !unexpired {
		return nil, ErrNomadSandboxMigrationConflict
	}
	request := nomadMigrationCPURequest(reservation)
	if err := request.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations
		SET cpu_preflight_request=$2,cpu_preflight_requested_at=clock_timestamp()
		WHERE operation_id=$1 AND cpu_preflight_request IS NULL`, assignment.OperationID, payload); err != nil {
		return nil, err
	}
	stored, err := loadNomadMigrationCPUPreflight(ctx, tx, assignment.OperationID)
	if err != nil {
		return nil, err
	}
	if err := stored.validateCurrent(reservation); err != nil || !stored.fresh {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &stored.request, nil
}

// AuthorizeNomadSandboxMigrationTargetCPUPreflight derives the destination
// request only from the immutable source request and authenticated receipt.
// No caller can choose a different CPU history, destination or resource lease.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationTargetCPUPreflight(ctx context.Context, assignment runtimecontrol.MigrationAssignment) (*protocol.MigrationCPUPreflightRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, assignment)
	if err != nil {
		return nil, err
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePreparing {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationSourceAndTarget(ctx, tx, reservation, assignment); err != nil {
		return nil, err
	}
	stored, err := loadNomadMigrationCPUPreflight(ctx, tx, assignment.OperationID)
	if err != nil {
		return nil, err
	}
	if stored.validateCurrent(reservation) != nil || !stored.fresh || stored.source == nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	request := stored.targetRequest()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// CommitNomadSandboxMigrationCPUPreflight is called only with a response from
// the authenticated exact-boot node channel. Content validation cannot itself
// authenticate a host. First receipt wins; retries must preserve every field.
func (s *PGSandboxStore) CommitNomadSandboxMigrationCPUPreflight(ctx context.Context, assignment runtimecontrol.MigrationAssignment, request protocol.MigrationCPUPreflightRequest, result protocol.MigrationCPUPreflight) error {
	if err := result.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, assignment)
	if err != nil {
		return err
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePreparing {
		return ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationSourceAndTarget(ctx, tx, reservation, assignment); err != nil {
		return err
	}
	stored, err := loadNomadMigrationCPUPreflight(ctx, tx, assignment.OperationID)
	if err != nil {
		return err
	}
	if stored.validateCurrent(reservation) != nil || !stored.fresh || result.Launch.LaunchAttempt != reservation.SourceSlot.LaunchAttempt {
		return ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationCPULineage(ctx, tx, reservation.SourceSlot, result.Launch); err != nil {
		return err
	}
	expected, prior := stored.request, stored.source
	if !request.IsSource() {
		if stored.source == nil {
			return ErrNomadSandboxMigrationConflict
		}
		expected, prior = stored.targetRequest(), stored.destination
	}
	want, _ := expected.Digest()
	if result.RequestDigest != want {
		return ErrNomadSandboxMigrationConflict
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if prior != nil {
		previous, _ := json.Marshal(prior)
		if !bytes.Equal(previous, payload) {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	query := `UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_source=$2 WHERE operation_id=$1`
	if !request.IsSource() {
		query = `UPDATE manager.sandbox_runtime_migrations SET cpu_preflight_destination=$2 WHERE operation_id=$1`
	}
	if _, err := tx.Exec(ctx, query, assignment.OperationID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// The current carrier's original claim decides whether history must come from
// restore. A node cannot strip that link and substitute its host feature set.
// Prior commands are immutable, and only a fully committed migration can be
// followed by another lifecycle transaction for the same sandbox.
func validateNomadMigrationCPULineage(ctx context.Context, tx pgx.Tx, source *RuntimeSlot, launch protocol.MigrationCPULaunch) error {
	var payload []byte
	var operation, phase string
	err := tx.QueryRow(ctx, `SELECT m.restore_request,m.operation_id,l.phase
		FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
		WHERE m.target_slot_id=$1`, source.ID).Scan(&payload, &operation, &phase)
	if err == pgx.ErrNoRows {
		if launch.Restored != nil {
			return ErrNomadSandboxMigrationConflict
		}
		return nil
	}
	if err != nil {
		return err
	}
	var restored protocol.MigrationRestoreRequest
	if operation != source.ClaimOperationID || phase != string(SandboxLifecyclePhaseCommitted) ||
		json.Unmarshal(payload, &restored) != nil || launch.ValidateRestore(restored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}

func nomadMigrationCaptureIdentity(r *NomadSandboxMigrationReservation) protocol.MigrationCaptureRequest {
	s := r.SourceSlot
	return protocol.MigrationCaptureRequest{Target: nomadMigrationSlotTarget(s), OperationID: r.Lifecycle.ID,
		LifecycleEpoch: r.Lifecycle.Epoch, SandboxID: r.Lifecycle.SandboxID, SourceGeneration: r.Lifecycle.FromGeneration,
		AssignmentRevision: s.ClaimRuntimeAssignmentRevision, BindingDigest: hex.EncodeToString(r.SourceBindingDigest),
		ResourceLeaseDigest: hex.EncodeToString(s.ResourceLeaseDigest), ProcdInstanceID: r.SourceProcdInstanceID}
}

func nomadMigrationSlotTarget(s *RuntimeSlot) protocol.NodeChannelTarget {
	return protocol.NodeChannelTarget{SlotID: s.ID, ClusterID: s.ClusterID, AllocationID: s.AllocationID,
		NodeID: s.NodeID, NodeUID: s.NodeUID, NodeBootID: s.NodeBootID, ControlEndpoint: s.ControlEndpoint}
}

func nomadMigrationCPURequest(r *NomadSandboxMigrationReservation) protocol.MigrationCPUPreflightRequest {
	source := nomadMigrationCaptureIdentity(r)
	return protocol.MigrationCPUPreflightRequest{Target: source.Target, Source: source, SourceResources: r.SourceSlot.ResourceLease,
		Destination: nomadMigrationSlotTarget(r.TargetSlot), DestinationResources: r.TargetResourceLease}
}

func (p *nomadMigrationCPUPreflight) targetRequest() protocol.MigrationCPUPreflightRequest {
	request := p.request
	request.Target, request.Launch = request.Destination, &p.source.Launch
	return request
}

func (p *nomadMigrationCPUPreflight) validateCurrent(r *NomadSandboxMigrationReservation) error {
	expected, err := nomadMigrationCPURequest(r).Digest()
	actual, actualErr := p.request.Digest()
	if err != nil || actualErr != nil || expected != actual ||
		(p.source != nil && p.source.Launch.LaunchAttempt != r.SourceSlot.LaunchAttempt) {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}

func loadNomadMigrationCPUPreflight(ctx context.Context, tx pgx.Tx, operationID string) (*nomadMigrationCPUPreflight, error) {
	var request, source, destination []byte
	p := &nomadMigrationCPUPreflight{}
	if err := tx.QueryRow(ctx, `SELECT cpu_preflight_request,cpu_preflight_source,cpu_preflight_destination,
		COALESCE(cpu_preflight_requested_at <= clock_timestamp() AND
			cpu_preflight_requested_at + ($2 * INTERVAL '1 second') > clock_timestamp(),false)
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, operationID,
		int64(NomadMigrationCPUPreflightTTL.Seconds())).Scan(&request, &source, &destination, &p.fresh); err != nil {
		return nil, err
	}
	if json.Unmarshal(request, &p.request) != nil || !p.request.IsSource() || p.request.Validate() != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(source) != 0 {
		p.source = &protocol.MigrationCPUPreflight{}
		if json.Unmarshal(source, p.source) != nil || p.source.ValidateFor(p.request) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	if len(destination) != 0 {
		p.destination = &protocol.MigrationCPUPreflight{}
		if p.source == nil || json.Unmarshal(destination, p.destination) != nil || p.destination.ValidateFor(p.targetRequest()) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	return p, nil
}

// First execution authorization requires live matching placements and both
// receipts. Recovery of an already committed command does not call this gate.
func requireNomadMigrationCPUPreflight(ctx context.Context, tx pgx.Tx, r *NomadSandboxMigrationReservation) error {
	p, err := loadNomadMigrationCPUPreflight(ctx, tx, r.Lifecycle.ID)
	if err != nil {
		return err
	}
	if !p.fresh || p.source == nil || p.destination == nil || p.validateCurrent(r) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}

// Publication retains the CPU profile that authorized capture. A frozen source
// cannot be re-probed, and a slow upload must not erase its recovery evidence.
func nomadMigrationCapturedCPULaunch(ctx context.Context, tx pgx.Tx, capture protocol.MigrationCaptureRequest) (*protocol.MigrationCPULaunch, error) {
	p, err := loadNomadMigrationCPUPreflight(ctx, tx, capture.OperationID)
	if err != nil {
		return nil, err
	}
	if p.source == nil || p.destination == nil || p.request.Source != capture {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &p.source.Launch, nil
}
