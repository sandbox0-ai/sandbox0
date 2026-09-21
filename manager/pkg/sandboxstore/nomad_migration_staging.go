package sandboxstore

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type nomadMigrationStaging struct {
	request                             protocol.MigrationStagingRequest
	source, destination                 *protocol.MigrationStagingReserved
	sourceRelease, destinationRelease   bool
	sourceReleased, destinationReleased *protocol.MigrationStagingReleased
}

func (p *nomadMigrationStaging) targetRequest() protocol.MigrationStagingRequest {
	r := p.request
	r.Target = r.Destination
	return r
}

// Every pair acquires the lower durable node UID first, regardless of direction.
// This prevents simultaneous A->B and B->A evacuations from holding opposite pools.
func (p *nomadMigrationStaging) next() *protocol.MigrationStagingRequest {
	if p.sourceRelease || p.destinationRelease {
		return nil
	}
	sourceFirst := p.request.Source.Target.NodeUID < p.request.Destination.NodeUID
	if p.source == nil && (sourceFirst || p.destination != nil) {
		r := p.request
		return &r
	}
	if p.destination == nil && (!sourceFirst || p.source != nil) {
		r := p.targetRequest()
		return &r
	}
	return nil
}

func migrationStagingRequest(r *NomadSandboxMigrationReservation, bytes int64, inodes uint64) protocol.MigrationStagingRequest {
	source := nomadMigrationCaptureIdentity(r)
	digest, _ := r.TargetResourceLease.Digest()
	return protocol.MigrationStagingRequest{Target: source.Target, Source: source, Destination: nomadMigrationSlotTarget(r.TargetSlot),
		DestinationResourceLeaseDigest: strings.TrimPrefix(digest, "sha256:"), Bytes: bytes, Inodes: inodes}
}

func (p *nomadMigrationStaging) validateCurrent(r *NomadSandboxMigrationReservation) error {
	if p.request != migrationStagingRequest(r, p.request.Bytes, p.request.Inodes) {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}

func loadNomadMigrationStaging(ctx context.Context, tx pgx.Tx, id string) (*nomadMigrationStaging, error) {
	var request, source, destination, sourceReleased, destinationReleased []byte
	p := &nomadMigrationStaging{}
	if err := tx.QueryRow(ctx, `SELECT staging_request,staging_source_receipt,staging_destination_receipt,
        staging_source_release_requested,staging_destination_release_requested,staging_source_release_receipt,staging_destination_release_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id).Scan(&request, &source, &destination, &p.sourceRelease,
		&p.destinationRelease, &sourceReleased, &destinationReleased); err != nil {
		return nil, err
	}
	if len(request) == 0 {
		return nil, nil
	}
	if json.Unmarshal(request, &p.request) != nil || p.request.Validate() != nil || !p.request.IsSource() || p.request.Source.OperationID != id {
		return nil, ErrNomadSandboxMigrationConflict
	}
	for _, side := range []struct {
		payload []byte
		result  **protocol.MigrationStagingReserved
		request protocol.MigrationStagingRequest
	}{
		{source, &p.source, p.request}, {destination, &p.destination, p.targetRequest()},
	} {
		if len(side.payload) != 0 {
			*side.result = &protocol.MigrationStagingReserved{}
			if json.Unmarshal(side.payload, *side.result) != nil || (*side.result).ValidateFor(side.request) != nil {
				return nil, ErrNomadSandboxMigrationConflict
			}
		}
	}
	for _, side := range []struct {
		payload    []byte
		result     **protocol.MigrationStagingReleased
		request    protocol.MigrationStagingRequest
		authorized bool
	}{
		{sourceReleased, &p.sourceReleased, p.request, p.sourceRelease}, {destinationReleased, &p.destinationReleased, p.targetRequest(), p.destinationRelease},
	} {
		if len(side.payload) != 0 {
			*side.result = &protocol.MigrationStagingReleased{}
			if !side.authorized || json.Unmarshal(side.payload, *side.result) != nil || (*side.result).ValidateFor(side.request) != nil {
				return nil, ErrNomadSandboxMigrationConflict
			}
		}
	}
	if p.request.Source.Target.NodeUID < p.request.Destination.NodeUID {
		if p.destination != nil && p.source == nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else if p.source != nil && p.destination == nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return p, nil
}

// AuthorizeNomadSandboxMigrationStaging persists both command identities before
// either dispatch. Retries acquire one side at a time without changing budgets,
// placement or the original reservation/CPU deadline.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationStaging(ctx context.Context, a runtimecontrol.MigrationAssignment) (*protocol.MigrationStagingRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	r, err := lockNomadMigrationAuthority(ctx, tx, a)
	if err != nil {
		return nil, err
	}
	if err := validateMigrationStagingAdmission(ctx, tx, r, a); err != nil {
		return nil, err
	}
	p, err := loadNomadMigrationStaging(ctx, tx, a.OperationID)
	if err != nil {
		return nil, err
	}
	if p == nil {
		// This is a system admission floor, not an image-size guarantee. The
		// hard memory lease plus 64 MiB of serialization headroom is rounded
		// to filesystem blocks; XFS still bounds actual producer writes.
		bytes := (r.SourceSlot.ResourceLease.MemoryBytes + 64<<20 + 4095) / 4096 * 4096
		request := migrationStagingRequest(r, bytes, runtimecheckpoint.MaxFiles*8)
		if err := request.Validate(); err != nil {
			return nil, err
		}
		payload, err := json.Marshal(request)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET staging_request=$2 WHERE operation_id=$1`, a.OperationID, payload); err != nil {
			return nil, err
		}
		p = &nomadMigrationStaging{request: request}
	}
	if p.validateCurrent(r) != nil || p.sourceRelease || p.destinationRelease {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return p.next(), nil
}

func validateMigrationStagingAdmission(ctx context.Context, tx pgx.Tx, r *NomadSandboxMigrationReservation, a runtimecontrol.MigrationAssignment) error {
	if r.Lifecycle.Phase != SandboxLifecyclePhasePreparing {
		return ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationSourceAndTarget(ctx, tx, r, a); err != nil {
		return err
	}
	if err := requireNomadMigrationCPUPreflight(ctx, tx, r); err != nil {
		return err
	}
	var fresh bool
	if err := tx.QueryRow(ctx, `SELECT created_at <= clock_timestamp() AND created_at + INTERVAL '2 minutes' > clock_timestamp()
        FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, a.OperationID).Scan(&fresh); err != nil {
		return err
	}
	if !fresh {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}

func (s *PGSandboxStore) CommitNomadSandboxMigrationStaging(ctx context.Context, a runtimecontrol.MigrationAssignment, request protocol.MigrationStagingRequest, receipt protocol.MigrationStagingReserved) error {
	if err := receipt.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	r, err := lockNomadMigrationAuthority(ctx, tx, a)
	if err != nil {
		return err
	}
	if err := validateMigrationStagingAdmission(ctx, tx, r, a); err != nil {
		return err
	}
	p, err := loadNomadMigrationStaging(ctx, tx, a.OperationID)
	if err != nil {
		return err
	}
	if p == nil || p.validateCurrent(r) != nil || p.sourceRelease || p.destinationRelease {
		return ErrNomadSandboxMigrationConflict
	}
	expected, prior := p.request, p.source
	query := `UPDATE manager.sandbox_runtime_migrations SET staging_source_receipt=$2 WHERE operation_id=$1`
	if !request.IsSource() {
		expected, prior = p.targetRequest(), p.destination
		query = `UPDATE manager.sandbox_runtime_migrations SET staging_destination_receipt=$2 WHERE operation_id=$1`
	}
	if request != expected {
		return ErrNomadSandboxMigrationConflict
	}
	if prior != nil {
		if *prior != receipt {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	if next := p.next(); next == nil || *next != request {
		return ErrNomadSandboxMigrationConflict
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, query, a.OperationID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func requireNomadMigrationStaging(ctx context.Context, tx pgx.Tx, r *NomadSandboxMigrationReservation) error {
	p, err := loadNomadMigrationStaging(ctx, tx, r.Lifecycle.ID)
	if err != nil {
		return err
	}
	if p == nil || p.source == nil || p.destination == nil || p.sourceRelease || p.destinationRelease || p.validateCurrent(r) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}
