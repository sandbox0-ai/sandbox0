package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type nomadMigrationCapturePeer struct {
	request             protocol.MigrationCapturePeerRequest
	source, destination *protocol.MigrationCapturePeerPrepared
	disabled            bool
}

func (p *nomadMigrationCapturePeer) targetRequest() protocol.MigrationCapturePeerRequest {
	r := p.request
	r.Staging.Target = r.Staging.Destination
	return r
}

func (p *nomadMigrationCapturePeer) next() *protocol.MigrationCapturePeerRequest {
	if p == nil || p.disabled {
		return nil
	}
	// The receiver must retain its permission before the source can be told
	// where to send workload memory. This is separate from pool lock order.
	if p.destination == nil {
		r := p.targetRequest()
		return &r
	}
	if p.source == nil {
		r := p.request
		return &r
	}
	return nil
}

// Called under the existing operation lock in the transaction committing the
// final staging receipt. No source-preparation reader can see the receipts
// without also seeing this optional pending decision. Older endpoints and
// exhausted regional-upload admission retain the existing transfer path.
func initializeNomadMigrationCapturePeer(ctx context.Context, tx pgx.Tx, id string, staging *nomadMigrationStaging) error {
	if staging.source == nil || staging.destination == nil || staging.request.CaptureUpload.Version == 0 ||
		staging.source.Peer == (runtimecheckpoint.PeerEndpoint{}) || staging.destination.Peer == (runtimecheckpoint.PeerEndpoint{}) {
		return nil
	}
	p := &nomadMigrationCapturePeer{request: protocol.MigrationCapturePeerRequest{Staging: staging.request, Source: *staging.source, Destination: *staging.destination}}
	if err := p.request.Validate(); err != nil {
		// Individually valid node receipts may still advertise the same peer
		// endpoint or certificate. Such a pair cannot use this optimization.
		return nil
	}
	source, _ := p.request.Digest()
	destination, _ := p.targetRequest().Digest()
	payload, err := json.Marshal(p.request)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_peer_request=$2,
        capture_peer_source_digest=$3,capture_peer_destination_digest=$4 WHERE operation_id=$1 AND capture_peer_request IS NULL`, id, payload, source, destination)
	return err
}

func loadNomadMigrationCapturePeer(ctx context.Context, tx pgx.Tx, id string, staging *nomadMigrationStaging) (*nomadMigrationCapturePeer, error) {
	var payload, source, destination []byte
	var sourceDigest, destinationDigest *string
	p := &nomadMigrationCapturePeer{}
	if err := tx.QueryRow(ctx, `SELECT capture_peer_request,capture_peer_source_digest,capture_peer_destination_digest,
        capture_peer_source_receipt,capture_peer_destination_receipt,capture_peer_disabled
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id).
		Scan(&payload, &sourceDigest, &destinationDigest, &source, &destination, &p.disabled); err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		if sourceDigest != nil || destinationDigest != nil || len(source) != 0 || len(destination) != 0 || p.disabled {
			return nil, ErrNomadSandboxMigrationConflict
		}
		return nil, nil
	}
	if staging == nil || staging.source == nil || staging.destination == nil || json.Unmarshal(payload, &p.request) != nil ||
		p.request.Staging != staging.request || p.request.Source != *staging.source || p.request.Destination != *staging.destination {
		return nil, ErrNomadSandboxMigrationConflict
	}
	wantSource, err := p.request.Digest()
	wantDestination, destErr := p.targetRequest().Digest()
	if err != nil || destErr != nil || sourceDigest == nil || *sourceDigest != wantSource || destinationDigest == nil || *destinationDigest != wantDestination {
		return nil, ErrNomadSandboxMigrationConflict
	}
	for _, side := range []struct {
		payload []byte
		result  **protocol.MigrationCapturePeerPrepared
		request protocol.MigrationCapturePeerRequest
	}{{source, &p.source, p.request}, {destination, &p.destination, p.targetRequest()}} {
		if len(side.payload) != 0 {
			*side.result = &protocol.MigrationCapturePeerPrepared{}
			if json.Unmarshal(side.payload, *side.result) != nil || (*side.result).ValidateFor(side.request) != nil {
				return nil, ErrNomadSandboxMigrationConflict
			}
		}
	}
	if p.source != nil && p.destination == nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return p, nil
}

func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationCapturePeer(ctx context.Context, a runtimecontrol.MigrationAssignment) (*protocol.MigrationCapturePeerRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	_, staging, err := lockNomadMigrationCapturePeer(ctx, tx, a)
	if err != nil {
		return nil, err
	}
	p, err := loadNomadMigrationCapturePeer(ctx, tx, a.OperationID, staging)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return p.next(), nil
}

func lockNomadMigrationCapturePeer(ctx context.Context, tx pgx.Tx, a runtimecontrol.MigrationAssignment) (*NomadSandboxMigrationReservation, *nomadMigrationStaging, error) {
	r, err := lockNomadMigrationAuthority(ctx, tx, a)
	if err != nil {
		return nil, nil, err
	}
	if err := validateMigrationStagingAdmission(ctx, tx, r, a); err != nil {
		return nil, nil, err
	}
	staging, err := loadNomadMigrationStaging(ctx, tx, a.OperationID)
	if err != nil {
		return nil, nil, err
	}
	if staging == nil || staging.validateCurrent(r) != nil || staging.source == nil || staging.destination == nil || staging.sourceRelease || staging.destinationRelease {
		return nil, nil, ErrNomadSandboxMigrationConflict
	}
	var fresh bool
	if err := tx.QueryRow(ctx, `SELECT preparation_request IS NULL AND capture_request IS NULL FROM manager.sandbox_runtime_migrations
        WHERE operation_id=$1`, a.OperationID).Scan(&fresh); err != nil {
		return nil, nil, err
	}
	if !fresh {
		return nil, nil, ErrNomadSandboxMigrationConflict
	}
	return r, staging, nil
}

// A nil receipt records fallback after an unsuccessful optional node call.
// It does not assert that the call had no effect: staging release and normal
// image preparation still cancel/join and remove any retained node cache.
func (s *PGSandboxStore) CommitNomadSandboxMigrationCapturePeer(ctx context.Context, a runtimecontrol.MigrationAssignment,
	request protocol.MigrationCapturePeerRequest, receipt *protocol.MigrationCapturePeerPrepared) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if receipt != nil {
		if err := receipt.ValidateFor(request); err != nil {
			return err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, staging, err := lockNomadMigrationCapturePeer(ctx, tx, a)
	if err != nil {
		return err
	}
	p, err := loadNomadMigrationCapturePeer(ctx, tx, a.OperationID, staging)
	if err != nil {
		return err
	}
	if p == nil {
		return ErrNomadSandboxMigrationConflict
	}
	expected, prior := p.request, p.source
	query := `UPDATE manager.sandbox_runtime_migrations SET capture_peer_source_receipt=$2 WHERE operation_id=$1`
	if !request.Staging.IsSource() {
		expected, prior = p.targetRequest(), p.destination
		query = `UPDATE manager.sandbox_runtime_migrations SET capture_peer_destination_receipt=$2 WHERE operation_id=$1`
	}
	if request != expected {
		return ErrNomadSandboxMigrationConflict
	}
	if prior != nil || p.disabled {
		// A stale failed observation cannot disable an acknowledged grant, and
		// a late success cannot reverse an already committed fallback decision.
		return tx.Commit(ctx)
	}
	if next := p.next(); next == nil || *next != request {
		return ErrNomadSandboxMigrationConflict
	}
	if receipt == nil {
		_, err = tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_peer_disabled=TRUE WHERE operation_id=$1`, a.OperationID)
	} else {
		var payload []byte
		payload, err = json.Marshal(receipt)
		if err == nil {
			_, err = tx.Exec(ctx, query, a.OperationID, payload)
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
