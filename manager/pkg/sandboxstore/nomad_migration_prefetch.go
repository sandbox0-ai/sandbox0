package sandboxstore

import (
	"context"
	"encoding/json"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadSandboxMigrationImagePrefetch commits permission to fill only
// the already-reserved target cache. Publication, image preparation, writer
// handoff and command readiness remain separate existing transactions.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationImagePrefetch(ctx context.Context, publication protocol.MigrationPublicationRequest, plan protocol.MigrationPublicationPlan) (*protocol.MigrationImagePrefetchRequest, error) {
	if err := plan.ValidateFor(publication); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, publication.Assignment)
	if err != nil {
		return nil, err
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var storedPublication, published, payload []byte
	var publicationDigest, storedDigest *string
	var preparationStarted bool
	if err := tx.QueryRow(ctx, `SELECT publication_request,publication_digest,publication_receipt,
        target_image_prefetch_request,target_image_prefetch_digest,
        target_image_request IS NOT NULL OR source_fence_request IS NOT NULL
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, publication.Assignment.OperationID).
		Scan(&storedPublication, &publicationDigest, &published, &payload, &storedDigest, &preparationStarted); err != nil {
		return nil, err
	}
	wantPublication, _ := publication.Digest()
	var command protocol.MigrationPublicationRequest
	if publicationDigest == nil || *publicationDigest != wantPublication || json.Unmarshal(storedPublication, &command) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	actual, err := command.Digest()
	if err != nil || actual != wantPublication {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(published) != 0 {
		var receipt protocol.MigrationPublication
		if json.Unmarshal(published, &receipt) != nil || receipt.ValidateFor(command) != nil ||
			receipt.Reference != plan.Reference || receipt.Peer != plan.Peer {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	staging, err := loadNomadMigrationStaging(ctx, tx, publication.Assignment.OperationID)
	if err != nil {
		return nil, err
	}
	if staging == nil || staging.validateCurrent(reservation) != nil || staging.source == nil || staging.destination == nil ||
		staging.sourceRelease || staging.destinationRelease || staging.sourceReleased != nil || staging.destinationReleased != nil ||
		staging.destination.PeerCertificateSHA256 != publication.DestinationPeerCertificateSHA256 {
		return nil, ErrNomadSandboxMigrationConflict
	}
	request := protocol.MigrationImagePrefetchRequest{Staging: staging.targetRequest(), Publication: command, Plan: plan}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if len(payload) != 0 {
		var stored protocol.MigrationImagePrefetchRequest
		if storedDigest == nil || *storedDigest != want || json.Unmarshal(payload, &stored) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		actual, err := stored.Digest()
		if err != nil || actual != want {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &stored, nil
	}
	if preparationStarted || storedDigest != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationTarget(ctx, tx, reservation); err != nil {
		return nil, err
	}
	payload, err = json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations
        SET target_image_prefetch_request=$2,target_image_prefetch_digest=$3 WHERE operation_id=$1`, publication.Assignment.OperationID, payload, want); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}
