package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadSandboxMigrationImagePreparation persists the exact target
// download before dispatch. Its reserved carrier still has no writer; the
// image receipt is required before source detachment and target attachment.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationImagePreparation(ctx context.Context, assignment runtimecontrol.MigrationAssignment) (*protocol.MigrationImagePrepareRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, assignment)
	if err != nil {
		return nil, err
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing && reservation.Lifecycle.Phase != SandboxLifecyclePhaseCommitting {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var publication, receipt, payload []byte
	var storedDigest *string
	if err := tx.QueryRow(ctx, `SELECT publication_request,publication_receipt,target_image_request,target_image_digest
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&publication, &receipt, &payload, &storedDigest); err != nil {
		return nil, err
	}
	var command protocol.MigrationPublicationRequest
	var published protocol.MigrationPublication
	if json.Unmarshal(publication, &command) != nil || json.Unmarshal(receipt, &published) != nil || published.ValidateFor(command) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	digest, _ := command.Assignment.Digest()
	if digest != reservation.AssignmentDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	target := reservation.TargetSlot
	request := protocol.MigrationImagePrepareRequest{Target: protocol.NodeChannelTarget{
		SlotID: target.ID, ClusterID: target.ClusterID, AllocationID: target.AllocationID,
		NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, ControlEndpoint: target.ControlEndpoint},
		Publication: command, Receipt: published, Resources: reservation.TargetResourceLease}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if len(payload) != 0 {
		var stored protocol.MigrationImagePrepareRequest
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
	if err := validateNomadMigrationTarget(ctx, tx, reservation); err != nil {
		return nil, err
	}
	payload, err = json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET target_image_request=$2,target_image_digest=$3 WHERE operation_id=$1`, assignment.OperationID, payload, want); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// CommitNomadSandboxMigrationImagePreparation records an authenticated target
// receipt without changing writers, routing, runtime generation or capacity.
func (s *PGSandboxStore) CommitNomadSandboxMigrationImagePreparation(ctx context.Context, request protocol.MigrationImagePrepareRequest, receipt protocol.MigrationImagePrepared) error {
	if err := receipt.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, request.Publication.Assignment)
	if err != nil {
		return err
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing && reservation.Lifecycle.Phase != SandboxLifecyclePhaseCommitting {
		return ErrNomadSandboxMigrationConflict
	}
	var payload, prior []byte
	var storedDigest *string
	if err := tx.QueryRow(ctx, `SELECT target_image_request,target_image_digest,target_image_receipt FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, reservation.Lifecycle.ID).Scan(&payload, &storedDigest, &prior); err != nil {
		return err
	}
	want, _ := request.Digest()
	var stored protocol.MigrationImagePrepareRequest
	if storedDigest == nil || *storedDigest != want || json.Unmarshal(payload, &stored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	actual, err := stored.Digest()
	if err != nil || actual != want {
		return ErrNomadSandboxMigrationConflict
	}
	if len(prior) != 0 {
		var previous protocol.MigrationImagePrepared
		if json.Unmarshal(prior, &previous) != nil || previous != receipt {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	if err := validateNomadMigrationTarget(ctx, tx, reservation); err != nil {
		return err
	}
	payload, err = json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET target_image_receipt=$2 WHERE operation_id=$1`, reservation.Lifecycle.ID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func requireNomadMigrationTargetImage(ctx context.Context, tx pgx.Tx, reservation *NomadSandboxMigrationReservation) error {
	var payload, receipt []byte
	if err := tx.QueryRow(ctx, `SELECT target_image_request,target_image_receipt FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, reservation.Lifecycle.ID).Scan(&payload, &receipt); err != nil {
		return err
	}
	var request protocol.MigrationImagePrepareRequest
	var prepared protocol.MigrationImagePrepared
	if json.Unmarshal(payload, &request) != nil || json.Unmarshal(receipt, &prepared) != nil || prepared.ValidateFor(request) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	digest, _ := request.Publication.Assignment.Digest()
	t := reservation.TargetSlot
	if digest != reservation.AssignmentDigest || request.Target.SlotID != t.ID || request.Target.AllocationID != t.AllocationID ||
		request.Target.NodeUID != t.NodeUID || request.Target.NodeBootID != t.NodeBootID || request.Resources != reservation.TargetResourceLease {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}
