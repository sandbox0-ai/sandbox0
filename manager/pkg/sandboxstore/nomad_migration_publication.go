package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadSandboxMigrationPublication retains the exact completed cut
// before any execution-image upload. The caller obtains capture through the
// authenticated node channel and CPU eligibility from node preflight; the
// supplied digest must equal the launch profile retained before capture.
// Publication does not advance the head, writer epoch or runtime generation.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationPublication(ctx context.Context, assignment runtimecontrol.MigrationAssignment, capture protocol.MigrationCapture, cpuFeaturesDigest string) (*protocol.MigrationPublicationRequest, error) {
	if err := capture.Validate(); err != nil {
		return nil, err
	}
	if capture.State != protocol.MigrationCaptureComplete || capture.RootFS == nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, assignment)
	if err != nil {
		return nil, err
	}
	launch, err := nomadMigrationCapturedCPULaunch(ctx, tx, capture.Request)
	if err != nil {
		return nil, err
	}
	retainedCPU, err := launch.GuestCPUProfile().Digest()
	if err != nil || retainedCPU != cpuFeaturesDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	request := protocol.MigrationPublicationRequest{Capture: capture, Assignment: assignment,
		CompatibilityDigest: reservation.SourceSlot.CompatibilityDigest, CPUFeaturesDigest: cpuFeaturesDigest, CPULaunch: launch}
	staging, err := loadNomadMigrationStaging(ctx, tx, assignment.OperationID)
	if err != nil {
		return nil, err
	}
	if staging != nil && staging.destination != nil {
		if staging.validateCurrent(reservation) != nil || staging.sourceRelease || staging.destinationRelease {
			return nil, ErrNomadSandboxMigrationConflict
		}
		request.DestinationPeerCertificateSHA256 = staging.destination.PeerCertificateSHA256
		if grant := staging.request.CaptureUpload; grant.Version != 0 &&
			(grant.TeamID != assignment.Target.TeamID || grant.CompatibilityDigest != request.CompatibilityDigest || grant.CPUFeaturesDigest != cpuFeaturesDigest) {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	var capturePayload, payload []byte
	var captureDigest, publicationDigest *string
	if err := tx.QueryRow(ctx, `SELECT capture_request,capture_digest,publication_request,publication_digest
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&capturePayload, &captureDigest, &payload, &publicationDigest); err != nil {
		return nil, err
	}
	var storedCapture protocol.MigrationCaptureRequest
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing || captureDigest == nil ||
		*captureDigest != capture.RequestDigest || json.Unmarshal(capturePayload, &storedCapture) != nil || storedCapture != capture.Request {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(payload) != 0 {
		var stored protocol.MigrationPublicationRequest
		if json.Unmarshal(payload, &stored) != nil || publicationDigest == nil || *publicationDigest != want ||
			reservation.Lifecycle.PreparedGenerationID != capture.RootFS.Generation.GenerationID {
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
	if err := validateNomadMigrationPublicationSource(ctx, tx, reservation, request); err != nil {
		return nil, err
	}
	// Register the already-materialized filesystem cut in the common immutable
	// generation catalog before uploading memory. The lifecycle retains it as
	// prepared evidence; it does not become the filesystem's current head.
	if reservation.Lifecycle.PreparedGenerationID != "" {
		return nil, ErrNomadSandboxMigrationConflict
	}
	g := capture.RootFS.Generation
	generation, err := normalizeDurableRootFSGeneration(&RootFSGeneration{
		ID: g.GenerationID, FilesystemID: g.FilesystemID, ParentGenerationID: reservation.Lifecycle.ExpectedGenerationID,
		SourceOCIDigest: g.SourceOCIDigest, BaseArtifactDigest: g.BaseArtifactDigest, BaseBlockRoot: g.BaseBlockRoot,
		CurrentBlockHead: g.CurrentBlockHead, WriterEpoch: g.WriterEpoch, FormatGeneration: g.FormatGeneration,
		DurabilityState: g.DurabilityState, LocatorVersion: g.LocatorVersion, Descriptor: g.Descriptor,
	}, reservation.Lifecycle.ExpectedGenerationID)
	if err != nil {
		return nil, err
	}
	if err := insertPreparedRootFSGeneration(ctx, tx, generation); err != nil {
		return nil, err
	}
	if err := (sandboxStoreTx{tx: tx}).SetLifecycleTxnPreparedGeneration(ctx, reservation.Lifecycle.ID, generation.ID); err != nil {
		return nil, err
	}
	payload, err = json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET publication_request=$2,publication_digest=$3 WHERE operation_id=$1`, assignment.OperationID, payload, want); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// CommitNomadSandboxMigrationPublication records the authenticated source's
// immutable upload receipt. It intentionally leaves the lifecycle publishing:
// physical source fencing and writer transfer are separate required steps.
func (s *PGSandboxStore) CommitNomadSandboxMigrationPublication(ctx context.Context, request protocol.MigrationPublicationRequest, receipt protocol.MigrationPublication) error {
	if err := receipt.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, request.Assignment)
	if err != nil {
		return err
	}
	var payload, prior []byte
	var storedDigest *string
	if err := tx.QueryRow(ctx, `SELECT publication_request,publication_digest,publication_receipt FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.Assignment.OperationID).Scan(&payload, &storedDigest, &prior); err != nil {
		return err
	}
	want, _ := request.Digest()
	var stored protocol.MigrationPublicationRequest
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing || storedDigest == nil || *storedDigest != want || json.Unmarshal(payload, &stored) != nil ||
		reservation.Lifecycle.PreparedGenerationID != request.Capture.RootFS.Generation.GenerationID {
		return ErrNomadSandboxMigrationConflict
	}
	actual, err := stored.Digest()
	if err != nil || actual != want {
		return ErrNomadSandboxMigrationConflict
	}
	if len(prior) != 0 {
		var previous protocol.MigrationPublication
		if json.Unmarshal(prior, &previous) != nil || previous != receipt {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	if err := validateNomadMigrationPublicationSource(ctx, tx, reservation, request); err != nil {
		return err
	}
	payload, err = json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET publication_receipt=$2 WHERE operation_id=$1`, request.Assignment.OperationID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// A slow upload may outlive heartbeat/lease renewal. This immutable receipt is
// not execution authority, but must still match the unadvanced writer/head.
// Retirement, crash abandonment or another writer invalidates new publication.
func validateNomadMigrationPublicationSource(ctx context.Context, tx pgx.Tx, reservation *NomadSandboxMigrationReservation, request protocol.MigrationPublicationRequest) error {
	return validateNomadExecutionPublicationSource(ctx, tx, reservation.Lifecycle, reservation.SourceSlot,
		reservation.SourceWriterGrantID, reservation.SourceBindingDigest, request)
}

// The image publication boundary is shared by migration and memory pause.
// Validate the captured writer and immutable disk format without requiring a
// destination or treating an expired heartbeat as permission to publish newer disk.
func validateNomadExecutionPublicationSource(ctx context.Context, tx pgx.Tx, lifecycle *SandboxLifecycleTxn,
	slot *RuntimeSlot, writerID string, binding []byte, request protocol.MigrationPublicationRequest) error {
	source, err := request.SourceAssignment()
	if err != nil {
		return err
	}
	filesystem, initial, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, source.SandboxID)
	if err != nil {
		return err
	}
	grantRecord, err := getRootFSWriterGrantForUpdate(ctx, tx, writerID)
	if err != nil {
		return err
	}
	grant := grantRecord.RootFSWriterGrant
	capture, generation := request.Capture.Request, request.Capture.RootFS.Generation
	if grant.State != RootFSWriterGrantStateConsumed || grant.SlotID != capture.Target.SlotID ||
		grant.NodeUID != capture.Target.NodeUID || grant.NodeBootID != capture.Target.NodeBootID ||
		!bytes.Equal(grant.BindingDigest, binding) || hex.EncodeToString(grant.BindingDigest) != capture.BindingDigest ||
		filesystem.WriterEpoch != grant.WriterEpoch || filesystem.ID != grant.FilesystemID || filesystem.TeamID != source.TeamID ||
		initial.ID != lifecycle.ExpectedGenerationID || initial.ID != grant.InitialGenerationID || initial.LocatorVersion == math.MaxInt64 ||
		generation.FilesystemID != filesystem.ID || generation.WriterEpoch != grant.WriterEpoch ||
		generation.SourceOCIDigest != initial.SourceOCIDigest || generation.BaseArtifactDigest != initial.BaseArtifactDigest ||
		generation.BaseBlockRoot != initial.BaseBlockRoot || generation.FormatGeneration != initial.FormatGeneration ||
		generation.LocatorVersion != initial.LocatorVersion+1 || request.CompatibilityDigest != slot.CompatibilityDigest {
		return ErrNomadSandboxMigrationConflict
	}
	oldDescriptor, err := rootfsblock.DecodeDescriptor(initial.Descriptor)
	if err != nil {
		return err
	}
	newDescriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	if err != nil {
		return err
	}
	if oldDescriptor.LogicalSizeBytes != newDescriptor.LogicalSizeBytes || oldDescriptor.BlockSizeBytes != newDescriptor.BlockSizeBytes {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}
