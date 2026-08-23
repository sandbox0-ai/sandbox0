package sandboxstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

const (
	RootFSImportStatePending   = "pending"
	RootFSImportStateBuilding  = "building"
	RootFSImportStateReady     = "ready"
	RootFSImportStateAbandoned = "abandoned"

	MinRootFSImportLeaseTTL  = 5 * time.Second
	MaxRootFSImportLeaseTTL  = 15 * time.Minute
	MaxRootFSImportListLimit = 1000
)

var (
	ErrRootFSImportNotFound  = errors.New("rootfs import operation not found")
	ErrRootFSImportConflict  = errors.New("rootfs import operation conflict")
	ErrRootFSImportLeaseLost = errors.New("rootfs import operation lease lost")
)

// RootFSImportOperation is the durable, non-secret state of one OCI-to-block
// conversion. Local staging paths and registry credentials never enter it.
type RootFSImportOperation struct {
	ID              string
	SourceOCIDigest string
	Spec            rootfsimporter.OperationSpec
	State           string
	LeaseOwner      string
	LeaseToken      string
	LeaseExpiresAt  time.Time
	AttemptCount    int
	ArtifactDigest  string
	AbandonReason   string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ReadyAt         time.Time
	AbandonedAt     time.Time
}

// Lease returns the fenced identity of a currently leased build attempt.
func (o *RootFSImportOperation) Lease() (RootFSImportLease, error) {
	if o == nil || o.State != RootFSImportStateBuilding {
		return RootFSImportLease{}, fmt.Errorf("rootfs import operation is not building")
	}
	return normalizeRootFSImportLease(RootFSImportLease{
		OperationID: o.ID, WorkerID: o.LeaseOwner,
		Token: o.LeaseToken, ExpiresAt: o.LeaseExpiresAt,
	})
}

// RootFSImportLease fences all object-journal and publication mutations from
// a worker attempt.
type RootFSImportLease struct {
	OperationID string
	WorkerID    string
	Token       string
	ExpiresAt   time.Time
}

type BeginRootFSImportRequest struct {
	OperationID string
	Spec        rootfsimporter.OperationSpec
}

type PublishReadyRootFSImportRequest struct {
	Lease  RootFSImportLease
	Result rootfsimporter.BuildResult
}

type RootFSImportGarbageResult struct {
	RecoveredLeases int
	PurgedReady     int
	PurgedAbandoned int
	EnqueuedObjects int
}

// BeginRootFSImport persists exact import inputs before any side effect.
func (s *PGSandboxStore) BeginRootFSImport(
	ctx context.Context,
	req *BeginRootFSImportRequest,
) (*RootFSImportOperation, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs import store is not configured")
	}
	normalized, sourceDigest, err := normalizeBeginRootFSImport(req)
	if err != nil {
		return nil, err
	}
	options := normalized.Spec.BlockOptions
	_, err = s.pool.Exec(ctx, `
		INSERT INTO manager.rootfs_import_operations (
			operation_id, source_oci_ref, source_oci_digest,
			oci_os, oci_architecture, oci_variant, format_generation,
			procd_protocol, procd_digest, logical_size_bytes,
			block_data_range_bytes, block_pack_bytes, block_page_entries,
			object_prefix, state
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'pending')
		ON CONFLICT (operation_id) DO NOTHING
	`, normalized.OperationID, normalized.Spec.SourceOCIRef, sourceDigest.String(),
		normalized.Spec.Platform.OS, normalized.Spec.Platform.Architecture,
		normalized.Spec.Platform.Variant, normalized.Spec.FormatGeneration,
		normalized.Spec.ProcdProtocol, normalized.Spec.ProcdDigest,
		normalized.Spec.LogicalSizeBytes, options.DataRangeBytes, options.PackBytes,
		options.PageEntries, options.ObjectPrefix)
	if err != nil {
		return nil, fmt.Errorf("begin rootfs import operation: %w", err)
	}
	operation, err := scanRootFSImportOperation(s.pool.QueryRow(ctx,
		rootFSImportOperationSelectSQL()+" WHERE operation_id = $1", normalized.OperationID))
	if err != nil {
		return nil, fmt.Errorf("read rootfs import operation: %w", err)
	}
	if !rootFSImportOperationMatchesSpec(operation, normalized.Spec, sourceDigest.String()) {
		return nil, fmt.Errorf("%w: operation %s has different immutable inputs",
			ErrRootFSImportConflict, normalized.OperationID)
	}
	return operation, nil
}

func (s *PGSandboxStore) GetRootFSImportOperation(
	ctx context.Context,
	operationID string,
) (*RootFSImportOperation, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs import store is not configured")
	}
	operationID, err := rootfsimporter.NormalizeOperationID(operationID)
	if err != nil {
		return nil, err
	}
	operation, err := scanRootFSImportOperation(s.pool.QueryRow(ctx,
		rootFSImportOperationSelectSQL()+" WHERE operation_id = $1", operationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSImportNotFound, operationID)
	}
	if err != nil {
		return nil, fmt.Errorf("get rootfs import operation: %w", err)
	}
	return operation, nil
}

// ListRootFSImportOperations provides bounded operational enumeration without
// changing lease ownership.
func (s *PGSandboxStore) ListRootFSImportOperations(
	ctx context.Context,
	states []string,
	limit int,
) ([]*RootFSImportOperation, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs import store is not configured")
	}
	if limit <= 0 || limit > MaxRootFSImportListLimit {
		return nil, fmt.Errorf("rootfs import list limit must be within 1..%d", MaxRootFSImportListLimit)
	}
	normalizedStates, err := normalizeRootFSImportStates(states)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, rootFSImportOperationSelectSQL()+`
		WHERE state = ANY($1)
		ORDER BY created_at, operation_id
		LIMIT $2
	`, normalizedStates, limit)
	if err != nil {
		return nil, fmt.Errorf("list rootfs import operations: %w", err)
	}
	defer rows.Close()
	operations := make([]*RootFSImportOperation, 0)
	for rows.Next() {
		operation, scanErr := scanRootFSImportOperation(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan rootfs import operation: %w", scanErr)
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs import operations: %w", err)
	}
	return operations, nil
}

// LeaseNextRootFSImport atomically claims the oldest pending or expired build.
func (s *PGSandboxStore) LeaseNextRootFSImport(
	ctx context.Context,
	workerID string,
	leaseTTL time.Duration,
) (*RootFSImportOperation, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs import store is not configured")
	}
	workerID, err := normalizeRootFSImportWorkerID(workerID)
	if err != nil {
		return nil, err
	}
	if err := validateRootFSImportLeaseTTL(leaseTTL); err != nil {
		return nil, err
	}
	leaseToken, err := newRootFSImportLeaseToken()
	if err != nil {
		return nil, err
	}
	operation, err := scanRootFSImportOperation(s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT operation_id
			FROM manager.rootfs_import_operations
			WHERE state = 'pending'
				OR (state = 'building' AND lease_expires_at <= clock_timestamp())
			ORDER BY CASE WHEN state = 'building' THEN 0 ELSE 1 END,
				created_at, operation_id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE manager.rootfs_import_operations operation
		SET state = 'building', lease_owner = $1, lease_token = $2,
			lease_expires_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond'),
			attempt_count = attempt_count + 1, updated_at = NOW()
		FROM candidate
		WHERE operation.operation_id = candidate.operation_id
		RETURNING `+rootFSImportOperationReturningColumns("operation"),
		workerID, leaseToken, leaseTTL.Milliseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lease rootfs import operation: %w", err)
	}
	return operation, nil
}

func (s *PGSandboxStore) RenewRootFSImportLease(
	ctx context.Context,
	lease RootFSImportLease,
	leaseTTL time.Duration,
) (RootFSImportLease, error) {
	if s == nil || s.pool == nil {
		return RootFSImportLease{}, fmt.Errorf("rootfs import store is not configured")
	}
	normalized, err := normalizeRootFSImportLease(lease)
	if err != nil {
		return RootFSImportLease{}, err
	}
	if err := validateRootFSImportLeaseTTL(leaseTTL); err != nil {
		return RootFSImportLease{}, err
	}
	var expiresAt time.Time
	err = s.pool.QueryRow(ctx, `
		UPDATE manager.rootfs_import_operations
		SET lease_expires_at = clock_timestamp() + ($4::bigint * INTERVAL '1 millisecond'),
			updated_at = NOW()
		WHERE operation_id = $1 AND state = 'building'
			AND lease_owner = $2 AND lease_token = $3
			AND lease_expires_at > clock_timestamp()
		RETURNING lease_expires_at
	`, normalized.OperationID, normalized.WorkerID, normalized.Token,
		leaseTTL.Milliseconds()).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RootFSImportLease{}, fmt.Errorf("%w: %s", ErrRootFSImportLeaseLost, normalized.OperationID)
	}
	if err != nil {
		return RootFSImportLease{}, fmt.Errorf("renew rootfs import lease: %w", err)
	}
	normalized.ExpiresAt = expiresAt
	return normalized, nil
}

// ReleaseRootFSImportLease makes a transiently failed operation immediately
// available without waiting for lease expiry.
func (s *PGSandboxStore) ReleaseRootFSImportLease(
	ctx context.Context,
	lease RootFSImportLease,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs import store is not configured")
	}
	normalized, err := normalizeRootFSImportLease(lease)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_import_operations
		SET state = 'pending', lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, updated_at = NOW()
		WHERE operation_id = $1 AND state = 'building'
			AND lease_owner = $2 AND lease_token = $3
			AND lease_expires_at > clock_timestamp()
	`, normalized.OperationID, normalized.WorkerID, normalized.Token)
	if err != nil {
		return fmt.Errorf("release rootfs import lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", ErrRootFSImportLeaseLost, normalized.OperationID)
	}
	return nil
}

func rootFSImportOperationSelectSQL() string {
	return `
		SELECT operation_id, source_oci_ref, source_oci_digest,
			oci_os, oci_architecture, oci_variant, format_generation,
			procd_protocol, procd_digest, logical_size_bytes,
			block_data_range_bytes, block_pack_bytes, block_page_entries,
			object_prefix, state, lease_owner, lease_token, lease_expires_at,
			attempt_count, result_artifact_digest, abandon_reason,
			created_at, updated_at, ready_at, abandoned_at
		FROM manager.rootfs_import_operations `
}

func rootFSImportOperationReturningColumns(alias string) string {
	prefix := alias + "."
	return prefix + "operation_id, " + prefix + "source_oci_ref, " + prefix + "source_oci_digest, " +
		prefix + "oci_os, " + prefix + "oci_architecture, " + prefix + "oci_variant, " +
		prefix + "format_generation, " + prefix + "procd_protocol, " + prefix + "procd_digest, " +
		prefix + "logical_size_bytes, " + prefix + "block_data_range_bytes, " + prefix + "block_pack_bytes, " +
		prefix + "block_page_entries, " + prefix + "object_prefix, " + prefix + "state, " +
		prefix + "lease_owner, " + prefix + "lease_token, " + prefix + "lease_expires_at, " +
		prefix + "attempt_count, " + prefix + "result_artifact_digest, " + prefix + "abandon_reason, " +
		prefix + "created_at, " + prefix + "updated_at, " + prefix + "ready_at, " + prefix + "abandoned_at"
}

func scanRootFSImportOperation(row sandboxRecordScanner) (*RootFSImportOperation, error) {
	var operation RootFSImportOperation
	var leaseOwner, leaseToken, artifactDigest *string
	var leaseExpiresAt, readyAt, abandonedAt *time.Time
	if err := row.Scan(
		&operation.ID, &operation.Spec.SourceOCIRef, &operation.SourceOCIDigest,
		&operation.Spec.Platform.OS, &operation.Spec.Platform.Architecture,
		&operation.Spec.Platform.Variant, &operation.Spec.FormatGeneration,
		&operation.Spec.ProcdProtocol, &operation.Spec.ProcdDigest,
		&operation.Spec.LogicalSizeBytes, &operation.Spec.BlockOptions.DataRangeBytes,
		&operation.Spec.BlockOptions.PackBytes, &operation.Spec.BlockOptions.PageEntries,
		&operation.Spec.BlockOptions.ObjectPrefix, &operation.State,
		&leaseOwner, &leaseToken, &leaseExpiresAt, &operation.AttemptCount,
		&artifactDigest, &operation.AbandonReason, &operation.CreatedAt,
		&operation.UpdatedAt, &readyAt, &abandonedAt,
	); err != nil {
		return nil, err
	}
	if leaseOwner != nil {
		operation.LeaseOwner = *leaseOwner
	}
	if leaseToken != nil {
		operation.LeaseToken = *leaseToken
	}
	if leaseExpiresAt != nil {
		operation.LeaseExpiresAt = *leaseExpiresAt
	}
	if artifactDigest != nil {
		operation.ArtifactDigest = *artifactDigest
	}
	if readyAt != nil {
		operation.ReadyAt = *readyAt
	}
	if abandonedAt != nil {
		operation.AbandonedAt = *abandonedAt
	}
	return &operation, nil
}

func normalizeBeginRootFSImport(
	req *BeginRootFSImportRequest,
) (*BeginRootFSImportRequest, digest.Digest, error) {
	if req == nil {
		return nil, "", fmt.Errorf("rootfs import request is required")
	}
	operationID, err := rootfsimporter.NormalizeOperationID(req.OperationID)
	if err != nil {
		return nil, "", err
	}
	spec, err := rootfsimporter.NormalizeOperationSpec(req.Spec)
	if err != nil {
		return nil, "", err
	}
	sourceDigest, err := rootfsimporter.PinnedSourceDigest(spec.SourceOCIRef)
	if err != nil {
		return nil, "", err
	}
	return &BeginRootFSImportRequest{OperationID: operationID, Spec: spec}, sourceDigest, nil
}

func rootFSImportOperationMatchesSpec(
	operation *RootFSImportOperation,
	spec rootfsimporter.OperationSpec,
	sourceDigest string,
) bool {
	return operation != nil && operation.SourceOCIDigest == sourceDigest && operation.Spec == spec
}

func normalizeRootFSImportStates(states []string) ([]string, error) {
	if len(states) == 0 || len(states) > 4 {
		return nil, fmt.Errorf("rootfs import states must contain 1..4 values")
	}
	allowed := map[string]bool{
		RootFSImportStatePending: true, RootFSImportStateBuilding: true,
		RootFSImportStateReady: true, RootFSImportStateAbandoned: true,
	}
	seen := make(map[string]struct{}, len(states))
	result := make([]string, 0, len(states))
	for _, state := range states {
		if !allowed[state] {
			return nil, fmt.Errorf("invalid rootfs import state %q", state)
		}
		if _, found := seen[state]; found {
			return nil, fmt.Errorf("duplicate rootfs import state %q", state)
		}
		seen[state] = struct{}{}
		result = append(result, state)
	}
	sort.Strings(result)
	return result, nil
}

func normalizeRootFSImportWorkerID(value string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 256 {
		return "", fmt.Errorf("rootfs import worker ID must contain 1..256 canonical bytes")
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._:-", character) {
			continue
		}
		return "", fmt.Errorf("rootfs import worker ID contains an invalid character")
	}
	return value, nil
}

func validateRootFSImportLeaseTTL(value time.Duration) error {
	if value < MinRootFSImportLeaseTTL || value > MaxRootFSImportLeaseTTL || value%time.Millisecond != 0 {
		return fmt.Errorf("rootfs import lease TTL must be a whole millisecond within %s..%s",
			MinRootFSImportLeaseTTL, MaxRootFSImportLeaseTTL)
	}
	return nil
}

func normalizeRootFSImportLease(value RootFSImportLease) (RootFSImportLease, error) {
	operationID, err := rootfsimporter.NormalizeOperationID(value.OperationID)
	if err != nil {
		return RootFSImportLease{}, err
	}
	workerID, err := normalizeRootFSImportWorkerID(value.WorkerID)
	if err != nil {
		return RootFSImportLease{}, err
	}
	if len(value.Token) != 64 {
		return RootFSImportLease{}, fmt.Errorf("rootfs import lease token must be 32 canonical bytes")
	}
	decoded, err := hex.DecodeString(value.Token)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != value.Token {
		return RootFSImportLease{}, fmt.Errorf("rootfs import lease token must be 32 canonical bytes")
	}
	value.OperationID, value.WorkerID = operationID, workerID
	return value, nil
}

func newRootFSImportLeaseToken() (string, error) {
	payload := make([]byte, 32)
	if _, err := rand.Read(payload); err != nil {
		return "", fmt.Errorf("generate rootfs import lease token: %w", err)
	}
	return hex.EncodeToString(payload), nil
}

// RootFSImportPublicationJournal binds the generic pre-PUT interface to one
// fenced PostgreSQL import lease.
type RootFSImportPublicationJournal struct {
	store *PGSandboxStore
	lease RootFSImportLease
}

func NewRootFSImportPublicationJournal(
	store *PGSandboxStore,
	lease RootFSImportLease,
) (*RootFSImportPublicationJournal, error) {
	if store == nil || store.pool == nil {
		return nil, fmt.Errorf("rootfs import store is not configured")
	}
	normalized, err := normalizeRootFSImportLease(lease)
	if err != nil {
		return nil, err
	}
	return &RootFSImportPublicationJournal{store: store, lease: normalized}, nil
}

func (j *RootFSImportPublicationJournal) PrepareObject(
	ctx context.Context,
	operationID string,
	reference rootfsblock.ObjectReference,
) error {
	if j == nil || j.store == nil || operationID != j.lease.OperationID {
		return fmt.Errorf("rootfs import publication journal operation does not match its lease")
	}
	return j.store.PrepareRootFSImportObject(ctx, j.lease, reference)
}

func (j *RootFSImportPublicationJournal) MarkObjectPublished(
	ctx context.Context,
	operationID string,
	reference rootfsblock.ObjectReference,
) error {
	if j == nil || j.store == nil || operationID != j.lease.OperationID {
		return fmt.Errorf("rootfs import publication journal operation does not match its lease")
	}
	return j.store.MarkRootFSImportObjectPublished(ctx, j.lease, reference)
}

var _ rootfsimporter.ObjectPublicationJournal = (*RootFSImportPublicationJournal)(nil)

// PrepareRootFSImportObject durably records exact object intent before PUT.
func (s *PGSandboxStore) PrepareRootFSImportObject(
	ctx context.Context,
	lease RootFSImportLease,
	reference rootfsblock.ObjectReference,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs import store is not configured")
	}
	normalizedLease, err := normalizeRootFSImportLease(lease)
	if err != nil {
		return err
	}
	if err := rootfsblock.ValidateObjectReference(reference); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin rootfs import object preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := lockRootFSImportLease(ctx, tx, normalizedLease)
	if err != nil {
		return err
	}
	if state == RootFSImportStateReady {
		if err := verifyRootFSImportObject(ctx, tx, normalizedLease.OperationID, reference, true); err != nil {
			return err
		}
		return commitRootFSImportTx(ctx, tx, "existing ready object preparation")
	}
	var pendingDeletion bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key = $1)
	`, reference.Key).Scan(&pendingDeletion); err != nil {
		return fmt.Errorf("check rootfs import object deletion fence: %w", err)
	}
	if pendingDeletion {
		return fmt.Errorf("%w: rootfs import object %s is pending deletion",
			ErrRootFSImportConflict, reference.Key)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_materialization_objects (
			object_key, object_kind, object_size, checksum
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (object_key) DO NOTHING
	`, reference.Key, reference.Kind, reference.Size, reference.Checksum); err != nil {
		return fmt.Errorf("register rootfs import object: %w", err)
	}
	var kind, checksum string
	var size int64
	if err := tx.QueryRow(ctx, `
		SELECT object_kind, object_size, checksum
		FROM manager.rootfs_materialization_objects
		WHERE object_key = $1 FOR UPDATE
	`, reference.Key).Scan(&kind, &size, &checksum); err != nil {
		return fmt.Errorf("read rootfs import object: %w", err)
	}
	if kind != reference.Kind || size != reference.Size || checksum != reference.Checksum {
		return fmt.Errorf("%w: rootfs import object %s has different immutable metadata",
			ErrRootFSImportConflict, reference.Key)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_import_operation_objects (
			operation_id, object_key, upload_state
		) VALUES ($1, $2, 'prepared')
		ON CONFLICT (operation_id, object_key) DO UPDATE
		SET updated_at = NOW()
	`, normalizedLease.OperationID, reference.Key); err != nil {
		return fmt.Errorf("link rootfs import object: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_import_operations SET updated_at = NOW()
		WHERE operation_id = $1 AND state = 'building'
	`, normalizedLease.OperationID); err != nil {
		return fmt.Errorf("touch rootfs import operation: %w", err)
	}
	return commitRootFSImportTx(ctx, tx, "object preparation")
}

// MarkRootFSImportObjectPublished records completion only after immutable PUT.
func (s *PGSandboxStore) MarkRootFSImportObjectPublished(
	ctx context.Context,
	lease RootFSImportLease,
	reference rootfsblock.ObjectReference,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs import store is not configured")
	}
	normalizedLease, err := normalizeRootFSImportLease(lease)
	if err != nil {
		return err
	}
	if err := rootfsblock.ValidateObjectReference(reference); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin rootfs import publication mark: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := lockRootFSImportLease(ctx, tx, normalizedLease)
	if err != nil {
		return err
	}
	if state == RootFSImportStateReady {
		if err := verifyRootFSImportObject(ctx, tx, normalizedLease.OperationID, reference, true); err != nil {
			return err
		}
		return commitRootFSImportTx(ctx, tx, "existing ready publication mark")
	}
	if err := verifyRootFSImportObject(ctx, tx, normalizedLease.OperationID, reference, false); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_import_operation_objects
		SET upload_state = 'published', updated_at = NOW()
		WHERE operation_id = $1 AND object_key = $2
	`, normalizedLease.OperationID, reference.Key)
	if err != nil {
		return fmt.Errorf("mark rootfs import object published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: rootfs import object %s was not prepared",
			ErrRootFSImportConflict, reference.Key)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_materialization_objects
		SET uploaded_at = COALESCE(uploaded_at, NOW()), updated_at = NOW()
		WHERE object_key = $1
	`, reference.Key); err != nil {
		return fmt.Errorf("mark shared rootfs block object uploaded: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_import_operations SET updated_at = NOW()
		WHERE operation_id = $1 AND state = 'building'
	`, normalizedLease.OperationID); err != nil {
		return fmt.Errorf("touch rootfs import operation publication: %w", err)
	}
	return commitRootFSImportTx(ctx, tx, "publication mark")
}

func lockRootFSImportLease(
	ctx context.Context,
	tx pgx.Tx,
	lease RootFSImportLease,
) (string, error) {
	var state string
	var owner, token *string
	var expiresAt *time.Time
	var leaseCurrent bool
	err := tx.QueryRow(ctx, `
		SELECT state, lease_owner, lease_token, lease_expires_at,
			COALESCE(lease_expires_at > clock_timestamp(), FALSE)
		FROM manager.rootfs_import_operations
		WHERE operation_id = $1 FOR UPDATE
	`, lease.OperationID).Scan(&state, &owner, &token, &expiresAt, &leaseCurrent)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrRootFSImportNotFound, lease.OperationID)
	}
	if err != nil {
		return "", fmt.Errorf("lock rootfs import operation: %w", err)
	}
	if state == RootFSImportStateReady {
		return state, nil
	}
	if state != RootFSImportStateBuilding || owner == nil || token == nil || expiresAt == nil ||
		*owner != lease.WorkerID || *token != lease.Token || !leaseCurrent {
		return "", fmt.Errorf("%w: %s", ErrRootFSImportLeaseLost, lease.OperationID)
	}
	return state, nil
}

func verifyRootFSImportObject(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
	reference rootfsblock.ObjectReference,
	requirePublished bool,
) error {
	var kind, checksum, uploadState string
	var size int64
	var uploadedAt *time.Time
	err := tx.QueryRow(ctx, `
		SELECT object_record.object_kind, object_record.object_size,
			object_record.checksum, object_record.uploaded_at,
			operation_object.upload_state
		FROM manager.rootfs_import_operation_objects operation_object
		JOIN manager.rootfs_materialization_objects object_record USING (object_key)
		WHERE operation_object.operation_id = $1 AND operation_object.object_key = $2
	`, operationID, reference.Key).Scan(&kind, &size, &checksum, &uploadedAt, &uploadState)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: rootfs import object %s was not prepared",
			ErrRootFSImportConflict, reference.Key)
	}
	if err != nil {
		return fmt.Errorf("read rootfs import object: %w", err)
	}
	if kind != reference.Kind || size != reference.Size || checksum != reference.Checksum {
		return fmt.Errorf("%w: rootfs import object %s has different immutable metadata",
			ErrRootFSImportConflict, reference.Key)
	}
	if requirePublished && (uploadState != "published" || uploadedAt == nil) {
		return fmt.Errorf("%w: rootfs import object %s is not durably published",
			ErrRootFSImportConflict, reference.Key)
	}
	return nil
}

func commitRootFSImportTx(ctx context.Context, tx pgx.Tx, action string) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs import %s: %w", action, err)
	}
	return nil
}

// PublishReadyRootFSImport atomically installs attested artifact metadata,
// exact object reachability, and the terminal operation result. An exact retry
// after commit-response loss succeeds without reviving the expired lease.
func (s *PGSandboxStore) PublishReadyRootFSImport(
	ctx context.Context,
	req *PublishReadyRootFSImportRequest,
) (*RootFSBaseArtifact, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs import store is not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("ready rootfs import publication is required")
	}
	lease, err := normalizeRootFSImportLease(req.Lease)
	if err != nil {
		return nil, err
	}
	if err := req.Result.Validate(); err != nil {
		return nil, fmt.Errorf("validate ready rootfs import result: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin ready rootfs import publication: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := scanRootFSImportOperation(tx.QueryRow(ctx,
		rootFSImportOperationSelectSQL()+" WHERE operation_id = $1 FOR UPDATE", lease.OperationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSImportNotFound, lease.OperationID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock ready rootfs import operation: %w", err)
	}
	attestation, attestationBytes, artifactDigest, err := validateRootFSImportResult(operation, req.Result)
	if err != nil {
		return nil, err
	}
	if operation.State == RootFSImportStateReady {
		artifact, err := validateReadyRootFSImportRetry(
			ctx, tx, operation, req.Result.DescriptorBytes, req.Result.References,
			attestationBytes, artifactDigest.String(),
		)
		if err != nil {
			return nil, err
		}
		if err := commitRootFSImportTx(ctx, tx, "existing ready publication"); err != nil {
			return nil, err
		}
		return artifact, nil
	}
	if operation.State != RootFSImportStateBuilding || operation.LeaseOwner != lease.WorkerID ||
		operation.LeaseToken != lease.Token {
		return nil, fmt.Errorf("%w: %s", ErrRootFSImportLeaseLost, lease.OperationID)
	}
	for _, reference := range req.Result.References {
		if err := verifyRootFSImportObject(ctx, tx, operation.ID, reference, true); err != nil {
			return nil, err
		}
	}
	artifactInsert, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_base_artifacts (
			artifact_digest, source_oci_ref, source_oci_digest,
			manifest_digest, config_digest, base_block_root,
			format_generation, oci_os, oci_architecture, oci_variant,
			procd_protocol, procd_digest, logical_size_bytes,
			descriptor_digest, state, descriptor, attestation,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, 'ready', $15, $16, NOW(), NOW()
		)
		ON CONFLICT (artifact_digest) DO NOTHING
	`, artifactDigest.String(), attestation.SourceOCIRef, attestation.SourceOCIDigest,
		attestation.ManifestDigest, attestation.ConfigDigest, attestation.BaseBlockRoot,
		attestation.FormatGeneration, attestation.Platform.OS,
		attestation.Platform.Architecture, attestation.Platform.Variant,
		attestation.ProcdProtocol, attestation.ProcdDigest, attestation.LogicalSizeBytes,
		attestation.DescriptorDigest, req.Result.DescriptorBytes, attestationBytes)
	if err != nil {
		return nil, fmt.Errorf("insert ready rootfs base artifact: %w", err)
	}
	artifact, err := scanRootFSBaseArtifact(tx.QueryRow(ctx,
		rootFSBaseArtifactSelectSQL()+" WHERE artifact_digest = $1 FOR UPDATE", artifactDigest.String()))
	if err != nil {
		return nil, fmt.Errorf("read attested rootfs base artifact: %w", err)
	}
	if !rootFSBaseArtifactMatchesAttestation(artifact, req.Result.DescriptorBytes, attestationBytes, attestation) {
		return nil, fmt.Errorf("%w: artifact %s has different immutable attestation",
			ErrRootFSBaseArtifactConflict, artifactDigest)
	}
	if artifactInsert.RowsAffected() == 1 {
		for _, reference := range req.Result.References {
			if _, err := tx.Exec(ctx, `
				INSERT INTO manager.rootfs_base_artifact_objects (artifact_digest, object_key)
				VALUES ($1, $2)
			`, artifact.ArtifactDigest, reference.Key); err != nil {
				return nil, fmt.Errorf("link ready rootfs artifact object %s: %w", reference.Key, err)
			}
		}
	}
	if err := verifyRootFSBaseArtifactObjects(ctx, tx, artifact.ArtifactDigest, req.Result.References); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_import_operations
		SET state = 'ready', lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, result_artifact_digest = $4,
			ready_at = NOW(), updated_at = NOW()
		WHERE operation_id = $1 AND state = 'building'
			AND lease_owner = $2 AND lease_token = $3
			AND lease_expires_at > clock_timestamp()
	`, operation.ID, lease.WorkerID, lease.Token, artifact.ArtifactDigest)
	if err != nil {
		return nil, fmt.Errorf("complete ready rootfs import operation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: %s", ErrRootFSImportLeaseLost, operation.ID)
	}
	if err := commitRootFSImportTx(ctx, tx, "ready publication"); err != nil {
		return nil, err
	}
	return artifact, nil
}

func (s *PGSandboxStore) AbandonRootFSImport(
	ctx context.Context,
	lease RootFSImportLease,
	reason string,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs import store is not configured")
	}
	normalized, err := normalizeRootFSImportLease(lease)
	if err != nil {
		return err
	}
	if reason == "" || reason != strings.TrimSpace(reason) || len(reason) > 4096 {
		return fmt.Errorf("rootfs import abandon reason must contain 1..4096 canonical bytes")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_import_operations
		SET state = 'abandoned', lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, abandon_reason = $4,
			abandoned_at = NOW(), updated_at = NOW()
		WHERE operation_id = $1 AND state = 'building'
			AND lease_owner = $2 AND lease_token = $3
			AND lease_expires_at > clock_timestamp()
	`, normalized.OperationID, normalized.WorkerID, normalized.Token, reason)
	if err != nil {
		return fmt.Errorf("abandon rootfs import operation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", ErrRootFSImportLeaseLost, normalized.OperationID)
	}
	return nil
}

// ReconcileRootFSImportGarbage recovers expired leases and removes bounded
// terminal journals. Ready artifact edges retain live objects; failed-build
// extras are released into the existing deletion queue.
func (s *PGSandboxStore) ReconcileRootFSImportGarbage(
	ctx context.Context,
	terminalRetention time.Duration,
	limit int,
) (*RootFSImportGarbageResult, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs import store is not configured")
	}
	if terminalRetention <= 0 || terminalRetention%time.Millisecond != 0 {
		return nil, fmt.Errorf("rootfs import terminal retention must be positive whole milliseconds")
	}
	if limit <= 0 || limit > MaxRootFSImportListLimit {
		return nil, fmt.Errorf("rootfs import garbage limit must be within 1..%d", MaxRootFSImportListLimit)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin rootfs import garbage reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result := &RootFSImportGarbageResult{}
	tag, err := tx.Exec(ctx, `
		WITH expired AS (
			SELECT operation_id
			FROM manager.rootfs_import_operations
			WHERE state = 'building' AND lease_expires_at <= clock_timestamp()
			ORDER BY lease_expires_at, operation_id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE manager.rootfs_import_operations operation
		SET state = 'pending', lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, updated_at = NOW()
		FROM expired
		WHERE operation.operation_id = expired.operation_id
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("recover expired rootfs import leases: %w", err)
	}
	result.RecoveredLeases = int(tag.RowsAffected())
	rows, err := tx.Query(ctx, `
		SELECT operation_id, state
		FROM manager.rootfs_import_operations
		WHERE state IN ('ready', 'abandoned')
			AND updated_at <= NOW() - ($1::bigint * INTERVAL '1 millisecond')
		ORDER BY updated_at, operation_id
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, terminalRetention.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("list terminal rootfs import operations: %w", err)
	}
	type terminalImport struct{ id, state string }
	terminal := make([]terminalImport, 0)
	for rows.Next() {
		var item terminalImport
		if err := rows.Scan(&item.id, &item.state); err != nil {
			rows.Close()
			return nil, err
		}
		terminal = append(terminal, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate terminal rootfs import operations: %w", err)
	}
	rows.Close()
	for _, operation := range terminal {
		objectRows, err := tx.Query(ctx, `
			SELECT object_key FROM manager.rootfs_import_operation_objects
			WHERE operation_id = $1 ORDER BY object_key
		`, operation.id)
		if err != nil {
			return nil, fmt.Errorf("list terminal rootfs import objects: %w", err)
		}
		var objectKeys []string
		for objectRows.Next() {
			var key string
			if err := objectRows.Scan(&key); err != nil {
				objectRows.Close()
				return nil, err
			}
			objectKeys = append(objectKeys, key)
		}
		if err := objectRows.Err(); err != nil {
			objectRows.Close()
			return nil, err
		}
		objectRows.Close()
		if _, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_import_operations
			WHERE operation_id = $1 AND state = $2
		`, operation.id, operation.state); err != nil {
			return nil, fmt.Errorf("purge terminal rootfs import %s: %w", operation.id, err)
		}
		if operation.state == RootFSImportStateReady {
			result.PurgedReady++
		} else {
			result.PurgedAbandoned++
		}
		for _, objectKey := range objectKeys {
			enqueued, err := releaseUnreferencedRootFSMaterializationObject(ctx, tx, objectKey, "")
			if err != nil {
				return nil, err
			}
			if enqueued {
				result.EnqueuedObjects++
			}
		}
	}
	if err := commitRootFSImportTx(ctx, tx, "garbage reconciliation"); err != nil {
		return nil, err
	}
	return result, nil
}

func validateRootFSImportResult(
	operation *RootFSImportOperation,
	result rootfsimporter.BuildResult,
) (rootfsimporter.ReadyArtifactAttestation, []byte, digest.Digest, error) {
	if operation == nil {
		return rootfsimporter.ReadyArtifactAttestation{}, nil, "", fmt.Errorf("rootfs import operation is required")
	}
	if result.SourceOCIRef != operation.Spec.SourceOCIRef ||
		result.SourceOCIDigest.String() != operation.SourceOCIDigest ||
		result.Platform.OS != operation.Spec.Platform.OS ||
		result.Platform.Architecture != operation.Spec.Platform.Architecture ||
		result.Platform.Variant != operation.Spec.Platform.Variant ||
		result.ProcdDigest.String() != operation.Spec.ProcdDigest ||
		result.LogicalSizeBytes != operation.Spec.LogicalSizeBytes {
		return rootfsimporter.ReadyArtifactAttestation{}, nil, "",
			fmt.Errorf("%w: rootfs import result does not match its durable inputs", ErrRootFSImportConflict)
	}
	prefix := operation.Spec.BlockOptions.ObjectPrefix + "/"
	for _, reference := range result.References {
		if !strings.HasPrefix(reference.Key, prefix) {
			return rootfsimporter.ReadyArtifactAttestation{}, nil, "",
				fmt.Errorf("%w: rootfs import object escaped its operation prefix", ErrRootFSImportConflict)
		}
	}
	attestation, payload, artifactDigest, err := result.Attest(
		operation.Spec.FormatGeneration, operation.Spec.ProcdProtocol,
	)
	if err != nil {
		return rootfsimporter.ReadyArtifactAttestation{}, nil, "", err
	}
	return attestation, payload, artifactDigest, nil
}

func rootFSBaseArtifactMatchesAttestation(
	artifact *RootFSBaseArtifact,
	descriptor, attestationBytes []byte,
	attestation rootfsimporter.ReadyArtifactAttestation,
) bool {
	return artifact != nil && artifact.ArtifactDigest == digest.FromBytes(attestationBytes).String() &&
		artifact.SourceOCIRef == attestation.SourceOCIRef &&
		artifact.SourceOCIDigest == attestation.SourceOCIDigest &&
		artifact.ManifestDigest == attestation.ManifestDigest && artifact.ConfigDigest == attestation.ConfigDigest &&
		artifact.BaseBlockRoot == attestation.BaseBlockRoot &&
		artifact.FormatGeneration == attestation.FormatGeneration &&
		artifact.Platform == (RootFSArtifactPlatform{
			OS: attestation.Platform.OS, Architecture: attestation.Platform.Architecture,
			Variant: attestation.Platform.Variant,
		}) && artifact.ProcdProtocol == attestation.ProcdProtocol &&
		artifact.ProcdDigest == attestation.ProcdDigest &&
		artifact.LogicalSizeBytes == attestation.LogicalSizeBytes &&
		artifact.DescriptorDigest == attestation.DescriptorDigest &&
		artifact.State == RootFSBaseArtifactStateReady &&
		bytes.Equal(artifact.Descriptor, descriptor) && bytes.Equal(artifact.Attestation, attestationBytes)
}

func verifyRootFSBaseArtifactObjects(
	ctx context.Context,
	tx pgx.Tx,
	artifactDigest string,
	references []rootfsblock.ObjectReference,
) error {
	rows, err := tx.Query(ctx, `
		SELECT artifact_object.object_key, object_record.object_kind,
			object_record.object_size, object_record.checksum, object_record.uploaded_at
		FROM manager.rootfs_base_artifact_objects artifact_object
		JOIN manager.rootfs_materialization_objects object_record USING (object_key)
		WHERE artifact_object.artifact_digest = $1
		ORDER BY artifact_object.object_key
	`, artifactDigest)
	if err != nil {
		return fmt.Errorf("list ready rootfs artifact objects: %w", err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		if index >= len(references) {
			return fmt.Errorf("%w: ready rootfs artifact object set has extra entries", ErrRootFSImportConflict)
		}
		var key, kind, checksum string
		var size int64
		var uploadedAt *time.Time
		if err := rows.Scan(&key, &kind, &size, &checksum, &uploadedAt); err != nil {
			return err
		}
		expected := references[index]
		if key != expected.Key || kind != expected.Kind || size != expected.Size ||
			checksum != expected.Checksum || uploadedAt == nil {
			return fmt.Errorf("%w: ready rootfs artifact object set differs", ErrRootFSImportConflict)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if index != len(references) {
		return fmt.Errorf("%w: ready rootfs artifact object set is incomplete", ErrRootFSImportConflict)
	}
	return nil
}

func validateReadyRootFSImportRetry(
	ctx context.Context,
	tx pgx.Tx,
	operation *RootFSImportOperation,
	descriptor []byte,
	references []rootfsblock.ObjectReference,
	attestationBytes []byte,
	artifactDigest string,
) (*RootFSBaseArtifact, error) {
	if operation.ArtifactDigest != artifactDigest {
		return nil, fmt.Errorf("%w: ready rootfs import result changed", ErrRootFSImportConflict)
	}
	attestation, err := rootfsimporter.DecodeReadyArtifactAttestation(attestationBytes)
	if err != nil {
		return nil, err
	}
	artifact, err := scanRootFSBaseArtifact(tx.QueryRow(ctx,
		rootFSBaseArtifactSelectSQL()+" WHERE artifact_digest = $1 FOR UPDATE", artifactDigest))
	if err != nil {
		return nil, fmt.Errorf("read ready rootfs import retry artifact: %w", err)
	}
	if !rootFSBaseArtifactMatchesAttestation(artifact, descriptor, attestationBytes, attestation) {
		return nil, fmt.Errorf("%w: ready rootfs import artifact changed", ErrRootFSImportConflict)
	}
	if err := verifyRootFSBaseArtifactObjects(ctx, tx, artifactDigest, references); err != nil {
		return nil, err
	}
	return artifact, nil
}
