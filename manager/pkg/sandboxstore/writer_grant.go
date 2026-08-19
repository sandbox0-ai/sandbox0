package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	// RootFSWriterBindingVersion must stay wire-compatible with the current
	// rootfshandoff writer binding. It is local to avoid coupling persistence to
	// the node handoff package.
	RootFSWriterBindingVersion = 1
	// RootFSWriterCrashAbandonProofVersion is the canonical node terminal-proof
	// format accepted by the regional crash-recovery transaction.
	RootFSWriterCrashAbandonProofVersion = 1
	// RootFSWriterMaxRenewGrace bounds how long an expired lease may be
	// recovered by the same durable node owner. It is not a fencing timeout.
	RootFSWriterMaxRenewGrace = 5 * time.Second
	// RootFSWriterCrashAbandonGrace prevents an expired lease from being
	// abandoned while an exact owner renewal is still permitted. It is a server
	// policy and is deliberately absent from the request type.
	RootFSWriterCrashAbandonGrace = RootFSWriterMaxRenewGrace

	RootFSWriterGrantStateIssued   = "issued"
	RootFSWriterGrantStateConsumed = "consumed"
	RootFSWriterGrantStateRetiring = "retiring"
	RootFSWriterGrantStateRetired  = "retired"
	RootFSWriterGrantStateCanceled = "canceled"

	RootFSWriterRetireKindPlannedPublish = "planned_publish"
	RootFSWriterRetireKindPrelaunchAbort = "prelaunch_abort"
	RootFSWriterRetireKindCrashAbandon   = "crash_abandon"

	// RootFSWriterCrashAbandonReason is persisted on the aborted lifecycle
	// transaction. It distinguishes recovery from a successful planned pause.
	RootFSWriterCrashAbandonReason = "runtime crashed; reverted to last durable RootFS generation"
)

var (
	ErrRootFSWriterGrantNotFound     = errors.New("rootfs writer grant not found")
	ErrRootFSWriterGrantConflict     = errors.New("rootfs writer grant conflict")
	ErrRootFSWriterGrantInvalidState = errors.New("rootfs writer grant invalid state")
	ErrRootFSWriterGrantExpired      = errors.New("rootfs writer grant expired")
	ErrRootFSWriterLeaseExpired      = errors.New("rootfs writer lease expired")
	ErrRootFSWriterFenceNotMature    = errors.New("rootfs writer fence is not mature")
	ErrRootFSWriterEpochConflict     = errors.New("rootfs writer epoch conflict")
)

// RootFSWriterGrant is the regional single-writer ownership record. The raw
// bearer token is deliberately absent; PostgreSQL stores only its SHA-256.
type RootFSWriterGrant struct {
	ID               string
	FilesystemID     string
	SandboxID        string
	ClaimID          string
	SlotID           string
	IssueOperationID string
	WriterEpoch      int64
	State            string
	// InitialHeadLayerID is the historical column name retained for rolling
	// compatibility. InitialGenerationID is the canonical opaque durable head
	// identity and does not imply a rootfs_layers row.
	InitialHeadLayerID  string
	InitialGenerationID string
	BindingVersion      int
	BindingDigest       []byte
	NodeUID             string
	NodeBootID          string
	PodNamespace        string
	PodName             string
	PodUID              string
	NodeName            string
	GateParent          string
	RuntimeGeneration   string
	// ConsumerNodeUID is durable ownership. ConsumerCtldPodUID records only
	// the first authenticated ctld Pod that completed Consume for audit.
	ConsumerNodeUID    string
	ConsumerCtldPodUID string
	ConsumeExpiresAt   time.Time
	ConsumedAt         time.Time
	LeaseExpiresAt     time.Time
	// AuthorityObservedAt is PostgreSQL NOW() from the same read that returned
	// this grant. It is not persisted in the grant row; authority responses use
	// it to avoid scheduling renewal from an untrusted node clock.
	AuthorityObservedAt time.Time
	RetireOperationID   string
	RetireKind          string
	RetireProofDigest   []byte
	RetireStartedAt     time.Time
	RetiredAt           time.Time
	CanceledAt          time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type rootFSWriterGrantRecord struct {
	RootFSWriterGrant
	tokenDigest []byte
	databaseNow time.Time
}

// IssuedRootFSWriterGrant returns the caller-supplied bearer token without
// persisting it. Exact Issue retries must supply the same token.
type IssuedRootFSWriterGrant struct {
	Grant    *RootFSWriterGrant
	RawToken string
}

type IssueRootFSWriterGrantRequest struct {
	GrantID              string
	SandboxID            string
	ExpectedFilesystemID string
	ClaimID              string
	SlotID               string
	OperationID          string
	RawToken             string
	BindingVersion       int
	BindingDigest        []byte
	NodeUID              string
	NodeBootID           string
	PodNamespace         string
	PodName              string
	PodUID               string
	NodeName             string
	GateParent           string
	RuntimeGeneration    string
	InitialHeadLayerID   string
	InitialGenerationID  string
	ExpectedWriterEpoch  int64
	ConsumeExpiresAt     time.Time
}

type ConsumeRootFSWriterGrantRequest struct {
	GrantID            string
	WriterEpoch        int64
	RawToken           string
	BindingVersion     int
	BindingDigest      []byte
	ConsumerNodeUID    string
	ConsumerCtldPodUID string
	LeaseTTL           time.Duration
}

type CancelRootFSWriterGrantRequest struct {
	GrantID        string
	WriterEpoch    int64
	OperationID    string
	BindingVersion int
	BindingDigest  []byte
}

type BeginRootFSWriterRetireRequest struct {
	GrantID                string
	WriterEpoch            int64
	OperationID            string
	BindingVersion         int
	BindingDigest          []byte
	ExpectedOldHeadLayerID string
}

// BeginRootFSWriterCrashAbandonRequest establishes the regional fencing point
// before the node produces a crash terminal proof. The transition is allowed
// only after the exact consumed lease can no longer renew and the durable head
// is still the generation at which the writer was issued.
type BeginRootFSWriterCrashAbandonRequest struct {
	GrantID                 string
	WriterEpoch             int64
	OperationID             string
	BindingVersion          int
	BindingDigest           []byte
	NodeUID                 string
	NodeBootID              string
	ExpectedOldGenerationID string
}

// CompleteRootFSWriterPrelaunchAbortRequest retires a consumed grant without
// publishing a new head. It is valid only after the authenticated node has
// proved the A-to-B transition never committed and the local handoff is fully
// tombstoned. The method is transaction-only so callers also hold the sandbox
// lifecycle lock while removing regional ownership.
type CompleteRootFSWriterPrelaunchAbortRequest struct {
	GrantID                string
	WriterEpoch            int64
	OperationID            string
	BindingVersion         int
	BindingDigest          []byte
	ProofDigest            []byte
	ExpectedOldHeadLayerID string
}

// CompleteRootFSWriterCrashAbandonRequest terminally abandons an unsealed
// writer after its lease is no longer renewable and an authenticated node has
// durably proved that every physical writer is gone. ProofDigest is evidence
// already validated by the caller; lease expiry by itself is never evidence.
//
// The transaction preserves ExpectedOldGenerationID as the durable head,
// pauses the exact crashed runtime, and aborts its lifecycle transaction with
// RootFSWriterCrashAbandonReason. It never reports a successful planned pause.
type CompleteRootFSWriterCrashAbandonRequest struct {
	LifecycleTxnID          string
	GrantID                 string
	WriterEpoch             int64
	OperationID             string
	BindingVersion          int
	BindingDigest           []byte
	ProofVersion            int
	ProofDigest             []byte
	NodeUID                 string
	NodeBootID              string
	ExpectedOldGenerationID string
}

// CompleteRootFSWriterRetireAndPublishRequest binds the detach-and-seal proof
// to one immutable layer and the filesystem head observed when the writer was
// issued. Lifecycle state remains in sandbox_lifecycle_txns; callers can use
// this method through RootFSWriterGrantTx and commit that txn atomically.
type CompleteRootFSWriterRetireAndPublishRequest struct {
	LifecycleTxnID         string
	GrantID                string
	WriterEpoch            int64
	OperationID            string
	BindingVersion         int
	BindingDigest          []byte
	ProofDigest            []byte
	ExpectedOldHeadLayerID string
	RootFSState            *SandboxRootFSState
}

// CompleteRootFSWriterRetireAndPublishGenerationRequest publishes one sealed
// block-COW generation. It is separate from the legacy layer request so the
// two storage formats cannot accidentally share a head mutation path.
type CompleteRootFSWriterRetireAndPublishGenerationRequest struct {
	LifecycleTxnID          string
	GrantID                 string
	WriterEpoch             int64
	OperationID             string
	BindingVersion          int
	BindingDigest           []byte
	ProofDigest             []byte
	ExpectedOldGenerationID string
	Generation              *RootFSGeneration
}

type RenewRootFSWriterGrantRequest struct {
	GrantID         string
	WriterEpoch     int64
	BindingVersion  int
	BindingDigest   []byte
	ConsumerNodeUID string
}

// RenewRootFSWriterGrantResult preserves request order while allowing one
// stale grant to fail without rolling back leases renewed for other grants in
// the same authenticated node batch.
type RenewRootFSWriterGrantResult struct {
	Grant *RootFSWriterGrant
	Err   error
}

// RootFSWriterLeaseRenewalPolicy is supplied by trusted manager
// configuration, never by the ctld request body.
type RootFSWriterLeaseRenewalPolicy struct {
	LeaseTTL    time.Duration
	GracePeriod time.Duration
}

// RootFSWriterGrantStore is kept separate from SandboxStore so consumers that
// do not participate in the new writer lane do not acquire placeholder state.
type RootFSWriterGrantStore interface {
	IssueRootFSWriterGrant(context.Context, *IssueRootFSWriterGrantRequest) (*IssuedRootFSWriterGrant, error)
	ConsumeRootFSWriterGrant(context.Context, *ConsumeRootFSWriterGrantRequest) (*RootFSWriterGrant, error)
	CancelRootFSWriterGrant(context.Context, *CancelRootFSWriterGrantRequest) (*RootFSWriterGrant, error)
	BeginRootFSWriterRetire(context.Context, *BeginRootFSWriterRetireRequest) (*RootFSWriterGrant, error)
	RenewRootFSWriterGrant(context.Context, *RenewRootFSWriterGrantRequest, RootFSWriterLeaseRenewalPolicy) (*RootFSWriterGrant, error)
	GetRootFSWriterGrant(context.Context, string) (*RootFSWriterGrant, error)
	ListExpiredRootFSWriterGrants(context.Context, int) ([]*RootFSWriterGrant, error)
}

// RootFSWriterGrantBatchStore amortizes the PostgreSQL transaction and query
// cost of node-level lease renewal. It is separate from the base interface so
// small test stores and callers that do not need the high-density path do not
// need placeholder implementations.
type RootFSWriterGrantBatchStore interface {
	RenewRootFSWriterGrants(context.Context, []*RenewRootFSWriterGrantRequest, RootFSWriterLeaseRenewalPolicy) ([]RenewRootFSWriterGrantResult, error)
}

// RootFSWriterGrantTx exposes the same transitions inside an existing locked
// sandbox transaction so lifecycle state and writer retirement can later be
// committed atomically without copying lifecycle truth into the grant row.
type RootFSWriterGrantTx interface {
	IssueRootFSWriterGrant(context.Context, *IssueRootFSWriterGrantRequest) (*IssuedRootFSWriterGrant, error)
	ConsumeRootFSWriterGrant(context.Context, *ConsumeRootFSWriterGrantRequest) (*RootFSWriterGrant, error)
	CancelRootFSWriterGrant(context.Context, *CancelRootFSWriterGrantRequest) (*RootFSWriterGrant, error)
	BeginRootFSWriterRetire(context.Context, *BeginRootFSWriterRetireRequest) (*RootFSWriterGrant, error)
	BeginRootFSWriterPrelaunchAbort(context.Context, *BeginRootFSWriterRetireRequest) (*RootFSWriterGrant, error)
	RenewRootFSWriterGrant(context.Context, *RenewRootFSWriterGrantRequest, RootFSWriterLeaseRenewalPolicy) (*RootFSWriterGrant, error)
	CompleteRootFSWriterRetireAndPublish(context.Context, *CompleteRootFSWriterRetireAndPublishRequest) (*RootFSWriterGrant, error)
	CompleteRootFSWriterRetireAndPublishGeneration(context.Context, *CompleteRootFSWriterRetireAndPublishGenerationRequest) (*RootFSWriterGrant, error)
	CompleteRootFSWriterPrelaunchAbort(context.Context, *CompleteRootFSWriterPrelaunchAbortRequest) (*RootFSWriterGrant, error)
}

// RootFSWriterCrashAbandonStore exposes only the regional fencing point. Node
// proof collection happens after Begin and before the transaction-only Complete.
type RootFSWriterCrashAbandonStore interface {
	BeginRootFSWriterCrashAbandon(context.Context, *BeginRootFSWriterCrashAbandonRequest) (*RootFSWriterGrant, error)
}

// RootFSWriterCrashAbandonTx completes a fenced crash recovery under the
// existing sandbox lock so runtime and lifecycle state change atomically.
type RootFSWriterCrashAbandonTx interface {
	CompleteRootFSWriterCrashAbandon(context.Context, *CompleteRootFSWriterCrashAbandonRequest) (*RootFSWriterGrant, error)
}

var _ RootFSWriterGrantStore = (*PGSandboxStore)(nil)
var _ RootFSWriterGrantBatchStore = (*PGSandboxStore)(nil)
var _ RootFSWriterGrantTx = sandboxStoreTx{}
var _ RootFSWriterCrashAbandonStore = (*PGSandboxStore)(nil)
var _ RootFSWriterCrashAbandonTx = sandboxStoreTx{}

type rootFSWriterGrantDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *PGSandboxStore) IssueRootFSWriterGrant(ctx context.Context, req *IssueRootFSWriterGrantRequest) (*IssuedRootFSWriterGrant, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var result *IssuedRootFSWriterGrant
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = issueRootFSWriterGrant(ctx, tx, req)
		return err
	})
	return result, err
}

func (t sandboxStoreTx) IssueRootFSWriterGrant(ctx context.Context, req *IssueRootFSWriterGrantRequest) (*IssuedRootFSWriterGrant, error) {
	return issueRootFSWriterGrant(ctx, t.tx, req)
}

func (s *PGSandboxStore) ConsumeRootFSWriterGrant(ctx context.Context, req *ConsumeRootFSWriterGrantRequest) (*RootFSWriterGrant, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var result *RootFSWriterGrant
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = consumeRootFSWriterGrant(ctx, tx, req)
		return err
	})
	return result, err
}

func (t sandboxStoreTx) ConsumeRootFSWriterGrant(ctx context.Context, req *ConsumeRootFSWriterGrantRequest) (*RootFSWriterGrant, error) {
	return consumeRootFSWriterGrant(ctx, t.tx, req)
}

func (s *PGSandboxStore) CancelRootFSWriterGrant(ctx context.Context, req *CancelRootFSWriterGrantRequest) (*RootFSWriterGrant, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var result *RootFSWriterGrant
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = cancelRootFSWriterGrant(ctx, tx, req)
		return err
	})
	return result, err
}

func (t sandboxStoreTx) CancelRootFSWriterGrant(ctx context.Context, req *CancelRootFSWriterGrantRequest) (*RootFSWriterGrant, error) {
	return cancelRootFSWriterGrant(ctx, t.tx, req)
}

func (s *PGSandboxStore) BeginRootFSWriterRetire(ctx context.Context, req *BeginRootFSWriterRetireRequest) (*RootFSWriterGrant, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var result *RootFSWriterGrant
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = beginRootFSWriterRetire(ctx, tx, req, RootFSWriterRetireKindPlannedPublish)
		return err
	})
	return result, err
}

func (t sandboxStoreTx) BeginRootFSWriterRetire(ctx context.Context, req *BeginRootFSWriterRetireRequest) (*RootFSWriterGrant, error) {
	return beginRootFSWriterRetire(ctx, t.tx, req, RootFSWriterRetireKindPlannedPublish)
}

func (t sandboxStoreTx) BeginRootFSWriterPrelaunchAbort(ctx context.Context, req *BeginRootFSWriterRetireRequest) (*RootFSWriterGrant, error) {
	return beginRootFSWriterRetire(ctx, t.tx, req, RootFSWriterRetireKindPrelaunchAbort)
}

func (s *PGSandboxStore) BeginRootFSWriterCrashAbandon(ctx context.Context, req *BeginRootFSWriterCrashAbandonRequest) (*RootFSWriterGrant, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var result *RootFSWriterGrant
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = beginRootFSWriterCrashAbandon(ctx, tx, req)
		return err
	})
	return result, err
}

func (s *PGSandboxStore) RenewRootFSWriterGrant(ctx context.Context, req *RenewRootFSWriterGrantRequest, policy RootFSWriterLeaseRenewalPolicy) (*RootFSWriterGrant, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var result *RootFSWriterGrant
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = renewRootFSWriterGrant(ctx, tx, req, policy)
		return err
	})
	return result, err
}

// RenewRootFSWriterGrants renews a node batch with one PostgreSQL transaction
// and three bounded queries regardless of batch size. Individual stale or
// mismatched grants are reported per item and do not roll back valid renewals.
func (s *PGSandboxStore) RenewRootFSWriterGrants(
	ctx context.Context,
	requests []*RenewRootFSWriterGrantRequest,
	policy RootFSWriterLeaseRenewalPolicy,
) ([]RenewRootFSWriterGrantResult, error) {
	if s == nil || s.pool == nil || len(requests) == 0 {
		return nil, nil
	}
	var results []RenewRootFSWriterGrantResult
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		var err error
		results, err = renewRootFSWriterGrants(ctx, tx, requests, policy)
		return err
	})
	return results, err
}

func (t sandboxStoreTx) RenewRootFSWriterGrant(ctx context.Context, req *RenewRootFSWriterGrantRequest, policy RootFSWriterLeaseRenewalPolicy) (*RootFSWriterGrant, error) {
	return renewRootFSWriterGrant(ctx, t.tx, req, policy)
}

func (t sandboxStoreTx) CompleteRootFSWriterRetireAndPublish(ctx context.Context, req *CompleteRootFSWriterRetireAndPublishRequest) (*RootFSWriterGrant, error) {
	return completeRootFSWriterRetireAndPublish(ctx, t.tx, req)
}

func (t sandboxStoreTx) CompleteRootFSWriterRetireAndPublishGeneration(
	ctx context.Context,
	req *CompleteRootFSWriterRetireAndPublishGenerationRequest,
) (*RootFSWriterGrant, error) {
	return completeRootFSWriterRetireAndPublishGeneration(ctx, t.tx, req)
}

func (t sandboxStoreTx) CompleteRootFSWriterPrelaunchAbort(ctx context.Context, req *CompleteRootFSWriterPrelaunchAbortRequest) (*RootFSWriterGrant, error) {
	return completeRootFSWriterPrelaunchAbort(ctx, t.tx, req)
}

func (t sandboxStoreTx) CompleteRootFSWriterCrashAbandon(ctx context.Context, req *CompleteRootFSWriterCrashAbandonRequest) (*RootFSWriterGrant, error) {
	return completeRootFSWriterCrashAbandon(ctx, t, req)
}

func (s *PGSandboxStore) GetRootFSWriterGrant(ctx context.Context, grantID string) (*RootFSWriterGrant, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(grantID) == "" {
		return nil, nil
	}
	record, err := getRootFSWriterGrant(ctx, s.pool, strings.TrimSpace(grantID))
	if err != nil {
		return nil, err
	}
	return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
}

func (s *PGSandboxStore) ListExpiredRootFSWriterGrants(ctx context.Context, limit int) ([]*RootFSWriterGrant, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, rootFSWriterGrantSelectSQL()+`
		WHERE state IN ($1, $2)
			AND lease_expires_at IS NOT NULL
			AND lease_expires_at <= NOW()
		ORDER BY lease_expires_at ASC
		LIMIT $3
	`, RootFSWriterGrantStateConsumed, RootFSWriterGrantStateRetiring, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired rootfs writer grants: %w", err)
	}
	defer rows.Close()
	grants := make([]*RootFSWriterGrant, 0)
	for rows.Next() {
		record, err := scanRootFSWriterGrant(rows)
		if err != nil {
			return nil, err
		}
		grants = append(grants, cloneRootFSWriterGrant(&record.RootFSWriterGrant))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired rootfs writer grants: %w", err)
	}
	return grants, nil
}

func (s *PGSandboxStore) withRootFSWriterGrantTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs writer grant tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs writer grant tx: %w", err)
	}
	return nil
}

func issueRootFSWriterGrant(ctx context.Context, db rootFSWriterGrantDB, req *IssueRootFSWriterGrantRequest) (*IssuedRootFSWriterGrant, error) {
	normalized, tokenDigest, err := validateIssueRootFSWriterGrantRequest(req)
	if err != nil {
		return nil, err
	}

	var teamID string
	if err := db.QueryRow(ctx, `
		SELECT team_id
		FROM manager.sandboxes
		WHERE sandbox_id = $1
			AND deleted_at IS NULL
		FOR UPDATE
	`, normalized.SandboxID).Scan(&teamID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, normalized.SandboxID)
		}
		return nil, fmt.Errorf("lock sandbox for rootfs writer grant: %w", err)
	}

	filesystemID, err := ensureRootFSWriterFilesystem(ctx, db, normalized.SandboxID, teamID)
	if err != nil {
		return nil, err
	}
	if normalized.ExpectedFilesystemID != "" && normalized.ExpectedFilesystemID != filesystemID {
		return nil, fmt.Errorf("%w: expected filesystem %s, got %s", ErrRootFSWriterGrantConflict, normalized.ExpectedFilesystemID, filesystemID)
	}

	var currentHead, filesystemTeamID string
	var currentEpoch int64
	if err := db.QueryRow(ctx, `
		SELECT COALESCE(head_generation_id, head_layer_id, ''), writer_epoch, team_id
		FROM manager.rootfs_filesystems
		WHERE filesystem_id = $1
		FOR UPDATE
	`, filesystemID).Scan(&currentHead, &currentEpoch, &filesystemTeamID); err != nil {
		return nil, fmt.Errorf("lock rootfs filesystem for writer grant: %w", err)
	}
	if filesystemTeamID != teamID {
		return nil, fmt.Errorf("%w: filesystem team %s does not match sandbox team %s", ErrRootFSWriterGrantConflict, filesystemTeamID, teamID)
	}

	existing, err := getRootFSWriterGrantByOperation(ctx, db, normalized.OperationID)
	if err == nil {
		if existing.FilesystemID != filesystemID || !rootFSWriterGrantMatchesIssue(existing, normalized, tokenDigest[:]) {
			return nil, fmt.Errorf("%w: issue operation %s has different immutable fields", ErrRootFSWriterGrantConflict, normalized.OperationID)
		}
		return &IssuedRootFSWriterGrant{Grant: cloneRootFSWriterGrant(&existing.RootFSWriterGrant), RawToken: normalized.RawToken}, nil
	}
	if !errors.Is(err, ErrRootFSWriterGrantNotFound) {
		return nil, err
	}

	if currentHead != normalized.InitialGenerationID {
		return nil, fmt.Errorf("%w: expected generation %q, got %q", ErrRootFSWriterGrantConflict, normalized.InitialGenerationID, currentHead)
	}
	if currentEpoch != normalized.ExpectedWriterEpoch {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrRootFSWriterEpochConflict, normalized.ExpectedWriterEpoch, currentEpoch)
	}
	var live bool
	if err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.rootfs_writer_grants
			WHERE filesystem_id = $1
				AND state IN ($2, $3, $4)
		)
	`, filesystemID, RootFSWriterGrantStateIssued, RootFSWriterGrantStateConsumed, RootFSWriterGrantStateRetiring).Scan(&live); err != nil {
		return nil, fmt.Errorf("check live rootfs writer grant: %w", err)
	}
	if live {
		return nil, fmt.Errorf("%w: filesystem %s already has a live writer", ErrRootFSWriterGrantConflict, filesystemID)
	}

	var writerEpoch int64
	if err := db.QueryRow(ctx, `
		UPDATE manager.rootfs_filesystems
		SET writer_epoch = writer_epoch + 1,
			updated_at = NOW()
		WHERE filesystem_id = $1
			AND writer_epoch = $2
		RETURNING writer_epoch
	`, filesystemID, normalized.ExpectedWriterEpoch).Scan(&writerEpoch); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: expected %d", ErrRootFSWriterEpochConflict, normalized.ExpectedWriterEpoch)
		}
		return nil, fmt.Errorf("advance rootfs writer epoch: %w", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO manager.rootfs_writer_grants (
			grant_id, filesystem_id, sandbox_id, claim_id, slot_id,
			issue_operation_id, writer_epoch, state, initial_head_layer_id, initial_generation_id,
			binding_version, binding_digest, token_digest, node_uid, node_boot_id,
			runtime_pod_namespace, runtime_pod_name, runtime_pod_uid,
			runtime_node_name, runtime_gate_parent, runtime_generation,
			consume_expires_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
			$16, $17, $18, $19, $20, $21, $22, NOW(), NOW())
	`, normalized.GrantID, filesystemID, normalized.SandboxID, normalized.ClaimID, normalized.SlotID,
		normalized.OperationID, writerEpoch, RootFSWriterGrantStateIssued, normalized.InitialHeadLayerID, normalized.InitialGenerationID,
		normalized.BindingVersion, normalized.BindingDigest, tokenDigest[:], normalized.NodeUID,
		normalized.NodeBootID, normalized.PodNamespace, normalized.PodName, normalized.PodUID,
		normalized.NodeName, normalized.GateParent, normalized.RuntimeGeneration, normalized.ConsumeExpiresAt)
	if err != nil {
		return nil, mapRootFSWriterGrantConflict("insert rootfs writer grant", err)
	}
	record, err := getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	return &IssuedRootFSWriterGrant{Grant: cloneRootFSWriterGrant(&record.RootFSWriterGrant), RawToken: normalized.RawToken}, nil
}

func ensureRootFSWriterFilesystem(ctx context.Context, db rootFSWriterGrantDB, sandboxID, teamID string) (string, error) {
	var filesystemID string
	err := db.QueryRow(ctx, `
		SELECT filesystem_id
		FROM manager.sandbox_rootfs_bindings
		WHERE sandbox_id = $1
	`, sandboxID).Scan(&filesystemID)
	if err == nil {
		return filesystemID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("load sandbox rootfs binding for writer grant: %w", err)
	}
	filesystemID = sandboxID
	if _, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (
			filesystem_id, team_id, created_at, updated_at
		)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (filesystem_id) DO NOTHING
	`, filesystemID, teamID); err != nil {
		return "", fmt.Errorf("ensure rootfs filesystem for writer grant: %w", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO manager.sandbox_rootfs_bindings (
			sandbox_id, filesystem_id, team_id, created_at, updated_at
		)
		VALUES ($1, $2, $3, NOW(), NOW())
		ON CONFLICT (sandbox_id) DO NOTHING
	`, sandboxID, filesystemID, teamID); err != nil {
		return "", fmt.Errorf("ensure sandbox rootfs binding for writer grant: %w", err)
	}
	if err := db.QueryRow(ctx, `
		SELECT filesystem_id
		FROM manager.sandbox_rootfs_bindings
		WHERE sandbox_id = $1
	`, sandboxID).Scan(&filesystemID); err != nil {
		return "", fmt.Errorf("reload sandbox rootfs binding for writer grant: %w", err)
	}
	return filesystemID, nil
}

func consumeRootFSWriterGrant(ctx context.Context, db rootFSWriterGrantDB, req *ConsumeRootFSWriterGrantRequest) (*RootFSWriterGrant, error) {
	normalized, tokenDigest, err := validateConsumeRootFSWriterGrantRequest(req)
	if err != nil {
		return nil, err
	}
	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants AS g
		SET state = $2,
			consumer_node_uid = $3,
			consumer_ctld_pod_uid = $4,
			consumed_at = NOW(),
			lease_expires_at = NOW() + ($5::bigint * INTERVAL '1 millisecond'),
			updated_at = NOW()
		FROM manager.rootfs_filesystems AS filesystem
		WHERE g.grant_id = $1
			AND g.state = $6
			AND g.writer_epoch = $7
			AND g.token_digest = $8
			AND g.binding_digest = $9
			AND g.binding_version = $10
			AND g.node_uid = $3
			AND g.consume_expires_at > NOW()
			AND filesystem.filesystem_id = g.filesystem_id
			AND filesystem.writer_epoch = g.writer_epoch
	`, normalized.GrantID, RootFSWriterGrantStateConsumed, normalized.ConsumerNodeUID,
		normalized.ConsumerCtldPodUID, normalized.LeaseTTL.Milliseconds(), RootFSWriterGrantStateIssued,
		normalized.WriterEpoch, tokenDigest[:], normalized.BindingDigest, normalized.BindingVersion)
	if err != nil {
		return nil, fmt.Errorf("consume rootfs writer grant: %w", err)
	}
	record, err := getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() > 0 {
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if !rootFSWriterGrantMatchesConsume(record, normalized, tokenDigest[:]) {
		return nil, fmt.Errorf("%w: consume request does not match grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.State == RootFSWriterGrantStateIssued && !record.ConsumeExpiresAt.After(record.databaseNow) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSWriterGrantExpired, normalized.GrantID)
	}
	if record.State == RootFSWriterGrantStateConsumed && record.ConsumerNodeUID == normalized.ConsumerNodeUID {
		if !record.LeaseExpiresAt.After(record.databaseNow) {
			return nil, fmt.Errorf("%w: %s", ErrRootFSWriterLeaseExpired, normalized.GrantID)
		}
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if currentEpoch, epochErr := getRootFSWriterEpoch(ctx, db, record.FilesystemID); epochErr == nil && currentEpoch != normalized.WriterEpoch {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrRootFSWriterEpochConflict, normalized.WriterEpoch, currentEpoch)
	}
	return nil, rootFSWriterGrantStateError(record)
}

func renewRootFSWriterGrant(
	ctx context.Context,
	db rootFSWriterGrantDB,
	req *RenewRootFSWriterGrantRequest,
	policy RootFSWriterLeaseRenewalPolicy,
) (*RootFSWriterGrant, error) {
	normalized, err := validateRenewRootFSWriterGrantRequest(req, policy)
	if err != nil {
		return nil, err
	}
	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants AS g
		SET lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 millisecond'),
			updated_at = NOW()
		FROM manager.rootfs_filesystems AS filesystem
		WHERE g.grant_id = $1
			AND g.state = $3
			AND g.writer_epoch = $4
			AND g.binding_version = $5
			AND g.binding_digest = $6
			AND g.node_uid = $7
			AND g.consumer_node_uid = $7
			AND g.lease_expires_at > NOW() - ($8::bigint * INTERVAL '1 millisecond')
			AND filesystem.filesystem_id = g.filesystem_id
			AND filesystem.writer_epoch = g.writer_epoch
	`, normalized.GrantID, policy.LeaseTTL.Milliseconds(), RootFSWriterGrantStateConsumed,
		normalized.WriterEpoch, normalized.BindingVersion,
		normalized.BindingDigest, normalized.ConsumerNodeUID, policy.GracePeriod.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("renew rootfs writer grant: %w", err)
	}
	record, err := getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if !rootFSWriterGrantMatchesRenew(record, normalized) {
		return nil, fmt.Errorf("%w: renew request does not match grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.State != RootFSWriterGrantStateConsumed {
		return nil, rootFSWriterGrantStateError(record)
	}
	if record.ConsumerNodeUID != normalized.ConsumerNodeUID {
		return nil, fmt.Errorf("%w: node %s does not own grant %s", ErrRootFSWriterGrantConflict,
			normalized.ConsumerNodeUID, normalized.GrantID)
	}
	if currentEpoch, epochErr := getRootFSWriterEpoch(ctx, db, record.FilesystemID); epochErr == nil && currentEpoch != normalized.WriterEpoch {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrRootFSWriterEpochConflict, normalized.WriterEpoch, currentEpoch)
	}
	if !record.LeaseExpiresAt.Add(policy.GracePeriod).After(record.databaseNow) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSWriterLeaseExpired, normalized.GrantID)
	}
	return nil, rootFSWriterGrantStateError(record)
}

func renewRootFSWriterGrants(
	ctx context.Context,
	db rootFSWriterGrantDB,
	requests []*RenewRootFSWriterGrantRequest,
	policy RootFSWriterLeaseRenewalPolicy,
) ([]RenewRootFSWriterGrantResult, error) {
	results := make([]RenewRootFSWriterGrantResult, len(requests))
	normalized := make([]*RenewRootFSWriterGrantRequest, len(requests))
	grantIDs := make([]string, 0, len(requests))
	epochs := make([]int64, 0, len(requests))
	versions := make([]int32, 0, len(requests))
	digests := make([][]byte, 0, len(requests))
	nodeUIDs := make([]string, 0, len(requests))
	requestCounts := make(map[string]int, len(requests))
	for index, request := range requests {
		value, err := validateRenewRootFSWriterGrantRequest(request, policy)
		if err != nil {
			results[index].Err = err
			continue
		}
		normalized[index] = value
		requestCounts[value.GrantID]++
	}
	for index, value := range normalized {
		if value == nil || results[index].Err != nil {
			continue
		}
		if requestCounts[value.GrantID] != 1 {
			results[index].Err = fmt.Errorf("duplicate grant_id %q in renewal batch", value.GrantID)
			continue
		}
		grantIDs = append(grantIDs, value.GrantID)
		epochs = append(epochs, value.WriterEpoch)
		versions = append(versions, int32(value.BindingVersion))
		digests = append(digests, value.BindingDigest)
		nodeUIDs = append(nodeUIDs, value.ConsumerNodeUID)
	}
	if len(grantIDs) == 0 {
		return results, nil
	}

	rows, err := db.Query(ctx, `
		WITH input AS (
			SELECT *
			FROM unnest($1::text[], $2::bigint[], $3::integer[], $4::bytea[], $5::text[])
				AS value(grant_id, writer_epoch, binding_version, binding_digest, consumer_node_uid)
		), authority_clock AS (
			SELECT NOW() AS observed_at
		)
		UPDATE manager.rootfs_writer_grants AS g
		SET lease_expires_at = authority_clock.observed_at + ($6::bigint * INTERVAL '1 millisecond'),
			updated_at = authority_clock.observed_at
		FROM input, authority_clock, manager.rootfs_filesystems AS filesystem
		WHERE g.grant_id = input.grant_id
			AND g.state = $7
			AND g.writer_epoch = input.writer_epoch
			AND g.binding_version = input.binding_version
			AND g.binding_digest = input.binding_digest
			AND g.node_uid = input.consumer_node_uid
			AND g.consumer_node_uid = input.consumer_node_uid
			AND g.lease_expires_at > authority_clock.observed_at - ($8::bigint * INTERVAL '1 millisecond')
			AND filesystem.filesystem_id = g.filesystem_id
			AND filesystem.writer_epoch = g.writer_epoch
		RETURNING g.grant_id
	`, grantIDs, epochs, versions, digests, nodeUIDs, policy.LeaseTTL.Milliseconds(),
		RootFSWriterGrantStateConsumed, policy.GracePeriod.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("renew rootfs writer grant batch: %w", err)
	}
	updated := make(map[string]struct{}, len(grantIDs))
	for rows.Next() {
		var grantID string
		if err := rows.Scan(&grantID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan renewed rootfs writer grant batch: %w", err)
		}
		updated[grantID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate renewed rootfs writer grant batch: %w", err)
	}
	rows.Close()

	recordRows, err := db.Query(ctx, rootFSWriterGrantSelectSQL()+` WHERE grant_id = ANY($1::text[])`, grantIDs)
	if err != nil {
		return nil, fmt.Errorf("load renewed rootfs writer grant batch: %w", err)
	}
	records := make(map[string]*rootFSWriterGrantRecord, len(grantIDs))
	filesystemIDs := make([]string, 0, len(grantIDs))
	for recordRows.Next() {
		record, scanErr := scanRootFSWriterGrant(recordRows)
		if scanErr != nil {
			recordRows.Close()
			return nil, scanErr
		}
		records[record.ID] = record
		filesystemIDs = append(filesystemIDs, record.FilesystemID)
	}
	if err := recordRows.Err(); err != nil {
		recordRows.Close()
		return nil, fmt.Errorf("iterate renewed rootfs writer grants: %w", err)
	}
	recordRows.Close()

	currentEpochs := make(map[string]int64, len(filesystemIDs))
	if len(filesystemIDs) != 0 {
		epochRows, queryErr := db.Query(ctx, `
			SELECT filesystem_id, writer_epoch
			FROM manager.rootfs_filesystems
			WHERE filesystem_id = ANY($1::text[])
		`, filesystemIDs)
		if queryErr != nil {
			return nil, fmt.Errorf("load rootfs writer epochs for renewal batch: %w", queryErr)
		}
		for epochRows.Next() {
			var filesystemID string
			var epoch int64
			if scanErr := epochRows.Scan(&filesystemID, &epoch); scanErr != nil {
				epochRows.Close()
				return nil, fmt.Errorf("scan rootfs writer epoch for renewal batch: %w", scanErr)
			}
			currentEpochs[filesystemID] = epoch
		}
		if queryErr := epochRows.Err(); queryErr != nil {
			epochRows.Close()
			return nil, fmt.Errorf("iterate rootfs writer epochs for renewal batch: %w", queryErr)
		}
		epochRows.Close()
	}

	for index, request := range normalized {
		if request == nil || results[index].Err != nil {
			continue
		}
		record := records[request.GrantID]
		if record == nil {
			results[index].Err = fmt.Errorf("%w: %s", ErrRootFSWriterGrantNotFound, request.GrantID)
			continue
		}
		if _, ok := updated[request.GrantID]; ok {
			results[index].Grant = cloneRootFSWriterGrant(&record.RootFSWriterGrant)
			continue
		}
		results[index].Err = classifyRootFSWriterGrantRenewal(record, request, policy, currentEpochs[record.FilesystemID])
	}
	return results, nil
}

func classifyRootFSWriterGrantRenewal(
	record *rootFSWriterGrantRecord,
	request *RenewRootFSWriterGrantRequest,
	policy RootFSWriterLeaseRenewalPolicy,
	currentEpoch int64,
) error {
	if !rootFSWriterGrantMatchesRenew(record, request) {
		return fmt.Errorf("%w: renew request does not match grant %s", ErrRootFSWriterGrantConflict, request.GrantID)
	}
	if record.State != RootFSWriterGrantStateConsumed {
		return rootFSWriterGrantStateError(record)
	}
	if record.ConsumerNodeUID != request.ConsumerNodeUID {
		return fmt.Errorf("%w: node %s does not own grant %s", ErrRootFSWriterGrantConflict,
			request.ConsumerNodeUID, request.GrantID)
	}
	if currentEpoch != 0 && currentEpoch != request.WriterEpoch {
		return fmt.Errorf("%w: expected %d, got %d", ErrRootFSWriterEpochConflict, request.WriterEpoch, currentEpoch)
	}
	if !record.LeaseExpiresAt.Add(policy.GracePeriod).After(record.databaseNow) {
		return fmt.Errorf("%w: %s", ErrRootFSWriterLeaseExpired, request.GrantID)
	}
	return rootFSWriterGrantStateError(record)
}

func cancelRootFSWriterGrant(ctx context.Context, db rootFSWriterGrantDB, req *CancelRootFSWriterGrantRequest) (*RootFSWriterGrant, error) {
	normalized, err := validateCancelRootFSWriterGrantRequest(req)
	if err != nil {
		return nil, err
	}
	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET state = $2,
			canceled_at = NOW(),
			updated_at = NOW()
		WHERE grant_id = $1
			AND state = $3
			AND writer_epoch = $4
			AND issue_operation_id = $5
			AND binding_version = $6
			AND binding_digest = $7
	`, normalized.GrantID, RootFSWriterGrantStateCanceled, RootFSWriterGrantStateIssued,
		normalized.WriterEpoch, normalized.OperationID, normalized.BindingVersion, normalized.BindingDigest)
	if err != nil {
		return nil, fmt.Errorf("cancel rootfs writer grant: %w", err)
	}
	record, err := getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesCancel(record, normalized) {
		return nil, fmt.Errorf("%w: cancel request does not match grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if tag.RowsAffected() > 0 || record.State == RootFSWriterGrantStateCanceled {
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	return nil, rootFSWriterGrantStateError(record)
}

func beginRootFSWriterRetire(
	ctx context.Context,
	db rootFSWriterGrantDB,
	req *BeginRootFSWriterRetireRequest,
	retireKind string,
) (*RootFSWriterGrant, error) {
	normalized, err := validateBeginRootFSWriterRetireRequest(req)
	if err != nil {
		return nil, err
	}
	if retireKind != RootFSWriterRetireKindPlannedPublish && retireKind != RootFSWriterRetireKindPrelaunchAbort {
		return nil, fmt.Errorf("unsupported rootfs writer retire kind %q", retireKind)
	}
	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants AS g
		SET state = $2,
			retire_operation_id = $3,
			retire_kind = $9,
			retire_started_at = NOW(),
			updated_at = NOW()
		FROM manager.rootfs_filesystems AS filesystem
		WHERE g.grant_id = $1
			AND g.state = $4
			AND g.writer_epoch = $5
			AND g.binding_version = $6
			AND g.binding_digest = $7
			AND g.initial_generation_id = $8
			AND g.lease_expires_at > NOW()
			AND filesystem.filesystem_id = g.filesystem_id
			AND filesystem.writer_epoch = g.writer_epoch
			AND COALESCE(filesystem.head_generation_id, filesystem.head_layer_id, '') = $8
	`, normalized.GrantID, RootFSWriterGrantStateRetiring, normalized.OperationID,
		RootFSWriterGrantStateConsumed, normalized.WriterEpoch, normalized.BindingVersion,
		normalized.BindingDigest, normalized.ExpectedOldHeadLayerID, retireKind)
	if err != nil {
		return nil, mapRootFSWriterGrantConflict("begin rootfs writer retire", err)
	}
	record, err := getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesRetireBase(record, normalized.GrantID, normalized.WriterEpoch, normalized.BindingVersion, normalized.BindingDigest) {
		return nil, fmt.Errorf("%w: retire request does not match grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.InitialGenerationID != normalized.ExpectedOldHeadLayerID {
		return nil, fmt.Errorf("%w: grant %s was issued at head %q, not %q", ErrRootFSHeadConflict,
			normalized.GrantID, record.InitialGenerationID, normalized.ExpectedOldHeadLayerID)
	}
	if tag.RowsAffected() > 0 {
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if record.State == RootFSWriterGrantStateConsumed && !record.LeaseExpiresAt.After(record.databaseNow) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSWriterLeaseExpired, normalized.GrantID)
	}
	if record.State == RootFSWriterGrantStateConsumed {
		if currentEpoch, epochErr := getRootFSWriterEpoch(ctx, db, record.FilesystemID); epochErr == nil && currentEpoch != normalized.WriterEpoch {
			return nil, fmt.Errorf("%w: expected %d, got %d", ErrRootFSWriterEpochConflict, normalized.WriterEpoch, currentEpoch)
		}
	}
	if (record.State == RootFSWriterGrantStateRetiring || record.State == RootFSWriterGrantStateRetired) &&
		record.RetireOperationID == normalized.OperationID && record.RetireKind == retireKind {
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if record.RetireOperationID != "" && record.RetireOperationID != normalized.OperationID {
		return nil, fmt.Errorf("%w: grant %s is bound to retire operation %s", ErrRootFSWriterGrantConflict, normalized.GrantID, record.RetireOperationID)
	}
	if record.RetireKind != "" && record.RetireKind != retireKind {
		return nil, fmt.Errorf("%w: grant %s is bound to retire kind %s", ErrRootFSWriterGrantConflict, normalized.GrantID, record.RetireKind)
	}
	return nil, rootFSWriterGrantStateError(record)
}

func beginRootFSWriterCrashAbandon(
	ctx context.Context,
	db rootFSWriterGrantDB,
	req *BeginRootFSWriterCrashAbandonRequest,
) (*RootFSWriterGrant, error) {
	normalized, err := validateBeginRootFSWriterCrashAbandonRequest(req)
	if err != nil {
		return nil, err
	}
	record, err := getRootFSWriterGrantForUpdate(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesRetireBase(record, normalized.GrantID, normalized.WriterEpoch,
		normalized.BindingVersion, normalized.BindingDigest) ||
		record.NodeUID != normalized.NodeUID || record.NodeBootID != normalized.NodeBootID ||
		record.ConsumerNodeUID != normalized.NodeUID {
		return nil, fmt.Errorf("%w: crash abandon does not match grant %s",
			ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.InitialGenerationID != normalized.ExpectedOldGenerationID {
		return nil, fmt.Errorf("%w: grant %s was issued at generation %q, not %q",
			ErrRootFSHeadConflict, normalized.GrantID, record.InitialGenerationID, normalized.ExpectedOldGenerationID)
	}
	lifecycle, err := lockRootFSWriterLifecycleTxn(ctx, db, normalized.OperationID)
	if err != nil {
		return nil, err
	}
	if lifecycle.SandboxID != record.SandboxID || lifecycle.Kind != SandboxLifecycleKindPause ||
		!rootFSWriterCrashAbandonSource(lifecycle.Source) {
		return nil, fmt.Errorf("%w: lifecycle txn %s is not crash recovery",
			ErrRootFSWriterGrantConflict, normalized.OperationID)
	}
	if record.State == RootFSWriterGrantStateRetiring || record.State == RootFSWriterGrantStateRetired {
		exactActive := record.State == RootFSWriterGrantStateRetiring && !lifecycle.CancelRequested &&
			(lifecycle.Phase == SandboxLifecyclePhasePublishing || lifecycle.Phase == SandboxLifecyclePhaseCommitting)
		exactComplete := record.State == RootFSWriterGrantStateRetired &&
			lifecycle.Phase == SandboxLifecyclePhaseAborted && lifecycle.Error == RootFSWriterCrashAbandonReason
		if record.RetireOperationID == normalized.OperationID &&
			record.RetireKind == RootFSWriterRetireKindCrashAbandon && (exactActive || exactComplete) {
			return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
		}
		return nil, fmt.Errorf("%w: grant %s is bound to retire operation %s and kind %s",
			ErrRootFSWriterGrantConflict, normalized.GrantID, record.RetireOperationID, record.RetireKind)
	}
	if record.State != RootFSWriterGrantStateConsumed {
		return nil, rootFSWriterGrantStateError(record)
	}
	if record.RetireOperationID != "" || record.RetireKind != "" || len(record.RetireProofDigest) != 0 {
		return nil, fmt.Errorf("%w: consumed grant %s already has retirement state",
			ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.LeaseExpiresAt.IsZero() || record.LeaseExpiresAt.Add(RootFSWriterCrashAbandonGrace).After(record.databaseNow) {
		return nil, fmt.Errorf("%w: grant %s remains renewable", ErrRootFSWriterFenceNotMature, normalized.GrantID)
	}
	if lifecycle.CancelRequested ||
		lifecycle.Phase != SandboxLifecyclePhasePublishing && lifecycle.Phase != SandboxLifecyclePhaseCommitting ||
		lifecycle.FromGeneration <= 0 || strings.TrimSpace(lifecycle.FromPodNamespace) == "" ||
		strings.TrimSpace(lifecycle.FromPodName) == "" ||
		lifecycle.ExpectedHeadLayerID != "" && lifecycle.ExpectedHeadLayerID != normalized.ExpectedOldGenerationID ||
		lifecycle.PreparedHeadLayerID != "" {
		return nil, fmt.Errorf("%w: lifecycle txn %s cannot begin crash abandon",
			ErrRootFSWriterGrantInvalidState, normalized.OperationID)
	}
	runtimeMatch, err := lockRootFSWriterCrashRuntime(ctx, db, record, lifecycle, normalized.OperationID)
	if err != nil {
		return nil, err
	}
	if err := lockRootFSWriterCrashFallbackGeneration(
		ctx, db, record, normalized.ExpectedOldGenerationID, runtimeMatch.failedClaimDeletion,
	); err != nil {
		return nil, err
	}
	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET state = $2,
			retire_operation_id = $3,
			retire_kind = $4,
			retire_started_at = NOW(),
			updated_at = NOW()
		WHERE grant_id = $1
			AND state = $5
			AND writer_epoch = $6
			AND binding_version = $7
			AND binding_digest = $8
			AND node_uid = $9
			AND node_boot_id = $10
			AND consumer_node_uid = $9
			AND retire_operation_id = ''
			AND retire_kind = ''
			AND retire_proof_digest IS NULL
			AND lease_expires_at + ($11::bigint * INTERVAL '1 millisecond') <= NOW()
	`, normalized.GrantID, RootFSWriterGrantStateRetiring, normalized.OperationID,
		RootFSWriterRetireKindCrashAbandon, RootFSWriterGrantStateConsumed,
		normalized.WriterEpoch, normalized.BindingVersion, normalized.BindingDigest,
		normalized.NodeUID, normalized.NodeBootID, RootFSWriterCrashAbandonGrace.Milliseconds())
	if err != nil {
		return nil, mapRootFSWriterGrantConflict("begin rootfs writer crash abandon", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: grant %s changed while beginning crash abandon",
			ErrRootFSWriterGrantInvalidState, normalized.GrantID)
	}
	record, err = getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
}

func completeRootFSWriterRetireAndPublish(ctx context.Context, db rootFSWriterGrantDB, req *CompleteRootFSWriterRetireAndPublishRequest) (*RootFSWriterGrant, error) {
	normalized, err := validateCompleteRootFSWriterRetireAndPublishRequest(req)
	if err != nil {
		return nil, err
	}
	record, err := getRootFSWriterGrantForUpdate(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesRetire(record, normalized.GrantID, normalized.WriterEpoch, normalized.OperationID,
		normalized.BindingVersion, normalized.BindingDigest) {
		return nil, fmt.Errorf("%w: retire-and-publish request does not match grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.RetireKind != RootFSWriterRetireKindPlannedPublish {
		return nil, fmt.Errorf("%w: grant %s retire kind is %q", ErrRootFSWriterGrantConflict, normalized.GrantID, record.RetireKind)
	}
	if record.InitialGenerationID != normalized.ExpectedOldHeadLayerID {
		return nil, fmt.Errorf("%w: grant %s was issued at head %q, not %q", ErrRootFSHeadConflict,
			normalized.GrantID, record.InitialGenerationID, normalized.ExpectedOldHeadLayerID)
	}
	if record.SandboxID != normalized.RootFSState.SandboxID {
		return nil, fmt.Errorf("%w: grant sandbox %s does not match layer sandbox %s", ErrRootFSWriterGrantConflict,
			record.SandboxID, normalized.RootFSState.SandboxID)
	}
	lifecycle, err := lockRootFSWriterLifecycleTxn(ctx, db, normalized.LifecycleTxnID)
	if err != nil {
		return nil, err
	}
	if lifecycle.SandboxID != record.SandboxID || lifecycle.ExpectedHeadLayerID != normalized.ExpectedOldHeadLayerID {
		return nil, fmt.Errorf("%w: lifecycle txn %s does not match grant sandbox and expected head",
			ErrRootFSWriterGrantConflict, normalized.LifecycleTxnID)
	}
	if lifecycle.PreparedHeadLayerID != "" && lifecycle.PreparedHeadLayerID != normalized.RootFSState.LayerID {
		return nil, fmt.Errorf("%w: lifecycle txn %s prepared head %q, not %q", ErrRootFSHeadConflict,
			normalized.LifecycleTxnID, lifecycle.PreparedHeadLayerID, normalized.RootFSState.LayerID)
	}

	var filesystemTeamID, currentHead string
	var currentEpoch int64
	err = db.QueryRow(ctx, `
		SELECT filesystem.team_id, COALESCE(filesystem.head_generation_id, filesystem.head_layer_id, ''), filesystem.writer_epoch
		FROM manager.rootfs_filesystems AS filesystem
		JOIN manager.sandbox_rootfs_bindings AS binding
			ON binding.filesystem_id = filesystem.filesystem_id
		WHERE filesystem.filesystem_id = $1
			AND binding.sandbox_id = $2
		FOR UPDATE OF filesystem
	`, record.FilesystemID, record.SandboxID).Scan(&filesystemTeamID, &currentHead, &currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: filesystem binding for grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock rootfs filesystem for retire-and-publish: %w", err)
	}
	if filesystemTeamID != normalized.RootFSState.TeamID {
		return nil, fmt.Errorf("%w: filesystem team %s does not match layer team %s", ErrRootFSWriterGrantConflict,
			filesystemTeamID, normalized.RootFSState.TeamID)
	}
	if currentEpoch != normalized.WriterEpoch {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrRootFSWriterEpochConflict, normalized.WriterEpoch, currentEpoch)
	}

	if record.State == RootFSWriterGrantStateRetired {
		if lifecycle.Phase != SandboxLifecyclePhaseCommitted || lifecycle.PreparedHeadLayerID != normalized.RootFSState.LayerID {
			return nil, fmt.Errorf("%w: retired grant %s is not paired with committed lifecycle txn %s",
				ErrRootFSWriterGrantConflict, normalized.GrantID, normalized.LifecycleTxnID)
		}
		if !bytes.Equal(record.RetireProofDigest, normalized.ProofDigest) {
			return nil, fmt.Errorf("%w: grant %s was retired with different proof", ErrRootFSWriterGrantConflict, normalized.GrantID)
		}
		if currentHead != normalized.RootFSState.LayerID {
			return nil, fmt.Errorf("%w: retired grant %s published head %q, not %q", ErrRootFSHeadConflict,
				normalized.GrantID, currentHead, normalized.RootFSState.LayerID)
		}
		matches, matchErr := rootFSWriterLayerMatchesState(ctx, db, normalized.RootFSState)
		if matchErr != nil {
			return nil, matchErr
		}
		if !matches {
			return nil, fmt.Errorf("%w: published layer %s has different immutable fields", ErrRootFSWriterGrantConflict,
				normalized.RootFSState.LayerID)
		}
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if record.State != RootFSWriterGrantStateRetiring {
		return nil, rootFSWriterGrantStateError(record)
	}
	if lifecycle.CancelRequested || (lifecycle.Phase != SandboxLifecyclePhasePublishing && lifecycle.Phase != SandboxLifecyclePhaseCommitting) {
		return nil, fmt.Errorf("%w: lifecycle txn %s is not publishable", ErrRootFSWriterGrantInvalidState,
			normalized.LifecycleTxnID)
	}
	if len(record.RetireProofDigest) != 0 {
		return nil, fmt.Errorf("%w: retiring grant %s already has a proof", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if currentHead != normalized.ExpectedOldHeadLayerID {
		return nil, fmt.Errorf("%w: expected %q, got %q", ErrRootFSHeadConflict,
			normalized.ExpectedOldHeadLayerID, currentHead)
	}

	if err := saveRootFSLayer(ctx, db, normalized.RootFSState); err != nil {
		return nil, err
	}
	matches, err := rootFSWriterLayerMatchesState(ctx, db, normalized.RootFSState)
	if err != nil {
		return nil, err
	}
	if !matches {
		return nil, fmt.Errorf("%w: layer %s has different immutable fields", ErrRootFSWriterGrantConflict,
			normalized.RootFSState.LayerID)
	}

	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET head_layer_id = $2,
			base_image_ref = $3,
			base_image_digest = $4,
			updated_at = NOW()
		WHERE filesystem_id = $1
			AND writer_epoch = $5
			AND COALESCE(head_layer_id, '') = $6
	`, record.FilesystemID, normalized.RootFSState.LayerID, normalized.RootFSState.BaseImageRef,
		normalized.RootFSState.BaseImageDigest, normalized.WriterEpoch, normalized.ExpectedOldHeadLayerID)
	if err != nil {
		return nil, fmt.Errorf("publish rootfs writer head: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: filesystem %s", ErrRootFSHeadConflict, record.FilesystemID)
	}
	tag, err = db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET state = $2,
			retire_proof_digest = $3,
			retired_at = NOW(),
			lease_expires_at = NULL,
			updated_at = NOW()
		WHERE grant_id = $1
			AND state = $4
			AND writer_epoch = $5
			AND retire_operation_id = $6
			AND retire_kind = $7
			AND binding_version = $8
			AND binding_digest = $9
	`, normalized.GrantID, RootFSWriterGrantStateRetired, normalized.ProofDigest,
		RootFSWriterGrantStateRetiring, normalized.WriterEpoch, normalized.OperationID,
		RootFSWriterRetireKindPlannedPublish, normalized.BindingVersion, normalized.BindingDigest)
	if err != nil {
		return nil, fmt.Errorf("retire published rootfs writer grant: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: grant %s changed during publish", ErrRootFSWriterGrantInvalidState, normalized.GrantID)
	}
	tag, err = db.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			prepared_head_layer_id = $3,
			committed_at = NOW(),
			updated_at = NOW()
		WHERE txn_id = $1
			AND sandbox_id = $4
			AND expected_head_layer_id = $5
			AND phase IN ($6, $7)
			AND cancel_requested_at IS NULL
			AND (prepared_head_layer_id = '' OR prepared_head_layer_id = $3)
	`, normalized.LifecycleTxnID, SandboxLifecyclePhaseCommitted, normalized.RootFSState.LayerID,
		record.SandboxID, normalized.ExpectedOldHeadLayerID,
		SandboxLifecyclePhasePublishing, SandboxLifecyclePhaseCommitting)
	if err != nil {
		return nil, fmt.Errorf("commit rootfs writer lifecycle txn: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: lifecycle txn %s changed during publish", ErrRootFSWriterGrantInvalidState,
			normalized.LifecycleTxnID)
	}
	record, err = getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
}

func completeRootFSWriterRetireAndPublishGeneration(
	ctx context.Context,
	db rootFSWriterGrantDB,
	req *CompleteRootFSWriterRetireAndPublishGenerationRequest,
) (*RootFSWriterGrant, error) {
	normalized, err := validateCompleteRootFSWriterRetireAndPublishGenerationRequest(req)
	if err != nil {
		return nil, err
	}
	generation := normalized.Generation
	record, err := getRootFSWriterGrantForUpdate(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesRetire(record, normalized.GrantID, normalized.WriterEpoch, normalized.OperationID,
		normalized.BindingVersion, normalized.BindingDigest) ||
		record.RetireKind != RootFSWriterRetireKindPlannedPublish {
		return nil, fmt.Errorf("%w: generation publish does not match grant %s",
			ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.InitialGenerationID != normalized.ExpectedOldGenerationID {
		return nil, fmt.Errorf("%w: grant %s was issued at generation %q, not %q",
			ErrRootFSHeadConflict, normalized.GrantID, record.InitialGenerationID, normalized.ExpectedOldGenerationID)
	}
	if record.FilesystemID != generation.FilesystemID || record.WriterEpoch != generation.WriterEpoch {
		return nil, fmt.Errorf("%w: generation does not match writer filesystem and epoch",
			ErrRootFSWriterGrantConflict)
	}
	lifecycle, err := lockRootFSWriterLifecycleTxn(ctx, db, normalized.LifecycleTxnID)
	if err != nil {
		return nil, err
	}
	if lifecycle.SandboxID != record.SandboxID ||
		lifecycle.ExpectedHeadLayerID != "" && lifecycle.ExpectedHeadLayerID != normalized.ExpectedOldGenerationID ||
		lifecycle.PreparedHeadLayerID != "" && lifecycle.PreparedHeadLayerID != generation.ID {
		return nil, fmt.Errorf("%w: lifecycle txn %s does not match generation publish",
			ErrRootFSWriterGrantConflict, normalized.LifecycleTxnID)
	}

	var storageFormat, currentHead, baseArtifact string
	var currentEpoch int64
	var formatGeneration int
	err = db.QueryRow(ctx, `
		SELECT filesystem.storage_format,
			COALESCE(filesystem.head_generation_id, ''), filesystem.writer_epoch,
			COALESCE(filesystem.base_artifact_digest, ''), COALESCE(filesystem.format_generation, 0)
		FROM manager.rootfs_filesystems AS filesystem
		JOIN manager.sandbox_rootfs_bindings AS binding
			ON binding.filesystem_id = filesystem.filesystem_id
		WHERE filesystem.filesystem_id = $1
			AND binding.sandbox_id = $2
		FOR UPDATE OF filesystem
	`, record.FilesystemID, record.SandboxID).Scan(
		&storageFormat, &currentHead, &currentEpoch, &baseArtifact, &formatGeneration,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: filesystem binding for grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock block-cow rootfs filesystem for publish: %w", err)
	}
	if storageFormat != RootFSStorageFormatBlockCOWV1 || currentEpoch != normalized.WriterEpoch ||
		baseArtifact != generation.BaseArtifactDigest ||
		formatGeneration != generation.FormatGeneration {
		return nil, fmt.Errorf("%w: block-cow filesystem head or format changed", ErrRootFSHeadConflict)
	}
	if record.State == RootFSWriterGrantStateRetired {
		if currentHead != generation.ID {
			return nil, fmt.Errorf("%w: retired generation head is %q, not %q", ErrRootFSHeadConflict, currentHead, generation.ID)
		}
	} else if currentHead != normalized.ExpectedOldGenerationID {
		return nil, fmt.Errorf("%w: expected generation %q, got %q",
			ErrRootFSHeadConflict, normalized.ExpectedOldGenerationID, currentHead)
	}
	var oldSourceDigest, oldBaseArtifact, oldBaseRoot string
	if err := db.QueryRow(ctx, `
		SELECT source_oci_digest, base_artifact_digest, base_block_root
		FROM manager.rootfs_generations
		WHERE generation_id = $1
		FOR SHARE
	`, normalized.ExpectedOldGenerationID).Scan(
		&oldSourceDigest, &oldBaseArtifact, &oldBaseRoot,
	); err != nil {
		return nil, fmt.Errorf("lock previous rootfs generation: %w", err)
	}
	if generation.ParentGenerationID != normalized.ExpectedOldGenerationID ||
		generation.SourceOCIDigest != oldSourceDigest || generation.BaseArtifactDigest != oldBaseArtifact ||
		generation.BaseBlockRoot != oldBaseRoot {
		return nil, fmt.Errorf("%w: sealed generation changed immutable lineage", ErrRootFSGenerationConflict)
	}

	if record.State == RootFSWriterGrantStateRetired {
		if lifecycle.Phase != SandboxLifecyclePhaseCommitted || lifecycle.ExpectedHeadLayerID != normalized.ExpectedOldGenerationID ||
			lifecycle.PreparedHeadLayerID != generation.ID || !bytes.Equal(record.RetireProofDigest, normalized.ProofDigest) ||
			currentHead != generation.ID {
			return nil, fmt.Errorf("%w: retired generation publish is not an exact retry",
				ErrRootFSWriterGrantConflict)
		}
		matches, matchErr := rootFSWriterGenerationMatches(ctx, db, generation)
		if matchErr != nil {
			return nil, matchErr
		}
		if !matches {
			return nil, fmt.Errorf("%w: generation %s has different immutable fields",
				ErrRootFSGenerationConflict, generation.ID)
		}
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if record.State != RootFSWriterGrantStateRetiring {
		return nil, rootFSWriterGrantStateError(record)
	}
	if lifecycle.CancelRequested ||
		lifecycle.Phase != SandboxLifecyclePhasePublishing && lifecycle.Phase != SandboxLifecyclePhaseCommitting {
		return nil, fmt.Errorf("%w: lifecycle txn %s is not publishable",
			ErrRootFSWriterGrantInvalidState, normalized.LifecycleTxnID)
	}
	if len(record.RetireProofDigest) != 0 {
		return nil, fmt.Errorf("%w: retiring grant %s already has a proof",
			ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if err := ensureRootFSCompositeBacklogCapacity(ctx, db, generation); err != nil {
		return nil, err
	}

	if _, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_generations (
			generation_id, filesystem_id, parent_generation_id, source_oci_digest,
			base_artifact_digest, base_block_root, current_block_head, writer_epoch,
			format_generation, durability_state, locator_version, descriptor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
		ON CONFLICT (generation_id) DO NOTHING
	`, generation.ID, generation.FilesystemID, generation.ParentGenerationID, generation.SourceOCIDigest,
		generation.BaseArtifactDigest, generation.BaseBlockRoot, generation.CurrentBlockHead,
		generation.WriterEpoch, generation.FormatGeneration, generation.DurabilityState,
		generation.LocatorVersion, generation.Descriptor); err != nil {
		return nil, fmt.Errorf("insert sealed rootfs generation: %w", err)
	}
	matches, err := rootFSWriterGenerationMatches(ctx, db, generation)
	if err != nil {
		return nil, err
	}
	if !matches {
		return nil, fmt.Errorf("%w: generation %s has different immutable fields",
			ErrRootFSGenerationConflict, generation.ID)
	}
	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET head_generation_id = $2, updated_at = NOW()
		WHERE filesystem_id = $1
			AND storage_format = $3
			AND writer_epoch = $4
			AND head_generation_id = $5
	`, record.FilesystemID, generation.ID, RootFSStorageFormatBlockCOWV1,
		normalized.WriterEpoch, normalized.ExpectedOldGenerationID)
	if err != nil {
		return nil, fmt.Errorf("publish sealed rootfs generation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: filesystem %s", ErrRootFSHeadConflict, record.FilesystemID)
	}
	tag, err = db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET state = $2, retire_proof_digest = $3, retired_at = NOW(),
			lease_expires_at = NULL, updated_at = NOW()
		WHERE grant_id = $1 AND state = $4 AND writer_epoch = $5
			AND retire_operation_id = $6 AND retire_kind = $7
			AND binding_version = $8 AND binding_digest = $9
	`, normalized.GrantID, RootFSWriterGrantStateRetired, normalized.ProofDigest,
		RootFSWriterGrantStateRetiring, normalized.WriterEpoch, normalized.OperationID,
		RootFSWriterRetireKindPlannedPublish, normalized.BindingVersion, normalized.BindingDigest)
	if err != nil {
		return nil, fmt.Errorf("retire block-cow writer grant: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: grant %s changed during publish",
			ErrRootFSWriterGrantInvalidState, normalized.GrantID)
	}
	tag, err = db.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2, expected_head_layer_id = $3, prepared_head_layer_id = $4,
			committed_at = NOW(), updated_at = NOW()
		WHERE txn_id = $1 AND sandbox_id = $5
			AND (expected_head_layer_id = '' OR expected_head_layer_id = $3)
			AND phase IN ($6, $7) AND cancel_requested_at IS NULL
			AND (prepared_head_layer_id = '' OR prepared_head_layer_id = $4)
	`, normalized.LifecycleTxnID, SandboxLifecyclePhaseCommitted,
		normalized.ExpectedOldGenerationID, generation.ID, record.SandboxID,
		SandboxLifecyclePhasePublishing, SandboxLifecyclePhaseCommitting)
	if err != nil {
		return nil, fmt.Errorf("commit block-cow lifecycle txn: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: lifecycle txn %s changed during generation publish",
			ErrRootFSWriterGrantInvalidState, normalized.LifecycleTxnID)
	}
	record, err = getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
}

func rootFSWriterGenerationMatches(
	ctx context.Context,
	db rootFSWriterGrantDB,
	expected *RootFSGeneration,
) (bool, error) {
	actual, err := scanRootFSGeneration(db.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1
	`, expected.ID))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read sealed rootfs generation: %w", err)
	}
	return actual.ID == expected.ID && actual.FilesystemID == expected.FilesystemID &&
		actual.ParentGenerationID == expected.ParentGenerationID &&
		actual.SourceOCIDigest == expected.SourceOCIDigest &&
		actual.BaseArtifactDigest == expected.BaseArtifactDigest &&
		actual.BaseBlockRoot == expected.BaseBlockRoot &&
		actual.CurrentBlockHead == expected.CurrentBlockHead &&
		actual.WriterEpoch == expected.WriterEpoch &&
		actual.FormatGeneration == expected.FormatGeneration &&
		actual.DurabilityState == expected.DurabilityState &&
		actual.LocatorVersion == expected.LocatorVersion &&
		bytes.Equal(actual.Descriptor, expected.Descriptor), nil
}

func completeRootFSWriterPrelaunchAbort(
	ctx context.Context,
	db rootFSWriterGrantDB,
	req *CompleteRootFSWriterPrelaunchAbortRequest,
) (*RootFSWriterGrant, error) {
	normalized, err := validateCompleteRootFSWriterPrelaunchAbortRequest(req)
	if err != nil {
		return nil, err
	}
	record, err := getRootFSWriterGrantForUpdate(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesRetire(record, normalized.GrantID, normalized.WriterEpoch, normalized.OperationID,
		normalized.BindingVersion, normalized.BindingDigest) || record.RetireKind != RootFSWriterRetireKindPrelaunchAbort {
		return nil, fmt.Errorf("%w: prelaunch abort does not match grant %s", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.InitialGenerationID != normalized.ExpectedOldHeadLayerID {
		return nil, fmt.Errorf("%w: grant %s was issued at head %q, not %q", ErrRootFSHeadConflict,
			normalized.GrantID, record.InitialGenerationID, normalized.ExpectedOldHeadLayerID)
	}
	var currentHead string
	var currentEpoch int64
	if err := db.QueryRow(ctx, `
		SELECT COALESCE(head_generation_id, head_layer_id, ''), writer_epoch
		FROM manager.rootfs_filesystems
		WHERE filesystem_id = $1
		FOR UPDATE
	`, record.FilesystemID).Scan(&currentHead, &currentEpoch); err != nil {
		return nil, fmt.Errorf("lock rootfs filesystem for prelaunch abort: %w", err)
	}
	if currentEpoch != normalized.WriterEpoch {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrRootFSWriterEpochConflict, normalized.WriterEpoch, currentEpoch)
	}
	if currentHead != normalized.ExpectedOldHeadLayerID {
		return nil, fmt.Errorf("%w: expected unchanged head %q, got %q", ErrRootFSHeadConflict,
			normalized.ExpectedOldHeadLayerID, currentHead)
	}
	if record.State == RootFSWriterGrantStateRetired {
		if !bytes.Equal(record.RetireProofDigest, normalized.ProofDigest) {
			return nil, fmt.Errorf("%w: grant %s was retired with different prelaunch proof",
				ErrRootFSWriterGrantConflict, normalized.GrantID)
		}
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if record.State != RootFSWriterGrantStateRetiring {
		return nil, rootFSWriterGrantStateError(record)
	}
	if len(record.RetireProofDigest) != 0 {
		return nil, fmt.Errorf("%w: retiring grant %s already has a proof", ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	tag, err := db.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET state = $2,
			retire_proof_digest = $3,
			retired_at = NOW(),
			lease_expires_at = NULL,
			updated_at = NOW()
		WHERE grant_id = $1
			AND state = $4
			AND writer_epoch = $5
			AND retire_operation_id = $6
			AND retire_kind = $7
			AND binding_version = $8
			AND binding_digest = $9
	`, normalized.GrantID, RootFSWriterGrantStateRetired, normalized.ProofDigest,
		RootFSWriterGrantStateRetiring, normalized.WriterEpoch, normalized.OperationID,
		RootFSWriterRetireKindPrelaunchAbort, normalized.BindingVersion, normalized.BindingDigest)
	if err != nil {
		return nil, fmt.Errorf("complete rootfs writer prelaunch abort: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: grant %s changed during prelaunch abort",
			ErrRootFSWriterGrantInvalidState, normalized.GrantID)
	}
	record, err = getRootFSWriterGrant(ctx, db, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
}

func completeRootFSWriterCrashAbandon(
	ctx context.Context,
	tx sandboxStoreTx,
	req *CompleteRootFSWriterCrashAbandonRequest,
) (*RootFSWriterGrant, error) {
	normalized, err := validateCompleteRootFSWriterCrashAbandonRequest(req)
	if err != nil {
		return nil, err
	}
	record, err := getRootFSWriterGrantForUpdate(ctx, tx.tx, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesRetireBase(record, normalized.GrantID, normalized.WriterEpoch,
		normalized.BindingVersion, normalized.BindingDigest) ||
		record.NodeUID != normalized.NodeUID || record.NodeBootID != normalized.NodeBootID ||
		record.ConsumerNodeUID != normalized.NodeUID {
		return nil, fmt.Errorf("%w: crash abandon does not match grant %s",
			ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if record.InitialGenerationID != normalized.ExpectedOldGenerationID {
		return nil, fmt.Errorf("%w: grant %s was issued at generation %q, not %q",
			ErrRootFSHeadConflict, normalized.GrantID, record.InitialGenerationID, normalized.ExpectedOldGenerationID)
	}

	lifecycle, err := lockRootFSWriterLifecycleTxn(ctx, tx.tx, normalized.LifecycleTxnID)
	if err != nil {
		return nil, err
	}
	if lifecycle.SandboxID != record.SandboxID || lifecycle.Kind != SandboxLifecycleKindPause ||
		!rootFSWriterCrashAbandonSource(lifecycle.Source) {
		return nil, fmt.Errorf("%w: lifecycle txn %s is not crash recovery",
			ErrRootFSWriterGrantConflict, normalized.LifecycleTxnID)
	}

	if record.State == RootFSWriterGrantStateRetired {
		if record.RetireOperationID != normalized.OperationID ||
			record.RetireKind != RootFSWriterRetireKindCrashAbandon ||
			!bytes.Equal(record.RetireProofDigest, normalized.ProofDigest) ||
			lifecycle.Phase != SandboxLifecyclePhaseAborted ||
			lifecycle.Error != RootFSWriterCrashAbandonReason {
			return nil, fmt.Errorf("%w: retired crash abandon is not an exact retry",
				ErrRootFSWriterGrantConflict)
		}
		return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
	}
	if record.State != RootFSWriterGrantStateRetiring {
		return nil, rootFSWriterGrantStateError(record)
	}
	if record.RetireOperationID != normalized.OperationID ||
		record.RetireKind != RootFSWriterRetireKindCrashAbandon || len(record.RetireProofDigest) != 0 {
		return nil, fmt.Errorf("%w: retiring grant %s is not the exact crash-abandon operation",
			ErrRootFSWriterGrantConflict, normalized.GrantID)
	}
	if lifecycle.CancelRequested ||
		lifecycle.Phase != SandboxLifecyclePhasePublishing && lifecycle.Phase != SandboxLifecyclePhaseCommitting ||
		lifecycle.FromGeneration <= 0 || strings.TrimSpace(lifecycle.FromPodNamespace) == "" ||
		strings.TrimSpace(lifecycle.FromPodName) == "" ||
		lifecycle.ExpectedHeadLayerID != "" && lifecycle.ExpectedHeadLayerID != normalized.ExpectedOldGenerationID ||
		lifecycle.PreparedHeadLayerID != "" {
		return nil, fmt.Errorf("%w: lifecycle txn %s cannot abandon a crashed runtime",
			ErrRootFSWriterGrantInvalidState, normalized.LifecycleTxnID)
	}

	runtimeMatch, err := lockRootFSWriterCrashRuntime(ctx, tx.tx, record, lifecycle, normalized.LifecycleTxnID)
	if err != nil {
		return nil, err
	}

	if err := lockRootFSWriterCrashFallbackGeneration(
		ctx, tx.tx, record, normalized.ExpectedOldGenerationID, runtimeMatch.failedClaimDeletion,
	); err != nil {
		return nil, err
	}

	tag, err := tx.tx.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET state = $2,
			retire_proof_digest = $3,
			retired_at = NOW(),
			lease_expires_at = NULL,
			updated_at = NOW()
		WHERE grant_id = $1
			AND state = $4
			AND writer_epoch = $5
			AND binding_version = $6
			AND binding_digest = $7
			AND node_uid = $8
			AND node_boot_id = $9
			AND consumer_node_uid = $8
			AND retire_operation_id = $10
			AND retire_kind = $11
			AND retire_proof_digest IS NULL
	`, normalized.GrantID, RootFSWriterGrantStateRetired, normalized.ProofDigest,
		RootFSWriterGrantStateRetiring, normalized.WriterEpoch, normalized.BindingVersion,
		normalized.BindingDigest, normalized.NodeUID, normalized.NodeBootID,
		normalized.OperationID, RootFSWriterRetireKindCrashAbandon)
	if err != nil {
		return nil, mapRootFSWriterGrantConflict("crash-abandon rootfs writer grant", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: grant %s changed during crash abandon",
			ErrRootFSWriterGrantInvalidState, normalized.GrantID)
	}
	if runtimeMatch.active {
		if err := tx.MarkRuntimePaused(ctx, record.SandboxID, lifecycle.FromGeneration, record.databaseNow); err != nil {
			return nil, err
		}
	}
	if err := tx.AbortLifecycleTxn(ctx, normalized.LifecycleTxnID, RootFSWriterCrashAbandonReason); err != nil {
		return nil, err
	}
	record, err = getRootFSWriterGrant(ctx, tx.tx, normalized.GrantID)
	if err != nil {
		return nil, err
	}
	return cloneRootFSWriterGrant(&record.RootFSWriterGrant), nil
}

type rootFSWriterCrashRuntimeMatch struct {
	active              bool
	failedClaimDeletion bool
}

func lockRootFSWriterCrashRuntime(
	ctx context.Context,
	db rootFSWriterGrantDB,
	record *rootFSWriterGrantRecord,
	lifecycle *rootFSWriterLifecycleTxnRecord,
	lifecycleTxnID string,
) (rootFSWriterCrashRuntimeMatch, error) {
	match := rootFSWriterCrashRuntimeMatch{}
	var desiredState, currentPodNamespace, currentPodName string
	var runtimeGeneration int64
	var deletedAt pgtype.Timestamptz
	if err := db.QueryRow(ctx, `
		SELECT desired_state, current_pod_namespace, current_pod_name,
			runtime_generation, deleted_at
		FROM manager.sandboxes
		WHERE sandbox_id = $1
		FOR UPDATE
	`, record.SandboxID).Scan(
		&desiredState, &currentPodNamespace, &currentPodName, &runtimeGeneration, &deletedAt,
	); err != nil {
		return match, fmt.Errorf("lock crashed sandbox runtime: %w", err)
	}
	match.active = !deletedAt.Valid && desiredState == SandboxDesiredStateActive &&
		runtimeGeneration == lifecycle.FromGeneration &&
		currentPodNamespace == lifecycle.FromPodNamespace && currentPodName == lifecycle.FromPodName
	precommitResume := !deletedAt.Valid && desiredState == SandboxDesiredStatePaused &&
		currentPodNamespace == "" && currentPodName == "" && runtimeGeneration >= 0 &&
		runtimeGeneration+1 == lifecycle.FromGeneration
	match.failedClaimDeletion = desiredState == SandboxDesiredStateDeleted && deletedAt.Valid &&
		currentPodNamespace == "" && currentPodName == "" && runtimeGeneration == lifecycle.FromGeneration
	if !match.active && !precommitResume && !match.failedClaimDeletion {
		return match, fmt.Errorf("%w: sandbox runtime no longer matches crash lifecycle txn %s",
			ErrRootFSWriterGrantConflict, lifecycleTxnID)
	}
	return match, nil
}

func rootFSWriterCrashAbandonSource(source string) bool {
	switch source {
	case SandboxLifecycleSourceCrash, SandboxLifecycleSourceHealth, SandboxLifecycleSourceLost:
		return true
	default:
		return false
	}
}

func lockRootFSWriterCrashFallbackGeneration(
	ctx context.Context,
	db rootFSWriterGrantDB,
	record *rootFSWriterGrantRecord,
	expectedOldGenerationID string,
	allowMissingBinding bool,
) error {
	var storageFormat, currentHead string
	var currentEpoch int64
	var bindingExists bool
	err := db.QueryRow(ctx, `
		SELECT filesystem.storage_format, COALESCE(filesystem.head_generation_id, ''),
			filesystem.writer_epoch,
			EXISTS (
				SELECT 1 FROM manager.sandbox_rootfs_bindings AS binding
				WHERE binding.filesystem_id = filesystem.filesystem_id AND binding.sandbox_id = $2
			)
		FROM manager.rootfs_filesystems AS filesystem
		WHERE filesystem.filesystem_id = $1
		FOR UPDATE OF filesystem
	`, record.FilesystemID, record.SandboxID).Scan(&storageFormat, &currentHead, &currentEpoch, &bindingExists)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: filesystem binding for grant %s",
			ErrRootFSWriterGrantConflict, record.ID)
	}
	if err != nil {
		return fmt.Errorf("lock crashed rootfs filesystem: %w", err)
	}
	if !bindingExists && !allowMissingBinding {
		return fmt.Errorf("%w: filesystem binding for grant %s",
			ErrRootFSWriterGrantConflict, record.ID)
	}
	if storageFormat != RootFSStorageFormatBlockCOWV1 ||
		currentEpoch != record.WriterEpoch || currentHead != expectedOldGenerationID {
		return fmt.Errorf("%w: crashed writer durable head changed", ErrRootFSHeadConflict)
	}
	var oldGenerationExists bool
	if err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.rootfs_generations
			WHERE generation_id = $1
		)
	`, expectedOldGenerationID).Scan(&oldGenerationExists); err != nil {
		return fmt.Errorf("verify crash fallback generation: %w", err)
	}
	if !oldGenerationExists {
		return fmt.Errorf("%w: crash fallback generation %s is missing",
			ErrRootFSHeadConflict, expectedOldGenerationID)
	}
	return nil
}

func validateIssueRootFSWriterGrantRequest(req *IssueRootFSWriterGrantRequest) (*IssueRootFSWriterGrantRequest, [sha256.Size]byte, error) {
	var empty [sha256.Size]byte
	if req == nil {
		return nil, empty, fmt.Errorf("rootfs writer grant issue request is required")
	}
	normalized := *req
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.SandboxID = strings.TrimSpace(req.SandboxID)
	normalized.ExpectedFilesystemID = strings.TrimSpace(req.ExpectedFilesystemID)
	normalized.ClaimID = strings.TrimSpace(req.ClaimID)
	normalized.SlotID = strings.TrimSpace(req.SlotID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.NodeUID = strings.TrimSpace(req.NodeUID)
	normalized.NodeBootID = strings.TrimSpace(req.NodeBootID)
	normalized.PodNamespace = strings.TrimSpace(req.PodNamespace)
	normalized.PodName = strings.TrimSpace(req.PodName)
	normalized.PodUID = strings.TrimSpace(req.PodUID)
	normalized.NodeName = strings.TrimSpace(req.NodeName)
	normalized.GateParent = strings.TrimSpace(req.GateParent)
	normalized.RuntimeGeneration = strings.TrimSpace(req.RuntimeGeneration)
	normalized.InitialHeadLayerID = strings.TrimSpace(req.InitialHeadLayerID)
	normalized.InitialGenerationID = strings.TrimSpace(req.InitialGenerationID)
	if normalized.InitialGenerationID == "" {
		normalized.InitialGenerationID = normalized.InitialHeadLayerID
	}
	if normalized.InitialHeadLayerID == "" {
		// Keep the legacy column populated during the mixed-format migration. It
		// is an opaque identity and is not a foreign key to rootfs_layers.
		normalized.InitialHeadLayerID = normalized.InitialGenerationID
	}
	if normalized.InitialHeadLayerID != normalized.InitialGenerationID {
		return nil, [sha256.Size]byte{}, fmt.Errorf("initial head and generation must match")
	}
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	normalized.ConsumeExpiresAt = req.ConsumeExpiresAt.UTC().Truncate(time.Microsecond)
	for name, value := range map[string]string{
		"grant_id": normalized.GrantID, "sandbox_id": normalized.SandboxID,
		"claim_id": normalized.ClaimID, "slot_id": normalized.SlotID,
		"operation_id": normalized.OperationID, "node_uid": normalized.NodeUID,
		"node_boot_id":  normalized.NodeBootID,
		"pod_namespace": normalized.PodNamespace, "pod_name": normalized.PodName,
		"pod_uid": normalized.PodUID, "node_name": normalized.NodeName,
		"gate_parent": normalized.GateParent, "runtime_generation": normalized.RuntimeGeneration,
	} {
		if value == "" {
			return nil, empty, fmt.Errorf("%s is required", name)
		}
	}
	if len(normalized.RawToken) < sha256.Size {
		return nil, empty, fmt.Errorf("raw token must contain at least %d bytes", sha256.Size)
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, empty, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, empty, err
	}
	if normalized.ExpectedWriterEpoch < 0 {
		return nil, empty, fmt.Errorf("expected_writer_epoch must not be negative")
	}
	if normalized.ConsumeExpiresAt.IsZero() {
		return nil, empty, fmt.Errorf("consume_expires_at is required")
	}
	return &normalized, sha256.Sum256([]byte(normalized.RawToken)), nil
}

func validateConsumeRootFSWriterGrantRequest(req *ConsumeRootFSWriterGrantRequest) (*ConsumeRootFSWriterGrantRequest, [sha256.Size]byte, error) {
	var empty [sha256.Size]byte
	if req == nil {
		return nil, empty, fmt.Errorf("rootfs writer grant consume request is required")
	}
	normalized := *req
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.ConsumerNodeUID = strings.TrimSpace(req.ConsumerNodeUID)
	normalized.ConsumerCtldPodUID = strings.TrimSpace(req.ConsumerCtldPodUID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	if normalized.GrantID == "" || normalized.ConsumerNodeUID == "" || normalized.ConsumerCtldPodUID == "" {
		return nil, empty, fmt.Errorf("grant_id, consumer_node_uid, and consumer_ctld_pod_uid are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, empty, fmt.Errorf("writer_epoch must be positive")
	}
	if len(normalized.RawToken) < sha256.Size {
		return nil, empty, fmt.Errorf("raw token must contain at least %d bytes", sha256.Size)
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, empty, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, empty, err
	}
	if normalized.LeaseTTL < time.Millisecond {
		return nil, empty, fmt.Errorf("lease_ttl must be at least one millisecond")
	}
	return &normalized, sha256.Sum256([]byte(normalized.RawToken)), nil
}

func validateRenewRootFSWriterGrantRequest(
	req *RenewRootFSWriterGrantRequest,
	policy RootFSWriterLeaseRenewalPolicy,
) (*RenewRootFSWriterGrantRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("rootfs writer grant renew request is required")
	}
	normalized := *req
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.ConsumerNodeUID = strings.TrimSpace(req.ConsumerNodeUID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	if normalized.GrantID == "" || normalized.ConsumerNodeUID == "" {
		return nil, fmt.Errorf("grant_id and consumer_node_uid are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	if policy.LeaseTTL < time.Millisecond {
		return nil, fmt.Errorf("lease_ttl policy must be at least one millisecond")
	}
	if policy.GracePeriod < 0 || policy.GracePeriod > RootFSWriterMaxRenewGrace {
		return nil, fmt.Errorf("renew grace policy must be between zero and %s", RootFSWriterMaxRenewGrace)
	}
	return &normalized, nil
}

func validateCancelRootFSWriterGrantRequest(req *CancelRootFSWriterGrantRequest) (*CancelRootFSWriterGrantRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("rootfs writer grant cancel request is required")
	}
	normalized := *req
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	if normalized.GrantID == "" || normalized.OperationID == "" {
		return nil, fmt.Errorf("grant_id and operation_id are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	return &normalized, nil
}

func validateBeginRootFSWriterRetireRequest(req *BeginRootFSWriterRetireRequest) (*BeginRootFSWriterRetireRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("begin rootfs writer retire request is required")
	}
	normalized := *req
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.ExpectedOldHeadLayerID = strings.TrimSpace(req.ExpectedOldHeadLayerID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	if normalized.GrantID == "" || normalized.OperationID == "" {
		return nil, fmt.Errorf("grant_id and operation_id are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	return &normalized, nil
}

func validateBeginRootFSWriterCrashAbandonRequest(
	req *BeginRootFSWriterCrashAbandonRequest,
) (*BeginRootFSWriterCrashAbandonRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("begin rootfs writer crash abandon request is required")
	}
	normalized := *req
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.NodeUID = strings.TrimSpace(req.NodeUID)
	normalized.NodeBootID = strings.TrimSpace(req.NodeBootID)
	normalized.ExpectedOldGenerationID = strings.TrimSpace(req.ExpectedOldGenerationID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	if normalized.GrantID == "" || normalized.OperationID == "" || normalized.NodeUID == "" ||
		normalized.NodeBootID == "" || normalized.ExpectedOldGenerationID == "" {
		return nil, fmt.Errorf("grant_id, operation_id, node_uid, node_boot_id, and expected_old_generation_id are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	return &normalized, nil
}

func validateCompleteRootFSWriterRetireAndPublishRequest(req *CompleteRootFSWriterRetireAndPublishRequest) (*CompleteRootFSWriterRetireAndPublishRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("complete rootfs writer retire-and-publish request is required")
	}
	normalized := *req
	normalized.LifecycleTxnID = strings.TrimSpace(req.LifecycleTxnID)
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	normalized.ProofDigest = append([]byte(nil), req.ProofDigest...)
	normalized.ExpectedOldHeadLayerID = strings.TrimSpace(req.ExpectedOldHeadLayerID)
	if normalized.LifecycleTxnID == "" || normalized.GrantID == "" || normalized.OperationID == "" {
		return nil, fmt.Errorf("lifecycle_txn_id, grant_id, and operation_id are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("proof_digest", normalized.ProofDigest); err != nil {
		return nil, err
	}
	if req.RootFSState == nil {
		return nil, fmt.Errorf("rootfs_state is required")
	}
	state := *req.RootFSState
	state.LayerChain = append([]*SandboxRootFSLayer(nil), req.RootFSState.LayerChain...)
	state.SnapshotParentChain = append([]string(nil), req.RootFSState.SnapshotParentChain...)
	if err := validateRootFSState(&state); err != nil {
		return nil, err
	}
	expectedOldHead := normalized.ExpectedOldHeadLayerID
	if state.ExpectedHeadLayerID != "" && strings.TrimSpace(state.ExpectedHeadLayerID) != expectedOldHead {
		return nil, fmt.Errorf("expected_head_layer_id %q does not match expected old head %q",
			state.ExpectedHeadLayerID, expectedOldHead)
	}
	state.SandboxID = strings.TrimSpace(state.SandboxID)
	state.TeamID = strings.TrimSpace(state.TeamID)
	state.LayerID = strings.TrimSpace(state.LayerID)
	state.ParentLayerID = strings.TrimSpace(state.ParentLayerID)
	state.ExpectedHeadLayerID = expectedOldHead
	normalized.ExpectedOldHeadLayerID = expectedOldHead
	normalized.RootFSState = &state
	return &normalized, nil
}

func validateCompleteRootFSWriterRetireAndPublishGenerationRequest(
	req *CompleteRootFSWriterRetireAndPublishGenerationRequest,
) (*CompleteRootFSWriterRetireAndPublishGenerationRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("complete rootfs writer generation publish request is required")
	}
	normalized := *req
	normalized.LifecycleTxnID = strings.TrimSpace(req.LifecycleTxnID)
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.ExpectedOldGenerationID = strings.TrimSpace(req.ExpectedOldGenerationID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	normalized.ProofDigest = append([]byte(nil), req.ProofDigest...)
	if normalized.LifecycleTxnID == "" || normalized.GrantID == "" || normalized.OperationID == "" ||
		normalized.ExpectedOldGenerationID == "" {
		return nil, fmt.Errorf("lifecycle_txn_id, grant_id, operation_id, and expected_old_generation_id are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("proof_digest", normalized.ProofDigest); err != nil {
		return nil, err
	}
	if req.Generation == nil {
		return nil, fmt.Errorf("generation is required")
	}
	generation := *req.Generation
	generation.ID = strings.TrimSpace(generation.ID)
	generation.FilesystemID = strings.TrimSpace(generation.FilesystemID)
	generation.ParentGenerationID = strings.TrimSpace(generation.ParentGenerationID)
	generation.SourceOCIDigest = strings.TrimSpace(generation.SourceOCIDigest)
	generation.BaseArtifactDigest = strings.TrimSpace(generation.BaseArtifactDigest)
	generation.BaseBlockRoot = strings.TrimSpace(generation.BaseBlockRoot)
	generation.CurrentBlockHead = strings.TrimSpace(generation.CurrentBlockHead)
	generation.DurabilityState = strings.TrimSpace(generation.DurabilityState)
	generation.Descriptor = append([]byte(nil), generation.Descriptor...)
	for name, value := range map[string]string{
		"generation_id": generation.ID, "filesystem_id": generation.FilesystemID,
		"parent_generation_id": generation.ParentGenerationID,
		"source_oci_digest":    generation.SourceOCIDigest,
		"base_artifact_digest": generation.BaseArtifactDigest,
		"base_block_root":      generation.BaseBlockRoot,
		"current_block_head":   generation.CurrentBlockHead,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"source_oci_digest":    generation.SourceOCIDigest,
		"base_artifact_digest": generation.BaseArtifactDigest,
		"base_block_root":      generation.BaseBlockRoot,
		"current_block_head":   generation.CurrentBlockHead,
	} {
		if parsed, err := digest.Parse(value); err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
			return nil, fmt.Errorf("%s must be a canonical sha256 digest", name)
		}
	}
	if generation.ParentGenerationID != normalized.ExpectedOldGenerationID ||
		generation.WriterEpoch != normalized.WriterEpoch || generation.FormatGeneration <= 0 ||
		generation.LocatorVersion <= 0 {
		return nil, fmt.Errorf("generation parent, epoch, format, or locator version is invalid")
	}
	descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("generation descriptor: %w", err)
	}
	if descriptor.MappingRoot.RootDigest != generation.CurrentBlockHead {
		return nil, fmt.Errorf("generation descriptor does not match current_block_head")
	}
	switch generation.DurabilityState {
	case rootfsblock.DurabilityS3:
		if descriptor.CompositeTail != nil {
			return nil, fmt.Errorf("s3_materialized generation cannot contain a composite tail")
		}
	case rootfsblock.DurabilityComposite:
		if descriptor.CompositeTail == nil {
			return nil, fmt.Errorf("composite_durable generation requires a composite tail")
		}
	default:
		return nil, fmt.Errorf("unsupported generation durability state %q", generation.DurabilityState)
	}
	normalized.Generation = &generation
	return &normalized, nil
}

func validateCompleteRootFSWriterPrelaunchAbortRequest(
	req *CompleteRootFSWriterPrelaunchAbortRequest,
) (*CompleteRootFSWriterPrelaunchAbortRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("complete rootfs writer prelaunch abort request is required")
	}
	normalized := *req
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.ExpectedOldHeadLayerID = strings.TrimSpace(req.ExpectedOldHeadLayerID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	normalized.ProofDigest = append([]byte(nil), req.ProofDigest...)
	if normalized.GrantID == "" || normalized.OperationID == "" {
		return nil, fmt.Errorf("grant_id and operation_id are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("proof_digest", normalized.ProofDigest); err != nil {
		return nil, err
	}
	return &normalized, nil
}

func validateCompleteRootFSWriterCrashAbandonRequest(
	req *CompleteRootFSWriterCrashAbandonRequest,
) (*CompleteRootFSWriterCrashAbandonRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("complete rootfs writer crash abandon request is required")
	}
	normalized := *req
	normalized.LifecycleTxnID = strings.TrimSpace(req.LifecycleTxnID)
	normalized.GrantID = strings.TrimSpace(req.GrantID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.NodeUID = strings.TrimSpace(req.NodeUID)
	normalized.NodeBootID = strings.TrimSpace(req.NodeBootID)
	normalized.ExpectedOldGenerationID = strings.TrimSpace(req.ExpectedOldGenerationID)
	normalized.BindingDigest = append([]byte(nil), req.BindingDigest...)
	normalized.ProofDigest = append([]byte(nil), req.ProofDigest...)
	if normalized.LifecycleTxnID == "" || normalized.GrantID == "" || normalized.OperationID == "" ||
		normalized.NodeUID == "" || normalized.NodeBootID == "" || normalized.ExpectedOldGenerationID == "" {
		return nil, fmt.Errorf("lifecycle_txn_id, grant_id, operation_id, node_uid, node_boot_id, and expected_old_generation_id are required")
	}
	if normalized.WriterEpoch <= 0 {
		return nil, fmt.Errorf("writer_epoch must be positive")
	}
	if err := validateRootFSWriterBindingVersion(normalized.BindingVersion); err != nil {
		return nil, err
	}
	if normalized.ProofVersion != RootFSWriterCrashAbandonProofVersion {
		return nil, fmt.Errorf("unsupported proof_version %d", normalized.ProofVersion)
	}
	if err := validateRootFSWriterDigest("binding_digest", normalized.BindingDigest); err != nil {
		return nil, err
	}
	if err := validateRootFSWriterDigest("proof_digest", normalized.ProofDigest); err != nil {
		return nil, err
	}
	return &normalized, nil
}

func validateRootFSWriterBindingVersion(version int) error {
	if version != RootFSWriterBindingVersion {
		return fmt.Errorf("unsupported binding_version %d", version)
	}
	return nil
}

func validateRootFSWriterDigest(name string, digest []byte) error {
	if len(digest) != sha256.Size {
		return fmt.Errorf("%s must contain exactly %d bytes", name, sha256.Size)
	}
	return nil
}

func rootFSWriterGrantMatchesIssue(record *rootFSWriterGrantRecord, req *IssueRootFSWriterGrantRequest, tokenDigest []byte) bool {
	return record != nil && req != nil &&
		record.ID == req.GrantID && record.SandboxID == req.SandboxID &&
		record.ClaimID == req.ClaimID && record.SlotID == req.SlotID &&
		record.IssueOperationID == req.OperationID && record.WriterEpoch == req.ExpectedWriterEpoch+1 &&
		record.InitialGenerationID == req.InitialGenerationID && record.NodeUID == req.NodeUID &&
		record.NodeBootID == req.NodeBootID && record.PodNamespace == req.PodNamespace &&
		record.PodName == req.PodName && record.PodUID == req.PodUID &&
		record.NodeName == req.NodeName && record.GateParent == req.GateParent &&
		record.RuntimeGeneration == req.RuntimeGeneration &&
		record.ConsumeExpiresAt.Equal(req.ConsumeExpiresAt) &&
		record.BindingVersion == req.BindingVersion &&
		bytes.Equal(record.BindingDigest, req.BindingDigest) &&
		bytes.Equal(record.tokenDigest, tokenDigest)
}

func rootFSWriterGrantMatchesConsume(record *rootFSWriterGrantRecord, req *ConsumeRootFSWriterGrantRequest, tokenDigest []byte) bool {
	return record != nil && req != nil && record.ID == req.GrantID &&
		record.WriterEpoch == req.WriterEpoch && record.NodeUID == req.ConsumerNodeUID &&
		record.BindingVersion == req.BindingVersion &&
		bytes.Equal(record.BindingDigest, req.BindingDigest) && bytes.Equal(record.tokenDigest, tokenDigest)
}

func rootFSWriterGrantMatchesRenew(record *rootFSWriterGrantRecord, req *RenewRootFSWriterGrantRequest) bool {
	return record != nil && req != nil && record.ID == req.GrantID &&
		record.WriterEpoch == req.WriterEpoch && record.NodeUID == req.ConsumerNodeUID &&
		record.BindingVersion == req.BindingVersion &&
		bytes.Equal(record.BindingDigest, req.BindingDigest)
}

func rootFSWriterGrantMatchesCancel(record *rootFSWriterGrantRecord, req *CancelRootFSWriterGrantRequest) bool {
	return record != nil && req != nil && record.ID == req.GrantID &&
		record.WriterEpoch == req.WriterEpoch && record.IssueOperationID == req.OperationID &&
		record.BindingVersion == req.BindingVersion &&
		bytes.Equal(record.BindingDigest, req.BindingDigest)
}

func rootFSWriterGrantMatchesRetire(record *rootFSWriterGrantRecord, grantID string, writerEpoch int64, operationID string, bindingVersion int, bindingDigest []byte) bool {
	return rootFSWriterGrantMatchesRetireBase(record, grantID, writerEpoch, bindingVersion, bindingDigest) &&
		record.RetireOperationID == operationID
}

func rootFSWriterGrantMatchesRetireBase(record *rootFSWriterGrantRecord, grantID string, writerEpoch int64, bindingVersion int, bindingDigest []byte) bool {
	return record != nil && record.ID == grantID && record.WriterEpoch == writerEpoch &&
		record.BindingVersion == bindingVersion &&
		bytes.Equal(record.BindingDigest, bindingDigest)
}

func rootFSWriterGrantStateError(record *rootFSWriterGrantRecord) error {
	if record == nil {
		return ErrRootFSWriterGrantNotFound
	}
	return fmt.Errorf("%w: grant %s is %s", ErrRootFSWriterGrantInvalidState, record.ID, record.State)
}

func mapRootFSWriterGrantConflict(action string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %s", ErrRootFSWriterGrantConflict, pgErr.ConstraintName)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func getRootFSWriterEpoch(ctx context.Context, db rootFSWriterGrantDB, filesystemID string) (int64, error) {
	var epoch int64
	if err := db.QueryRow(ctx, `
		SELECT writer_epoch
		FROM manager.rootfs_filesystems
		WHERE filesystem_id = $1
	`, filesystemID).Scan(&epoch); err != nil {
		return 0, fmt.Errorf("get rootfs writer epoch: %w", err)
	}
	return epoch, nil
}

func getRootFSWriterGrant(ctx context.Context, db rootFSWriterGrantDB, grantID string) (*rootFSWriterGrantRecord, error) {
	record, err := scanRootFSWriterGrant(db.QueryRow(ctx, rootFSWriterGrantSelectSQL()+` WHERE grant_id = $1`, strings.TrimSpace(grantID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSWriterGrantNotFound, grantID)
	}
	return record, err
}

func getRootFSWriterGrantForUpdate(ctx context.Context, db rootFSWriterGrantDB, grantID string) (*rootFSWriterGrantRecord, error) {
	record, err := scanRootFSWriterGrant(db.QueryRow(ctx,
		rootFSWriterGrantSelectSQL()+` WHERE grant_id = $1 FOR UPDATE`, strings.TrimSpace(grantID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSWriterGrantNotFound, grantID)
	}
	return record, err
}

type rootFSWriterLifecycleTxnRecord struct {
	SandboxID           string
	Kind                string
	Source              string
	Phase               string
	FromGeneration      int64
	FromPodNamespace    string
	FromPodName         string
	ExpectedHeadLayerID string
	PreparedHeadLayerID string
	Error               string
	CancelRequested     bool
}

func lockRootFSWriterLifecycleTxn(ctx context.Context, db rootFSWriterGrantDB, txnID string) (*rootFSWriterLifecycleTxnRecord, error) {
	var lifecycle rootFSWriterLifecycleTxnRecord
	err := db.QueryRow(ctx, `
		SELECT sandbox_id, kind, source, phase, from_generation,
			from_pod_namespace, from_pod_name,
			expected_head_layer_id, prepared_head_layer_id, error,
			cancel_requested_at IS NOT NULL
		FROM manager.sandbox_lifecycle_txns
		WHERE txn_id = $1
		FOR UPDATE
	`, strings.TrimSpace(txnID)).Scan(
		&lifecycle.SandboxID, &lifecycle.Kind, &lifecycle.Source, &lifecycle.Phase,
		&lifecycle.FromGeneration, &lifecycle.FromPodNamespace, &lifecycle.FromPodName,
		&lifecycle.ExpectedHeadLayerID, &lifecycle.PreparedHeadLayerID, &lifecycle.Error,
		&lifecycle.CancelRequested,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: lifecycle txn %s", ErrRootFSWriterGrantConflict, txnID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock rootfs writer lifecycle txn: %w", err)
	}
	return &lifecycle, nil
}

func rootFSWriterLayerMatchesState(ctx context.Context, db rootFSWriterGrantDB, state *SandboxRootFSState) (bool, error) {
	if state == nil {
		return false, nil
	}
	layer, err := scanRootFSLayer(db.QueryRow(ctx, `
		SELECT layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
			runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
			snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
			diff_size, diff_object_key, platform_os, platform_architecture,
			platform_variant, created_at
		FROM manager.rootfs_layers
		WHERE layer_id = $1
	`, state.LayerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load published rootfs layer %s: %w", state.LayerID, err)
	}
	return layer.ID == state.LayerID &&
		layer.ParentLayerID == state.ParentLayerID &&
		layer.SourceSandboxID == state.SandboxID &&
		layer.TeamID == state.TeamID &&
		layer.RuntimeGeneration == state.RuntimeGeneration &&
		layer.Runtime == state.Runtime &&
		layer.RuntimeHandler == state.RuntimeHandler &&
		layer.BaseImageRef == state.BaseImageRef &&
		layer.BaseImageDigest == state.BaseImageDigest &&
		layer.PlatformOS == state.PlatformOS &&
		layer.PlatformArchitecture == state.PlatformArchitecture &&
		layer.PlatformVariant == state.PlatformVariant &&
		layer.Snapshotter == state.Snapshotter &&
		layer.SnapshotParent == state.SnapshotParent &&
		slices.Equal(layer.SnapshotParentChain, state.SnapshotParentChain) &&
		layer.DiffDigest == state.DiffDigest &&
		layer.DiffID == state.DiffID &&
		layer.DiffMediaType == state.DiffMediaType &&
		layer.DiffSize == state.DiffSize &&
		layer.DiffObjectKey == state.DiffObjectKey, nil
}

func getRootFSWriterGrantByOperation(ctx context.Context, db rootFSWriterGrantDB, operationID string) (*rootFSWriterGrantRecord, error) {
	record, err := scanRootFSWriterGrant(db.QueryRow(ctx, rootFSWriterGrantSelectSQL()+` WHERE issue_operation_id = $1`, strings.TrimSpace(operationID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: operation %s", ErrRootFSWriterGrantNotFound, operationID)
	}
	return record, err
}

func rootFSWriterGrantSelectSQL() string {
	return `
		SELECT grant_id, filesystem_id, sandbox_id, claim_id, slot_id,
			issue_operation_id, writer_epoch, state, initial_head_layer_id, initial_generation_id,
			binding_version, binding_digest, token_digest, node_uid, node_boot_id,
			runtime_pod_namespace, runtime_pod_name, runtime_pod_uid,
			runtime_node_name, runtime_gate_parent, runtime_generation,
			consumer_node_uid, consumer_ctld_pod_uid, consume_expires_at,
			consumed_at, lease_expires_at, retire_operation_id, retire_kind,
			retire_proof_digest, retire_started_at, retired_at, canceled_at,
			created_at, updated_at, NOW()
		FROM manager.rootfs_writer_grants`
}

type rootFSWriterGrantScanner interface {
	Scan(...any) error
}

func scanRootFSWriterGrant(row rootFSWriterGrantScanner) (*rootFSWriterGrantRecord, error) {
	var record rootFSWriterGrantRecord
	var consumedAt, leaseExpiresAt, retireStartedAt, retiredAt, canceledAt pgtype.Timestamptz
	if err := row.Scan(
		&record.ID, &record.FilesystemID, &record.SandboxID, &record.ClaimID, &record.SlotID,
		&record.IssueOperationID, &record.WriterEpoch, &record.State, &record.InitialHeadLayerID, &record.InitialGenerationID,
		&record.BindingVersion, &record.BindingDigest, &record.tokenDigest, &record.NodeUID, &record.NodeBootID,
		&record.PodNamespace, &record.PodName, &record.PodUID,
		&record.NodeName, &record.GateParent, &record.RuntimeGeneration,
		&record.ConsumerNodeUID, &record.ConsumerCtldPodUID, &record.ConsumeExpiresAt,
		&consumedAt, &leaseExpiresAt, &record.RetireOperationID, &record.RetireKind,
		&record.RetireProofDigest, &retireStartedAt, &retiredAt, &canceledAt,
		&record.CreatedAt, &record.UpdatedAt, &record.databaseNow,
	); err != nil {
		return nil, err
	}
	if consumedAt.Valid {
		record.ConsumedAt = consumedAt.Time
	}
	if leaseExpiresAt.Valid {
		record.LeaseExpiresAt = leaseExpiresAt.Time
	}
	if retireStartedAt.Valid {
		record.RetireStartedAt = retireStartedAt.Time
	}
	if retiredAt.Valid {
		record.RetiredAt = retiredAt.Time
	}
	if canceledAt.Valid {
		record.CanceledAt = canceledAt.Time
	}
	record.AuthorityObservedAt = record.databaseNow
	record.BindingDigest = append([]byte(nil), record.BindingDigest...)
	record.tokenDigest = append([]byte(nil), record.tokenDigest...)
	record.RetireProofDigest = append([]byte(nil), record.RetireProofDigest...)
	return &record, nil
}

func cloneRootFSWriterGrant(record *RootFSWriterGrant) *RootFSWriterGrant {
	if record == nil {
		return nil
	}
	clone := *record
	clone.BindingDigest = append([]byte(nil), record.BindingDigest...)
	clone.RetireProofDigest = append([]byte(nil), record.RetireProofDigest...)
	return &clone
}
