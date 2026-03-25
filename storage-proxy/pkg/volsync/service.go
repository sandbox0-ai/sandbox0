package volsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	pathpkg "path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sirupsen/logrus"
)

const (
	defaultListLimit    = 256
	maxListLimit        = 2048
	writeCoalesceWindow = time.Second
	defaultReplicaLease = 5 * time.Minute
)

var (
	ErrReplicaNotFound        = errors.New("sync replica not found")
	ErrReplicaLeaseExpired    = errors.New("sync replica lease expired")
	ErrCursorRegression       = errors.New("sync cursor regression")
	ErrCursorAhead            = errors.New("sync cursor ahead of head")
	ErrReseedRequired         = errors.New("sync reseed required")
	ErrInvalidRetentionTarget = errors.New("invalid sync retention target")
	ErrInvalidChange          = errors.New("invalid sync change")
	ErrInvalidRequestID       = errors.New("invalid sync request id")
	ErrRequestIDConflict      = errors.New("sync request id reused with different payload")
	ErrConflictNotFound       = errors.New("sync conflict not found")
	ErrInvalidConflictStatus  = errors.New("invalid sync conflict status")
	ErrNamespaceIncompatible  = errors.New("sync namespace incompatible")
)

type repository interface {
	GetSandboxVolume(context.Context, string) (*db.SandboxVolume, error)
	WithTx(context.Context, func(pgx.Tx) error) error
	UpsertSyncReplica(context.Context, *db.SyncReplica) error
	UpsertSyncReplicaTx(context.Context, db.DB, *db.SyncReplica) error
	GetSyncReplica(context.Context, string, string) (*db.SyncReplica, error)
	GetSyncReplicaForUpdate(context.Context, pgx.Tx, string, string) (*db.SyncReplica, error)
	GetSyncNamespacePolicy(context.Context, string) (*db.SyncNamespacePolicy, error)
	GetSyncNamespacePolicyForUpdateTx(context.Context, pgx.Tx, string) (*db.SyncNamespacePolicy, error)
	UpsertSyncNamespacePolicyTx(context.Context, pgx.Tx, *db.SyncNamespacePolicy) error
	TouchSyncReplicaTx(context.Context, pgx.Tx, string, string, time.Time) error
	UpdateSyncReplicaCursorTx(context.Context, pgx.Tx, string, string, int64, time.Time) error
	GetSyncHead(context.Context, string) (int64, error)
	GetSyncRetentionState(context.Context, string) (*db.SyncRetentionState, error)
	GetSyncRetentionStateForUpdateTx(context.Context, pgx.Tx, string) (*db.SyncRetentionState, error)
	ListSyncJournalEntries(context.Context, string, int64, int) ([]*db.SyncJournalEntry, error)
	DeleteSyncJournalEntriesUpToTx(context.Context, pgx.Tx, string, int64) (int64, error)
	CreateSyncJournalEntryTx(context.Context, pgx.Tx, *db.SyncJournalEntry) error
	CreateSyncConflictTx(context.Context, pgx.Tx, *db.SyncConflict) error
	ListSyncConflicts(context.Context, string, string, int) ([]*db.SyncConflict, error)
	GetSyncConflict(context.Context, string, string) (*db.SyncConflict, error)
	UpdateSyncConflictTx(context.Context, pgx.Tx, string, string, string, *json.RawMessage) error
	GetSyncRequestTx(context.Context, pgx.Tx, string, string, string) (*db.SyncRequest, error)
	CreateSyncRequestTx(context.Context, pgx.Tx, *db.SyncRequest) error
	UpsertSyncRetentionStateTx(context.Context, pgx.Tx, *db.SyncRetentionState) error
	GetLatestSyncJournalEntryByNormalizedPath(context.Context, string, string) (*db.SyncJournalEntry, error)
	GetLatestSyncJournalEntryByNormalizedPathTx(context.Context, pgx.Tx, string, string) (*db.SyncJournalEntry, error)
}

// Service owns durable volume-sync metadata and journal behavior.
type Service struct {
	repo           repository
	logger         *logrus.Logger
	metrics        *obsmetrics.StorageProxyMetrics
	artifactWriter conflictArtifactWriter
	changeApplier  replicaChangeApplier
	barrier        volumeMutationBarrier
}

type conflictArtifactWriter interface {
	MaterializeConflict(context.Context, *db.SandboxVolume, *db.SyncConflict) (*ArtifactMaterialization, error)
}

type ArtifactMaterialization struct {
	SizeBytes int64
}

type volumeMutationBarrier interface {
	WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error
	WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error
}

type UpsertReplicaRequest struct {
	VolumeID      string
	TeamID        string
	ReplicaID     string
	DisplayName   string
	Platform      string
	RootPath      string
	CaseSensitive bool
	Capabilities  *pathnorm.FilesystemCapabilities
}

type ReplicaEnvelope struct {
	Replica *db.SyncReplica `json:"replica"`
	HeadSeq int64           `json:"head_seq"`
}

type ListChangesRequest struct {
	VolumeID string
	TeamID   string
	AfterSeq int64
	Limit    int
}

type ListChangesResponse struct {
	HeadSeq          int64                  `json:"head_seq"`
	RetainedAfterSeq int64                  `json:"retained_after_seq"`
	Changes          []*db.SyncJournalEntry `json:"changes"`
}

type ChangeRequest struct {
	EventType     string          `json:"event_type"`
	Path          string          `json:"path,omitempty"`
	OldPath       string          `json:"old_path,omitempty"`
	EntryKind     string          `json:"entry_kind,omitempty"`
	ContentBase64 *string         `json:"content_base64,omitempty"`
	Mode          *uint32         `json:"mode,omitempty"`
	ContentSHA256 *string         `json:"content_sha256,omitempty"`
	SizeBytes     *int64          `json:"size_bytes,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

type AppendChangesRequest struct {
	VolumeID  string
	TeamID    string
	ReplicaID string
	RequestID string
	BaseSeq   int64
	Changes   []ChangeRequest
}

type AppendChangesResponse struct {
	HeadSeq   int64                  `json:"head_seq"`
	Accepted  []*db.SyncJournalEntry `json:"accepted"`
	Conflicts []*db.SyncConflict     `json:"conflicts"`
}

type UpdateCursorRequest struct {
	VolumeID       string
	TeamID         string
	ReplicaID      string
	LastAppliedSeq int64
}

type CompactJournalRequest struct {
	VolumeID            string
	TeamID              string
	CompactedThroughSeq int64
}

type CompactJournalResponse struct {
	HeadSeq             int64 `json:"head_seq"`
	CompactedThroughSeq int64 `json:"compacted_through_seq"`
	DeletedEntries      int64 `json:"deleted_entries"`
}

type ListConflictsRequest struct {
	VolumeID string
	TeamID   string
	Status   string
	Limit    int
}

type ListConflictsResponse struct {
	Conflicts []*db.SyncConflict `json:"conflicts"`
}

type ResolveConflictRequest struct {
	VolumeID   string
	TeamID     string
	ConflictID string
	Status     string
	Resolution string
	Note       string
}

type NamespaceMutationRequest struct {
	VolumeID  string
	TeamID    string
	EventType string
	Path      string
	OldPath   string
}

type NamespaceCompatibilityError struct {
	Issues       []pathnorm.CompatibilityIssue   `json:"issues"`
	Capabilities pathnorm.FilesystemCapabilities `json:"capabilities"`
}

func (e *NamespaceCompatibilityError) Error() string {
	return ErrNamespaceIncompatible.Error()
}

func (e *NamespaceCompatibilityError) Unwrap() error {
	return ErrNamespaceIncompatible
}

func (s *Service) GetHead(ctx context.Context, volumeID, teamID string) (int64, error) {
	if _, err := s.getAccessibleVolume(ctx, volumeID, teamID); err != nil {
		return 0, err
	}
	return s.repo.GetSyncHead(ctx, volumeID)
}

type RemoteChange struct {
	VolumeID   string
	TeamID     string
	SandboxID  string
	EventType  string
	Path       string
	OldPath    string
	OccurredAt time.Time
}

func NewService(repo repository, logger *logrus.Logger) *Service {
	return &Service{repo: repo, logger: logger}
}

func (s *Service) SetConflictArtifactWriter(writer conflictArtifactWriter) {
	s.artifactWriter = writer
}

func (s *Service) SetMetrics(metrics *obsmetrics.StorageProxyMetrics) {
	s.metrics = metrics
}

func (s *Service) SetReplicaChangeApplier(applier replicaChangeApplier) {
	s.changeApplier = applier
}

func (s *Service) SetVolumeMutationBarrier(barrier volumeMutationBarrier) {
	s.barrier = barrier
}

func (s *Service) UpsertReplica(ctx context.Context, req *UpsertReplicaRequest) (resp *ReplicaEnvelope, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("upsert_replica", start, err)
		if err == nil && resp != nil {
			s.observeReplicaLag("upsert_replica", resp.HeadSeq, resp.Replica.LastAppliedSeq)
		}
	}()
	if _, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID); err != nil {
		return nil, err
	}
	if req.ReplicaID == "" {
		return nil, ErrReplicaNotFound
	}

	now := time.Now().UTC()
	requestedCapabilities := pathnorm.NormalizeFilesystemCapabilities(req.Platform, req.CaseSensitive, req.Capabilities)
	if err := s.repo.WithTx(ctx, func(tx pgx.Tx) error {
		policy, err := s.getNamespacePolicyTx(ctx, tx, req.VolumeID)
		if err != nil {
			return err
		}
		effectiveCapabilities := mergeNamespaceCapabilities(policy, requestedCapabilities)
		if err := s.repo.UpsertSyncNamespacePolicyTx(ctx, tx, &db.SyncNamespacePolicy{
			VolumeID:     req.VolumeID,
			TeamID:       req.TeamID,
			Capabilities: effectiveCapabilities,
			UpdatedAt:    now,
		}); err != nil {
			return err
		}
		return s.repo.UpsertSyncReplicaTx(ctx, tx, &db.SyncReplica{
			ID:             req.ReplicaID,
			VolumeID:       req.VolumeID,
			TeamID:         req.TeamID,
			DisplayName:    req.DisplayName,
			Platform:       req.Platform,
			RootPath:       req.RootPath,
			CaseSensitive:  effectiveCapabilities.CaseSensitive,
			Capabilities:   effectiveCapabilities,
			LastSeenAt:     now,
			LastAppliedSeq: 0,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}); err != nil {
		return nil, err
	}
	head, err := s.repo.GetSyncHead(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	stored, err := s.repo.GetSyncReplica(ctx, req.VolumeID, req.ReplicaID)
	if err != nil {
		return nil, translateReplicaErr(err)
	}
	return &ReplicaEnvelope{Replica: stored, HeadSeq: head}, nil
}

func (s *Service) GetReplica(ctx context.Context, volumeID, teamID, replicaID string) (resp *ReplicaEnvelope, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("get_replica", start, err)
		if err == nil && resp != nil {
			s.observeReplicaLag("get_replica", resp.HeadSeq, resp.Replica.LastAppliedSeq)
		}
	}()
	if _, err := s.getAccessibleVolume(ctx, volumeID, teamID); err != nil {
		return nil, err
	}
	replica, err := s.repo.GetSyncReplica(ctx, volumeID, replicaID)
	if err != nil {
		return nil, translateReplicaErr(err)
	}
	head, err := s.repo.GetSyncHead(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	return &ReplicaEnvelope{Replica: replica, HeadSeq: head}, nil
}

func (s *Service) ValidateNamespaceMutation(ctx context.Context, req *NamespaceMutationRequest) error {
	if req == nil {
		return nil
	}
	if _, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID); err != nil {
		return err
	}
	policy, err := s.getNamespacePolicy(ctx, req.VolumeID)
	if err != nil {
		return err
	}
	if !pathnorm.RequiresPortableNameAudit(policy) {
		return nil
	}

	issues := issuesForNamespaceMutation(req, policy)
	if len(issues) > 0 {
		return &NamespaceCompatibilityError{
			Issues:       issues,
			Capabilities: policy,
		}
	}

	if !requiresCasefoldReservation(req.EventType) {
		return nil
	}
	conflict, err := s.detectNamespaceReservationConflict(ctx, req, policy)
	if err != nil {
		return err
	}
	if conflict == nil {
		return nil
	}
	return &NamespaceCompatibilityError{
		Issues:       []pathnorm.CompatibilityIssue{*conflict},
		Capabilities: policy,
	}
}

func (s *Service) ListChanges(ctx context.Context, req *ListChangesRequest) (resp *ListChangesResponse, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("list_changes", start, err)
		if err == nil && resp != nil {
			s.observeReplicaLag("list_changes", resp.HeadSeq, req.AfterSeq)
		}
	}()
	if _, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID); err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	head, err := s.repo.GetSyncHead(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	retention, err := s.getRetentionState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	if req.AfterSeq < retention.CompactedThroughSeq {
		s.observeReseedRequired("list_changes")
		return nil, newReseedRequiredError(retention.CompactedThroughSeq, head)
	}
	changes, err := s.repo.ListSyncJournalEntries(ctx, req.VolumeID, req.AfterSeq, limit)
	if err != nil {
		return nil, err
	}
	return &ListChangesResponse{
		HeadSeq:          head,
		RetainedAfterSeq: retention.CompactedThroughSeq,
		Changes:          changes,
	}, nil
}

func (s *Service) AppendReplicaChanges(ctx context.Context, req *AppendChangesRequest) (resp *AppendChangesResponse, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("append_changes", start, err)
		if err == nil && resp != nil {
			s.observeReplicaLag("append_changes", resp.HeadSeq, req.BaseSeq)
		}
	}()
	volume, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.RequestID) == "" {
		return nil, ErrInvalidRequestID
	}
	requestFingerprint, err := fingerprintAppendChangesRequest(req)
	if err != nil {
		return nil, err
	}

	var result *AppendChangesResponse
	run := func(runCtx context.Context) error {
		return s.repo.WithTx(runCtx, func(tx pgx.Tx) error {
			replica, err := s.repo.GetSyncReplicaForUpdate(runCtx, tx, req.VolumeID, req.ReplicaID)
			if err != nil {
				return translateReplicaErr(err)
			}
			policy, err := s.getNamespacePolicyTx(runCtx, tx, req.VolumeID)
			if err != nil {
				return err
			}
			replica.Capabilities = mergeNamespaceCapabilities(policy, replica.Capabilities)
			replica.CaseSensitive = replica.Capabilities.CaseSensitive

			storedRequest, err := s.repo.GetSyncRequestTx(runCtx, tx, req.VolumeID, req.ReplicaID, req.RequestID)
			if err == nil {
				if storedRequest.RequestFingerprint != requestFingerprint {
					s.observeRequestReplay("collision")
					return ErrRequestIDConflict
				}
				s.observeRequestReplay("replay")
				result, err = decodeAppendChangesResponse(storedRequest.ResponsePayload)
				return err
			}
			if err != nil && !errors.Is(err, db.ErrNotFound) {
				return err
			}

			if err := validateReplicaLease(replica, time.Now().UTC()); err != nil {
				return err
			}

			head, err := s.repo.GetSyncHead(runCtx, req.VolumeID)
			if err != nil {
				return err
			}
			retention, err := s.getRetentionStateTx(runCtx, tx, req.VolumeID)
			if err != nil {
				return err
			}
			if req.BaseSeq > head {
				return ErrCursorAhead
			}
			if req.BaseSeq < retention.CompactedThroughSeq {
				s.observeReseedRequired("append_changes")
				return newReseedRequiredError(retention.CompactedThroughSeq, head)
			}
			if req.BaseSeq < replica.LastAppliedSeq {
				return ErrCursorRegression
			}

			now := time.Now().UTC()
			response := &AppendChangesResponse{
				HeadSeq:   head,
				Accepted:  []*db.SyncJournalEntry{},
				Conflicts: []*db.SyncConflict{},
			}
			for _, change := range req.Changes {
				entry, conflict, err := s.prepareReplicaEntry(runCtx, tx, req, replica, change, now)
				if err != nil {
					return err
				}
				if conflict != nil {
					artifactEntry, err := s.materializeConflict(runCtx, volume, conflict, now)
					if err != nil {
						return err
					}
					if err := s.repo.CreateSyncConflictTx(runCtx, tx, conflict); err != nil {
						return err
					}
					s.observeConflict("replica", conflict.Reason)
					s.logger.WithFields(logrus.Fields{
						"volume_id":     req.VolumeID,
						"replica_id":    req.ReplicaID,
						"conflict_id":   conflict.ID,
						"reason":        conflict.Reason,
						"artifact_path": conflict.ArtifactPath,
					}).Info("Recorded volume sync conflict for replica change")
					if artifactEntry != nil {
						if err := s.repo.CreateSyncJournalEntryTx(runCtx, tx, artifactEntry); err != nil {
							return err
						}
						response.HeadSeq = artifactEntry.Seq
					}
					response.Conflicts = append(response.Conflicts, conflict)
					continue
				}
				if s.changeApplier != nil {
					if err := s.changeApplier.ApplyChange(runCtx, volume, change); err != nil {
						return err
					}
				}
				if err := s.repo.CreateSyncJournalEntryTx(runCtx, tx, entry); err != nil {
					return err
				}
				response.HeadSeq = entry.Seq
				response.Accepted = append(response.Accepted, entry)
			}

			if err := s.repo.TouchSyncReplicaTx(runCtx, tx, req.VolumeID, req.ReplicaID, now); err != nil {
				return translateReplicaErr(err)
			}
			payload, err := encodeAppendChangesResponse(response)
			if err != nil {
				return err
			}
			if err := s.repo.CreateSyncRequestTx(runCtx, tx, &db.SyncRequest{
				VolumeID:           req.VolumeID,
				ReplicaID:          req.ReplicaID,
				RequestID:          req.RequestID,
				RequestFingerprint: requestFingerprint,
				ResponsePayload:    payload,
				CreatedAt:          now,
			}); err != nil {
				return err
			}
			result = response
			return nil
		})
	}
	if s.barrier != nil {
		err = s.barrier.WithShared(ctx, req.VolumeID, run)
	} else {
		err = run(ctx)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return &AppendChangesResponse{
			Accepted:  []*db.SyncJournalEntry{},
			Conflicts: []*db.SyncConflict{},
		}, nil
	}
	return result, nil
}

func (s *Service) UpdateReplicaCursor(ctx context.Context, req *UpdateCursorRequest) (resp *ReplicaEnvelope, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("update_cursor", start, err)
		if err == nil && resp != nil {
			s.observeReplicaLag("update_cursor", resp.HeadSeq, resp.Replica.LastAppliedSeq)
		}
	}()
	if _, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID); err != nil {
		return nil, err
	}
	err = s.repo.WithTx(ctx, func(tx pgx.Tx) error {
		replica, err := s.repo.GetSyncReplicaForUpdate(ctx, tx, req.VolumeID, req.ReplicaID)
		if err != nil {
			return translateReplicaErr(err)
		}
		if err := validateReplicaLease(replica, time.Now().UTC()); err != nil {
			return err
		}
		head, err := s.repo.GetSyncHead(ctx, req.VolumeID)
		if err != nil {
			return err
		}
		retention, err := s.getRetentionStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return err
		}
		if req.LastAppliedSeq > head {
			return ErrCursorAhead
		}
		if req.LastAppliedSeq < retention.CompactedThroughSeq {
			s.observeReseedRequired("update_cursor")
			return newReseedRequiredError(retention.CompactedThroughSeq, head)
		}
		if req.LastAppliedSeq < replica.LastAppliedSeq {
			return ErrCursorRegression
		}
		return s.repo.UpdateSyncReplicaCursorTx(ctx, tx, req.VolumeID, req.ReplicaID, req.LastAppliedSeq, time.Now().UTC())
	})
	if err != nil {
		return nil, err
	}
	return s.GetReplica(ctx, req.VolumeID, req.TeamID, req.ReplicaID)
}

func (s *Service) CompactJournal(ctx context.Context, req *CompactJournalRequest) (resp *CompactJournalResponse, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("compact_journal", start, err)
		if err != nil {
			s.observeCompaction("failure", 0)
			return
		}
		if resp != nil {
			s.observeCompaction("success", resp.DeletedEntries)
		}
	}()
	if _, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID); err != nil {
		return nil, err
	}
	if req.CompactedThroughSeq < 0 {
		return nil, ErrInvalidRetentionTarget
	}

	var result *CompactJournalResponse
	run := func(runCtx context.Context) error {
		return s.repo.WithTx(runCtx, func(tx pgx.Tx) error {
			head, err := s.repo.GetSyncHead(runCtx, req.VolumeID)
			if err != nil {
				return err
			}
			if req.CompactedThroughSeq > head {
				return ErrInvalidRetentionTarget
			}

			retention, err := s.getRetentionStateTx(runCtx, tx, req.VolumeID)
			if err != nil {
				return err
			}
			target := req.CompactedThroughSeq
			if target < retention.CompactedThroughSeq {
				target = retention.CompactedThroughSeq
			}

			deletedEntries := int64(0)
			if target > retention.CompactedThroughSeq {
				deletedEntries, err = s.repo.DeleteSyncJournalEntriesUpToTx(runCtx, tx, req.VolumeID, target)
				if err != nil {
					return err
				}
				if err := s.repo.UpsertSyncRetentionStateTx(runCtx, tx, &db.SyncRetentionState{
					VolumeID:            req.VolumeID,
					TeamID:              req.TeamID,
					CompactedThroughSeq: target,
					UpdatedAt:           time.Now().UTC(),
				}); err != nil {
					return err
				}
			}

			result = &CompactJournalResponse{
				HeadSeq:             head,
				CompactedThroughSeq: target,
				DeletedEntries:      deletedEntries,
			}
			return nil
		})
	}
	if s.barrier != nil {
		err = s.barrier.WithExclusive(ctx, req.VolumeID, run)
	} else {
		err = run(ctx)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return &CompactJournalResponse{}, nil
	}
	return result, nil
}

func (s *Service) ListConflicts(ctx context.Context, req *ListConflictsRequest) (resp *ListConflictsResponse, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("list_conflicts", start, err)
	}()
	if _, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID); err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	conflicts, err := s.repo.ListSyncConflicts(ctx, req.VolumeID, req.Status, limit)
	if err != nil {
		return nil, err
	}
	return &ListConflictsResponse{Conflicts: conflicts}, nil
}

func (s *Service) ResolveConflict(ctx context.Context, req *ResolveConflictRequest) (resp *db.SyncConflict, err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("resolve_conflict", start, err)
	}()
	if _, err := s.getAccessibleVolume(ctx, req.VolumeID, req.TeamID); err != nil {
		return nil, err
	}
	if !isMutableConflictStatus(req.Status) {
		return nil, ErrInvalidConflictStatus
	}

	metadata := mustMarshalMetadata(map[string]any{
		"resolution":  req.Resolution,
		"note":        req.Note,
		"resolved_at": time.Now().UTC().Format(time.RFC3339),
	})

	if err := s.repo.WithTx(ctx, func(tx pgx.Tx) error {
		conflict, err := s.repo.GetSyncConflict(ctx, req.VolumeID, req.ConflictID)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return ErrConflictNotFound
			}
			return err
		}
		if conflict.TeamID != req.TeamID {
			return ErrConflictNotFound
		}
		if err := s.repo.UpdateSyncConflictTx(ctx, tx, req.VolumeID, req.ConflictID, req.Status, metadata); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return ErrConflictNotFound
			}
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	conflict, err := s.repo.GetSyncConflict(ctx, req.VolumeID, req.ConflictID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, ErrConflictNotFound
		}
		return nil, err
	}
	if conflict.TeamID != req.TeamID {
		return nil, ErrConflictNotFound
	}
	return conflict, nil
}

func (s *Service) RecordRemoteChange(ctx context.Context, change *RemoteChange) (err error) {
	start := time.Now()
	defer func() {
		s.observeOperation("record_remote_change", start, err)
	}()
	if change == nil || change.VolumeID == "" || change.TeamID == "" {
		return nil
	}
	if change.EventType == "" {
		return nil
	}

	volume, err := s.getAccessibleVolume(ctx, change.VolumeID, change.TeamID)
	if err != nil {
		return err
	}

	now := change.OccurredAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	normalizedPath := NormalizePath(change.Path)
	if change.EventType == db.SyncEventWrite && normalizedPath != "" {
		latest, err := s.repo.GetLatestSyncJournalEntryByNormalizedPath(ctx, change.VolumeID, normalizedPath)
		if err == nil && latest != nil && latest.Source == db.SyncSourceSandbox && latest.EventType == db.SyncEventWrite && latest.Path == change.Path && now.Sub(latest.CreatedAt) <= writeCoalesceWindow {
			return nil
		}
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return err
		}
	}

	return s.repo.WithTx(ctx, func(tx pgx.Tx) error {
		entry := &db.SyncJournalEntry{
			VolumeID:          change.VolumeID,
			TeamID:            change.TeamID,
			Source:            db.SyncSourceSandbox,
			EventType:         NormalizeEventType(change.EventType),
			Path:              change.Path,
			NormalizedPath:    normalizedPath,
			Tombstone:         change.EventType == db.SyncEventRemove,
			CreatedAt:         now,
			NormalizedOldPath: stringPtr(NormalizePath(change.OldPath)),
			OldPath:           nonEmptyStringPtr(change.OldPath),
		}
		if change.SandboxID != "" {
			entry.Metadata = mustMarshalMetadata(map[string]any{
				"sandbox_id": change.SandboxID,
			})
		}

		conflict, err := s.detectLatestConflict(ctx, tx, change.VolumeID, change.TeamID, nil, 0, entry.Path, entry.OldPath, entry.NormalizedPath, entry.NormalizedOldPath)
		if err != nil {
			return err
		}
		if conflict != nil {
			artifactEntry, err := s.materializeConflict(ctx, volume, conflict, now)
			if err != nil {
				return err
			}
			if err := s.repo.CreateSyncConflictTx(ctx, tx, conflict); err != nil {
				return err
			}
			s.observeConflict("sandbox", conflict.Reason)
			s.logger.WithFields(logrus.Fields{
				"volume_id":     change.VolumeID,
				"sandbox_id":    change.SandboxID,
				"conflict_id":   conflict.ID,
				"reason":        conflict.Reason,
				"artifact_path": conflict.ArtifactPath,
			}).Info("Recorded volume sync conflict for sandbox change")
			if artifactEntry != nil {
				if err := s.repo.CreateSyncJournalEntryTx(ctx, tx, artifactEntry); err != nil {
					return err
				}
			}
		}

		return s.repo.CreateSyncJournalEntryTx(ctx, tx, entry)
	})
}

func (s *Service) prepareReplicaEntry(ctx context.Context, tx pgx.Tx, req *AppendChangesRequest, replica *db.SyncReplica, change ChangeRequest, now time.Time) (*db.SyncJournalEntry, *db.SyncConflict, error) {
	eventType := NormalizeEventType(change.EventType)
	if eventType == "" {
		return nil, nil, ErrInvalidChange
	}

	pathValue := change.Path
	oldPathValue := change.OldPath
	normalizedPath := NormalizePath(pathValue)
	normalizedOldPath := NormalizePath(oldPathValue)
	if eventType != db.SyncEventInvalidate && normalizedPath == "" && normalizedOldPath == "" {
		return nil, nil, ErrInvalidChange
	}

	if pathnorm.RequiresPortableNameAudit(replica.Capabilities) {
		compatibilityConflict, err := buildCompatibilityConflictForChange(req, replica, change)
		if err != nil {
			return nil, nil, err
		}
		if compatibilityConflict != nil {
			return nil, compatibilityConflict, nil
		}
	}

	if !replica.Capabilities.CaseSensitive || replica.Capabilities.UnicodeNormalizationInsensitive {
		conflict, err := s.detectReplicaCasefoldConflict(
			ctx, tx, req.VolumeID, req.TeamID, &req.ReplicaID, req.BaseSeq,
			eventType, pathValue, nonEmptyStringPtr(oldPathValue), normalizedPath, nonEmptyStringPtr(normalizedOldPath),
		)
		if err != nil {
			return nil, nil, err
		}
		if conflict != nil {
			return nil, conflict, nil
		}
	}

	conflict, err := s.detectLatestConflict(
		ctx, tx, req.VolumeID, req.TeamID, &req.ReplicaID, req.BaseSeq,
		pathValue, nonEmptyStringPtr(oldPathValue), normalizedPath, nonEmptyStringPtr(normalizedOldPath),
	)
	if err != nil {
		return nil, nil, err
	}
	if conflict != nil {
		return nil, conflict, nil
	}

	entry := &db.SyncJournalEntry{
		VolumeID:       req.VolumeID,
		TeamID:         req.TeamID,
		Source:         db.SyncSourceReplica,
		ReplicaID:      &replica.ID,
		EventType:      eventType,
		Path:           pathValue,
		NormalizedPath: normalizedPath,
		OldPath:        nonEmptyStringPtr(oldPathValue),
		Tombstone:      eventType == db.SyncEventRemove,
		ContentSHA256:  change.ContentSHA256,
		SizeBytes:      change.SizeBytes,
		CreatedAt:      now,
	}
	if normalizedOldPath != "" {
		entry.NormalizedOldPath = &normalizedOldPath
	}
	if len(change.Metadata) > 0 {
		meta := json.RawMessage(slices.Clone(change.Metadata))
		entry.Metadata = &meta
	}
	return entry, nil, nil
}

func (s *Service) detectReplicaCasefoldConflict(
	ctx context.Context,
	tx pgx.Tx,
	volumeID, teamID string,
	replicaID *string,
	baseSeq int64,
	eventType string,
	pathValue string,
	oldPathValue *string,
	normalizedPath string,
	normalizedOldPath *string,
) (*db.SyncConflict, error) {
	type candidate struct {
		normalizedPath string
		incomingPath   string
	}

	candidates := make([]candidate, 0, 2)
	if normalizedPath != "" {
		candidates = append(candidates, candidate{
			normalizedPath: normalizedPath,
			incomingPath:   pathValue,
		})
	}
	if normalizedOldPath != nil && *normalizedOldPath != "" && *normalizedOldPath != normalizedPath {
		candidates = append(candidates, candidate{
			normalizedPath: *normalizedOldPath,
			incomingPath:   derefString(oldPathValue),
		})
	}

	for _, candidate := range candidates {
		latest, err := s.repo.GetLatestSyncJournalEntryByNormalizedPathTx(ctx, tx, volumeID, candidate.normalizedPath)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if latest.Seq > baseSeq || latest.Tombstone || latest.Path == "" || latest.Path == candidate.incomingPath {
			continue
		}
		if NormalizePath(latest.Path) != candidate.normalizedPath {
			continue
		}
		if eventType == db.SyncEventRename &&
			normalizedOldPath != nil &&
			*normalizedOldPath == normalizedPath &&
			latest.Path == derefString(oldPathValue) {
			continue
		}

		reason := "casefold_collision"
		if eventType == db.SyncEventRename && normalizedOldPath != nil && *normalizedOldPath == normalizedPath {
			reason = "case_only_rename_conflict"
		}
		return buildSyncConflict(
			volumeID, teamID, replicaID, baseSeq,
			firstNonEmpty(pathValue, derefString(oldPathValue)),
			candidate.normalizedPath,
			nonEmptyStringPtr(pathValue),
			oldPathValue,
			latest,
			reason,
		), nil
	}

	return nil, nil
}

func (s *Service) detectLatestConflict(
	ctx context.Context,
	tx pgx.Tx,
	volumeID, teamID string,
	replicaID *string,
	baseSeq int64,
	pathValue string,
	oldPathValue *string,
	normalizedPath string,
	normalizedOldPath *string,
) (*db.SyncConflict, error) {
	candidates := make([]string, 0, 2)
	if normalizedPath != "" {
		candidates = append(candidates, normalizedPath)
	}
	if normalizedOldPath != nil && *normalizedOldPath != "" && *normalizedOldPath != normalizedPath {
		candidates = append(candidates, *normalizedOldPath)
	}
	for _, candidate := range candidates {
		latest, err := s.repo.GetLatestSyncJournalEntryByNormalizedPathTx(ctx, tx, volumeID, candidate)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if latest.Seq <= baseSeq {
			continue
		}
		if latest.Source == db.SyncSourceReplica && latest.ReplicaID != nil && replicaID != nil && *latest.ReplicaID == *replicaID {
			continue
		}

		return buildSyncConflict(
			volumeID, teamID, replicaID, baseSeq,
			firstNonEmpty(pathValue, derefString(oldPathValue)),
			candidate,
			nonEmptyStringPtr(pathValue),
			oldPathValue,
			latest,
			conflictReason(latest, normalizedPath, pathValue),
		), nil
	}
	return nil, nil
}

func buildSyncConflict(
	volumeID, teamID string,
	replicaID *string,
	baseSeq int64,
	conflictPath, normalizedPath string,
	incomingPath, incomingOldPath *string,
	latest *db.SyncJournalEntry,
	reason string,
) *db.SyncConflict {
	existingSeq := latest.Seq
	return &db.SyncConflict{
		ID:              uuid.NewString(),
		VolumeID:        volumeID,
		TeamID:          teamID,
		ReplicaID:       replicaID,
		Path:            conflictPath,
		NormalizedPath:  normalizedPath,
		ArtifactPath:    buildConflictArtifactPath(conflictPath, replicaID, latest.Seq),
		IncomingPath:    incomingPath,
		IncomingOldPath: incomingOldPath,
		ExistingSeq:     &existingSeq,
		Reason:          reason,
		Status:          db.SyncConflictStatusOpen,
		Metadata: mustMarshalMetadata(map[string]any{
			"latest_seq":   latest.Seq,
			"latest_path":  latest.Path,
			"latest_event": latest.EventType,
			"base_seq":     baseSeq,
		}),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func buildCompatibilityConflictForChange(req *AppendChangesRequest, replica *db.SyncReplica, change ChangeRequest) (*db.SyncConflict, error) {
	issues := make([]pathnorm.CompatibilityIssue, 0, 2)
	for _, candidate := range []string{change.Path, change.OldPath} {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		issues = append(issues, pathnorm.ValidatePathCompatibility(candidate, replica.Capabilities)...)
	}
	issues = pathnorm.DeduplicateIssues(issues)
	if len(issues) == 0 {
		return nil, nil
	}

	primary := issues[0]
	conflictPath := primary.Path
	if conflictPath == "" {
		conflictPath = firstNonEmpty(change.Path, change.OldPath)
	}
	conflict := &db.SyncConflict{
		ID:              uuid.NewString(),
		VolumeID:        req.VolumeID,
		TeamID:          req.TeamID,
		ReplicaID:       &replica.ID,
		Path:            conflictPath,
		NormalizedPath:  NormalizePath(conflictPath),
		IncomingPath:    nonEmptyStringPtr(change.Path),
		IncomingOldPath: nonEmptyStringPtr(change.OldPath),
		Reason:          primary.Code,
		Status:          db.SyncConflictStatusOpen,
		Metadata: mustMarshalMetadata(map[string]any{
			"base_seq":     req.BaseSeq,
			"issues":       issues,
			"capabilities": replica.Capabilities,
		}),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	return conflict, nil
}

func issuesForNamespaceMutation(req *NamespaceMutationRequest, caps pathnorm.FilesystemCapabilities) []pathnorm.CompatibilityIssue {
	candidates := make([]string, 0, 1)
	switch NormalizeEventType(req.EventType) {
	case db.SyncEventCreate:
		candidates = append(candidates, req.Path)
	case db.SyncEventRename:
		candidates = append(candidates, req.Path)
	default:
		return nil
	}

	issues := make([]pathnorm.CompatibilityIssue, 0)
	for _, candidate := range candidates {
		issues = append(issues, pathnorm.ValidatePathCompatibility(candidate, caps)...)
	}
	return pathnorm.DeduplicateIssues(issues)
}

func requiresCasefoldReservation(eventType string) bool {
	switch NormalizeEventType(eventType) {
	case db.SyncEventCreate, db.SyncEventRename:
		return true
	default:
		return false
	}
}

func (s *Service) detectNamespaceReservationConflict(ctx context.Context, req *NamespaceMutationRequest, caps pathnorm.FilesystemCapabilities) (*pathnorm.CompatibilityIssue, error) {
	if req == nil || (!caps.UnicodeNormalizationInsensitive && caps.CaseSensitive) {
		return nil, nil
	}
	targetPath := strings.TrimSpace(req.Path)
	if targetPath == "" {
		return nil, nil
	}
	normalizedTarget := NormalizePath(targetPath)
	if normalizedTarget == "" {
		return nil, nil
	}
	latest, err := s.repo.GetLatestSyncJournalEntryByNormalizedPath(ctx, req.VolumeID, normalizedTarget)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if latest.Tombstone || latest.Path == "" || latest.Path == targetPath {
		return nil, nil
	}
	if NormalizeEventType(req.EventType) == db.SyncEventRename &&
		NormalizePath(req.OldPath) == normalizedTarget &&
		latest.Path == req.OldPath {
		return nil, nil
	}
	issue := pathnorm.BuildCasefoldCollisionIssue(normalizedTarget, []string{latest.Path, targetPath})
	return &issue, nil
}

func (s *Service) getAccessibleVolume(ctx context.Context, volumeID, teamID string) (*db.SandboxVolume, error) {
	if volumeID == "" || teamID == "" {
		return nil, db.ErrNotFound
	}
	volume, err := s.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if volume.TeamID != teamID {
		return nil, db.ErrNotFound
	}
	return volume, nil
}

func translateReplicaErr(err error) error {
	if errors.Is(err, db.ErrNotFound) {
		return ErrReplicaNotFound
	}
	return err
}

func validateReplicaLease(replica *db.SyncReplica, now time.Time) error {
	if replica == nil {
		return ErrReplicaNotFound
	}
	if replica.LastSeenAt.IsZero() {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if now.Sub(replica.LastSeenAt) > defaultReplicaLease {
		return ErrReplicaLeaseExpired
	}
	return nil
}

func (s *Service) observeOperation(operation string, started time.Time, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	var status string
	switch {
	case err == nil:
		status = "success"
	case errors.Is(err, ErrReseedRequired):
		status = "reseed_required"
	case errors.Is(err, ErrCursorAhead), errors.Is(err, ErrCursorRegression), errors.Is(err, ErrReplicaLeaseExpired), errors.Is(err, ErrRequestIDConflict), errors.Is(err, ErrConflictNotFound), errors.Is(err, ErrNamespaceIncompatible):
		status = "conflict"
	case errors.Is(err, ErrInvalidChange), errors.Is(err, ErrInvalidConflictStatus), errors.Is(err, ErrInvalidRequestID), errors.Is(err, ErrInvalidRetentionTarget):
		status = "invalid"
	case errors.Is(err, ErrReplicaNotFound), errors.Is(err, db.ErrNotFound):
		status = "not_found"
	default:
		status = "error"
	}
	s.metrics.VolumeSyncOperationsTotal.WithLabelValues(operation, status).Inc()
	s.metrics.VolumeSyncOperationDuration.WithLabelValues(operation).Observe(time.Since(started).Seconds())
}

func (s *Service) observeConflict(source, reason string) {
	if s == nil || s.metrics == nil {
		return
	}
	if source == "" {
		source = "unknown"
	}
	if reason == "" {
		reason = "unknown"
	}
	s.metrics.VolumeSyncConflictsTotal.WithLabelValues(source, reason).Inc()
}

func (s *Service) observeReseedRequired(operation string) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.VolumeSyncReseedTotal.WithLabelValues(operation).Inc()
}

func (s *Service) observeRequestReplay(result string) {
	if s == nil || s.metrics == nil {
		return
	}
	if result == "" {
		result = "unknown"
	}
	s.metrics.VolumeSyncRequestReplayTotal.WithLabelValues(result).Inc()
}

func (s *Service) observeReplicaLag(operation string, headSeq, replicaSeq int64) {
	if s == nil || s.metrics == nil {
		return
	}
	lag := headSeq - replicaSeq
	if lag < 0 {
		lag = 0
	}
	s.metrics.VolumeSyncReplicaLag.WithLabelValues(operation).Observe(float64(lag))
}

func (s *Service) observeCompaction(status string, deletedEntries int64) {
	if s == nil || s.metrics == nil {
		return
	}
	if status == "" {
		status = "unknown"
	}
	s.metrics.VolumeSyncCompactionsTotal.WithLabelValues(status).Inc()
	s.metrics.VolumeSyncCompactedEntries.WithLabelValues(status).Observe(float64(deletedEntries))
}

type ReseedRequiredError struct {
	RetainedAfterSeq int64
	HeadSeq          int64
}

func (e *ReseedRequiredError) Error() string {
	return ErrReseedRequired.Error()
}

func (e *ReseedRequiredError) Unwrap() error {
	return ErrReseedRequired
}

func newReseedRequiredError(retainedAfterSeq, headSeq int64) error {
	return &ReseedRequiredError{
		RetainedAfterSeq: retainedAfterSeq,
		HeadSeq:          headSeq,
	}
}

func (s *Service) getRetentionState(ctx context.Context, volumeID string) (*db.SyncRetentionState, error) {
	state, err := s.repo.GetSyncRetentionState(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return &db.SyncRetentionState{VolumeID: volumeID}, nil
		}
		return nil, err
	}
	return state, nil
}

func (s *Service) getRetentionStateTx(ctx context.Context, tx pgx.Tx, volumeID string) (*db.SyncRetentionState, error) {
	state, err := s.repo.GetSyncRetentionStateForUpdateTx(ctx, tx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return &db.SyncRetentionState{VolumeID: volumeID}, nil
		}
		return nil, err
	}
	return state, nil
}

func (s *Service) getNamespacePolicy(ctx context.Context, volumeID string) (pathnorm.FilesystemCapabilities, error) {
	policy, err := s.repo.GetSyncNamespacePolicy(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return pathnorm.FilesystemCapabilities{}, nil
		}
		return pathnorm.FilesystemCapabilities{}, err
	}
	return policy.Capabilities, nil
}

func (s *Service) getNamespacePolicyTx(ctx context.Context, tx pgx.Tx, volumeID string) (pathnorm.FilesystemCapabilities, error) {
	policy, err := s.repo.GetSyncNamespacePolicyForUpdateTx(ctx, tx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return pathnorm.FilesystemCapabilities{}, nil
		}
		return pathnorm.FilesystemCapabilities{}, err
	}
	return policy.Capabilities, nil
}

func mergeNamespaceCapabilities(policy, requested pathnorm.FilesystemCapabilities) pathnorm.FilesystemCapabilities {
	if !pathnorm.RequiresPortableNameAudit(policy) {
		return requested
	}
	return pathnorm.MergeFilesystemCapabilities(policy, requested)
}

func encodeAppendChangesResponse(resp *AppendChangesResponse) (*json.RawMessage, error) {
	payload, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal sync append response: %w", err)
	}
	raw := json.RawMessage(payload)
	return &raw, nil
}

func decodeAppendChangesResponse(payload *json.RawMessage) (*AppendChangesResponse, error) {
	if payload == nil || len(*payload) == 0 {
		return &AppendChangesResponse{
			Accepted:  []*db.SyncJournalEntry{},
			Conflicts: []*db.SyncConflict{},
		}, nil
	}
	var resp AppendChangesResponse
	if err := json.Unmarshal(*payload, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal sync append response: %w", err)
	}
	if resp.Accepted == nil {
		resp.Accepted = []*db.SyncJournalEntry{}
	}
	if resp.Conflicts == nil {
		resp.Conflicts = []*db.SyncConflict{}
	}
	return &resp, nil
}

func fingerprintAppendChangesRequest(req *AppendChangesRequest) (string, error) {
	payload, err := json.Marshal(struct {
		BaseSeq int64           `json:"base_seq"`
		Changes []ChangeRequest `json:"changes"`
	}{
		BaseSeq: req.BaseSeq,
		Changes: req.Changes,
	})
	if err != nil {
		return "", fmt.Errorf("marshal sync append request fingerprint: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func NormalizePath(p string) string {
	return pathnorm.CanonicalPathKey(p)
}

func NormalizeEventType(eventType string) string {
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case db.SyncEventCreate:
		return db.SyncEventCreate
	case db.SyncEventWrite:
		return db.SyncEventWrite
	case db.SyncEventRemove:
		return db.SyncEventRemove
	case db.SyncEventRename:
		return db.SyncEventRename
	case db.SyncEventChmod:
		return db.SyncEventChmod
	case db.SyncEventInvalidate:
		return db.SyncEventInvalidate
	default:
		return ""
	}
}

func conflictReason(latest *db.SyncJournalEntry, normalizedPath, incomingPath string) string {
	if latest == nil {
		return "concurrent_update"
	}
	if latest.Path != "" && incomingPath != "" && latest.Path != incomingPath && NormalizePath(latest.Path) == normalizedPath {
		return "casefold_collision"
	}
	return "concurrent_update"
}

func isMutableConflictStatus(status string) bool {
	switch status {
	case db.SyncConflictStatusResolved, db.SyncConflictStatusIgnored:
		return true
	default:
		return false
	}
}

func buildConflictArtifactPath(conflictPath string, replicaID *string, existingSeq int64) string {
	cleaned := pathpkg.Clean("/" + strings.TrimSpace(conflictPath))
	if cleaned == "/" {
		cleaned = "/conflict"
	}
	dir, file := pathpkg.Split(cleaned)
	if file == "" {
		file = "conflict"
	}
	ext := pathpkg.Ext(file)
	base := strings.TrimSuffix(file, ext)
	actor := "sandbox"
	if replicaID != nil && *replicaID != "" {
		actor = sanitizeArtifactPart(*replicaID)
	}
	artifactName := base + ".sandbox0-conflict-" + actor + "-seq-" + strconv.FormatInt(existingSeq, 10) + ext
	if dir == "" {
		return "/" + artifactName
	}
	return pathpkg.Join(dir, artifactName)
}

func sanitizeArtifactPart(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "replica"
	}
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	result := strings.Trim(b.String(), "-")
	if result == "" {
		return "replica"
	}
	return result
}

func mustMarshalMetadata(v map[string]any) *json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	raw := json.RawMessage(payload)
	return &raw
}

func nonEmptyStringPtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func stringPtr(v string) *string {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (s *Service) materializeConflict(ctx context.Context, volume *db.SandboxVolume, conflict *db.SyncConflict, now time.Time) (*db.SyncJournalEntry, error) {
	if s.artifactWriter == nil || volume == nil || conflict == nil || strings.TrimSpace(conflict.ArtifactPath) == "" {
		return nil, nil
	}
	materialized, err := s.artifactWriter.MaterializeConflict(ctx, volume, conflict)
	if err != nil {
		return nil, err
	}
	var sizeBytes *int64
	if materialized != nil {
		sizeBytes = int64Ptr(materialized.SizeBytes)
		conflict.Metadata = mergeMetadata(conflict.Metadata, map[string]any{
			"artifact_materialized":    true,
			"artifact_size_bytes":      materialized.SizeBytes,
			"artifact_materialized_at": now.Format(time.RFC3339),
		})
	}
	return &db.SyncJournalEntry{
		VolumeID:       conflict.VolumeID,
		TeamID:         conflict.TeamID,
		Source:         db.SyncSourceSandbox,
		EventType:      db.SyncEventWrite,
		Path:           conflict.ArtifactPath,
		NormalizedPath: NormalizePath(conflict.ArtifactPath),
		Tombstone:      false,
		SizeBytes:      sizeBytes,
		Metadata: mustMarshalMetadata(map[string]any{
			"conflict_artifact": true,
			"conflict_id":       conflict.ID,
			"reason":            conflict.Reason,
		}),
		CreatedAt: now,
	}, nil
}

func mergeMetadata(existing *json.RawMessage, additional map[string]any) *json.RawMessage {
	if len(additional) == 0 {
		return existing
	}
	merged := make(map[string]any)
	if existing != nil && len(*existing) > 0 {
		_ = json.Unmarshal(*existing, &merged)
	}
	for k, v := range additional {
		merged[k] = v
	}
	return mustMarshalMetadata(merged)
}

func (s *Service) String() string {
	return "volsync.Service"
}
