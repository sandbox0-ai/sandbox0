package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
)

type upsertSyncReplicaRequest struct {
	DisplayName   string                           `json:"display_name"`
	Platform      string                           `json:"platform"`
	RootPath      string                           `json:"root_path"`
	CaseSensitive bool                             `json:"case_sensitive"`
	Capabilities  *pathnorm.FilesystemCapabilities `json:"capabilities,omitempty"`
}

type appendSyncChangesRequest struct {
	RequestID string                  `json:"request_id"`
	BaseSeq   int64                   `json:"base_seq"`
	Changes   []volsync.ChangeRequest `json:"changes"`
}

type updateSyncCursorRequest struct {
	LastAppliedSeq int64 `json:"last_applied_seq"`
}

type resolveSyncConflictRequest struct {
	Status     string `json:"status"`
	Resolution string `json:"resolution"`
	Note       string `json:"note"`
}

type createSyncBootstrapRequest struct {
	SnapshotName        string                           `json:"snapshot_name"`
	SnapshotDescription string                           `json:"snapshot_description"`
	CaseSensitive       *bool                            `json:"case_sensitive,omitempty"`
	Capabilities        *pathnorm.FilesystemCapabilities `json:"capabilities,omitempty"`
}

type syncBootstrapResponse struct {
	Snapshot            snapshotResponse `json:"snapshot"`
	ReplayAfterSeq      int64            `json:"replay_after_seq"`
	ArchiveDownloadPath string           `json:"archive_download_path"`
}

type syncBootstrapCompatibilityConflictDetails struct {
	Reason       string                          `json:"reason"`
	SnapshotID   string                          `json:"snapshot_id"`
	Capabilities pathnorm.FilesystemCapabilities `json:"capabilities"`
	Issues       []pathnorm.CompatibilityIssue   `json:"issues"`
}

type syncReseedRequiredDetails struct {
	Reason           string `json:"reason"`
	RetainedAfterSeq int64  `json:"retained_after_seq"`
	HeadSeq          int64  `json:"head_seq"`
}

func (s *Server) upsertSyncReplica(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync unavailable")
		return
	}

	var req upsertSyncReplicaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	resp, err := s.syncMgr.UpsertReplica(r.Context(), &volsync.UpsertReplicaRequest{
		VolumeID:      r.PathValue("id"),
		TeamID:        claims.TeamID,
		ReplicaID:     r.PathValue("replica_id"),
		DisplayName:   req.DisplayName,
		Platform:      req.Platform,
		RootPath:      req.RootPath,
		CaseSensitive: req.CaseSensitive,
		Capabilities:  req.Capabilities,
	})
	if err != nil {
		s.writeSyncError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func (s *Server) getSyncReplica(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync unavailable")
		return
	}

	resp, err := s.syncMgr.GetReplica(r.Context(), r.PathValue("id"), claims.TeamID, r.PathValue("replica_id"))
	if err != nil {
		s.writeSyncError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func (s *Server) createSyncBootstrap(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil || s.snapshotMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync bootstrap unavailable")
		return
	}

	var req createSyncBootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	volumeID := r.PathValue("id")
	legacyCaseSensitive := true
	if req.CaseSensitive != nil {
		legacyCaseSensitive = *req.CaseSensitive
	}
	capabilities := pathnorm.NormalizeFilesystemCapabilities("", legacyCaseSensitive, req.Capabilities)
	snapshotName := req.SnapshotName
	if snapshotName == "" {
		snapshotName = "sync-bootstrap-" + time.Now().UTC().Format("20060102-150405")
	}

	var (
		replayAfterSeq int64
		snap           *db.Snapshot
		handled        bool
		loadedHead     bool
		err            error
	)
	run := func(runCtx context.Context) error {
		replayAfterSeq, err = s.syncMgr.GetHead(runCtx, volumeID, claims.TeamID)
		if err != nil {
			return err
		}
		loadedHead = true

		snap, err = s.snapshotMgr.CreateSnapshotSimple(runCtx, &snapshot.CreateSnapshotRequest{
			VolumeID:    volumeID,
			Name:        snapshotName,
			Description: req.SnapshotDescription,
			TeamID:      claims.TeamID,
			UserID:      claims.UserID,
		})
		if err != nil {
			return err
		}
		if !pathnorm.RequiresPortableNameAudit(capabilities) {
			return nil
		}

		issues, err := s.snapshotMgr.ListSnapshotCompatibilityIssues(runCtx, &snapshot.ListSnapshotCompatibilityIssuesRequest{
			VolumeID:     volumeID,
			SnapshotID:   snap.ID,
			TeamID:       claims.TeamID,
			Capabilities: capabilities,
		})
		if err != nil {
			s.cleanupSyncBootstrapSnapshot(runCtx, volumeID, snap.ID, claims.TeamID)
			return err
		}
		if len(issues) == 0 {
			return nil
		}

		s.cleanupSyncBootstrapSnapshot(runCtx, volumeID, snap.ID, claims.TeamID)
		_ = spec.WriteError(
			w,
			http.StatusConflict,
			spec.CodeConflict,
			"bootstrap snapshot contains namespace entries incompatible with the requested replica capabilities",
			syncBootstrapCompatibilityConflictDetails{
				Reason:       "namespace_incompatible",
				SnapshotID:   snap.ID,
				Capabilities: capabilities,
				Issues:       issues,
			},
		)
		handled = true
		snap = nil
		return nil
	}
	if s.barrier != nil {
		err = s.barrier.WithExclusive(r.Context(), volumeID, run)
	} else {
		err = run(r.Context())
	}
	if err != nil {
		if !loadedHead {
			s.writeSyncError(w, err)
			return
		}
		s.handleSnapshotError(w, err)
		return
	}
	if handled || snap == nil {
		return
	}

	resp := syncBootstrapResponse{
		Snapshot: snapshotResponse{
			ID:          snap.ID,
			VolumeID:    snap.VolumeID,
			Name:        snap.Name,
			Description: snap.Description,
			SizeBytes:   snap.SizeBytes,
			CreatedAt:   snap.CreatedAt.Format("2006-01-02T15:04:05Z"),
		},
		ReplayAfterSeq: replayAfterSeq,
		ArchiveDownloadPath: fmt.Sprintf(
			"/api/v1/sandboxvolumes/%s/sync/bootstrap/archive?snapshot_id=%s",
			volumeID,
			snap.ID,
		),
	}
	if snap.ExpiresAt != nil {
		expires := snap.ExpiresAt.Format("2006-01-02T15:04:05Z")
		resp.Snapshot.ExpiresAt = &expires
	}

	_ = spec.WriteSuccess(w, http.StatusCreated, resp)
}

func (s *Server) cleanupSyncBootstrapSnapshot(ctx context.Context, volumeID, snapshotID, teamID string) {
	if s.snapshotMgr == nil || snapshotID == "" {
		return
	}
	if err := s.snapshotMgr.DeleteSnapshot(ctx, volumeID, snapshotID, teamID); err != nil {
		s.logger.WithError(err).WithField("snapshot_id", snapshotID).WithField("volume_id", volumeID).Warn("Failed to cleanup rejected sync bootstrap snapshot")
	}
}

func (s *Server) downloadSyncBootstrapArchive(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.snapshotMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync bootstrap archive unavailable")
		return
	}

	volumeID := r.PathValue("id")
	snapshotID := r.URL.Query().Get("snapshot_id")
	if snapshotID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "missing snapshot_id")
		return
	}

	if _, err := s.snapshotMgr.GetSnapshot(r.Context(), volumeID, snapshotID, claims.TeamID); err != nil {
		s.handleSnapshotError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", "sync-bootstrap-"+snapshotID+".tar.gz"))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	if err := s.snapshotMgr.ExportSnapshotArchive(r.Context(), &snapshot.ExportSnapshotRequest{
		VolumeID:   volumeID,
		SnapshotID: snapshotID,
		TeamID:     claims.TeamID,
	}, w); err != nil {
		s.logger.WithError(err).WithField("snapshot_id", snapshotID).WithField("volume_id", volumeID).Error("Export sync bootstrap archive failed")
	}
}

func (s *Server) listSyncChanges(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync unavailable")
		return
	}

	afterSeq, err := parseInt64Query(r, "after")
	if err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	limit, err := parseIntQuery(r, "limit")
	if err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	resp, err := s.syncMgr.ListChanges(r.Context(), &volsync.ListChangesRequest{
		VolumeID: r.PathValue("id"),
		TeamID:   claims.TeamID,
		AfterSeq: afterSeq,
		Limit:    limit,
	})
	if err != nil {
		s.writeSyncError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func (s *Server) listSyncConflicts(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync unavailable")
		return
	}

	limit, err := parseIntQuery(r, "limit")
	if err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	resp, err := s.syncMgr.ListConflicts(r.Context(), &volsync.ListConflictsRequest{
		VolumeID: r.PathValue("id"),
		TeamID:   claims.TeamID,
		Status:   r.URL.Query().Get("status"),
		Limit:    limit,
	})
	if err != nil {
		s.writeSyncError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func (s *Server) resolveSyncConflict(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync unavailable")
		return
	}

	var req resolveSyncConflictRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	conflict, err := s.syncMgr.ResolveConflict(r.Context(), &volsync.ResolveConflictRequest{
		VolumeID:   r.PathValue("id"),
		TeamID:     claims.TeamID,
		ConflictID: r.PathValue("conflict_id"),
		Status:     req.Status,
		Resolution: req.Resolution,
		Note:       req.Note,
	})
	if err != nil {
		s.writeSyncError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, conflict)
}

func (s *Server) appendSyncChanges(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync unavailable")
		return
	}

	var req appendSyncChangesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if req.RequestID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, volsync.ErrInvalidRequestID.Error())
		return
	}

	resp, err := s.syncMgr.AppendReplicaChanges(r.Context(), &volsync.AppendChangesRequest{
		VolumeID:  r.PathValue("id"),
		TeamID:    claims.TeamID,
		ReplicaID: r.PathValue("replica_id"),
		RequestID: req.RequestID,
		BaseSeq:   req.BaseSeq,
		Changes:   req.Changes,
	})
	if err != nil {
		s.writeSyncError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func (s *Server) updateSyncReplicaCursor(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.syncMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeInternal, "volume sync unavailable")
		return
	}

	var req updateSyncCursorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	resp, err := s.syncMgr.UpdateReplicaCursor(r.Context(), &volsync.UpdateCursorRequest{
		VolumeID:       r.PathValue("id"),
		TeamID:         claims.TeamID,
		ReplicaID:      r.PathValue("replica_id"),
		LastAppliedSeq: req.LastAppliedSeq,
	})
	if err != nil {
		s.writeSyncError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func (s *Server) writeSyncError(w http.ResponseWriter, err error) {
	var reseedErr *volsync.ReseedRequiredError
	switch {
	case errors.Is(err, volsync.ErrReplicaNotFound), errors.Is(err, volsync.ErrConflictNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
	case errors.As(err, &reseedErr):
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, volsync.ErrReseedRequired.Error(), syncReseedRequiredDetails{
			Reason:           "reseed_required",
			RetainedAfterSeq: reseedErr.RetainedAfterSeq,
			HeadSeq:          reseedErr.HeadSeq,
		})
	case errors.Is(err, volsync.ErrCursorAhead), errors.Is(err, volsync.ErrCursorRegression), errors.Is(err, volsync.ErrReplicaLeaseExpired), errors.Is(err, volsync.ErrRequestIDConflict):
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, err.Error())
	case errors.Is(err, volsync.ErrInvalidChange), errors.Is(err, volsync.ErrInvalidConflictStatus), errors.Is(err, volsync.ErrInvalidRequestID), errors.Is(err, volsync.ErrInvalidRetentionTarget):
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
	default:
		s.logger.WithError(err).Error("Volume sync request failed")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
	}
}

func parseInt64Query(r *http.Request, key string) (int64, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func parseIntQuery(r *http.Request, key string) (int, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}
