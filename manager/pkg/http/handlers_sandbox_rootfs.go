package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func (s *Server) createSandboxRootFSSnapshot(c *gin.Context) {
	sandboxID := c.Param("id")
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	var req service.CreateSandboxRootFSSnapshotRequest
	if err := bindOptionalJSON(c, &req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	rootFS, ok := s.requireSandboxRootFS(c)
	if !ok {
		return
	}
	snapshot, err := rootFS.CreateSandboxRootFSSnapshot(c.Request.Context(), sandboxID, claims.TeamID, &req)
	if err != nil {
		s.writeSandboxRootFSError(c, "create rootfs snapshot", sandboxID, err)
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, snapshot)
}

func (s *Server) listSandboxRootFSSnapshots(c *gin.Context) {
	sandboxID := c.Param("id")
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	rootFS, ok := s.requireSandboxRootFS(c)
	if !ok {
		return
	}
	resp, err := rootFS.ListSandboxRootFSSnapshots(c.Request.Context(), sandboxID, claims.TeamID)
	if err != nil {
		s.writeSandboxRootFSError(c, "list rootfs snapshots", sandboxID, err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, resp)
}

func (s *Server) getSandboxRootFSSnapshot(c *gin.Context) {
	snapshotID := c.Param("snapshot_id")
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	rootFS, ok := s.requireSandboxRootFS(c)
	if !ok {
		return
	}
	snapshot, err := rootFS.GetSandboxRootFSSnapshot(c.Request.Context(), snapshotID, claims.TeamID)
	if err != nil {
		s.writeSandboxRootFSError(c, "get rootfs snapshot", "", err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, snapshot)
}

func (s *Server) deleteSandboxRootFSSnapshot(c *gin.Context) {
	snapshotID := c.Param("snapshot_id")
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	rootFS, ok := s.requireSandboxRootFS(c)
	if !ok {
		return
	}
	if err := rootFS.DeleteSandboxRootFSSnapshot(c.Request.Context(), snapshotID, claims.TeamID); err != nil {
		s.writeSandboxRootFSError(c, "delete rootfs snapshot", "", err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"deleted": true})
}

func (s *Server) restoreSandboxRootFS(c *gin.Context) {
	sandboxID := c.Param("id")
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	var req service.RestoreSandboxRootFSRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	req.SnapshotID = strings.TrimSpace(req.SnapshotID)
	if req.SnapshotID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "snapshot_id is required")
		return
	}
	rootFS, ok := s.requireSandboxRootFS(c)
	if !ok {
		return
	}
	resp, err := rootFS.RestoreSandboxRootFS(c.Request.Context(), sandboxID, claims.TeamID, &req)
	if err != nil {
		s.writeSandboxRootFSError(c, "restore rootfs", sandboxID, err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, resp)
}

func (s *Server) forkSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	var req service.ForkSandboxRequest
	if err := bindOptionalJSON(c, &req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	req.OperationID = sandboxClaimOperationID(claims)
	req.StartedAt = sandboxClaimIngressStartedAt(claims)
	forker := s.sandboxForker
	if forker == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox fork backend is not configured")
		return
	}
	resp, err := forker.ForkSandbox(c.Request.Context(), sandboxID, claims.TeamID, claims.UserID, &req)
	if err != nil {
		s.writeSandboxRootFSError(c, "fork sandbox", sandboxID, err)
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, resp)
}

func (s *Server) requireSandboxRootFS(c *gin.Context) (SandboxRootFSService, bool) {
	if s.sandboxRootFS == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox RootFS service is not configured")
		return nil, false
	}
	return s.sandboxRootFS, true
}

func (s *Server) rebaseSandboxRootFS(c *gin.Context) {
	sandboxID := c.Param("id")
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	var req service.RebaseSandboxRootFSRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	req.OperationID = sandboxClaimOperationID(claims)
	req.StartedAt = sandboxClaimIngressStartedAt(claims)
	rebaser := s.sandboxRootFSRebaser
	if rebaser == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable,
			"sandbox RootFS rebase backend is not configured")
		return
	}
	response, err := rebaser.RebaseSandboxRootFS(
		c.Request.Context(), sandboxID, claims.TeamID, &req,
	)
	if err != nil {
		s.writeSandboxRootFSError(c, "rebase sandbox rootfs", sandboxID, err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}

func bindOptionalJSON(c *gin.Context, target any) error {
	if c.Request.Body == nil || c.Request.Body == http.NoBody {
		return nil
	}
	if c.Request.ContentLength == 0 {
		return nil
	}
	err := c.ShouldBindJSON(target)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (s *Server) writeSandboxRootFSError(c *gin.Context, action, sandboxID string, err error) {
	s.logger.Error("Failed sandbox rootfs operation",
		zap.String("action", action),
		zap.String("sandboxID", sandboxID),
		zap.Error(err),
	)
	switch {
	case apierror.IsNotFound(err), errors.Is(err, sandboxstore.ErrSandboxRecordNotFound), errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound):
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
	case apierror.IsForbidden(err):
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "forbidden")
	case apierror.IsConflict(err):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
	case errors.Is(err, service.ErrSandboxRootFSRequiresPausedSandbox):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "sandbox rootfs operation requires a paused sandbox")
	case errors.Is(err, service.ErrSandboxRootFSSourceRequiresRunningOrPaused):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "sandbox rootfs source operation requires a running or paused sandbox")
	case errors.Is(err, service.ErrRootFSSnapshotExpired):
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
	case errors.Is(err, service.ErrInvalidRootFSRebaseRequest):
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
	case errors.Is(err, sandboxstore.ErrRootFSFilesystemConflict), errors.Is(err, sandboxstore.ErrRootFSHeadConflict), errors.Is(err, sandboxstore.ErrRootFSFilesystemNotFound):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
	case errors.Is(err, service.ErrSandboxCheckpointRequiresCtld):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox checkpoint requires ctld")
	case errors.Is(err, service.ErrSandboxLifecycleUnavailable):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
	case errors.Is(err, service.ErrSandboxRootFSStoreUnavailable):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox rootfs store is unavailable")
	default:
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to %s: %v", action, err))
	}
}
