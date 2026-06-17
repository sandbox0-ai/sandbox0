package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	snapshot, err := s.sandboxService.CreateSandboxRootFSSnapshot(c.Request.Context(), sandboxID, claims.TeamID, &req)
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
	resp, err := s.sandboxService.ListSandboxRootFSSnapshots(c.Request.Context(), sandboxID, claims.TeamID)
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
	snapshot, err := s.sandboxService.GetSandboxRootFSSnapshot(c.Request.Context(), snapshotID, claims.TeamID)
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
	if err := s.sandboxService.DeleteSandboxRootFSSnapshot(c.Request.Context(), snapshotID, claims.TeamID); err != nil {
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
	resp, err := s.sandboxService.RestoreSandboxRootFS(c.Request.Context(), sandboxID, claims.TeamID, &req)
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
	resp, err := s.sandboxService.ForkSandbox(c.Request.Context(), sandboxID, claims.TeamID, claims.UserID, &req)
	if err != nil {
		s.writeSandboxRootFSError(c, "fork sandbox", sandboxID, err)
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, resp)
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
	case apierrors.IsNotFound(err), errors.Is(err, service.ErrSandboxRecordNotFound), errors.Is(err, service.ErrRootFSSnapshotNotFound):
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
	case apierrors.IsForbidden(err):
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "forbidden")
	case errors.Is(err, service.ErrSandboxRootFSRequiresPausedSandbox):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "sandbox rootfs operation requires a paused sandbox")
	case errors.Is(err, service.ErrRootFSFilesystemConflict), errors.Is(err, service.ErrRootFSHeadConflict), errors.Is(err, service.ErrRootFSFilesystemNotFound):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
	case errors.Is(err, service.ErrSandboxRootFSStoreUnavailable):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox rootfs store is unavailable")
	default:
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to %s: %v", action, err))
	}
}
