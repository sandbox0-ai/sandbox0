package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type updateSandboxRequest struct {
	Config *service.SandboxUpdateConfig `json:"config"`
}

// claimSandbox claims a sandbox
func (s *Server) claimSandbox(c *gin.Context) {
	var req service.ClaimRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	req.TeamID = claims.TeamID
	req.UserID = claims.UserID

	if req.Template == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template is required")
		return
	}

	resp, err := s.sandboxService.ClaimSandbox(c.Request.Context(), &req)
	if err != nil {
		if errors.Is(err, service.ErrInvalidClaimRequest) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		if apierrors.IsNotFound(err) || errors.Is(err, service.ErrRootFSSnapshotNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, err.Error())
			return
		}
		if errors.Is(err, service.ErrClaimConflict) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
			return
		}
		if errors.Is(err, service.ErrClaimStartThrottled) {
			retryAfter := int(service.ClaimStartRetryAfter(err).Seconds())
			if retryAfter < 1 {
				retryAfter = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfter))
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
			return
		}
		if errors.Is(err, service.ErrDataPlaneNotReady) {
			c.Header("Retry-After", "1")
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
			return
		}
		if errors.Is(err, service.ErrQuotaExceeded) {
			spec.JSONError(c, http.StatusTooManyRequests, "quota_exceeded", err.Error())
			return
		}
		s.logger.Error("Failed to claim sandbox",
			zap.String("template", req.Template),
			zap.String("teamID", req.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to claim sandbox: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusCreated, resp)
}

// listSandboxes lists all sandboxes for the authenticated team
func (s *Server) listSandboxes(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	// Parse query parameters
	req := &service.ListSandboxesRequest{
		TeamID:     claims.TeamID,
		Status:     c.Query("status"),
		TemplateID: c.Query("template_id"),
	}
	if req.Status != "" && !isValidSandboxListStatus(req.Status) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid status parameter")
		return
	}

	// Parse paused filter
	if pausedStr := c.Query("paused"); pausedStr != "" {
		paused, err := strconv.ParseBool(pausedStr)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid paused parameter")
			return
		}
		req.Paused = &paused
	}

	// Parse limit
	if limitStr := c.Query("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid limit parameter")
			return
		}
		if limit < 1 || limit > 200 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "limit must be between 1 and 200")
			return
		}
		req.Limit = limit
	}

	// Parse offset
	if offsetStr := c.Query("offset"); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid offset parameter")
			return
		}
		if offset < 0 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "offset must be >= 0")
			return
		}
		req.Offset = offset
	}

	resp, err := s.sandboxService.ListSandboxes(c.Request.Context(), req)
	if err != nil {
		s.logger.Error("Failed to list sandboxes",
			zap.String("teamID", claims.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to list sandboxes: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}

func isValidSandboxListStatus(status string) bool {
	switch status {
	case service.SandboxStatusStarting,
		service.SandboxStatusRunning,
		service.SandboxStatusPaused,
		service.SandboxStatusTerminating,
		service.SandboxStatusFailed:
		return true
	default:
		return false
	}
}

// getSandbox gets a sandbox
func (s *Server) getSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	// Verify team ownership
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, sandbox)
}

// getSandboxInternal gets sandbox for internal trusted callers without team ownership enforcement.
func (s *Server) getSandboxInternal(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox (internal)",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}
	spec.JSONSuccess(c, http.StatusOK, sandbox)
}

// updateSandbox updates sandbox configuration
func (s *Server) updateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var req updateSandboxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	if req.Config == nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "config is required")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	if req.Config.AutoResume != nil && !*req.Config.AutoResume {
		if service.SandboxAppServicesHaveResumeRoute(sandbox.Services) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
				"cannot disable auto_resume while service routes have resume=true; remove or update those routes first")
			return
		}
	}
	resultAutoResume := sandbox.AutoResume
	if req.Config.AutoResume != nil {
		resultAutoResume = *req.Config.AutoResume
	}
	if !resultAutoResume && service.SandboxAppServicesHaveResumeRoute(req.Config.Services) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
			"cannot set resume=true on public routes when sandbox auto_resume is disabled")
		return
	}

	updated, err := s.sandboxService.UpdateSandbox(c.Request.Context(), sandboxID, req.Config)
	if err != nil {
		if errors.Is(err, service.ErrInvalidClaimRequest) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		if errors.Is(err, service.ErrQuotaExceeded) {
			spec.JSONError(c, http.StatusTooManyRequests, "quota_exceeded", err.Error())
			return
		}
		s.logger.Error("Failed to update sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to update sandbox: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, updated)
}

// getSandboxStatus gets a sandbox status
func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	// Verify team ownership
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	status, err := s.sandboxService.GetSandboxStatus(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox status",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, status)
}

// terminateSandbox terminates a sandbox
func (s *Server) terminateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	err = s.sandboxService.TerminateSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to terminate sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to terminate sandbox: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"message": "sandbox terminated successfully",
	})
}

// pauseSandbox pauses a sandbox
func (s *Server) pauseSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	resp, err := s.sandboxService.PauseSandboxAndWait(c.Request.Context(), sandboxID)
	if err != nil {
		s.writeSandboxLifecycleTransitionError(c, "pause", sandboxID, err)
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}

// resumeSandbox resumes a sandbox
func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	resp, err := s.sandboxService.ResumeSandboxAndWait(c.Request.Context(), sandboxID)
	if err != nil {
		s.writeSandboxLifecycleTransitionError(c, "resume", sandboxID, err)
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}

func (s *Server) writeSandboxLifecycleTransitionError(c *gin.Context, action, sandboxID string, err error) {
	s.logger.Error("Failed to change sandbox lifecycle state",
		zap.String("action", action),
		zap.String("sandboxID", sandboxID),
		zap.Error(err),
	)
	switch {
	case apierrors.IsConflict(err):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, fmt.Sprintf("sandbox %s conflicts with another lifecycle operation", action))
	case apierrors.IsNotFound(err):
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
	case errors.Is(err, service.ErrQuotaExceeded):
		spec.JSONError(c, http.StatusTooManyRequests, "quota_exceeded", err.Error())
	case errors.Is(err, service.ErrSandboxCheckpointRequiresCtld):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox checkpoint pause requires ctld")
	case errors.Is(err, context.DeadlineExceeded):
		spec.JSONError(c, http.StatusGatewayTimeout, spec.CodeUnavailable, fmt.Sprintf("timed out waiting for sandbox to %s", action))
	case errors.Is(err, context.Canceled):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, fmt.Sprintf("canceled while waiting for sandbox to %s", action))
	default:
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to %s sandbox: %v", action, err))
	}
}

// refreshSandbox refreshes sandbox TTL
func (s *Server) refreshSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	// Parse optional request body
	var req service.RefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	resp, err := s.sandboxService.RefreshSandbox(c.Request.Context(), sandboxID, &req)
	if err != nil {
		if errors.Is(err, service.ErrInvalidClaimRequest) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		s.logger.Error("Failed to refresh sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to refresh sandbox: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}
