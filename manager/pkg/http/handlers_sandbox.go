package http

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

type updateSandboxRequest struct {
	Config *service.SandboxConfig `json:"config"`
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

	// Validate: if disabling auto_resume, check no exposed ports have resume=true
	if req.Config.AutoResume != nil && !*req.Config.AutoResume {
		for _, p := range sandbox.ExposedPorts {
			if p.Resume {
				spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
					"cannot disable auto_resume while exposed ports have resume=true; remove or update those ports first")
				return
			}
		}
	}

	updated, err := s.sandboxService.UpdateSandbox(c.Request.Context(), sandboxID, req.Config)
	if err != nil {
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

	resp, err := s.sandboxService.PauseSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to pause sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to pause sandbox: %v", err))
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

	resp, err := s.sandboxService.ResumeSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to resume sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to resume sandbox: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
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
	// Ignore error - body is optional
	_ = c.ShouldBindJSON(&req)

	resp, err := s.sandboxService.RefreshSandbox(c.Request.Context(), sandboxID, &req)
	if err != nil {
		s.logger.Error("Failed to refresh sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to refresh sandbox: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}

// getSandboxStats gets a sandbox stats
func (s *Server) getSandboxStats(c *gin.Context) {
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

	stats, err := s.sandboxService.GetSandboxResourceUsage(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox stats",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to get sandbox stats: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, stats)
}
