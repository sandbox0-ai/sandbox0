package http

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// claimSandbox claims a sandbox
func (s *Server) claimSandbox(c *gin.Context) {
	var req service.ClaimRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("invalid request: %v", err),
		})
		return
	}

	// Validate required fields
	if req.TeamID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "team_id is required",
		})
		return
	}
	if req.Template == "" || req.Namespace == "" {
		cfg := config.LoadManagerConfig()
		if req.Template == "" {
			req.Template = cfg.DefaultTemplateName
		}
		if req.Namespace == "" {
			req.Namespace = cfg.DefaultTemplateNamespace
		}
	}

	resp, err := s.sandboxService.ClaimSandbox(c.Request.Context(), &req)
	if err != nil {
		s.logger.Error("Failed to claim sandbox",
			zap.String("template", req.Template),
			zap.String("namespace", req.Namespace),
			zap.String("teamID", req.TeamID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to claim sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

// getSandbox gets a sandbox
func (s *Server) getSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	// Verify team ownership
	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	c.JSON(http.StatusOK, sandbox)
}

// getSandboxStatus gets a sandbox status
func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	// Verify team ownership
	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	status, err := s.sandboxService.GetSandboxStatus(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox status",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, status)
}

// terminateSandbox terminates a sandbox
func (s *Server) terminateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	err := s.sandboxService.TerminateSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to terminate sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to terminate sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "sandbox terminated successfully",
	})
}

// pauseSandbox pauses a sandbox
func (s *Server) pauseSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	resp, err := s.sandboxService.PauseSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to pause sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to pause sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// resumeSandbox resumes a sandbox
func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	resp, err := s.sandboxService.ResumeSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to resume sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to resume sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// refreshSandbox refreshes sandbox TTL
func (s *Server) refreshSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to refresh sandbox: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// getSandboxStats gets a sandbox stats
func (s *Server) getSandboxStats(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "sandbox_id is required",
		})
		return
	}

	// Get team ID from claims for ownership verification
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "missing authentication",
		})
		return
	}

	// Verify ownership
	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("sandbox not found: %v", err),
		})
		return
	}

	if sandbox.TeamID != claims.TeamID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "sandbox belongs to a different team",
		})
		return
	}

	stats, err := s.sandboxService.GetSandboxResourceUsage(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox stats",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to get sandbox stats: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}
