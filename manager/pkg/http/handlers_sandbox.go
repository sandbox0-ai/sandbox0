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
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
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
		if errors.Is(err, service.ErrClaimConflict) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
			return
		}
		if errors.Is(err, service.ErrDataPlaneNotReady) {
			c.Header("Retry-After", "1")
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
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
		req.Limit = limit
	}

	// Parse offset
	if offsetStr := c.Query("offset"); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid offset parameter")
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
		if service.PublicGatewayHasResumeRoute(sandbox.PublicGateway) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
				"cannot disable auto_resume while public gateway routes have resume=true; remove or update those routes first")
			return
		}
	}
	resultAutoResume := sandbox.AutoResume
	if req.Config.AutoResume != nil {
		resultAutoResume = *req.Config.AutoResume
	}
	if !resultAutoResume && req.Config.PublicGateway != nil && service.PublicGatewayHasResumeRoute(req.Config.PublicGateway) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
			"cannot set resume=true on public gateway routes when sandbox auto_resume is disabled")
		return
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

// getSandboxLogs gets sandbox process logs.
func (s *Server) getSandboxLogs(c *gin.Context) {
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

	tailLines, err := parseBoundedInt64Query(c, "tail_lines", service.DefaultSandboxLogTailLines, service.MaxSandboxLogTailLines)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	limitBytes, err := parseOptionalBoundedInt64Query(c, "limit_bytes", service.MaxSandboxLogLimitBytes)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	follow, err := parseBoolQuery(c, "follow")
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	previous, err := parseBoolQuery(c, "previous")
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	timestamps, err := parseBoolQuery(c, "timestamps")
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	sinceSeconds, err := parseOptionalPositiveInt64Query(c, "since_seconds")
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	options := &service.SandboxLogsOptions{
		Container:    c.Query("container"),
		TailLines:    tailLines,
		LimitBytes:   limitBytes,
		Previous:     previous,
		Timestamps:   timestamps,
		SinceSeconds: sinceSeconds,
	}
	if follow {
		s.streamSandboxLogs(c, sandboxID, claims.TeamID, options)
		return
	}

	logs, err := s.sandboxService.GetSandboxLogs(c.Request.Context(), sandboxID, claims.TeamID, options)
	if err != nil {
		s.writeSandboxLogsError(c, sandboxID, claims.TeamID, err)
		return
	}

	writeSandboxLogHeaders(c, logs.SandboxID, logs.PodName, logs.Container, logs.Previous)
	c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(logs.Logs))
}

func (s *Server) streamSandboxLogs(c *gin.Context, sandboxID, teamID string, options *service.SandboxLogsOptions) {
	if err := proxy.DisableResponseDeadlines(c.Writer); err != nil {
		s.logger.Debug("Failed to disable sandbox log stream response deadlines",
			zap.String("sandboxID", sandboxID),
			zap.String("teamID", teamID),
			zap.Error(err),
		)
	}
	stream, err := s.sandboxService.StreamSandboxLogs(c.Request.Context(), sandboxID, teamID, options)
	if err != nil {
		s.writeSandboxLogsError(c, sandboxID, teamID, err)
		return
	}
	defer stream.Body.Close()

	c.Header("Content-Type", "text/plain; charset=utf-8")
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")
	writeSandboxLogHeaders(c, stream.SandboxID, stream.PodName, stream.Container, stream.Previous)
	c.Status(http.StatusOK)
	c.Writer.Flush()

	writer := flushingResponseWriter{ResponseWriter: c.Writer}
	if _, err := io.Copy(writer, stream.Body); err != nil {
		if !errors.Is(err, context.Canceled) {
			s.logger.Debug("Sandbox log stream ended with error",
				zap.String("sandboxID", sandboxID),
				zap.String("teamID", teamID),
				zap.Error(err),
			)
		}
	}
}

func writeSandboxLogHeaders(c *gin.Context, sandboxID, podName, container string, previous bool) {
	c.Header("X-Sandbox-ID", sandboxID)
	c.Header("X-Sandbox-Pod-Name", podName)
	c.Header("X-Sandbox-Log-Container", container)
	c.Header("X-Sandbox-Log-Previous", strconv.FormatBool(previous))
}

func (s *Server) writeSandboxLogsError(c *gin.Context, sandboxID, teamID string, err error) {
	s.logger.Error("Failed to get sandbox logs",
		zap.String("sandboxID", sandboxID),
		zap.String("teamID", teamID),
		zap.Error(err),
	)
	switch {
	case errors.Is(err, service.ErrSandboxTeamMismatch):
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
	case errors.Is(err, service.ErrSandboxLogContainerNotFound):
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
	case apierrors.IsNotFound(err):
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
	default:
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to get sandbox logs: %v", err))
	}
}

type flushingResponseWriter struct {
	gin.ResponseWriter
}

func (w flushingResponseWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.Flush()
	return n, err
}

func parseBoundedInt64Query(c *gin.Context, name string, defaultValue, maxValue int64) (int64, error) {
	raw := c.Query(name)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 1 || value > maxValue {
		return 0, fmt.Errorf("%s must be between 1 and %d", name, maxValue)
	}
	return value, nil
}

func parseOptionalBoundedInt64Query(c *gin.Context, name string, maxValue int64) (int64, error) {
	raw := c.Query(name)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 1 || value > maxValue {
		return 0, fmt.Errorf("%s must be between 1 and %d", name, maxValue)
	}
	return value, nil
}

func parseOptionalPositiveInt64Query(c *gin.Context, name string) (*int64, error) {
	raw := c.Query(name)
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 1 {
		return nil, fmt.Errorf("%s must be greater than 0", name)
	}
	return &value, nil
}

func parseBoolQuery(c *gin.Context, name string) (bool, error) {
	raw := c.Query(name)
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return value, nil
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
		s.writeSandboxPowerTransitionError(c, "pause", sandboxID, err)
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
		s.writeSandboxPowerTransitionError(c, "resume", sandboxID, err)
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}

func (s *Server) writeSandboxPowerTransitionError(c *gin.Context, action, sandboxID string, err error) {
	s.logger.Error("Failed to change sandbox power state",
		zap.String("action", action),
		zap.String("sandboxID", sandboxID),
		zap.Error(err),
	)
	switch {
	case errors.Is(err, service.ErrSandboxPowerTransitionSuperseded):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, fmt.Sprintf("sandbox %s was superseded by a newer power transition", action))
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
