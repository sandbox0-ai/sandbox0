package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/appservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
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
	req.StartedAt = sandboxClaimIngressStartedAt(claims)
	req.OperationID = sandboxClaimOperationID(claims)
	if req.Template == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(req.Template)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.Template = canonicalTemplateID
	if s.templateStoreEnabled && s.templateStore != nil {
		tpl, err := s.templateStore.GetTemplateForTeam(c.Request.Context(), claims.TeamID, req.Template)
		if err != nil {
			s.logger.Error("Failed to check template creation status",
				zap.String("template", req.Template),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check template status")
			return
		}
		if tpl != nil && !tpl.ReadyForClaim() {
			writeManagerTemplateNotReady(c, tpl)
			return
		}
	}

	claimer := s.sandboxClaimer
	if claimer == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox claim backend is not configured")
		return
	}
	resp, err := claimer.ClaimSandbox(c.Request.Context(), &req)
	if err != nil {
		if errors.Is(err, service.ErrInvalidClaimRequest) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		if apierror.IsNotFound(err) || errors.Is(err, service.ErrTemplateNotFound) ||
			errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, err.Error())
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

	if resp.CommandReadyDuration > 0 {
		c.Header("Server-Timing", fmt.Sprintf(
			"sandbox0-command-ready;dur=%.3f", float64(resp.CommandReadyDuration)/float64(time.Millisecond),
		))
		slo := "missed"
		if resp.CommandReadyWithinSLO {
			slo = "met"
		}
		c.Header("Sandbox0-Command-Ready-SLO", slo)
	}
	spec.JSONSuccess(c, http.StatusCreated, resp)
}

func sandboxClaimIngressStartedAt(claims *internalauth.Claims) time.Time {
	if claims == nil || claims.Audit == nil || claims.Audit.IngressStartedAt == nil {
		return time.Time{}
	}
	return claims.Audit.IngressStartedAt.UTC()
}

func sandboxClaimOperationID(claims *internalauth.Claims) string {
	if claims == nil || claims.Audit == nil {
		return ""
	}
	return strings.TrimSpace(claims.Audit.OperationID)
}

func writeManagerTemplateNotReady(c *gin.Context, tpl *template.Template) {
	message := template.ErrTemplateNotReady.Error()
	if tpl != nil && tpl.Status != nil && tpl.Status.Creation != nil {
		switch tpl.Status.Creation.State {
		case v1alpha1.TemplateCreationStateCreating:
			c.Header("Retry-After", "1")
			message = "template creation is still in progress"
		case v1alpha1.TemplateCreationStateFailed:
			message = "template creation failed; delete and recreate the template"
		}
	}
	spec.JSONError(c, http.StatusConflict, spec.CodeTemplateNotReady, message)
}

// listSandboxes lists all sandboxes for the authenticated team
func (s *Server) listSandboxes(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	// Parse query parameters
	req := &sandboxstore.ListSandboxesRequest{
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

	if s.sandboxReader == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox query service is not configured")
		return
	}
	resp, err := s.sandboxReader.ListSandboxes(c.Request.Context(), req)
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
	case managerapi.SandboxStatusStarting,
		managerapi.SandboxStatusRunning,
		managerapi.SandboxStatusPaused,
		managerapi.SandboxStatusTerminating,
		managerapi.SandboxStatusFailed:
		return true
	default:
		return false
	}
}

func requireSandboxID(c *gin.Context) (string, bool) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return "", false
	}
	return sandboxID, true
}

func requireAuthenticatedClaims(c *gin.Context) (*internalauth.Claims, bool) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return nil, false
	}
	return claims, true
}

func (s *Server) getOwnedSandbox(c *gin.Context, sandboxID string, claims *internalauth.Claims, failureLog string) (*managerapi.Sandbox, bool) {
	if s.sandboxReader == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox query service is not configured")
		return nil, false
	}
	sandbox, err := s.sandboxReader.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		if failureLog != "" {
			s.logger.Error(failureLog,
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
		}
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return nil, false
	}
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return nil, false
	}
	return sandbox, true
}

// getSandbox gets a sandbox
func (s *Server) getSandbox(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	sandbox, ok := s.getOwnedSandbox(c, sandboxID, claims, "Failed to get sandbox")
	if !ok {
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

	if s.sandboxReader == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox query service is not configured")
		return
	}
	sandbox, err := s.sandboxReader.GetSandbox(c.Request.Context(), sandboxID)
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

// getSandboxTemplateSourceInternal returns the durable source template context
// to a trusted scheduler or cluster-gateway caller.
func (s *Server) getSandboxTemplateSourceInternal(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil || strings.TrimSpace(claims.TeamID) == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing team authentication")
		return
	}
	if !claims.IsSystemToken() &&
		!internalauth.HasPermission(c.Request.Context(), gatewayauthn.PermSandboxRead) &&
		!hasInternalGatewayWildcard(claims.Permissions) {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox:read permission is required")
		return
	}
	if s.sandboxSourceResolver == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox template source resolver is not configured")
		return
	}
	source, err := s.sandboxSourceResolver.ResolveSandboxTemplateSource(c.Request.Context(), sandboxID, claims.TeamID)
	if err != nil {
		switch {
		case errors.Is(err, template.ErrTemplateSourceNotFound):
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, err.Error())
		case errors.Is(err, template.ErrTemplateSourceForbidden):
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
		case errors.Is(err, template.ErrTemplateSourceNotReady):
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
		default:
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
		}
		return
	}
	spec.JSONSuccess(c, http.StatusOK, source)
}

func hasInternalGatewayWildcard(permissions []string) bool {
	for _, permission := range permissions {
		if permission == "*:*" {
			return true
		}
	}
	return false
}

// updateSandbox updates sandbox configuration
func (s *Server) updateSandbox(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
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

	sandbox, ok := s.getOwnedSandbox(c, sandboxID, claims, "")
	if !ok {
		return
	}

	if req.Config.AutoResume != nil && !*req.Config.AutoResume {
		if appservice.SandboxAppServicesHaveResumeRoute(sandbox.Services) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
				"cannot disable auto_resume while service routes have resume=true; remove or update those routes first")
			return
		}
	}
	resultAutoResume := sandbox.AutoResume
	if req.Config.AutoResume != nil {
		resultAutoResume = *req.Config.AutoResume
	}
	if !resultAutoResume && appservice.SandboxAppServicesHaveResumeRoute(req.Config.Services) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
			"cannot set resume=true on public routes when sandbox auto_resume is disabled")
		return
	}

	if s.sandboxUpdater == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox update service is not configured")
		return
	}
	updated, err := s.sandboxUpdater.UpdateSandbox(c.Request.Context(), sandboxID, req.Config)
	if err != nil {
		if errors.Is(err, service.ErrInvalidClaimRequest) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		if errors.Is(err, service.ErrQuotaExceeded) {
			spec.JSONError(c, http.StatusTooManyRequests, "quota_exceeded", err.Error())
			return
		}
		if errors.Is(err, service.ErrSandboxRuntimeUpdateUnavailable) {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
			return
		}
		if apierror.IsConflict(err) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "sandbox conflicts with another lifecycle operation")
			return
		}
		if apierror.IsNotFound(err) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
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
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	if _, ok := s.getOwnedSandbox(c, sandboxID, claims, "Failed to get sandbox"); !ok {
		return
	}

	status, err := s.sandboxReader.GetSandboxStatus(c.Request.Context(), sandboxID)
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
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	if _, ok := s.getOwnedSandbox(c, sandboxID, claims, ""); !ok {
		return
	}

	if s.sandboxTerminator == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox termination is unavailable")
		return
	}
	err := s.sandboxTerminator.TerminateSandbox(c.Request.Context(), sandboxID)
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
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	if _, ok := s.getOwnedSandbox(c, sandboxID, claims, ""); !ok {
		return
	}

	if s.sandboxPauser == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox pause is unavailable")
		return
	}
	resp, err := s.sandboxPauser.PauseSandboxAndWait(c.Request.Context(), sandboxID)
	if err != nil {
		s.writeSandboxLifecycleTransitionError(c, "pause", sandboxID, err)
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}

// resumeSandbox resumes a sandbox
func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	if _, ok := s.getOwnedSandbox(c, sandboxID, claims, ""); !ok {
		return
	}

	if s.sandboxResumer == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox resume is unavailable")
		return
	}
	resp, err := s.sandboxResumer.ResumeSandboxAndWait(c.Request.Context(), sandboxID)
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
	case errors.Is(err, service.ErrSandboxLifecycleUnavailable):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
	case apierror.IsConflict(err):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, fmt.Sprintf("sandbox %s conflicts with another lifecycle operation", action))
	case apierror.IsNotFound(err):
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
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	if _, ok := s.getOwnedSandbox(c, sandboxID, claims, ""); !ok {
		return
	}

	// Parse optional request body
	var req service.RefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	if s.sandboxUpdater == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox update service is not configured")
		return
	}
	resp, err := s.sandboxUpdater.RefreshSandbox(c.Request.Context(), sandboxID, &req)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidClaimRequest):
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		case apierror.IsConflict(err):
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "sandbox termination is in progress")
			return
		case apierror.IsNotFound(err):
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
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
