package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// getInternalSandbox returns cluster-local sandbox metadata for trusted
// internal callers such as regional-gateway.
func (s *Server) getInternalSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	sandbox, err := s.managerClient.GetSandboxInternal(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get internal sandbox from manager",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		if errors.Is(err, client.ErrSandboxNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
			return
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager service unavailable")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, sandbox)
}

// resumeInternalSandbox requests a sandbox resume for trusted internal callers
// after the caller has already performed region-level authorization.
func (s *Server) resumeInternalSandbox(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.TeamID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing internal team context")
		return
	}

	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	if err := s.managerClient.ResumeSandbox(c.Request.Context(), sandboxID, authCtx.UserID, authCtx.TeamID); err != nil {
		s.logger.Warn("Internal sandbox resume failed",
			zap.String("sandbox_id", sandboxID),
			zap.String("team_id", authCtx.TeamID),
			zap.String("user_id", authCtx.UserID),
			zap.Error(err),
		)
		if errors.Is(err, client.ErrSandboxNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
			return
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox resume failed")
		return
	}

	if s.sandboxAddrCache != nil {
		s.sandboxAddrCache.Delete(sandboxCacheKey(authCtx.TeamID, sandboxID))
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "sandbox resume requested"})
}
