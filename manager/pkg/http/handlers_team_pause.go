package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// pauseRunningSandboxesForTeamInternal accepts a trusted billing enforcement
// request. It queues durable checkpoint pauses only for sandboxes in this
// manager's data-plane cluster.
func (s *Server) pauseRunningSandboxesForTeamInternal(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if !claims.IsSystemToken() {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system token is required")
		return
	}
	teamID := strings.TrimSpace(c.Param("team_id"))
	if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return
	}
	if s.sandboxService == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox service is unavailable")
		return
	}

	result, err := s.sandboxService.PauseActiveSandboxesForTeam(c.Request.Context(), teamID)
	if err != nil {
		s.logger.Error("Failed to pause running sandboxes for restricted team",
			zap.String("team_id", teamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to pause running sandboxes")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, result)
}
