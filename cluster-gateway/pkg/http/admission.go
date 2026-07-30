package http

import (
	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/admission"
)

// enforceRuntimeStartAdmission covers runtime starts that don't necessarily
// enter through a usage-starting API route, including SSH and public exposure
// auto-resume.
func (s *Server) enforceRuntimeStartAdmission(c *gin.Context, teamID string) bool {
	requireState := s.cfg != nil && s.cfg.AdmissionRequireState
	if s.admissionStore == nil && !requireState {
		// A gateway without the shared control-plane database is a self-hosted
		// configuration and has no hosted billing admission projection.
		return true
	}
	return admission.EnforceUsageStart(
		c,
		s.admissionStore,
		s.logger,
		requireState,
		teamID,
	)
}
