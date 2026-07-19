package http

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// sandboxObservabilityIngestOverloadAdmission reuses the cluster gateway's
// aggregate platform guard so body buffering cannot become the first unbounded
// work performed for an ingest request.
func (s *Server) sandboxObservabilityIngestOverloadAdmission() gin.HandlerFunc {
	if s != nil && s.publicOverloadGuard != nil {
		return s.publicOverloadGuard.Admit()
	}
	return sandboxObservabilityIngestUnavailable(
		"sandbox observability overload guard is unavailable",
	)
}

// sandboxObservabilityIngestActiveRequestAdmission holds a team-scoped lease
// before the request body is read and releases it after the ingest handler
// finishes.
func (s *Server) sandboxObservabilityIngestActiveRequestAdmission() gin.HandlerFunc {
	if s != nil && s.teamQuotaController != nil {
		return s.teamQuotaController.AdmitActiveRequests(false)
	}
	return sandboxObservabilityIngestUnavailable("team quota is unavailable")
}

func sandboxObservabilityIngestUnavailable(message string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Retry-After", "1")
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, message)
		c.Abort()
	}
}
