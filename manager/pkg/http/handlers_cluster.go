package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// getClusterSummary returns cluster summary information
func (s *Server) getClusterSummary(c *gin.Context) {
	summary, err := s.clusterService.GetClusterSummary(c.Request.Context())
	if err != nil {
		s.logger.Error("Failed to get cluster summary", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get cluster summary")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, summary)
}

// getTemplateStats returns template statistics
func (s *Server) getTemplateStats(c *gin.Context) {
	stats, err := s.clusterService.GetTemplateStats(c.Request.Context())
	if err != nil {
		s.logger.Error("Failed to get template stats", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template stats")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, stats)
}
