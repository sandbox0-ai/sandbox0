package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// getClusterSummary returns cluster summary information
func (s *Server) getClusterSummary(c *gin.Context) {
	summary, err := s.clusterService.GetClusterSummary(c.Request.Context())
	if err != nil {
		s.logger.Error("Failed to get cluster summary", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to get cluster summary",
		})
		return
	}

	c.JSON(http.StatusOK, summary)
}

// getTemplateStats returns template statistics
func (s *Server) getTemplateStats(c *gin.Context) {
	stats, err := s.clusterService.GetTemplateStats(c.Request.Context())
	if err != nil {
		s.logger.Error("Failed to get template stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to get template stats",
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}
