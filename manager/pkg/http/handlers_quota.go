package http

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
)

type putTeamQuotaRequest struct {
	LimitValue *int64 `json:"limit_value"`
}

func (s *Server) getTeamQuota(c *gin.Context) {
	teamID, dimension, ok := s.quotaRequestScope(c)
	if !ok {
		return
	}
	if s.quotaRepo == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "quota is unavailable")
		return
	}
	limit, err := s.quotaRepo.GetLimit(c.Request.Context(), teamID, dimension)
	if err != nil {
		s.logger.Error("Failed to get quota limit", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get quota")
		return
	}
	if limit == nil {
		spec.JSONSuccess(c, http.StatusOK, gin.H{
			"team_id":     teamID,
			"dimension":   dimension,
			"limit_value": nil,
		})
		return
	}
	spec.JSONSuccess(c, http.StatusOK, limit)
}

func (s *Server) putTeamQuota(c *gin.Context) {
	teamID, dimension, ok := s.quotaRequestScope(c)
	if !ok {
		return
	}
	if s.quotaRepo == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "quota is unavailable")
		return
	}
	var req putTeamQuotaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	if req.LimitValue == nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "limit_value is required")
		return
	}
	if *req.LimitValue < 0 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "limit_value must be non-negative")
		return
	}
	limit := &quota.Limit{TeamID: teamID, Dimension: dimension, LimitValue: *req.LimitValue}
	if err := s.quotaRepo.PutLimit(c.Request.Context(), limit); err != nil {
		s.logger.Error("Failed to put quota limit", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update quota")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, limit)
}

func (s *Server) deleteTeamQuota(c *gin.Context) {
	teamID, dimension, ok := s.quotaRequestScope(c)
	if !ok {
		return
	}
	if s.quotaRepo == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "quota is unavailable")
		return
	}
	if err := s.quotaRepo.DeleteLimit(c.Request.Context(), teamID, dimension); err != nil {
		s.logger.Error("Failed to delete quota limit", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete quota")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"deleted": true})
}

func (s *Server) quotaRequestScope(c *gin.Context) (string, quota.Dimension, bool) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return "", "", false
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return "", "", false
	}
	dimension := quota.Dimension(c.Param("dimension"))
	if !quota.KnownDimension(dimension) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "unknown quota dimension")
		return "", "", false
	}
	return claims.TeamID, dimension, true
}
