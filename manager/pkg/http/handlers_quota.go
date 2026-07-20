package http

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
)

type putQuotaPolicyRequest struct {
	LimitValue *int64 `json:"limit_value"`
	IntervalMS *int64 `json:"interval_ms"`
	BurstValue *int64 `json:"burst_value"`
}

func (s *Server) listTeamQuotas(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return
	}
	statuses := make([]quota.Status, 0, len(quota.Dimensions()))
	for _, dimension := range quota.Dimensions() {
		status, err := s.teamQuotaStatus(c, claims.TeamID, dimension)
		if err != nil {
			s.writeTeamQuotaError(c, err)
			return
		}
		statuses = append(statuses, status)
	}
	spec.JSONSuccess(c, http.StatusOK, statuses)
}

func (s *Server) getTeamQuota(c *gin.Context) {
	teamID, dimension, ok := s.teamQuotaRequestScope(c)
	if !ok {
		return
	}
	s.writeTeamQuotaStatus(c, teamID, dimension)
}

func (s *Server) writeTeamQuotaStatus(c *gin.Context, teamID string, dimension quota.Dimension) {
	status, err := s.teamQuotaStatus(c, teamID, dimension)
	if err != nil {
		s.writeTeamQuotaError(c, err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, status)
}

func (s *Server) teamQuotaStatus(c *gin.Context, teamID string, dimension quota.Dimension) (quota.Status, error) {
	if s.quotaRepo == nil {
		return quota.Status{}, fmt.Errorf("quota repository is unavailable")
	}
	policy, err := s.quotaRepo.GetPolicy(c.Request.Context(), teamID, dimension)
	if err != nil {
		return quota.Status{}, fmt.Errorf("get quota policy: %w", err)
	}
	var current int64
	if quota.KindForDimension(dimension) == quota.KindCapacity {
		current, err = s.quotaRepo.CurrentUsage(c.Request.Context(), teamID, dimension)
		if err != nil {
			return quota.Status{}, fmt.Errorf("get quota usage: %w", err)
		}
	}
	return quota.NewStatus(teamID, dimension, policy, current), nil
}

func (s *Server) writeTeamQuotaError(c *gin.Context, err error) {
	s.logger.Error("Failed to get quota", zap.Error(err))
	if strings.Contains(err.Error(), "repository is unavailable") {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "quota is unavailable")
		return
	}
	spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get quota")
}

func (s *Server) putTeamQuotaInternal(c *gin.Context) {
	teamID, dimension, ok := s.internalQuotaRequestScope(c)
	if !ok {
		return
	}
	if s.quotaRepo == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "quota is unavailable")
		return
	}
	policy, ok := s.bindQuotaPolicy(c, teamID, dimension)
	if !ok {
		return
	}
	if err := s.quotaRepo.PutPolicy(c.Request.Context(), policy); err != nil {
		s.logger.Error("Failed to put quota policy", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update quota")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, policy)
}

func (s *Server) bindQuotaPolicy(c *gin.Context, teamID string, dimension quota.Dimension) (*quota.Policy, bool) {
	var req putQuotaPolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return nil, false
	}
	if req.LimitValue == nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "limit_value is required")
		return nil, false
	}
	policy := &quota.Policy{
		TeamID:     teamID,
		Dimension:  dimension,
		Kind:       quota.KindForDimension(dimension),
		LimitValue: *req.LimitValue,
	}
	if req.IntervalMS != nil {
		policy.IntervalMS = *req.IntervalMS
	}
	if req.BurstValue != nil {
		policy.BurstValue = *req.BurstValue
	}
	if policy.Kind == quota.KindRate {
		if req.IntervalMS == nil {
			policy.IntervalMS = int64(time.Second / time.Millisecond)
		}
		if req.BurstValue == nil {
			policy.BurstValue = policy.LimitValue
		}
	}
	if err := quota.ValidatePolicyValues(dimension, policy.LimitValue, policy.IntervalMS, policy.BurstValue); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return nil, false
	}
	return policy, true
}

func (s *Server) deleteTeamQuotaInternal(c *gin.Context) {
	teamID, dimension, ok := s.internalQuotaRequestScope(c)
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

func (s *Server) teamQuotaRequestScope(c *gin.Context) (string, quota.Dimension, bool) {
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

func (s *Server) internalQuotaRequestScope(c *gin.Context) (string, quota.Dimension, bool) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return "", "", false
	}
	if !claims.IsSystemToken() {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system token is required")
		return "", "", false
	}
	teamID := strings.TrimSpace(c.Param("team_id"))
	if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return "", "", false
	}
	dimension := quota.Dimension(c.Param("dimension"))
	if !quota.KnownDimension(dimension) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "unknown quota dimension")
		return "", "", false
	}
	return teamID, dimension, true
}
