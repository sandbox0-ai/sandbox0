package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func (s *Server) listCredentialSources(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	records, err := s.credentialSourceService.ListSources(c.Request.Context(), claims.TeamID)
	if err != nil {
		s.logger.Error("Failed to list credential sources", zap.String("teamID", claims.TeamID), zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to list credential sources: %v", err))
		return
	}
	spec.JSONSuccess(c, http.StatusOK, records)
}

func (s *Server) getCredentialSource(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	name := c.Param("name")
	record, err := s.credentialSourceService.GetSource(c.Request.Context(), claims.TeamID, name)
	if err != nil {
		s.logger.Error("Failed to get credential source", zap.String("teamID", claims.TeamID), zap.String("name", name), zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to get credential source: %v", err))
		return
	}
	if record == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "credential source not found")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, record)
}

func (s *Server) createCredentialSource(c *gin.Context) {
	var req egressauth.CredentialSourceWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	record, err := s.credentialSourceService.PutSource(c.Request.Context(), claims.TeamID, &req)
	if err != nil {
		s.logger.Error("Failed to create credential source", zap.String("teamID", claims.TeamID), zap.String("name", req.Name), zap.Error(err))
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("failed to create credential source: %v", err))
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, record)
}

func (s *Server) updateCredentialSource(c *gin.Context) {
	var req egressauth.CredentialSourceWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	name := c.Param("name")
	if name == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "source name is required")
		return
	}
	req.Name = name

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	record, err := s.credentialSourceService.PutSource(c.Request.Context(), claims.TeamID, &req)
	if err != nil {
		s.logger.Error("Failed to update credential source", zap.String("teamID", claims.TeamID), zap.String("name", req.Name), zap.Error(err))
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("failed to update credential source: %v", err))
		return
	}
	spec.JSONSuccess(c, http.StatusOK, record)
}

func (s *Server) deleteCredentialSource(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	name := c.Param("name")
	if err := s.credentialSourceService.DeleteSource(c.Request.Context(), claims.TeamID, name); err != nil {
		s.logger.Error("Failed to delete credential source", zap.String("teamID", claims.TeamID), zap.String("name", name), zap.Error(err))
		if errors.Is(err, egressauth.ErrCredentialSourceInUse) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "credential source is still referenced by sandbox bindings")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to delete credential source: %v", err))
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"deleted": true})
}
