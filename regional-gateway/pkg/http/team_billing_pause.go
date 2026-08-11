package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/admission"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

type pauseRunningSandboxesRequest struct {
	Version *int64 `json:"version"`
}

// pauseRunningSandboxesForRestrictedTeam turns the current billing admission
// restriction into checkpoint pauses across the region. The expected version
// makes a delayed retry harmless after a top-up has already restored admission.
func (s *Server) pauseRunningSandboxesForRestrictedTeam(c *gin.Context) {
	teamID := strings.TrimSpace(c.Param("team_id"))
	if _, err := uuid.Parse(teamID); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id must be a UUID")
		return
	}

	var request pauseRunningSandboxesRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid pause request")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid pause request")
		return
	}
	if request.Version == nil || *request.Version < 0 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "version must not be negative")
		return
	}
	if s.admissionStore == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "admission state store is unavailable")
		return
	}

	record, found, err := s.admissionStore.Get(c.Request.Context(), teamID)
	if err != nil {
		s.logger.Error("Failed to read team admission state for billing pause", zap.Error(err))
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "admission state is unavailable")
		return
	}
	if !found || record.Version != *request.Version || record.State != admission.StateRestricted {
		spec.JSONSuccess(c, http.StatusOK, gin.H{"applied": false, "requested": 0})
		return
	}

	if s.schedulerRouter == nil {
		s.proxyToDefaultClusterGateway(c)
		return
	}
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	c.Request.URL.Path = "/api/v1/teams/" + teamID + "/pause-running-sandboxes"
	c.Request.URL.RawPath = ""
	s.proxyToScheduler(c, authCtx)
}
