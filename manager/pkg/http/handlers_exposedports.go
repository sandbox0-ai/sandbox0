package http

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"go.uber.org/zap"
)

type updateExposedPortsRequest struct {
	Ports []service.ExposedPortConfig `json:"ports"`
}

// exposedPortResponse represents an exposed port with public URL
type exposedPortResponse struct {
	Port      int    `json:"port"`
	Resume    bool   `json:"resume"`
	PublicURL string `json:"public_url,omitempty"`
}

// buildExposedPortResponses builds response with public URLs
func (s *Server) buildExposedPortResponses(sandboxID string, ports []service.ExposedPortConfig) []exposedPortResponse {
	responses := make([]exposedPortResponse, len(ports))
	for i, p := range ports {
		responses[i] = exposedPortResponse{
			Port:   p.Port,
			Resume: p.Resume,
		}
		if s.publicRootDomain != "" && s.publicRegionID != "" {
			if publicURL, err := s.buildPublicURL(sandboxID, p.Port); err == nil {
				responses[i].PublicURL = publicURL
			}
		}
	}
	return responses
}

// buildPublicURL constructs the public URL for an exposed port
func (s *Server) buildPublicURL(sandboxID string, port int) (string, error) {
	label, err := naming.BuildExposureHostLabel(sandboxID, port)
	if err != nil {
		return "", err
	}
	rootDomain := strings.TrimSpace(s.publicRootDomain)
	if rootDomain == "" {
		rootDomain = "sandbox0.app"
	}
	return fmt.Sprintf("%s.%s.%s", label, s.publicRegionID, rootDomain), nil
}

// getExposureDomain returns the exposure domain (regionID.rootDomain)
func (s *Server) getExposureDomain() string {
	rootDomain := strings.TrimSpace(s.publicRootDomain)
	if rootDomain == "" {
		rootDomain = "sandbox0.app"
	}
	if s.publicRegionID == "" {
		return ""
	}
	return s.publicRegionID + "." + rootDomain
}

// getExposedPorts gets the exposed ports for a sandbox
func (s *Server) getExposedPorts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}

	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	response := gin.H{
		"sandbox_id":    sandboxID,
		"exposed_ports": s.buildExposedPortResponses(sandboxID, sandbox.ExposedPorts),
	}
	if domain := s.getExposureDomain(); domain != "" {
		response["exposure_domain"] = domain
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}

// updateExposedPorts updates the exposed ports for a sandbox
func (s *Server) updateExposedPorts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var req updateExposedPortsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	// Validate: if any port has resume=true, sandbox auto_resume must be enabled
	if !sandbox.AutoResume {
		for _, p := range req.Ports {
			if p.Resume {
				spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
					"cannot set resume=true on exposed port when sandbox auto_resume is disabled")
				return
			}
		}
	}

	cfg := &service.SandboxConfig{
		ExposedPorts: req.Ports,
	}
	updated, err := s.sandboxService.UpdateSandbox(c.Request.Context(), sandboxID, cfg)
	if err != nil {
		s.logger.Error("Failed to update exposed ports",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to update exposed ports: %v", err))
		return
	}

	response := gin.H{
		"sandbox_id":    sandboxID,
		"exposed_ports": s.buildExposedPortResponses(sandboxID, updated.ExposedPorts),
	}
	if domain := s.getExposureDomain(); domain != "" {
		response["exposure_domain"] = domain
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}

// clearExposedPorts clears all exposed ports for a sandbox
func (s *Server) clearExposedPorts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	cfg := &service.SandboxConfig{
		ExposedPorts: []service.ExposedPortConfig{},
	}
	updated, err := s.sandboxService.UpdateSandbox(c.Request.Context(), sandboxID, cfg)
	if err != nil {
		s.logger.Error("Failed to clear exposed ports",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to clear exposed ports: %v", err))
		return
	}

	response := gin.H{
		"sandbox_id":    sandboxID,
		"exposed_ports": s.buildExposedPortResponses(sandboxID, updated.ExposedPorts),
	}
	if domain := s.getExposureDomain(); domain != "" {
		response["exposure_domain"] = domain
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}

// deleteExposedPort deletes a specific exposed port for a sandbox
func (s *Server) deleteExposedPort(c *gin.Context) {
	sandboxID := c.Param("id")
	portStr := c.Param("port")
	if sandboxID == "" || portStr == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and port are required")
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid port number")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	// Remove the specific port
	newPorts := make([]service.ExposedPortConfig, 0, len(sandbox.ExposedPorts))
	found := false
	for _, p := range sandbox.ExposedPorts {
		if p.Port == port {
			found = true
			continue
		}
		newPorts = append(newPorts, p)
	}
	if !found {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "port not found in exposed ports")
		return
	}

	cfg := &service.SandboxConfig{
		ExposedPorts: newPorts,
	}
	updated, err := s.sandboxService.UpdateSandbox(c.Request.Context(), sandboxID, cfg)
	if err != nil {
		s.logger.Error("Failed to delete exposed port",
			zap.String("sandboxID", sandboxID),
			zap.Int("port", port),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to delete exposed port: %v", err))
		return
	}

	response := gin.H{
		"sandbox_id":    sandboxID,
		"exposed_ports": s.buildExposedPortResponses(sandboxID, updated.ExposedPorts),
	}
	if domain := s.getExposureDomain(); domain != "" {
		response["exposure_domain"] = domain
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}
