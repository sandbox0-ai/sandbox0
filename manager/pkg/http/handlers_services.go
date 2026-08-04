package http

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/appservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"go.uber.org/zap"
)

func (s *Server) getExposureDomain() string {
	return appservice.SandboxAppDomain(s.publicRegionID, s.publicRootDomain)
}

func (s *Server) listSandboxServices(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	sandbox, ok := s.getOwnedSandbox(c, sandboxID, claims, "")
	if !ok {
		return
	}

	exposureDomain := s.getExposureDomain()
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"sandbox_id":      sandboxID,
		"services":        appservice.SandboxAppServiceViewsForExposure(sandboxID, exposureDomain, sandbox.Services),
		"exposure_domain": exposureDomain,
	})
}

func (s *Server) updateSandboxServices(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}

	var req struct {
		Services *[]managerapi.SandboxAppService `json:"services"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	if req.Services == nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "services is required")
		return
	}
	services := *req.Services

	sandbox, ok := s.getOwnedSandbox(c, sandboxID, claims, "")
	if !ok {
		return
	}
	if !sandbox.AutoResume && appservice.SandboxAppServicesHaveResumeRoute(services) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
			"cannot set resume=true on public routes when sandbox auto_resume is disabled")
		return
	}

	updated, err := s.sandboxService.UpdateSandbox(c.Request.Context(), sandboxID, &service.SandboxUpdateConfig{
		Services: services,
	})
	if err != nil {
		s.logger.Error("Failed to update sandbox services",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("failed to update sandbox services: %v", err))
		return
	}

	exposureDomain := s.getExposureDomain()
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"sandbox_id":      sandboxID,
		"services":        appservice.SandboxAppServiceViewsForExposure(sandboxID, exposureDomain, updated.Services),
		"exposure_domain": exposureDomain,
	})
}
