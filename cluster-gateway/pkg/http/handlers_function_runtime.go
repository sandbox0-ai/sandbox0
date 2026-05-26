package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func (s *Server) healthFunctionRuntimeService(c *gin.Context) {
	if s.managerClient == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager client unavailable")
		return
	}
	sandboxID := c.Param("id")
	serviceID := c.Param("service_id")
	sandbox, err := s.managerClient.GetSandboxInternal(c.Request.Context(), sandboxID)
	if err != nil {
		if errors.Is(err, client.ErrSandboxNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
			return
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager service unavailable")
		return
	}
	if sandboxRuntimeMissing(sandbox) {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox runtime is not ready")
		return
	}
	for i := range sandbox.Services {
		service := sandbox.Services[i]
		if service.ID != serviceID {
			continue
		}
		targetURL, err := withPort(sandbox.InternalAddr, service.Port)
		if err != nil {
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid sandbox address")
			return
		}
		healthPath := "/"
		if service.HealthCheck != nil && service.HealthCheck.Path != "" {
			healthPath = service.HealthCheck.Path
		}
		targetURL.Path = healthPath
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
		if err != nil {
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "create health request failed")
			return
		}
		resp, err := s.outboundHTTPClient().Do(req)
		if err != nil {
			s.logger.Debug("Function runtime health check failed",
				zap.String("sandbox_id", sandboxID),
				zap.String("service_id", serviceID),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "service is not ready")
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, fmt.Sprintf("service health returned %d", resp.StatusCode))
			return
		}
		spec.JSONSuccess(c, http.StatusOK, gin.H{"ready": true})
		return
	}
	spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "service not found")
}
