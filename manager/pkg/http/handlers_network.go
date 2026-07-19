package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

// getNetworkPolicy gets the network policy for a sandbox.
func (s *Server) getNetworkPolicy(c *gin.Context) {
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
		s.logger.Error("Failed to get sandbox for network policy",
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

	networkPolicy, err := s.sandboxService.GetNetworkPolicy(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get network policy",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to get network policy: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, networkPolicy)
}

// updateNetworkPolicy updates the network policy for a sandbox.
func (s *Server) updateNetworkPolicy(c *gin.Context) {
	if !limitJSONRequestBody(c, "network policy request body", template.MaxObjectRequestBytes) {
		return
	}
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	var req v1alpha1.SandboxNetworkPolicy
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	if err := validateSandboxNetworkPolicyMode(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if err := service.ValidateSandboxNetworkPolicySize(&req); err != nil {
		if writeResourceTooLarge(c, err, "sandbox network policy") {
			return
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox for network policy update",
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

	updated, err := s.sandboxService.UpdateNetworkPolicy(c.Request.Context(), sandboxID, &req)
	if err != nil {
		if writeResourceTooLarge(c, err, "sandbox network policy") {
			return
		}
		if errors.Is(err, service.ErrInvalidNetworkPolicy) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		if writeTeamQuotaMutationError(c, err) {
			return
		}
		s.logger.Error("Failed to update network policy",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("failed to update network policy: %v", err))
		return
	}

	spec.JSONSuccess(c, http.StatusOK, updated)
}

func validateSandboxNetworkPolicyMode(policy *v1alpha1.SandboxNetworkPolicy) error {
	if policy == nil || policy.Mode == "" {
		return fmt.Errorf("mode is required")
	}
	switch policy.Mode {
	case v1alpha1.NetworkModeAllowAll, v1alpha1.NetworkModeBlockAll:
		return nil
	default:
		return fmt.Errorf("mode must be %q or %q", v1alpha1.NetworkModeAllowAll, v1alpha1.NetworkModeBlockAll)
	}
}
