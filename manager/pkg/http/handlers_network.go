package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"go.uber.org/zap"
)

// getNetworkPolicy gets the network policy for a sandbox.
func (s *Server) getNetworkPolicy(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	if _, ok := s.getOwnedSandbox(c, sandboxID, claims, "Failed to get sandbox for network policy"); !ok {
		return
	}

	if s.sandboxNetworkPolicy == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox network policy service is not configured")
		return
	}
	networkPolicy, err := s.sandboxNetworkPolicy.GetNetworkPolicy(c.Request.Context(), sandboxID)
	if err != nil {
		if errors.Is(err, service.ErrDataPlaneNotReady) {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
			return
		}
		if apierror.IsConflict(err) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
			return
		}
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
	sandboxID, ok := requireSandboxID(c)
	if !ok {
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

	claims, ok := requireAuthenticatedClaims(c)
	if !ok {
		return
	}
	if _, ok := s.getOwnedSandbox(c, sandboxID, claims, "Failed to get sandbox for network policy update"); !ok {
		return
	}

	if s.sandboxNetworkPolicy == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox network policy service is not configured")
		return
	}
	updated, err := s.sandboxNetworkPolicy.UpdateNetworkPolicy(c.Request.Context(), sandboxID, &req)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidNetworkPolicy):
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		case errors.Is(err, service.ErrSandboxRuntimeUpdateUnavailable), errors.Is(err, service.ErrDataPlaneNotReady):
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
			return
		case apierror.IsConflict(err):
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
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
