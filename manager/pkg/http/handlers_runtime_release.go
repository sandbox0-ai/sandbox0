package http

import (
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

// Root-only release tooling signs a scoped, synthetic probe token. Targeting
// workers is never accepted on the public claim route.
func (s *Server) probeRuntimeRelease(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil || claims.Caller != internalauth.ServiceClusterGateway || claims.TeamID != "sandbox0-runtime-release-probe" || !slices.Contains(claims.Permissions, "runtime:release-probe") {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "runtime release probe permission required")
		return
	}
	var req struct {
		Template string                  `json:"template"`
		Target   service.ClaimNodeTarget `json:"target"`
	}
	if c.ShouldBindJSON(&req) != nil || req.Template == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid probe request")
		return
	}
	if s.runtimeReleaseProbes == nil || s.sandboxClaimer == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "probe backend unavailable")
		return
	}
	operation := sandboxClaimOperationID(claims)
	valid, err := s.runtimeReleaseProbes.ValidateRuntimeReleaseProbe(c.Request.Context(), operation, req.Target.NodeID, req.Target.NodeUID, req.Target.NodeBootID)
	if err != nil || !valid {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "probe target has no live release authorization")
		return
	}
	ttl := int32(300)
	response, err := s.sandboxClaimer.ClaimSandbox(c.Request.Context(), &service.ClaimRequest{
		TeamID: claims.TeamID, UserID: claims.UserID, Template: req.Template, OperationID: operation, TargetNode: &req.Target,
		Config: &sandboxstore.SandboxConfig{TTL: &ttl, HardTTL: &ttl},
	})
	if err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "candidate probe failed")
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, response)
}
