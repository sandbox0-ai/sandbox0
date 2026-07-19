package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func writeIdentityResourceMutationError(c *gin.Context, err error) bool {
	if identity.IsIdentityPayloadTooLarge(err) {
		spec.JSONError(
			c,
			http.StatusRequestEntityTooLarge,
			"request_too_large",
			"identity resource is too large",
		)
		return true
	}
	if !identity.IsIdentityResourceLimitExceeded(err) {
		return false
	}
	var details any
	var limitErr *identity.IdentityResourceLimitExceededError
	if errors.As(err, &limitErr) {
		details = gin.H{
			"scope":    limitErr.Scope,
			"scope_id": limitErr.ScopeID,
			"resource": limitErr.Resource,
			"limit":    limitErr.Limit,
		}
	}
	spec.JSONError(
		c,
		http.StatusTooManyRequests,
		spec.CodeIdentityLimitExceeded,
		"identity resource limit exceeded",
		details,
	)
	return true
}
