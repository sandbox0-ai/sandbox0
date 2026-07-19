package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func writeTeamQuotaMutationError(c *gin.Context, err error) bool {
	switch {
	case teamquota.IsExceeded(err):
		spec.JSONError(c, http.StatusTooManyRequests, spec.CodeQuotaExceeded, "team quota exceeded")
		return true
	case teamquota.IsUnavailable(err):
		c.Header("Retry-After", "1")
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "team quota is unavailable")
		return true
	default:
		return false
	}
}
