package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func writeTeamQuotaMutationError(c *gin.Context, err error) bool {
	switch {
	case errors.Is(err, service.ErrQuotaExceeded), teamquota.IsExceeded(err):
		setTeamQuotaRetryAfter(c, err)
		spec.JSONError(c, http.StatusTooManyRequests, spec.CodeQuotaExceeded, "team quota exceeded")
		return true
	case errors.Is(err, service.ErrTeamQuotaUnavailable), teamquota.IsUnavailable(err):
		c.Header("Retry-After", "1")
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "team quota is unavailable")
		return true
	default:
		return false
	}
}
