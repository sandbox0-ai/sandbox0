package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

// ExternalRateLimiter applies rate limiting only for non-internal auth.
type ExternalRateLimiter struct {
	limiter *gatewaymiddleware.RateLimiter
}

func NewExternalRateLimiter(limiter *gatewaymiddleware.RateLimiter) *ExternalRateLimiter {
	return &ExternalRateLimiter{limiter: limiter}
}

func (e *ExternalRateLimiter) RateLimit() gin.HandlerFunc {
	return e.RateLimitDimension(quota.DimensionAPIRequests)
}

// RateLimitDimension applies one rate-quota dimension only to external calls.
// Internal regional-gateway forwarding has already consumed the regional quota.
func (e *ExternalRateLimiter) RateLimitDimension(dimension quota.Dimension) gin.HandlerFunc {
	return func(c *gin.Context) {
		if e == nil || e.limiter == nil {
			c.Next()
			return
		}
		authCtx := GetAuthContext(c)
		if authCtx != nil && authCtx.AuthMethod == authn.AuthMethodInternal {
			c.Next()
			return
		}

		e.limiter.RateLimitDimension(dimension)(c)
	}
}
