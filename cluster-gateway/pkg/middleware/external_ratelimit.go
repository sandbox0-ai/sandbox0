package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
)

// ExternalRateLimiter applies rate limiting only for non-internal auth.
type ExternalRateLimiter struct {
	limiter *gatewaymiddleware.RateLimiter
}

func NewExternalRateLimiter(limiter *gatewaymiddleware.RateLimiter) *ExternalRateLimiter {
	return &ExternalRateLimiter{limiter: limiter}
}

func (e *ExternalRateLimiter) RateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := GetAuthContext(c)
		if authCtx != nil && authCtx.AuthMethod == authn.AuthMethodInternal {
			c.Next()
			return
		}

		e.limiter.RateLimit()(c)
	}
}
