package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const sandboxClaimIngressKey = "sandbox0.sandbox_claim_ingress_started_at"

// captureSandboxClaimIngress records the first trusted regional observation
// before authentication, admission, scheduling, or proxy work begins.
func captureSandboxClaimIngress(now func() time.Time) gin.HandlerFunc {
	if now == nil {
		now = time.Now
	}
	return func(c *gin.Context) {
		if c.Request != nil && c.Request.Method == http.MethodPost &&
			strings.TrimSuffix(c.Request.URL.Path, "/") == "/api/v1/sandboxes" {
			c.Set(sandboxClaimIngressKey, now().UTC())
		}
		c.Next()
	}
}

func sandboxClaimIngressStartedAt(c *gin.Context) time.Time {
	if c == nil {
		return time.Time{}
	}
	value, ok := c.Get(sandboxClaimIngressKey)
	if !ok {
		return time.Time{}
	}
	startedAt, _ := value.(time.Time)
	return startedAt.UTC()
}
