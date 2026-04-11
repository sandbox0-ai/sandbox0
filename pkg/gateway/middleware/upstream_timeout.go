package middleware

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
)

var upstreamTimeoutWhitelistPrefixes = []string{
	"/api/v1/sandboxes",
	"/api/v1/sandboxvolumes",
}

// UpstreamTimeoutWhitelist disables gateway proxy timeouts for long-running API
// routes that can legitimately outlive the default upstream timeout.
func UpstreamTimeoutWhitelist() gin.HandlerFunc {
	return func(c *gin.Context) {
		if RequestPathAllowedWithoutUpstreamTimeout(c.Request.URL.Path) {
			c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)
		}
		c.Next()
	}
}

// RequestPathAllowedWithoutUpstreamTimeout reports whether a request path is
// covered by the long-running upstream timeout whitelist.
func RequestPathAllowedWithoutUpstreamTimeout(path string) bool {
	for _, prefix := range upstreamTimeoutWhitelistPrefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}
