package middleware

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
)

// MarkLongLivedRequests tags streaming API routes so gateway server deadlines
// and upstream proxy timeouts can be relaxed for those requests only.
func MarkLongLivedRequests() gin.HandlerFunc {
	return func(c *gin.Context) {
		if RequestShouldBeLongLived(c.Request) {
			c.Request = proxy.WithLongLivedRequestRequest(c.Request)
		}
		c.Next()
	}
}

// RequestShouldBeLongLived reports whether the incoming request is expected to
// hold the connection open for an extended period.
func RequestShouldBeLongLived(req *http.Request) bool {
	if req == nil {
		return false
	}
	if proxy.IsWebSocketUpgrade(req) {
		return true
	}
	return isSandboxLogsFollowRequest(req)
}

func isSandboxLogsFollowRequest(req *http.Request) bool {
	if req.Method != http.MethodGet || req.URL == nil {
		return false
	}
	path := req.URL.Path
	if !strings.HasPrefix(path, "/api/v1/sandboxes/") || !strings.HasSuffix(path, "/logs") {
		return false
	}
	follow, err := strconv.ParseBool(req.URL.Query().Get("follow"))
	return err == nil && follow
}
