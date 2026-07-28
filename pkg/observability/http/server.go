package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/httpserver"
)

// ServerConfig configures HTTP server observability middleware.
type ServerConfig = httpserver.Config

// ServerMiddleware returns net/http middleware with tracing and optional logging.
func ServerMiddleware(cfg ServerConfig) func(http.Handler) http.Handler {
	return httpserver.Middleware(cfg)
}

// GinMiddleware returns gin middleware with tracing and optional logging.
func GinMiddleware(cfg ServerConfig) gin.HandlerFunc {
	observer := httpserver.NewObserver(cfg)
	return func(c *gin.Context) {
		request, observation := observer.Start(c.Request)
		if observation == nil {
			c.Next()
			return
		}
		defer observation.Close()

		c.Request = request
		c.Next()
		observation.Finish(httpserver.Result{
			Status:       c.Writer.Status(),
			Route:        c.FullPath(),
			RouteKnown:   true,
			ResponseSize: c.Writer.Size(),
			ClientIP:     c.ClientIP(),
		})
	}
}
