package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Recovery returns a gin middleware that recovers from panics
func Recovery(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				spanCtx := trace.SpanFromContext(c.Request.Context()).SpanContext()
				fields := []zap.Field{
					zap.Any("error", err),
					zap.String("stack", string(debug.Stack())),
				}
				if spanCtx.IsValid() {
					fields = append(fields,
						zap.String("trace_id", spanCtx.TraceID().String()),
						zap.String("span_id", spanCtx.SpanID().String()),
					)
				}

				logger.Error("Panic recovered",
					fields...,
				)

				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": "internal server error",
				})
			}
		}()
		c.Next()
	}
}
