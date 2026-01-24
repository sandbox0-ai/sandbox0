package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// RequestLogger provides request logging middleware
type RequestLogger struct {
	logger *zap.Logger
}

// NewRequestLogger creates a new request logger
func NewRequestLogger(logger *zap.Logger) *RequestLogger {
	return &RequestLogger{
		logger: logger,
	}
}

// Logger returns a gin middleware that logs requests
func (rl *RequestLogger) Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip logging for health check and readiness check
		path := c.Request.URL.Path
		if path == "/healthz" || path == "/readyz" {
			c.Next()
			return
		}

		// Generate request ID
		requestID := uuid.New().String()
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)

		// Record start time
		start := time.Now()

		// Process request
		c.Next()

		// Calculate latency
		latency := time.Since(start)

		// Get auth context
		authCtx := GetAuthContext(c)

		// Log fields
		fields := []zap.Field{
			zap.String("request_id", requestID),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", latency),
			zap.String("client_ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
		}

		if authCtx != nil {
			fields = append(fields,
				zap.String("team_id", authCtx.TeamID),
				zap.String("auth_method", string(authCtx.AuthMethod)),
			)
			if authCtx.UserID != "" {
				fields = append(fields, zap.String("user_id", authCtx.UserID))
			}
			if authCtx.APIKeyID != "" {
				fields = append(fields, zap.String("api_key_id", authCtx.APIKeyID))
			}
		}

		// Log based on status code
		status := c.Writer.Status()
		if status >= 500 {
			rl.logger.Error("HTTP request", fields...)
		} else if status >= 400 {
			rl.logger.Warn("HTTP request", fields...)
		} else {
			rl.logger.Info("HTTP request", fields...)
		}
	}
}

// GetRequestID extracts request ID from gin context
func GetRequestID(c *gin.Context) string {
	if v, exists := c.Get("request_id"); exists {
		return v.(string)
	}
	return ""
}
