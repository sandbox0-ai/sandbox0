package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/auth"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// InternalAuthMiddleware provides authentication middleware for internal service requests
type InternalAuthMiddleware struct {
	validator *internalauth.Validator
	logger    *zap.Logger
}

// NewInternalAuthMiddleware creates a new internal auth middleware
func NewInternalAuthMiddleware(validator *internalauth.Validator, logger *zap.Logger) *InternalAuthMiddleware {
	return &InternalAuthMiddleware{
		validator: validator,
		logger:    logger,
	}
}

// Authenticate returns a gin middleware that validates internal tokens from edge-gateway
func (m *InternalAuthMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract internal token from header
		token := c.GetHeader(internalauth.DefaultTokenHeader)
		if token == "" {
			// Try Authorization header as fallback
			authHeader := c.GetHeader("Authorization")
			if authHeader != "" && len(authHeader) > 7 && authHeader[:7] == "Bearer " {
				token = authHeader[7:]
			}
		}

		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing internal authentication token",
			})
			return
		}

		// Validate token
		claims, err := m.validator.Validate(token)
		if err != nil {
			m.logger.Warn("Internal auth validation failed",
				zap.String("error", err.Error()),
				zap.String("client_ip", c.ClientIP()),
			)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "unauthorized: " + err.Error(),
			})
			return
		}

		// Convert internalauth.Claims to auth.AuthContext
		authCtx := &auth.AuthContext{
			AuthMethod:  auth.AuthMethodInternal,
			TeamID:      claims.TeamID,
			UserID:      claims.UserID,
			Permissions: claims.Permissions,
		}

		// Store auth context in gin context
		c.Set("auth_context", authCtx)

		// Also store in request context for downstream use
		ctx := auth.WithAuthContext(c.Request.Context(), authCtx)
		c.Request = c.Request.WithContext(ctx)

		// Store internalauth claims in context as well
		ctx = internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// RequirePermission returns middleware that checks for a specific permission
func (m *InternalAuthMiddleware) RequirePermission(permission string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := GetAuthContext(c)
		if authCtx == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "not authenticated",
			})
			return
		}

		if !authCtx.HasPermission(permission) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "insufficient permissions",
			})
			return
		}

		c.Next()
	}
}

// GetAuthContext extracts auth context from gin context
func GetAuthContext(c *gin.Context) *auth.AuthContext {
	if v, exists := c.Get("auth_context"); exists {
		return v.(*auth.AuthContext)
	}
	return nil
}
