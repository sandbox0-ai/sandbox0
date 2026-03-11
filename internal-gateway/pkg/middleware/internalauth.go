package middleware

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// InternalAuthMiddleware provides authentication middleware for internal service requests
type InternalAuthMiddleware struct {
	validator *internalauth.Validator
	logger    *zap.Logger
}

var (
	ErrMissingInternalToken = errors.New("missing internal authentication token")
	ErrInvalidInternalToken = errors.New("invalid internal authentication token")
)

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
		authCtx, claims, err := m.authenticateRequest(c, true)
		if err != nil {
			if errors.Is(err, ErrInvalidInternalToken) {
				m.logger.Warn("Internal auth validation failed",
					zap.String("error", err.Error()),
					zap.String("client_ip", c.ClientIP()),
				)
			}
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": err.Error(),
			})
			return
		}

		m.setAuthContext(c, authCtx, claims)
		c.Next()
	}
}

// AuthenticateRequest validates internal credentials and returns auth context.
func (m *InternalAuthMiddleware) AuthenticateRequest(c *gin.Context) (*authn.AuthContext, *internalauth.Claims, error) {
	return m.authenticateRequest(c, false)
}

func (m *InternalAuthMiddleware) authenticateRequest(c *gin.Context, allowAuthHeader bool) (*authn.AuthContext, *internalauth.Claims, error) {
	if m.validator == nil {
		return nil, nil, ErrInvalidInternalToken
	}

	// Extract internal token from header
	token := c.GetHeader(internalauth.DefaultTokenHeader)
	if token == "" {
		// Try Authorization header as fallback
		if allowAuthHeader {
			authHeader := c.GetHeader(internalauth.AuthorizationHeader)
			if authHeader != "" && len(authHeader) > 7 && authHeader[:7] == "Bearer " {
				token = authHeader[7:]
			}
		}
	}

	if token == "" {
		return nil, nil, ErrMissingInternalToken
	}

	claims, err := m.validator.Validate(token)
	if err != nil {
		return nil, nil, ErrInvalidInternalToken
	}

	authCtx := &authn.AuthContext{
		AuthMethod:  authn.AuthMethodInternal,
		TeamID:      claims.TeamID,
		UserID:      claims.UserID,
		Permissions: claims.Permissions,
	}

	return authCtx, claims, nil
}

func (m *InternalAuthMiddleware) setAuthContext(c *gin.Context, authCtx *authn.AuthContext, claims *internalauth.Claims) {
	c.Set("auth_context", authCtx)

	ctx := authn.WithAuthContext(c.Request.Context(), authCtx)
	c.Request = c.Request.WithContext(ctx)

	if claims != nil {
		ctx = internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)
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
func GetAuthContext(c *gin.Context) *authn.AuthContext {
	if v, exists := c.Get("auth_context"); exists {
		return v.(*authn.AuthContext)
	}
	return nil
}
