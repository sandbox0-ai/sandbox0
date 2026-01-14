package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/sandbox0-ai/infra/edge-gateway/pkg/db"
	"github.com/sandbox0-ai/infra/pkg/auth"
	"go.uber.org/zap"
)

// AuthMiddleware provides authentication middleware
type AuthMiddleware struct {
	repo      *db.Repository
	jwtSecret []byte
	logger    *zap.Logger
}

// NewAuthMiddleware creates a new auth middleware
func NewAuthMiddleware(repo *db.Repository, jwtSecret string, logger *zap.Logger) *AuthMiddleware {
	return &AuthMiddleware{
		repo:      repo,
		jwtSecret: []byte(jwtSecret),
		logger:    logger,
	}
}

// Authenticate returns a gin middleware that authenticates requests
func (m *AuthMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing authorization header",
			})
			return
		}

		// Extract token from "Bearer <token>"
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid authorization header format",
			})
			return
		}

		token := parts[1]

		// Determine auth method based on token format
		var authCtx *auth.AuthContext
		var err error

		if strings.HasPrefix(token, "sb0_") {
			// API Key authentication
			authCtx, err = m.authenticateAPIKey(c, token)
		} else {
			// JWT authentication
			authCtx, err = m.authenticateJWT(c, token)
		}

		if err != nil {
			m.logger.Warn("Authentication failed",
				zap.String("error", err.Error()),
				zap.String("client_ip", c.ClientIP()),
			)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": err.Error(),
			})
			return
		}

		// Store auth context in gin context
		c.Set("auth_context", authCtx)
		// Also store in request context for downstream use
		ctx := auth.WithAuthContext(c.Request.Context(), authCtx)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// authenticateAPIKey validates an API key
func (m *AuthMiddleware) authenticateAPIKey(c *gin.Context, keyValue string) (*auth.AuthContext, error) {
	apiKey, err := m.repo.ValidateAPIKey(c.Request.Context(), keyValue)
	if err != nil {
		return nil, err
	}

	return &auth.AuthContext{
		AuthMethod:  auth.AuthMethodAPIKey,
		TeamID:      apiKey.TeamID,
		APIKeyID:    apiKey.ID,
		Roles:       apiKey.Roles,
		Permissions: auth.ExpandRolePermissions(apiKey.Roles),
	}, nil
}

// authenticateJWT validates a JWT token
func (m *AuthMiddleware) authenticateJWT(c *gin.Context, tokenString string) (*auth.AuthContext, error) {
	if len(m.jwtSecret) == 0 {
		return nil, ErrJWTNotConfigured
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidSigningMethod
		}
		return m.jwtSecret, nil
	})

	if err != nil {
		return nil, ErrInvalidToken
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrInvalidToken
	}

	// Extract claims
	teamID, _ := claims["team_id"].(string)
	userID, _ := claims["user_id"].(string)

	var roles []string
	if r, ok := claims["roles"].([]any); ok {
		for _, v := range r {
			if s, ok := v.(string); ok {
				roles = append(roles, s)
			}
		}
	}

	return &auth.AuthContext{
		AuthMethod:  auth.AuthMethodJWT,
		TeamID:      teamID,
		UserID:      userID,
		Roles:       roles,
		Permissions: auth.ExpandRolePermissions(roles),
	}, nil
}

// RequirePermission returns middleware that checks for a specific permission
func (m *AuthMiddleware) RequirePermission(permission string) gin.HandlerFunc {
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
