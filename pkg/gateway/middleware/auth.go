package middleware

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/auth"
	gatewayjwt "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/jwt"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/db"
	"go.uber.org/zap"
)

// AuthMiddleware provides authentication middleware
type AuthMiddleware struct {
	repo      *db.Repository
	jwtIssuer *gatewayjwt.Issuer
	jwtSecret []byte
	logger    *zap.Logger
}

// NewAuthMiddleware creates a new auth middleware
func NewAuthMiddleware(repo *db.Repository, jwtSecret string, jwtIssuer *gatewayjwt.Issuer, logger *zap.Logger) *AuthMiddleware {
	return &AuthMiddleware{
		repo:      repo,
		jwtIssuer: jwtIssuer,
		jwtSecret: []byte(jwtSecret),
		logger:    logger,
	}
}

// Authenticate returns a gin middleware that authenticates requests
func (m *AuthMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx, err := m.AuthenticateRequest(c)
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

		c.Set("auth_context", authCtx)
		ctx := auth.WithAuthContext(c.Request.Context(), authCtx)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// AuthenticateRequest validates credentials and returns the auth context.
func (m *AuthMiddleware) AuthenticateRequest(c *gin.Context) (*auth.AuthContext, error) {
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" {
		return nil, errors.New("missing authorization header")
	}

	// Extract token from "Bearer <token>"
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return nil, errors.New("invalid authorization header format")
	}

	token := parts[1]

	// Determine auth method based on token format
	if strings.HasPrefix(token, "s0_") {
		// API Key authentication
		return m.authenticateAPIKey(c, token)
	}

	// JWT authentication
	return m.authenticateJWT(c, token)
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
		Permissions: auth.ExpandRolesPermissions(apiKey.Roles),
	}, nil
}

// authenticateJWT validates a JWT token
func (m *AuthMiddleware) authenticateJWT(c *gin.Context, tokenString string) (*auth.AuthContext, error) {
	if len(m.jwtSecret) == 0 {
		return nil, ErrJWTNotConfigured
	}

	if m.jwtIssuer != nil {
		claims, err := m.jwtIssuer.ValidateAccessToken(tokenString)
		if err != nil {
			switch {
			case errors.Is(err, gatewayjwt.ErrJWTNotConfigured):
				return nil, ErrJWTNotConfigured
			case errors.Is(err, gatewayjwt.ErrTokenExpired):
				return nil, ErrExpiredToken
			case errors.Is(err, gatewayjwt.ErrInvalidSigningMethod):
				return nil, ErrInvalidSigningMethod
			default:
				return nil, ErrInvalidToken
			}
		}

		permissions := auth.ExpandRolePermissions(claims.TeamRole)
		if claims.IsAdmin {
			permissions = append(permissions, "*")
		}

		return &auth.AuthContext{
			AuthMethod:    auth.AuthMethodJWT,
			TeamID:        claims.TeamID,
			UserID:        claims.UserID,
			TeamRole:      claims.TeamRole,
			IsSystemAdmin: claims.IsAdmin,
			Permissions:   permissions,
		}, nil
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
	teamRole, _ := claims["team_role"].(string)
	isAdmin, _ := claims["is_admin"].(bool)

	permissions := auth.ExpandRolePermissions(teamRole)
	if isAdmin {
		permissions = append(permissions, "*")
	}

	return &auth.AuthContext{
		AuthMethod:    auth.AuthMethodJWT,
		TeamID:        teamID,
		UserID:        userID,
		TeamRole:      teamRole,
		IsSystemAdmin: isAdmin,
		Permissions:   permissions,
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
