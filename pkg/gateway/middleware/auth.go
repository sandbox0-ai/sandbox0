package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"go.uber.org/zap"
)

type apiKeyValidator interface {
	ValidateAPIKey(ctx context.Context, keyValue string) (*apikey.APIKey, error)
}

// JWTValidationMode controls which JWT token type the middleware accepts.
type JWTValidationMode string

const (
	JWTValidationModeAccess JWTValidationMode = "access"
	JWTValidationModeRegion JWTValidationMode = "region"
)

type AuthMiddlewareOption func(*AuthMiddleware)

// AuthMiddleware provides authentication middleware
type AuthMiddleware struct {
	apiKeys           apiKeyValidator
	jwtIssuer         *authn.Issuer
	jwtSecret         []byte
	logger            *zap.Logger
	jwtValidationMode JWTValidationMode
	requiredRegionID  string
}

// NewAuthMiddleware creates a new auth middleware
func NewAuthMiddleware(apiKeys apiKeyValidator, jwtSecret string, jwtIssuer *authn.Issuer, logger *zap.Logger, opts ...AuthMiddlewareOption) *AuthMiddleware {
	m := &AuthMiddleware{
		apiKeys:           apiKeys,
		jwtIssuer:         jwtIssuer,
		jwtSecret:         []byte(jwtSecret),
		logger:            logger,
		jwtValidationMode: JWTValidationModeAccess,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

// WithJWTValidationMode changes the accepted JWT token type.
func WithJWTValidationMode(mode JWTValidationMode) AuthMiddlewareOption {
	return func(m *AuthMiddleware) {
		switch mode {
		case JWTValidationModeRegion:
			m.jwtValidationMode = JWTValidationModeRegion
		default:
			m.jwtValidationMode = JWTValidationModeAccess
		}
	}
}

// WithRequiredRegionID binds accepted region tokens to a single region id.
func WithRequiredRegionID(regionID string) AuthMiddlewareOption {
	return func(m *AuthMiddleware) {
		m.requiredRegionID = strings.TrimSpace(regionID)
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
		ctx := authn.WithAuthContext(c.Request.Context(), authCtx)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// AuthenticateRequest validates credentials and returns the auth context.
func (m *AuthMiddleware) AuthenticateRequest(c *gin.Context) (*authn.AuthContext, error) {
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
func (m *AuthMiddleware) authenticateAPIKey(c *gin.Context, keyValue string) (*authn.AuthContext, error) {
	if m.apiKeys == nil {
		return nil, ErrInvalidToken
	}
	apiKey, err := m.apiKeys.ValidateAPIKey(c.Request.Context(), keyValue)
	if err != nil {
		return nil, err
	}

	return &authn.AuthContext{
		AuthMethod:  authn.AuthMethodAPIKey,
		TeamID:      apiKey.TeamID,
		APIKeyID:    apiKey.ID,
		Permissions: authn.ExpandRolesPermissions(apiKey.Roles),
	}, nil
}

// authenticateJWT validates a JWT token
func (m *AuthMiddleware) authenticateJWT(c *gin.Context, tokenString string) (*authn.AuthContext, error) {
	if len(m.jwtSecret) == 0 {
		return nil, ErrJWTNotConfigured
	}

	if m.jwtIssuer != nil {
		claims, err := m.validateJWT(tokenString)
		if err != nil {
			switch {
			case errors.Is(err, authn.ErrJWTNotConfigured):
				return nil, ErrJWTNotConfigured
			case errors.Is(err, authn.ErrTokenExpired):
				return nil, ErrExpiredToken
			case errors.Is(err, authn.ErrInvalidSigningMethod):
				return nil, ErrInvalidSigningMethod
			default:
				return nil, ErrInvalidToken
			}
		}

		permissions := authn.ExpandRolePermissions(claims.TeamRole)
		if claims.IsAdmin {
			permissions = append(permissions, "*")
		}

		return &authn.AuthContext{
			AuthMethod:    authn.AuthMethodJWT,
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

	permissions := authn.ExpandRolePermissions(teamRole)
	if isAdmin {
		permissions = append(permissions, "*")
	}

	return &authn.AuthContext{
		AuthMethod:    authn.AuthMethodJWT,
		TeamID:        teamID,
		UserID:        userID,
		TeamRole:      teamRole,
		IsSystemAdmin: isAdmin,
		Permissions:   permissions,
	}, nil
}

func (m *AuthMiddleware) validateJWT(tokenString string) (*authn.Claims, error) {
	switch m.jwtValidationMode {
	case JWTValidationModeRegion:
		if m.jwtIssuer == nil {
			return nil, authn.ErrJWTNotConfigured
		}
		claims, err := m.jwtIssuer.ValidateRegionToken(tokenString)
		if err != nil {
			return nil, err
		}
		if expectedIssuer := strings.TrimSpace(m.jwtIssuer.IssuerName()); expectedIssuer != "" && claims.Issuer != expectedIssuer {
			return nil, authn.ErrInvalidToken
		}
		if m.requiredRegionID != "" && strings.TrimSpace(claims.RegionID) != m.requiredRegionID {
			return nil, authn.ErrInvalidToken
		}
		return claims, nil
	default:
		if m.jwtIssuer != nil {
			claims, err := m.jwtIssuer.ValidateAccessToken(tokenString)
			if err != nil {
				return nil, err
			}
			if expectedIssuer := strings.TrimSpace(m.jwtIssuer.IssuerName()); expectedIssuer != "" && claims.Issuer != expectedIssuer {
				return nil, authn.ErrInvalidToken
			}
			return claims, nil
		}
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

	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrInvalidToken
	}
	if tokenType, _ := mapClaims["token_type"].(string); tokenType != "" && tokenType != "access" {
		return nil, ErrInvalidToken
	}

	claims := &authn.Claims{}
	if issuer, _ := mapClaims["iss"].(string); issuer != "" {
		claims.Issuer = issuer
	}
	if subject, _ := mapClaims["sub"].(string); subject != "" {
		claims.Subject = subject
	}
	claims.TeamID, _ = mapClaims["team_id"].(string)
	claims.UserID, _ = mapClaims["user_id"].(string)
	claims.TeamRole, _ = mapClaims["team_role"].(string)
	claims.RegionID, _ = mapClaims["region_id"].(string)
	claims.TokenType, _ = mapClaims["token_type"].(string)
	claims.IsAdmin, _ = mapClaims["is_admin"].(bool)
	return claims, nil
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

// RequireJWTAuth returns middleware that restricts access to JWT-authenticated users.
func (m *AuthMiddleware) RequireJWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := GetAuthContext(c)
		if authCtx == nil || authCtx.AuthMethod != authn.AuthMethodJWT || authCtx.UserID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "this API requires a user access token (human login); API keys are not supported",
			})
			return
		}

		c.Next()
	}
}

// RequireSystemAdmin returns middleware that restricts access to system admins.
func (m *AuthMiddleware) RequireSystemAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := GetAuthContext(c)
		if authCtx == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "not authenticated",
			})
			return
		}

		if !authCtx.IsSystemAdmin {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "system admin access required",
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
