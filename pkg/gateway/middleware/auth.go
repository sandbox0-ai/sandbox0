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
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type apiKeyValidator interface {
	ValidateAPIKey(ctx context.Context, keyValue string) (*apikey.APIKey, error)
}

type AuthMiddlewareOption func(*AuthMiddleware)

// AuthMiddleware provides authentication middleware
type AuthMiddleware struct {
	apiKeys              apiKeyValidator
	jwtIssuer            *authn.Issuer
	jwtSecret            []byte
	logger               *zap.Logger
	requiredTeamRegionID string
}

// NewAuthMiddleware creates a new auth middleware
func NewAuthMiddleware(apiKeys apiKeyValidator, jwtSecret string, jwtIssuer *authn.Issuer, logger *zap.Logger, opts ...AuthMiddlewareOption) *AuthMiddleware {
	m := &AuthMiddleware{
		apiKeys:   apiKeys,
		jwtIssuer: jwtIssuer,
		jwtSecret: []byte(jwtSecret),
		logger:    logger,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

// WithRequiredTeamRegionID restricts selected teams to a single home region.
func WithRequiredTeamRegionID(regionID string) AuthMiddlewareOption {
	return func(m *AuthMiddleware) {
		m.requiredTeamRegionID = strings.TrimSpace(regionID)
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

	permissions := make([]string, 0)
	teamID := ""
	teamRole := ""
	if claims.TokenType == "access" && strings.TrimSpace(claims.UserID) != "" {
		selectedTeamID, selectedTeamRole, err := m.resolveSelectedTeam(c.Request.Context(), c.GetHeader(internalauth.TeamIDHeader), claims)
		if err != nil {
			return nil, err
		}
		teamID = selectedTeamID
		teamRole = selectedTeamRole
		permissions = authn.ExpandRolePermissions(teamRole)
	}
	if claims.IsAdmin {
		permissions = append(permissions, "*")
	}

	return &authn.AuthContext{
		AuthMethod:    authn.AuthMethodJWT,
		TeamID:        teamID,
		UserID:        claims.UserID,
		TeamRole:      teamRole,
		IsSystemAdmin: claims.IsAdmin,
		Permissions:   permissions,
	}, nil
}

func (m *AuthMiddleware) validateJWT(tokenString string) (*authn.Claims, error) {
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
	claims.UserID, _ = mapClaims["user_id"].(string)
	claims.Email, _ = mapClaims["email"].(string)
	claims.Name, _ = mapClaims["name"].(string)
	claims.TeamGrants = parseTeamGrants(mapClaims["team_grants"])
	claims.TokenType, _ = mapClaims["token_type"].(string)
	claims.IsAdmin, _ = mapClaims["is_admin"].(bool)
	return claims, nil
}

func (m *AuthMiddleware) resolveSelectedTeam(ctx context.Context, headerTeamID string, claims *authn.Claims) (string, string, error) {
	selectedTeamID := strings.TrimSpace(headerTeamID)
	if selectedTeamID == "" {
		return "", "", nil
	}
	if grant, ok := claims.FindTeamGrant(selectedTeamID); ok {
		if err := m.validateTeamGrantRegion(grant); err != nil {
			return "", "", err
		}
		return selectedTeamID, strings.TrimSpace(grant.TeamRole), nil
	}
	return "", "", ErrSelectedTeamForbidden
}

func (m *AuthMiddleware) validateTeamGrantRegion(grant authn.TeamGrant) error {
	if m.requiredTeamRegionID == "" {
		return nil
	}
	if strings.TrimSpace(grant.HomeRegionID) != m.requiredTeamRegionID {
		return ErrSelectedTeamWrongRegion
	}
	return nil
}

func parseTeamGrants(raw any) []authn.TeamGrant {
	rawGrants, ok := raw.([]any)
	if !ok {
		return nil
	}
	grants := make([]authn.TeamGrant, 0, len(rawGrants))
	for _, entry := range rawGrants {
		grantMap, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		teamID, _ := grantMap["team_id"].(string)
		teamRole, _ := grantMap["team_role"].(string)
		homeRegionID, _ := grantMap["home_region_id"].(string)
		if strings.TrimSpace(teamID) == "" {
			continue
		}
		grants = append(grants, authn.TeamGrant{
			TeamID:       teamID,
			TeamRole:     teamRole,
			HomeRegionID: homeRegionID,
		})
	}
	return grants
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
