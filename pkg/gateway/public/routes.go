package public

import (
	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	licensinghttp "github.com/sandbox0-ai/sandbox0/pkg/licensing/http"
	"go.uber.org/zap"
)

type Deps struct {
	IdentityRepo            *identity.Repository
	APIKeyRepo              *apikey.Repository
	AuthMiddleware          *middleware.AuthMiddleware
	BuiltinProvider         *builtin.Provider
	OIDCManager             *oidc.Manager
	Entitlements            licensing.Entitlements
	JWTIssuer               *authn.Issuer
	RegionRepo              *tenantdir.Repository
	RequireCreateHomeRegion bool
	Logger                  *zap.Logger
}

// RegisterRoutes mounts the full self-hosted public surface.
func RegisterRoutes(router gin.IRouter, deps Deps) {
	RegisterIdentityRoutes(router, deps)
	RegisterAPIKeyRoutes(router, deps)
}

func newTeamHandler(deps Deps) *handlers.TeamHandler {
	teamOpts := make([]handlers.TeamHandlerOption, 0, 1)
	if deps.RequireCreateHomeRegion {
		teamOpts = append(teamOpts, handlers.WithCreateHomeRegionRequired(deps.RegionRepo))
	}
	return handlers.NewTeamHandler(deps.IdentityRepo, deps.Logger, teamOpts...)
}

// RegisterIdentityRoutes mounts global identity and team directory routes.
func RegisterIdentityRoutes(router gin.IRouter, deps Deps) {
	authOpts := make([]handlers.AuthHandlerOption, 0, 1)
	if deps.RequireCreateHomeRegion {
		authOpts = append(authOpts, handlers.WithCreateHomeRegionRequiredForAuth(deps.RegionRepo))
	}
	authHandler := handlers.NewAuthHandler(
		deps.IdentityRepo,
		deps.BuiltinProvider,
		deps.OIDCManager,
		deps.JWTIssuer,
		deps.Logger,
		authOpts...,
	)
	userHandler := handlers.NewUserHandler(deps.IdentityRepo, deps.Logger)
	teamHandler := newTeamHandler(deps)

	// ===== Public Auth Routes (no authentication required) =====
	auth := router.Group("/auth")
	{
		// Get available auth providers
		auth.GET("/providers", authHandler.GetAuthProviders)

		// Built-in auth
		auth.POST("/login", authHandler.Login)
		auth.POST("/register", authHandler.Register)
		auth.POST("/refresh", authHandler.RefreshToken)

		// OIDC auth
		auth.GET(
			"/oidc/:provider/login",
			licensinghttp.RequireFeature(deps.Entitlements, licensing.FeatureSSO, deps.Logger),
			authHandler.OIDCLogin,
		)
		auth.GET(
			"/oidc/:provider/callback",
			licensinghttp.RequireFeature(deps.Entitlements, licensing.FeatureSSO, deps.Logger),
			authHandler.OIDCCallback,
		)
		auth.POST(
			"/oidc/:provider/device/start",
			licensinghttp.RequireFeature(deps.Entitlements, licensing.FeatureSSO, deps.Logger),
			authHandler.OIDCDeviceStart,
		)
		auth.POST(
			"/oidc/:provider/device/poll",
			licensinghttp.RequireFeature(deps.Entitlements, licensing.FeatureSSO, deps.Logger),
			authHandler.OIDCDevicePoll,
		)
	}

	// ===== Protected Auth Routes =====
	authProtected := router.Group("/auth")
	authProtected.Use(deps.AuthMiddleware.Authenticate())
	authProtected.Use(deps.AuthMiddleware.RequireJWTAuth())
	{
		authProtected.POST("/logout", authHandler.Logout)
		authProtected.POST("/change-password", authHandler.ChangePassword)
	}

	// ===== User Management Routes =====
	users := router.Group("/users")
	users.Use(deps.AuthMiddleware.Authenticate())
	users.Use(deps.AuthMiddleware.RequireJWTAuth())
	{
		users.GET("/me", userHandler.GetCurrentUser)
		users.PUT("/me", userHandler.UpdateCurrentUser)
		users.GET("/me/identities", userHandler.GetUserIdentities)
		users.DELETE("/me/identities/:id", userHandler.DeleteUserIdentity)
	}

	// ===== Team Management Routes =====
	teams := router.Group("/teams")
	teams.Use(deps.AuthMiddleware.Authenticate())
	teams.Use(deps.AuthMiddleware.RequireJWTAuth())
	{
		teams.GET("", teamHandler.ListTeams)
		teams.POST("", teamHandler.CreateTeam)
		teams.GET("/:id", teamHandler.GetTeam)
		teams.PUT("/:id", teamHandler.UpdateTeam)
		teams.DELETE("/:id", teamHandler.DeleteTeam)

		// Team members
		teams.GET("/:id/members", teamHandler.ListTeamMembers)
		teams.POST("/:id/members", teamHandler.AddTeamMember)
		teams.PUT("/:id/members/:userId", teamHandler.UpdateTeamMember)
		teams.DELETE("/:id/members/:userId", teamHandler.RemoveTeamMember)
	}
}

// RegisterAPIKeyRoutes mounts home-region API key management routes.
func RegisterAPIKeyRoutes(router gin.IRouter, deps Deps) {
	if deps.APIKeyRepo != nil {
		apiKeyHandler := handlers.NewAPIKeyHandler(deps.APIKeyRepo, deps.IdentityRepo, deps.Logger)

		// ===== API Key Management Routes =====
		apiKeys := router.Group("/api-keys")
		apiKeys.Use(deps.AuthMiddleware.Authenticate())
		apiKeys.Use(deps.AuthMiddleware.RequireJWTAuth())
		{
			apiKeys.GET("", apiKeyHandler.ListAPIKeys)
			apiKeys.POST("", apiKeyHandler.CreateAPIKey)
			apiKeys.DELETE("/:id", apiKeyHandler.DeleteAPIKey)
			apiKeys.POST("/:id/deactivate", apiKeyHandler.DeactivateAPIKey)
		}
	}
}
