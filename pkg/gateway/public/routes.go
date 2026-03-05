package public

import (
	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/jwt"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/infra/pkg/gateway/db"
	"github.com/sandbox0-ai/infra/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/infra/pkg/gateway/middleware"
	"github.com/sandbox0-ai/infra/pkg/licensing"
	licensinghttp "github.com/sandbox0-ai/infra/pkg/licensing/http"
	"go.uber.org/zap"
)

type Deps struct {
	Repo            *db.Repository
	AuthMiddleware  *middleware.AuthMiddleware
	BuiltinProvider *builtin.Provider
	OIDCManager     *oidc.Manager
	Entitlements    licensing.Entitlements
	JWTIssuer       *jwt.Issuer
	Logger          *zap.Logger
}

func RegisterRoutes(router *gin.Engine, deps Deps) {
	authHandler := handlers.NewAuthHandler(
		deps.Repo,
		deps.BuiltinProvider,
		deps.OIDCManager,
		deps.JWTIssuer,
		deps.Logger,
	)
	userHandler := handlers.NewUserHandler(deps.Repo, deps.Logger)
	teamHandler := handlers.NewTeamHandler(deps.Repo, deps.Logger)
	apiKeyHandler := handlers.NewAPIKeyHandler(deps.Repo, deps.Logger)

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
	}

	// ===== Protected Auth Routes =====
	authProtected := router.Group("/auth")
	authProtected.Use(deps.AuthMiddleware.Authenticate())
	{
		authProtected.POST("/logout", authHandler.Logout)
		authProtected.POST("/change-password", authHandler.ChangePassword)
	}

	// ===== User Management Routes =====
	users := router.Group("/users")
	users.Use(deps.AuthMiddleware.Authenticate())
	{
		users.GET("/me", userHandler.GetCurrentUser)
		users.PUT("/me", userHandler.UpdateCurrentUser)
		users.GET("/me/identities", userHandler.GetUserIdentities)
		users.DELETE("/me/identities/:id", userHandler.DeleteUserIdentity)
	}

	// ===== Team Management Routes =====
	teams := router.Group("/teams")
	teams.Use(deps.AuthMiddleware.Authenticate())
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

	// ===== API Key Management Routes =====
	apiKeys := router.Group("/api-keys")
	apiKeys.Use(deps.AuthMiddleware.Authenticate())
	{
		apiKeys.GET("", apiKeyHandler.ListAPIKeys)
		apiKeys.POST("", apiKeyHandler.CreateAPIKey)
		apiKeys.DELETE("/:id", apiKeyHandler.DeleteAPIKey)
		apiKeys.POST("/:id/deactivate", apiKeyHandler.DeactivateAPIKey)
	}
}
