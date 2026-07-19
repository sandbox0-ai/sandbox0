package public

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	licensinghttp "github.com/sandbox0-ai/sandbox0/pkg/licensing/http"
	"go.uber.org/zap"
)

type Deps struct {
	IdentityRepo                     *identity.Repository
	APIKeyRepo                       *apikey.Repository
	AuthMiddleware                   *middleware.AuthMiddleware
	TeamDeletePreflight              handlers.TeamDeletePreflight
	TeamDeletionLifecycle            handlers.TeamDeletionLifecycle
	TeamDistributedAdmissionDisabler handlers.TeamDistributedAdmissionDisabler
	// TeamDeletionUnavailableReason fails DELETE /teams/{id} closed when this
	// entrypoint cannot coordinate the owning region.
	TeamDeletionUnavailableReason string
	BuiltinProvider               *builtin.Provider
	OIDCManager                   *oidc.Manager
	Entitlements                  licensing.Entitlements
	JWTIssuer                     *authn.Issuer
	RegionRepo                    *tenantdir.Repository
	RegionID                      string
	RequireCreateHomeRegion       bool
	// TeamRequestAdmission admits authenticated requests that resolve to an
	// explicitly attributed team against the configured request-rate policy.
	TeamRequestAdmission gin.HandlerFunc
	// TeamActiveRequestAdmission holds one regional concurrency lease for the
	// full lifetime of each authenticated request attributed to a team.
	TeamActiveRequestAdmission gin.HandlerFunc
	// TeamTrafficAdmission accounts ingress and egress bytes for authenticated
	// requests that resolve to an explicit team. It must run outside
	// TeamRequestAdmission so rejected responses are accounted as well.
	TeamTrafficAdmission gin.HandlerFunc
	// IdentityRequestAdmission is a platform-level overload guard for the
	// complete identity/auth surface. It is installed exactly once.
	IdentityRequestAdmission gin.HandlerFunc
	Logger                   *zap.Logger
}

// RegisterRoutes mounts the full self-hosted public surface.
func RegisterRoutes(router gin.IRouter, deps Deps) {
	RegisterIdentityRoutes(router, deps)
	RegisterUserSSHKeyRoutes(router, deps)
	RegisterAPIKeyRoutes(router, deps)
}

func newTeamHandler(deps Deps) *handlers.TeamHandler {
	teamOpts := make([]handlers.TeamHandlerOption, 0, 5)
	if deps.RequireCreateHomeRegion {
		teamOpts = append(teamOpts, handlers.WithCreateHomeRegionRequired(deps.RegionRepo))
	}
	if deps.TeamDeletePreflight != nil {
		teamOpts = append(teamOpts, handlers.WithTeamDeletePreflight(deps.TeamDeletePreflight))
	}
	if deps.TeamDeletionLifecycle != nil {
		teamOpts = append(teamOpts, handlers.WithTeamDeletionLifecycle(deps.TeamDeletionLifecycle))
	}
	if deps.TeamDistributedAdmissionDisabler != nil {
		teamOpts = append(teamOpts, handlers.WithTeamDistributedAdmissionDisabler(deps.TeamDistributedAdmissionDisabler))
	}
	if deps.TeamDeletionUnavailableReason != "" {
		teamOpts = append(
			teamOpts,
			handlers.WithTeamDeletionUnavailable(deps.TeamDeletionUnavailableReason),
		)
	}
	return handlers.NewTeamHandler(deps.IdentityRepo, deps.Logger, teamOpts...)
}

// RegisterIdentityRoutes mounts global identity and team directory routes.
func RegisterIdentityRoutes(router gin.IRouter, deps Deps) {
	if deps.IdentityRequestAdmission != nil {
		identityRouter := router.Group("")
		identityRouter.Use(deps.IdentityRequestAdmission)
		router = identityRouter
	}

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
		auth.POST("/web-login/exchange", authHandler.WebLoginExchange)

		// OIDC auth
		auth.GET(
			"/oidc/:provider/login",
			licensinghttp.RequireFeature(deps.Entitlements, licensing.FeatureSSO, deps.Logger),
			authHandler.OIDCLogin,
		)
		auth.GET(
			"/oidc/:provider/logout",
			licensinghttp.RequireFeature(deps.Entitlements, licensing.FeatureSSO, deps.Logger),
			authHandler.OIDCLogout,
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

	// Account identity and team bootstrap routes remain platform-attributed.
	// An optional selected team must not change their quota owner.
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
	}

	targetTeam := teams.Group("/:id")
	// Resolve path ownership from signed JWT grants before any team admission.
	// Handlers retain the authoritative live membership checks after admission.
	targetTeam.Use(attributeGrantedTargetTeam(deps.AuthMiddleware))
	useRequiredTeamAdmissions(targetTeam, deps)
	{
		targetTeam.GET("", teamHandler.GetTeam)
		targetTeam.PUT("", teamHandler.UpdateTeam)
		targetTeam.DELETE("", teamHandler.DeleteTeam)
		targetTeam.PUT("/owner", teamHandler.TransferTeamOwner)

		// Team members
		targetTeam.GET("/members", teamHandler.ListTeamMembers)
		targetTeam.POST("/members", teamHandler.AddTeamMember)
		targetTeam.PUT("/members/:userId", teamHandler.UpdateTeamMember)
		targetTeam.DELETE("/members/:userId", teamHandler.RemoveTeamMember)
	}
}

// RegisterUserSSHKeyRoutes mounts the user SSH key surface without the full
// identity and team management API. Federated regional gateways use this to
// keep SSH gateway authorization data region-local while global-gateway remains
// the identity entrypoint.
func RegisterUserSSHKeyRoutes(router gin.IRouter, deps Deps) {
	userHandler := handlers.NewUserHandler(deps.IdentityRepo, deps.Logger)

	sshKeys := router.Group("/users/me/ssh-keys")
	sshKeys.Use(deps.AuthMiddleware.Authenticate())
	sshKeys.Use(deps.AuthMiddleware.RequireJWTAuth())
	sshKeys.Use(requireSelectedTeam())
	useRequiredTeamAdmissions(sshKeys, deps)
	{
		sshKeys.GET("", userHandler.ListUserSSHPublicKeys)
		sshKeys.POST("", userHandler.CreateUserSSHPublicKey)
		sshKeys.DELETE("/:id", userHandler.DeleteUserSSHPublicKey)
	}
}

// RegisterAPIKeyRoutes mounts home-region API key management routes.
func RegisterAPIKeyRoutes(router gin.IRouter, deps Deps) {
	if deps.APIKeyRepo != nil {
		apiKeyHandler := handlers.NewAPIKeyHandler(deps.APIKeyRepo, deps.IdentityRepo, deps.RegionID, deps.Logger)

		apiKeySelf := router.Group("/api-keys")
		apiKeySelf.Use(deps.AuthMiddleware.Authenticate())
		useCurrentAPIKeyAdmissions(apiKeySelf, deps)
		{
			apiKeySelf.GET("/current", apiKeyHandler.GetCurrentAPIKey)
		}

		// ===== API Key Management Routes =====
		apiKeys := router.Group("/api-keys")
		apiKeys.Use(deps.AuthMiddleware.Authenticate())
		apiKeys.Use(deps.AuthMiddleware.RequireJWTAuth())
		apiKeys.Use(requireSelectedTeam())
		apiKeys.Use(deps.AuthMiddleware.RequirePermission(authn.PermAPIKeyManage))
		useRequiredTeamAdmissions(apiKeys, deps)
		{
			apiKeys.GET("", apiKeyHandler.ListAPIKeys)
			apiKeys.POST("", apiKeyHandler.CreateAPIKey)
			apiKeys.DELETE("/:id", apiKeyHandler.DeleteAPIKey)
			apiKeys.POST("/:id/deactivate", apiKeyHandler.DeactivateAPIKey)
		}
	}
}

func useCurrentAPIKeyAdmissions(group *gin.RouterGroup, deps Deps) {
	if group == nil {
		return
	}
	if deps.TeamTrafficAdmission != nil {
		group.Use(admitCurrentAPIKeyRequest(deps.TeamTrafficAdmission))
	}
	if deps.TeamActiveRequestAdmission != nil {
		group.Use(admitCurrentAPIKeyRequest(deps.TeamActiveRequestAdmission))
	}
	if deps.TeamRequestAdmission != nil {
		group.Use(admitCurrentAPIKeyRequest(deps.TeamRequestAdmission))
	}
}

func useRequiredTeamAdmissions(group *gin.RouterGroup, deps Deps) {
	if group == nil {
		return
	}
	if deps.TeamTrafficAdmission != nil {
		group.Use(deps.TeamTrafficAdmission)
	}
	if deps.TeamActiveRequestAdmission != nil {
		group.Use(deps.TeamActiveRequestAdmission)
	}
	if deps.TeamRequestAdmission != nil {
		group.Use(deps.TeamRequestAdmission)
	}
}

func requireSelectedTeam() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := middleware.GetAuthContext(c)
		if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "no team selected")
			c.Abort()
			return
		}
		c.Next()
	}
}

func attributeGrantedTargetTeam(authMiddleware *middleware.AuthMiddleware) gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := middleware.GetAuthContext(c)
		if authCtx == nil || strings.TrimSpace(authCtx.UserID) == "" {
			spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
			c.Abort()
			return
		}

		parsedTeamID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team id must be a valid UUID")
			c.Abort()
			return
		}
		teamID := parsedTeamID.String()
		setPathParam(c, "id", teamID)

		grant, err := authMiddleware.ResolveJWTTeamGrant(authCtx, teamID)
		if err != nil {
			spec.JSONError(
				c,
				http.StatusForbidden,
				spec.CodeForbidden,
				"not authorized for the target team",
			)
			c.Abort()
			return
		}

		targetAuthCtx := *authCtx
		targetAuthCtx.TeamID = teamID
		targetAuthCtx.TeamRole = strings.TrimSpace(grant.TeamRole)
		targetAuthCtx.Permissions = authn.ExpandRolePermissions(targetAuthCtx.TeamRole)
		if targetAuthCtx.IsSystemAdmin {
			targetAuthCtx.Permissions = append(targetAuthCtx.Permissions, "*")
		}
		c.Set("auth_context", &targetAuthCtx)
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &targetAuthCtx))
		c.Next()
	}
}

func setPathParam(c *gin.Context, key, value string) {
	for index := range c.Params {
		if c.Params[index].Key == key {
			c.Params[index].Value = value
			return
		}
	}
}

func admitCurrentAPIKeyRequest(admission gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := middleware.GetAuthContext(c)
		if admission == nil {
			c.Next()
			return
		}
		if authCtx == nil {
			spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
			c.Abort()
			return
		}
		if authCtx.AuthMethod == authn.AuthMethodAPIKey &&
			authCtx.IsSystemAdmin &&
			strings.TrimSpace(authCtx.APIKeyID) != "" {
			c.Next()
			return
		}
		if strings.TrimSpace(authCtx.TeamID) != "" {
			admission(c)
			return
		}
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "api key authentication required")
		c.Abort()
	}
}
