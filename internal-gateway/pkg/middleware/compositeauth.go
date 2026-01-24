package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/auth"
	gatewaymiddleware "github.com/sandbox0-ai/infra/pkg/gateway/middleware"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// CompositeAuthMiddleware tries internal auth first, then public auth.
type CompositeAuthMiddleware struct {
	internal *InternalAuthMiddleware
	public   *gatewaymiddleware.AuthMiddleware
	logger   *zap.Logger
}

func NewCompositeAuthMiddleware(internal *InternalAuthMiddleware, public *gatewaymiddleware.AuthMiddleware, logger *zap.Logger) *CompositeAuthMiddleware {
	return &CompositeAuthMiddleware{
		internal: internal,
		public:   public,
		logger:   logger,
	}
}

func (m *CompositeAuthMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx, claims, err := m.internal.AuthenticateRequest(c)
		if err == nil {
			m.internal.setAuthContext(c, authCtx, claims)
			c.Next()
			return
		}

		if c.GetHeader(internalauth.DefaultTokenHeader) != "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": err.Error(),
			})
			return
		}

		authCtx, err = m.public.AuthenticateRequest(c)
		if err != nil {
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
