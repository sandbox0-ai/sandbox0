package http

import (
	stdhttp "net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	"go.uber.org/zap"
)

func RequireFeature(entitlements licensing.Entitlements, feature licensing.Feature, logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		if entitlements == nil {
			spec.JSONError(
				c,
				stdhttp.StatusForbidden,
				spec.CodeNotLicensed,
				"feature is not licensed",
				gin.H{"feature": string(feature)},
			)
			c.Abort()
			return
		}

		err := entitlements.Require(feature)
		if err == nil {
			c.Next()
			return
		}

		if logger != nil {
			logger.Warn("Denied request because feature is not licensed",
				zap.String("feature", string(feature)),
				zap.Error(err),
			)
		}

		spec.JSONError(
			c,
			stdhttp.StatusForbidden,
			spec.CodeNotLicensed,
			"feature is not licensed",
			gin.H{"feature": string(feature)},
		)
		c.Abort()
	}
}
