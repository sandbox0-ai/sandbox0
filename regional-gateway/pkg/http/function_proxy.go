package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func (s *Server) proxyFunctionGatewayAPI(c *gin.Context) {
	if s.functionGatewayRouter == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "function gateway not available")
		return
	}

	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	token, err := s.generateInternalToken(authCtx, internalauth.ServiceFunctionGateway)
	if err != nil {
		s.logger.Error("Failed to generate internal token for function-gateway", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	s.applyInternalHeaders(c, token, authCtx)
	s.functionGatewayRouter.ProxyToTarget(c)
}
