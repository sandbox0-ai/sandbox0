package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

const (
	GatewayModeDirect = "direct"
	GatewayModeGlobal = "global"
)

type GatewayMetadataResponse struct {
	GatewayMode string `json:"gateway_mode"`
	Service     string `json:"service"`
}

// GatewayMetadata returns a handler that reports the public gateway mode for the current entrypoint.
func GatewayMetadata(service, gatewayMode string) gin.HandlerFunc {
	return func(c *gin.Context) {
		spec.JSONSuccess(c, http.StatusOK, GatewayMetadataResponse{
			GatewayMode: gatewayMode,
			Service:     service,
		})
	}
}
