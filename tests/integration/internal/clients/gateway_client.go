package clients

import (
	"net/http"
	"time"
)

// GatewayClient is a thin wrapper around the cluster-gateway API.
type GatewayClient struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewGatewayClient creates an cluster-gateway client with defaults.
func NewGatewayClient(baseURL, token string, timeout time.Duration) *GatewayClient {
	return &GatewayClient{
		BaseURL: baseURL,
		Token:   token,
		HTTP: &http.Client{
			Timeout: timeout,
		},
	}
}
