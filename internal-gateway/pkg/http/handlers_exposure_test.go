package http

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestResolveExposureFromRequestByHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	req.Header.Set("X-Sandbox-ID", "sb-demo")
	req.Header.Set("X-Exposure-Port", "3000")
	c.Request = req

	s := &Server{cfg: &config.InternalGatewayConfig{}}
	sb, port, label, err := s.resolveExposureFromRequest(c)
	if err != nil {
		t.Fatalf("resolveExposureFromRequest: %v", err)
	}
	if sb != "sb-demo" || port != 3000 || label != "" {
		t.Fatalf("unexpected parsed values: %s %d %s", sb, port, label)
	}
}

func TestResolveExposureFromRequestByHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("GET", "http://sb-demo--p3000.aws-us-east-1.sandbox0.app/", nil)
	c.Request = req

	s := &Server{
		cfg: &config.InternalGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicRootDomain: "sandbox0.app",
				PublicRegionID:   "aws-us-east-1",
			},
		},
	}

	sb, port, label, err := s.resolveExposureFromRequest(c)
	if err != nil {
		t.Fatalf("resolveExposureFromRequest: %v", err)
	}
	if sb != "sb-demo" || port != 3000 || label != "sb-demo--p3000" {
		t.Fatalf("unexpected parsed values: %s %d %s", sb, port, label)
	}
}
