package http

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestExposureLabelFromHost(t *testing.T) {
	s := &Server{
		cfg: &config.EdgeGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicRootDomain: "sandbox0.app",
				PublicRegionID:   "aws-us-east-1",
			},
		},
	}

	label, ok := s.exposureLabelFromHost("sb-demo--p3000.aws-us-east-1.sandbox0.app")
	if !ok {
		t.Fatalf("expected host to match")
	}
	if label != "sb-demo--p3000" {
		t.Fatalf("unexpected label: %s", label)
	}

	if _, ok := s.exposureLabelFromHost("sb-demo--p3000.sandbox0.app"); ok {
		t.Fatalf("expected host mismatch without region")
	}
}
