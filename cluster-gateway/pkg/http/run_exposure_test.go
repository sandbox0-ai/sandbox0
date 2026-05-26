package http

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestRunLabelFromHostUsesRunRootDomain(t *testing.T) {
	s := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicRootDomain:    "sandbox0.app",
				PublicRunRootDomain: "sandbox0.run",
				PublicRegionID:      "aws-us-east-1",
			},
		},
	}

	label, ok := s.runLabelFromHost("api-1234.aws-us-east-1.sandbox0.run")
	if !ok {
		t.Fatal("expected run host to match")
	}
	if label != "api-1234" {
		t.Fatalf("label = %q, want api-1234", label)
	}

	if _, ok := s.runLabelFromHost("api-1234.aws-us-east-1.sandbox0.app"); ok {
		t.Fatal("expected sandbox service root domain not to match run host")
	}
}
