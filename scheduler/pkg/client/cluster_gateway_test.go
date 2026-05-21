package client

import (
	"strings"
	"testing"
)

func TestClusterGatewayStatusErrorUsesSpecMessage(t *testing.T) {
	err := clusterGatewayStatusError(503, []byte(`{"success":false,"error":{"code":"unavailable","message":"cluster is draining"}}`))

	if err == nil {
		t.Fatal("clusterGatewayStatusError() = nil")
	}
	if err.Error() != "cluster-gateway error: cluster is draining" {
		t.Fatalf("error = %q, want spec message", err.Error())
	}
}

func TestClusterGatewayStatusErrorFallsBackToBody(t *testing.T) {
	err := clusterGatewayStatusError(502, []byte(`plain error`))

	if err == nil {
		t.Fatal("clusterGatewayStatusError() = nil")
	}
	if !strings.Contains(err.Error(), "unexpected status code 502: plain error") {
		t.Fatalf("error = %q, want status and body", err.Error())
	}
}
