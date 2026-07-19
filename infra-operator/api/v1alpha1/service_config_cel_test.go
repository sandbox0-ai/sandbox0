package v1alpha1

import (
	"os"
	"strings"
	"testing"
)

func TestSSHGatewayHandshakeTimeoutCELAllowsDefaultEmptyObject(t *testing.T) {
	source, err := os.ReadFile("service_api_config_types.go")
	if err != nil {
		t.Fatalf("read service_api_config_types.go: %v", err)
	}
	const marker = `XValidation:rule="!has(self.platformHandshakeTimeout) || duration(self.platformHandshakeTimeout) > duration('0s')"`
	if !strings.Contains(string(source), marker) {
		t.Fatalf("SSH gateway handshake timeout CEL rule does not guard an omitted default field")
	}
}
