package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
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

func TestPauseRunningSandboxesForTeamUsesSystemEndpoint(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceClusterGateway,
		PublicKey: publicKey,
	})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", request.Method)
		}
		if request.URL.Path != "/internal/v1/teams/team-a/pause-running-sandboxes" {
			t.Errorf("path = %q", request.URL.Path)
		}
		claims, err := validator.Validate(request.Header.Get(internalauth.DefaultTokenHeader))
		if err != nil {
			t.Errorf("validate system token: %v", err)
		} else if !claims.IsSystemToken() {
			t.Errorf("claims = %#v, want system token", claims)
		}
		_, _ = writer.Write([]byte(`{"success":true,"data":{"requested":2}}`))
	}))
	defer server.Close()

	client := &ClusterGatewayClient{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceScheduler,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: server.Client(),
	}
	result, err := client.PauseRunningSandboxesForTeam(context.Background(), server.URL, "team-a")
	if err != nil {
		t.Fatalf("PauseRunningSandboxesForTeam() error = %v", err)
	}
	if result.Requested != 2 {
		t.Fatalf("requested = %d, want 2", result.Requested)
	}
}
