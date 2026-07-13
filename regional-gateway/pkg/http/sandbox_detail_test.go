package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetSandboxDetailWithoutSSHEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sandboxID := "sandbox-a"
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sandboxes/"+sandboxID {
			t.Fatalf("path = %q, want sandbox detail path", r.URL.Path)
		}
		if err := spec.WriteSuccess(w, http.StatusOK, apispec.Sandbox{
			Id:     sandboxID,
			TeamId: "team-a",
		}); err != nil {
			t.Fatalf("write sandbox response: %v", err)
		}
	}))
	defer clusterGateway.Close()

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate internal auth key: %v", err)
	}
	server := &Server{
		cfg: &config.RegionalGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
			ProxyTimeout:             metav1.Duration{Duration: time.Second},
		},
		logger: zap.NewNop(),
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/"+sandboxID, nil)
	ctx.Params = gin.Params{{Key: "id", Value: sandboxID}}
	ctx.Set("auth_context", &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodJWT})

	server.getSandboxDetail(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	sandbox, apiErr, err := spec.DecodeResponse[apispec.Sandbox](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("API error: %v", apiErr)
	}
	if sandbox.Ssh != nil {
		t.Fatalf("SSH connection = %#v, want nil when endpoint is disabled", sandbox.Ssh)
	}
	if got := middleware.GetAuthContext(ctx); got == nil || got.TeamID != "team-a" {
		t.Fatalf("auth context = %#v, want team-a", got)
	}
}
