package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestCaptureSandboxClaimIngressUsesRegionalClock(t *testing.T) {
	gin.SetMode(gin.TestMode)
	want := time.Date(2026, time.August, 20, 8, 9, 10, 123456789, time.UTC)
	router := gin.New()
	router.Use(captureSandboxClaimIngress(func() time.Time { return want }))
	router.POST("/api/v1/sandboxes", func(c *gin.Context) {
		if got := sandboxClaimIngressStartedAt(c); !got.Equal(want) {
			t.Fatalf("ingress start = %s, want %s", got, want)
		}
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", nil)
	request.Header.Set("X-Sandbox0-Ingress-Started-At", "2000-01-01T00:00:00Z")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestGenerateInternalTokenCarriesTrustedClaimIngress(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	startedAt := time.Date(2026, time.August, 20, 8, 9, 10, 123456789, time.UTC)
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: internalauth.ServiceRegionalGateway, PrivateKey: privateKey, TTL: time.Minute,
	})}
	token, err := server.generateInternalTokenWithIngress(&authn.AuthContext{
		TeamID: "team-1", UserID: "user-1", Permissions: []string{authn.PermSandboxCreate},
	}, internalauth.ServiceScheduler, startedAt)
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target: internalauth.ServiceScheduler, PublicKey: publicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if claims.Audit == nil || claims.Audit.IngressStartedAt == nil ||
		!claims.Audit.IngressStartedAt.Equal(startedAt) {
		t.Fatalf("audit ingress start = %#v, want %s", claims.Audit, startedAt)
	}
}

func TestCaptureSandboxClaimIngressIgnoresOtherRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(captureSandboxClaimIngress(time.Now))
	router.GET("/api/v1/sandboxes", func(c *gin.Context) {
		if got := sandboxClaimIngressStartedAt(c); !got.IsZero() {
			t.Fatalf("unexpected ingress start %s", got)
		}
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}
