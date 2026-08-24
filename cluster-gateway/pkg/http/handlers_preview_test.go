package http

import (
	"crypto/ed25519"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	mgr "github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"go.uber.org/zap"
)

func TestPreviewBootstrapAndPrivateProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC()
	store := newMemoryPreviewGrantStore(time.Now)
	record := previewGrantRecord{
		ID:                "preview-1",
		SandboxID:         "sb-demo",
		TeamID:            "team-1",
		UserID:            "user-1",
		Port:              3000,
		Protocol:          "http",
		RuntimeGeneration: 4,
		BootstrapHash:     hashPreviewSecret("bootstrap-secret"),
		ExpiresAt:         now.Add(10 * time.Minute),
	}
	if err := store.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}

	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RequestURI(); got != "/api/v1/preview/http/3000/dashboard?q=1" {
			t.Errorf("procd request URI = %q", got)
		}
		if got := r.Header.Get(internalauth.DefaultTokenHeader); got == "" {
			t.Error("missing internal token")
		}
		if got := r.Header.Get("X-Sandbox0-Preview-Origin"); got != "https://sb-demo--p3000.aws-us-east-1.sandbox0.app" {
			t.Errorf("preview origin = %q", got)
		}
		if _, err := r.Cookie(previewCookieName); err == nil {
			t.Error("preview authorization cookie leaked to procd")
		}
		_, _ = w.Write([]byte("private preview"))
	}))
	defer procd.Close()

	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sandboxes/sb-demo" {
			_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
		_ = spec.WriteSuccess(w, http.StatusOK, mgr.Sandbox{
			ID:                "sb-demo",
			TeamID:            "team-1",
			InternalAddr:      procd.URL,
			Status:            mgr.SandboxStatusRunning,
			RuntimeGeneration: 4,
		})
	}))
	defer manager.Close()

	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{PublicExposureEnabled: true, PublicRootDomain: "sandbox0.app", PublicRegionID: "aws-us-east-1"},
			ProxyTimeout:  config.Duration{Duration: time.Second},
		},
		logger:          zap.NewNop(),
		managerClient:   client.NewManagerClient(manager.URL, generator, zap.NewNop(), time.Second),
		internalAuthGen: generator,
		previewGrants:   store,
	}
	router := gin.New()
	router.NoRoute(server.handlePublicExposureNoRoute)
	gateway := httptest.NewServer(router)
	defer gateway.Close()
	host := "sb-demo--p3000.aws-us-east-1.sandbox0.app"

	bootstrap := httptest.NewRequest(http.MethodGet, "https://"+host+previewBootstrapPath+"?token=preview-1.bootstrap-secret&next=%2Fdashboard%3Fq%3D1", nil)
	bootstrap.Host = host
	bootstrapRecorder := httptest.NewRecorder()
	router.ServeHTTP(bootstrapRecorder, bootstrap)
	if bootstrapRecorder.Code != http.StatusSeeOther {
		t.Fatalf("bootstrap status = %d, body = %s", bootstrapRecorder.Code, bootstrapRecorder.Body.String())
	}
	if got := bootstrapRecorder.Header().Get("Location"); got != "/dashboard?q=1" {
		t.Fatalf("bootstrap Location = %q", got)
	}
	result := bootstrapRecorder.Result()
	cookies := result.Cookies()
	if len(cookies) != 1 || cookies[0].Name != previewCookieName || !cookies[0].HttpOnly || !cookies[0].Secure || !cookies[0].Partitioned {
		t.Fatalf("bootstrap cookies = %#v", cookies)
	}
	if cookies[0].MaxAge != 0 || !cookies[0].Expires.IsZero() {
		t.Fatalf("preview cookie should be session-scoped: %#v", cookies[0])
	}
	if got := bootstrapRecorder.Header().Get("Referrer-Policy"); got != "no-referrer" {
		t.Fatalf("Referrer-Policy = %q", got)
	}

	replay := httptest.NewRequest(http.MethodGet, bootstrap.URL.String(), nil)
	replay.Host = host
	replayRecorder := httptest.NewRecorder()
	router.ServeHTTP(replayRecorder, replay)
	if replayRecorder.Code != http.StatusNotFound {
		t.Fatalf("bootstrap replay status = %d, want 404", replayRecorder.Code)
	}

	previewRequest, err := http.NewRequest(http.MethodGet, gateway.URL+"/dashboard?q=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	previewRequest.Host = host
	previewRequest.AddCookie(cookies[0])
	previewResponse, err := http.DefaultClient.Do(previewRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer previewResponse.Body.Close()
	body, _ := io.ReadAll(previewResponse.Body)
	if previewResponse.StatusCode != http.StatusOK {
		t.Fatalf("preview status = %d, body = %s", previewResponse.StatusCode, body)
	}
	if got := strings.TrimSpace(string(body)); got != "private preview" {
		t.Fatalf("preview body = %q", got)
	}
}

func TestPreviewCookieIsBoundToHostPortAndRuntimeGeneration(t *testing.T) {
	store := newMemoryPreviewGrantStore(time.Now)
	record := previewGrantRecord{
		ID:                "preview-1",
		SandboxID:         "sb-demo",
		TeamID:            "team-1",
		Port:              3000,
		RuntimeGeneration: 2,
		SessionHash:       hashPreviewSecret("session-secret"),
		ExpiresAt:         time.Now().Add(time.Minute),
	}
	if err := store.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	server := &Server{previewGrants: store}
	request := httptest.NewRequest(http.MethodGet, "https://example.test/", nil)
	request.AddCookie(&http.Cookie{Name: previewCookieName, Value: "preview-1.session-secret"})
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = request
	if _, ok := server.previewGrantForRequest(context, "sb-demo", 3001); ok {
		t.Fatal("preview cookie should not authorize a different port")
	}
	if grant, ok := server.previewGrantForRequest(context, "sb-demo", 3000); !ok || grant.RuntimeGeneration != 2 {
		t.Fatalf("matching preview grant = %#v, ok = %v", grant, ok)
	}
}

func TestPreviewGrantManagementIsBoundToCreatingUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := newMemoryPreviewGrantStore(time.Now)
	record := previewGrantRecord{
		ID:                "preview-1",
		SandboxID:         "sb-demo",
		TeamID:            "team-1",
		UserID:            "user-1",
		Port:              3000,
		Protocol:          "http",
		RuntimeGeneration: 1,
		ExpiresAt:         time.Now().Add(time.Minute),
	}
	if err := store.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}

	server := &Server{previewGrants: store}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		authCtx := &authn.AuthContext{TeamID: "team-1", UserID: "user-2"}
		c.Set("auth_context", authCtx)
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), authCtx))
		c.Next()
	})
	router.PUT("/api/v1/sandboxes/:id/previews/:preview_id", server.renewSandboxPreview)
	router.DELETE("/api/v1/sandboxes/:id/previews/:preview_id", server.deleteSandboxPreview)

	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		request := httptest.NewRequest(method, "/api/v1/sandboxes/sb-demo/previews/preview-1", strings.NewReader(`{"ttl_seconds": 60}`))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", method, response.Code)
		}
	}
	if _, err := store.Get(t.Context(), record.ID); err != nil {
		t.Fatalf("another team member changed the preview grant: %v", err)
	}
}
