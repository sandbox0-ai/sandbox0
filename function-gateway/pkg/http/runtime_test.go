package http

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
)

func TestMatchFunctionRouteChoosesLongestPrefix(t *testing.T) {
	service := mgr.SandboxAppService{
		Ingress: mgr.SandboxAppServiceIngress{
			Public: true,
			Routes: []mgr.SandboxAppServiceRoute{
				{ID: "root", PathPrefix: "/", Methods: []string{"GET"}},
				{ID: "api", PathPrefix: "/api", Methods: []string{"POST"}},
			},
		},
	}

	match := matchFunctionRoute(service, "/api/users", "POST")
	if !match.pathMatched || !match.methodAllowed {
		t.Fatalf("match = %+v, want matched allowed route", match)
	}
	if match.route == nil || match.route.ID != "api" {
		t.Fatalf("route = %+v, want api", match.route)
	}

	disallowed := matchFunctionRoute(service, "/api/users", "GET")
	if !disallowed.pathMatched || disallowed.methodAllowed {
		t.Fatalf("match = %+v, want path matched but method disallowed", disallowed)
	}
}

func TestAuthorizeFunctionRouteBearer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sum := sha256.Sum256([]byte("secret"))
	route := &mgr.SandboxAppServiceRoute{
		Auth: &mgr.PublicGatewayAuth{
			Mode:              mgr.PublicGatewayAuthModeBearer,
			BearerTokenSHA256: hex.EncodeToString(sum[:]),
		},
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	ctx.Request = req
	if !authorizeFunctionRoute(ctx, route) {
		t.Fatal("authorizeFunctionRoute returned false for valid bearer token")
	}

	rec = httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(rec)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	ctx.Request = req
	if authorizeFunctionRoute(ctx, route) {
		t.Fatal("authorizeFunctionRoute returned true for invalid bearer token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRewriteFunctionPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)

	rewriteFunctionPath(ctx, "/api", "/")
	if got := ctx.Request.URL.Path; got != "/v1/users" {
		t.Fatalf("path = %q, want /v1/users", got)
	}
}

func TestDecodeFunctionContextResponseAcceptsGatewayEnvelope(t *testing.T) {
	out, err := decodeFunctionContextResponse(strings.NewReader(`{"success":true,"data":{"id":"ctx-a","running":true}}`))
	if err != nil {
		t.Fatalf("decodeFunctionContextResponse() error = %v", err)
	}
	if out.ID != "ctx-a" || !out.Running || out.Paused {
		t.Fatalf("decoded context = %+v, want running ctx-a", out)
	}
}

func TestDecodeFunctionContextResponseAcceptsRawContextBody(t *testing.T) {
	out, err := decodeFunctionContextResponse(strings.NewReader(`{"id":"ctx-a","paused":true}`))
	if err != nil {
		t.Fatalf("decodeFunctionContextResponse() error = %v", err)
	}
	if out.ID != "ctx-a" || out.Running || !out.Paused {
		t.Fatalf("decoded context = %+v, want paused ctx-a", out)
	}
}
