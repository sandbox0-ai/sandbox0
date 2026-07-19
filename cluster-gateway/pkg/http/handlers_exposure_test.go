package http

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	clustermiddleware "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

func TestResolveExposureFromRequestBySignedRegionalForward(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	generator, authMiddleware := newExposureForwardAuth(t)
	token, err := generator.GenerateSystem(
		internalauth.ServiceClusterGateway,
		internalauth.GenerateOptions{},
	)
	if err != nil {
		t.Fatalf("generate regional forward token: %v", err)
	}
	req.Header.Set(forwardedExposureSandboxIDHeader, "sb-demo")
	req.Header.Set(forwardedExposurePortHeader, "3000")
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request = req

	s := &Server{
		cfg:            &config.ClusterGatewayConfig{},
		authMiddleware: authMiddleware,
	}
	sb, port, label, err := s.resolveExposureFromRequest(c)
	if err != nil {
		t.Fatalf("resolveExposureFromRequest: %v", err)
	}
	if sb != "sb-demo" || port != 3000 || label != "" {
		t.Fatalf("unexpected parsed values: %s %d %s", sb, port, label)
	}
	for _, header := range []string{
		forwardedExposureSandboxIDHeader,
		forwardedExposurePortHeader,
		internalauth.DefaultTokenHeader,
	} {
		if got := req.Header.Get(header); got != "" {
			t.Fatalf("%s leaked past trusted-hop resolution: %q", header, got)
		}
	}
}

func TestResolveExposureFromRequestIgnoresUntrustedForwardingHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest(
		http.MethodGet,
		"http://sb-owner--p3000.aws-us-east-1.sandbox0.app/",
		nil,
	)
	req.Header.Set(forwardedExposureSandboxIDHeader, "sb-victim")
	req.Header.Set(forwardedExposurePortHeader, "4000")
	req.Header.Set(internalauth.DefaultTokenHeader, "forged")
	c.Request = req

	s := &Server{
		cfg: &config.ClusterGatewayConfig{
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
	if sb != "sb-owner" || port != 3000 || label != "sb-owner--p3000" {
		t.Fatalf("unexpected parsed values: %s %d %s", sb, port, label)
	}
	for _, header := range []string{
		forwardedExposureSandboxIDHeader,
		forwardedExposurePortHeader,
		internalauth.DefaultTokenHeader,
	} {
		if got := req.Header.Get(header); got != "" {
			t.Fatalf("%s leaked past untrusted-hop resolution: %q", header, got)
		}
	}
}

func TestResolveExposureFromRequestIgnoresSignedNonRegionalForward(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	generator, authMiddleware := newExposureForwardAuthForCaller(
		t,
		internalauth.ServiceScheduler,
		[]string{internalauth.ServiceRegionalGateway, internalauth.ServiceScheduler},
	)
	token, err := generator.GenerateSystem(
		internalauth.ServiceClusterGateway,
		internalauth.GenerateOptions{},
	)
	if err != nil {
		t.Fatalf("generate scheduler token: %v", err)
	}
	req := httptest.NewRequest(
		http.MethodGet,
		"http://sb-owner--p3000.aws-us-east-1.sandbox0.app/",
		nil,
	)
	req.Header.Set(forwardedExposureSandboxIDHeader, "sb-victim")
	req.Header.Set(forwardedExposurePortHeader, "4000")
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request = req

	s := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicRootDomain: "sandbox0.app",
				PublicRegionID:   "aws-us-east-1",
			},
		},
		authMiddleware: authMiddleware,
	}
	sb, port, label, err := s.resolveExposureFromRequest(c)
	if err != nil {
		t.Fatalf("resolveExposureFromRequest: %v", err)
	}
	if sb != "sb-owner" || port != 3000 || label != "sb-owner--p3000" {
		t.Fatalf("unexpected parsed values: %s %d %s", sb, port, label)
	}
}

func TestResolveExposureFromRequestIgnoresTeamScopedRegionalForward(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	generator, authMiddleware := newExposureForwardAuth(t)
	token, err := generator.Generate(
		internalauth.ServiceClusterGateway,
		"team-victim",
		"user-victim",
		internalauth.GenerateOptions{},
	)
	if err != nil {
		t.Fatalf("generate team-scoped regional token: %v", err)
	}
	req := httptest.NewRequest(
		http.MethodGet,
		"http://sb-owner--p3000.aws-us-east-1.sandbox0.app/",
		nil,
	)
	req.Header.Set(forwardedExposureSandboxIDHeader, "sb-victim")
	req.Header.Set(forwardedExposurePortHeader, "4000")
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request = req

	s := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicRootDomain: "sandbox0.app",
				PublicRegionID:   "aws-us-east-1",
			},
		},
		authMiddleware: authMiddleware,
	}
	sb, port, label, err := s.resolveExposureFromRequest(c)
	if err != nil {
		t.Fatalf("resolveExposureFromRequest: %v", err)
	}
	if sb != "sb-owner" || port != 3000 || label != "sb-owner--p3000" {
		t.Fatalf("unexpected parsed values: %s %d %s", sb, port, label)
	}
}

func TestResolveExposureFromRequestByHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("GET", "http://sb-demo--p3000.aws-us-east-1.sandbox0.app/", nil)
	c.Request = req

	s := &Server{
		cfg: &config.ClusterGatewayConfig{
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

func TestPublicExposureSpoofCannotChargeVictimTeam(t *testing.T) {
	upstreamHeaders := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHeaders <- r.Header.Clone()
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	port := serverPort(t, upstream.URL)

	var managerMu sync.Mutex
	var managerSandboxIDs []string
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sandboxID, teamID string
		switch r.URL.Path {
		case "/internal/v1/sandboxes/sb-owner":
			sandboxID, teamID = "sb-owner", "team-owner"
		case "/internal/v1/sandboxes/sb-victim":
			sandboxID, teamID = "sb-victim", "team-victim"
		default:
			spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
		managerMu.Lock()
		managerSandboxIDs = append(managerSandboxIDs, sandboxID)
		managerMu.Unlock()
		_ = spec.WriteSuccess(w, http.StatusOK, &mgr.Sandbox{
			ID:           sandboxID,
			TeamID:       teamID,
			InternalAddr: "http://127.0.0.1:1",
			Services: []mgr.SandboxAppService{{
				ID:   "api",
				Port: port,
				Ingress: mgr.SandboxAppServiceIngress{
					Public: true,
					Routes: []mgr.SandboxAppServiceRoute{{
						ID:         "root",
						PathPrefix: "/",
						Methods:    []string{http.MethodGet},
					}},
				},
			}},
		})
	}))
	defer manager.Close()

	rateLimiter := &recordingExposureRateLimiter{}
	controller := gatewayteamquota.NewController(
		nil,
		nil,
		rateLimiter,
		nil,
		zap.NewNop(),
		gatewayteamquota.WithConcurrencyLimiter(allowingTeamQuotaConcurrencyLimiter{}),
		gatewayteamquota.WithNetworkLimiter(allowingTeamQuotaNetworkLimiter{}),
	)
	gateway := newSandboxServiceExposureTestServerWithManagerURLAndQuota(
		t,
		manager.URL,
		controller,
	)
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	request := newGatewayRequest(
		t,
		http.MethodGet,
		gatewayServer.URL,
		fmt.Sprintf("sb-owner--p%d.aws-us-east-1.sandbox0.app", port),
		"/",
	)
	request.Header.Set(forwardedExposureSandboxIDHeader, "sb-victim")
	request.Header.Set(forwardedExposurePortHeader, fmt.Sprintf("%d", port))
	request.Header.Set(internalauth.DefaultTokenHeader, "forged")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusOK)
	}

	managerMu.Lock()
	gotManagerSandboxIDs := append([]string(nil), managerSandboxIDs...)
	managerMu.Unlock()
	if len(gotManagerSandboxIDs) != 1 || gotManagerSandboxIDs[0] != "sb-owner" {
		t.Fatalf("manager sandbox lookups = %v, want [sb-owner]", gotManagerSandboxIDs)
	}
	if gotTeams := rateLimiter.Teams(); len(gotTeams) != 1 || gotTeams[0] != "team-owner" {
		t.Fatalf("quota teams = %v, want [team-owner]", gotTeams)
	}
	headers := <-upstreamHeaders
	for _, header := range []string{
		forwardedExposureSandboxIDHeader,
		forwardedExposurePortHeader,
		internalauth.DefaultTokenHeader,
	} {
		if got := headers.Get(header); got != "" {
			t.Fatalf("upstream received hop-by-hop header %s=%q", header, got)
		}
	}
}

type recordingExposureRateLimiter struct {
	mu    sync.Mutex
	teams []string
}

func (l *recordingExposureRateLimiter) Take(
	_ context.Context,
	teamID string,
	_ coreteamquota.Key,
	_ int64,
) (tokenbucket.Decision, error) {
	l.mu.Lock()
	l.teams = append(l.teams, teamID)
	l.mu.Unlock()
	return tokenbucket.Decision{Allowed: true, Remaining: 100}, nil
}

func (*recordingExposureRateLimiter) Invalidate(string, coreteamquota.Key) {}

func (l *recordingExposureRateLimiter) Teams() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.teams...)
}

func newExposureForwardAuth(
	t *testing.T,
) (*internalauth.Generator, *clustermiddleware.InternalAuthMiddleware) {
	return newExposureForwardAuthForCaller(
		t,
		internalauth.ServiceRegionalGateway,
		[]string{internalauth.ServiceRegionalGateway},
	)
}

func newExposureForwardAuthForCaller(
	t *testing.T,
	caller string,
	allowedCallers []string,
) (*internalauth.Generator, *clustermiddleware.InternalAuthMiddleware) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate internal auth key: %v", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     caller,
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:         internalauth.ServiceClusterGateway,
		PublicKey:      publicKey,
		AllowedCallers: allowedCallers,
	})
	return generator, clustermiddleware.NewInternalAuthMiddleware(validator, zap.NewNop())
}
