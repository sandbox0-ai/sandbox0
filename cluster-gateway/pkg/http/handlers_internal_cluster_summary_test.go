package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	clusterconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	clustermiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	schedulerclient "github.com/sandbox0-ai/sandbox0/scheduler/pkg/client"
	"go.uber.org/zap"
)

func TestClusterSummaryProxyExposesSandboxCapacitySignals(t *testing.T) {
	gin.SetMode(gin.TestMode)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	logger := zap.NewNop()
	obsProvider := newTestObservabilityProvider(t, "cluster-summary-test")

	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("manager request method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != "/internal/v1/cluster/summary" {
			t.Errorf("manager request path = %q, want /internal/v1/cluster/summary", r.URL.Path)
		}
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Error("manager request missing internal auth token")
		}
		if err := spec.WriteSuccess(w, http.StatusOK, schedulerclient.ClusterSummary{
			ClusterID:             "cluster-a",
			NodeCount:             2,
			TotalNodeCount:        2,
			SandboxNodeCount:      1,
			IdlePodCount:          1,
			ActivePodCount:        2,
			PendingActivePodCount: 1,
			TotalPodCount:         3,
		}); err != nil {
			t.Errorf("write manager response: %v", err)
		}
	}))
	defer manager.Close()

	proxy2Mgr, err := proxy.NewRouter(manager.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create manager proxy: %v", err)
	}

	clusterValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"scheduler"},
		ClockSkewTolerance: 5 * time.Second,
	})
	clusterServer := &Server{
		cfg: &clusterconfig.ClusterGatewayConfig{
			AuthMode: authModeInternal,
		},
		proxy2Mgr:       proxy2Mgr,
		authMiddleware:  clustermiddleware.NewInternalAuthMiddleware(clusterValidator, logger),
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute}),
		logger:          logger,
	}
	clusterServer.router = gin.New()
	internal := clusterServer.router.Group("/internal/v1")
	internal.Use(clusterServer.authMiddleware.Authenticate())
	internal.GET("/cluster/summary", clusterServer.getClusterSummary)

	clusterGateway := httptest.NewServer(clusterServer.router)
	defer clusterGateway.Close()

	schedulerClient := schedulerclient.NewClusterGatewayClient(
		internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "scheduler", PrivateKey: privateKey, TTL: time.Minute}),
		logger,
		obsProvider,
	)

	summary, err := schedulerClient.GetClusterSummary(context.Background(), clusterGateway.URL)
	if err != nil {
		t.Fatalf("GetClusterSummary() error = %v", err)
	}

	if summary.ClusterID != "cluster-a" {
		t.Fatalf("ClusterID = %q, want %q", summary.ClusterID, "cluster-a")
	}
	if summary.NodeCount != 2 {
		t.Fatalf("NodeCount = %d, want 2", summary.NodeCount)
	}
	if summary.TotalNodeCount != 2 {
		t.Fatalf("TotalNodeCount = %d, want 2", summary.TotalNodeCount)
	}
	if summary.SandboxNodeCount != 1 {
		t.Fatalf("SandboxNodeCount = %d, want 1", summary.SandboxNodeCount)
	}
	if summary.IdlePodCount != 1 {
		t.Fatalf("IdlePodCount = %d, want 1", summary.IdlePodCount)
	}
	if summary.ActivePodCount != 2 {
		t.Fatalf("ActivePodCount = %d, want 2", summary.ActivePodCount)
	}
	if summary.PendingActivePodCount != 1 {
		t.Fatalf("PendingActivePodCount = %d, want 1", summary.PendingActivePodCount)
	}
	if summary.TotalPodCount != 3 {
		t.Fatalf("TotalPodCount = %d, want 3", summary.TotalPodCount)
	}
}

func newTestObservabilityProvider(t *testing.T, serviceName string) *observability.Provider {
	t.Helper()
	provider, err := observability.New(observability.Config{
		ServiceName:    serviceName,
		Logger:         zap.NewNop(),
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
		TraceExporter: observability.TraceExporterConfig{
			Type: "noop",
		},
	})
	if err != nil {
		t.Fatalf("new observability provider: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	return provider
}
