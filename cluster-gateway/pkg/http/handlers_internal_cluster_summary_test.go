package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	clustermiddleware "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	clusterconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	managerv1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	managercontroller "github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	managerhttp "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	schedulerclient "github.com/sandbox0-ai/sandbox0/scheduler/pkg/client"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestClusterSummaryProxyExposesSandboxCapacitySignals(t *testing.T) {
	gin.SetMode(gin.TestMode)

	configPath := writeClusterSummaryManagerConfig(t, `
default_cluster_id: cluster-a
sandbox_pod_placement:
  node_selector:
    sandbox0.ai/node-role: sandbox
`)
	t.Setenv("CONFIG_PATH", configPath)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	logger := zap.NewNop()
	obsProvider := newTestObservabilityProvider(t, "cluster-summary-test")

	clusterService := service.NewClusterService(
		nil,
		newClusterSummaryTestPodLister(t,
			newClusterSummaryTestPod("ns-a", "idle-running", "template-a", "idle", corev1.PodRunning),
			newClusterSummaryTestPod("ns-a", "active-running", "template-a", "active", corev1.PodRunning),
			newClusterSummaryTestPod("ns-a", "active-pending", "template-a", "active", corev1.PodPending),
		),
		newClusterSummaryTestNodeLister(t,
			newClusterSummaryTestNode("node-sandbox", map[string]string{"sandbox0.ai/node-role": "sandbox"}),
			newClusterSummaryTestNode("node-system", map[string]string{"sandbox0.ai/node-role": "system"}),
		),
		staticClusterSummaryTemplateLister{},
		logger,
	)

	managerValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "manager",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"cluster-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	managerServer := managerhttp.NewServer(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		clusterService,
		managerValidator,
		logger,
		0,
		obsProvider,
		"",
		"",
	)
	manager := httptest.NewServer(managerServer.Handler())
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

func writeClusterSummaryManagerConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func newClusterSummaryTestPodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		if err := indexer.Add(pod); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}
	return corelisters.NewPodLister(indexer)
}

func newClusterSummaryTestNodeLister(t *testing.T, nodes ...*corev1.Node) corelisters.NodeLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if err := indexer.Add(node); err != nil {
			t.Fatalf("add node: %v", err)
		}
	}
	return corelisters.NewNodeLister(indexer)
}

func newClusterSummaryTestPod(namespace, name, templateID, poolType string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				"sandbox0.ai/template-id": templateID,
				"sandbox0.ai/pool-type":   poolType,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func newClusterSummaryTestNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

type staticClusterSummaryTemplateLister struct{}

func (staticClusterSummaryTemplateLister) List() ([]*managerv1alpha1.SandboxTemplate, error) {
	return nil, nil
}

func (staticClusterSummaryTemplateLister) Get(namespace, name string) (*managerv1alpha1.SandboxTemplate, error) {
	return nil, nil
}

var _ managercontroller.TemplateLister = staticClusterSummaryTemplateLister{}
