package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templreconciler "github.com/sandbox0-ai/sandbox0/pkg/template/reconciler"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSelectClusterForTemplatePrefersIdleCapacity(t *testing.T) {
	tpl := newRoutingTemplate("tmpl-a")
	clusterTemplateID := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
	server := newRoutingTestServer(
		tpl,
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 3, 5),
			newRoutingAllocation("cluster-b", 3, 5),
		},
		[]*template.Cluster{
			newRoutingCluster("cluster-a", 1),
			newRoutingCluster("cluster-b", 10),
		},
		&fakeRoutingReconciler{
			templateIdle: map[string]map[string]int32{
				"cluster-a": {clusterTemplateID: 2},
				"cluster-b": {clusterTemplateID: 1},
			},
			templateStatsAge: map[string]time.Duration{
				"cluster-a": time.Second,
				"cluster-b": time.Second,
			},
			clusterSummaries: map[string]*templreconciler.ClusterSummary{
				"cluster-a": {SandboxNodeCount: 1, TotalNodeCount: 1, TotalPodCount: 5},
				"cluster-b": {SandboxNodeCount: 10, TotalNodeCount: 10, TotalPodCount: 0},
			},
			clusterSummaryAge: map[string]time.Duration{
				"cluster-a": time.Second,
				"cluster-b": time.Second,
			},
		},
	)

	selected, _, selectedBy, err := server.selectClusterForTemplate(newRoutingContext(), "tmpl-a", "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected == nil || selected.ClusterID != "cluster-a" {
		t.Fatalf("selected cluster = %v, want cluster-a", clusterID(selected))
	}
	if selectedBy != "idle" {
		t.Fatalf("selectedBy = %q, want %q", selectedBy, "idle")
	}
}

func TestSelectClusterForTemplatePrefersHeadroomWhenIdleUnavailable(t *testing.T) {
	tpl := newRoutingTemplate("tmpl-a")
	server := newRoutingTestServer(
		tpl,
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 2, 4),
			newRoutingAllocation("cluster-b", 2, 4),
		},
		[]*template.Cluster{
			newRoutingCluster("cluster-a", 1),
			newRoutingCluster("cluster-b", 1),
		},
		&fakeRoutingReconciler{
			templateStatsAge: map[string]time.Duration{
				"cluster-a": time.Second,
				"cluster-b": time.Second,
			},
			clusterSummaries: map[string]*templreconciler.ClusterSummary{
				"cluster-a": {SandboxNodeCount: 2, TotalNodeCount: 4, TotalPodCount: 18},
				"cluster-b": {SandboxNodeCount: 3, TotalNodeCount: 4, TotalPodCount: 12},
			},
			clusterSummaryAge: map[string]time.Duration{
				"cluster-a": time.Second,
				"cluster-b": time.Second,
			},
		},
	)

	selected, _, selectedBy, err := server.selectClusterForTemplate(newRoutingContext(), "tmpl-a", "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected == nil || selected.ClusterID != "cluster-b" {
		t.Fatalf("selected cluster = %v, want cluster-b", clusterID(selected))
	}
	if selectedBy != "headroom" {
		t.Fatalf("selectedBy = %q, want %q", selectedBy, "headroom")
	}
}

func TestSelectClusterForTemplateUsesHeadroomWhenIdleStatsAreStale(t *testing.T) {
	tpl := newRoutingTemplate("tmpl-a")
	clusterTemplateID := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
	server := newRoutingTestServer(
		tpl,
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 2, 4),
			newRoutingAllocation("cluster-b", 2, 4),
		},
		[]*template.Cluster{
			newRoutingCluster("cluster-a", 5),
			newRoutingCluster("cluster-b", 1),
		},
		&fakeRoutingReconciler{
			templateIdle: map[string]map[string]int32{
				"cluster-a": {clusterTemplateID: 5},
			},
			templateStatsAge: map[string]time.Duration{
				"cluster-a": 3 * time.Second,
			},
			clusterSummaries: map[string]*templreconciler.ClusterSummary{
				"cluster-a": {SandboxNodeCount: 1, TotalNodeCount: 2, TotalPodCount: 9},
				"cluster-b": {SandboxNodeCount: 3, TotalNodeCount: 3, TotalPodCount: 10},
			},
			clusterSummaryAge: map[string]time.Duration{
				"cluster-a": time.Second,
				"cluster-b": time.Second,
			},
		},
	)

	selected, _, selectedBy, err := server.selectClusterForTemplate(newRoutingContext(), "tmpl-a", "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected == nil || selected.ClusterID != "cluster-b" {
		t.Fatalf("selected cluster = %v, want cluster-b", clusterID(selected))
	}
	if selectedBy != "headroom" {
		t.Fatalf("selectedBy = %q, want %q", selectedBy, "headroom")
	}
}

func TestSelectClusterForTemplateFallsBackToWeight(t *testing.T) {
	tpl := newRoutingTemplate("tmpl-a")
	server := newRoutingTestServer(
		tpl,
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 2, 4),
			newRoutingAllocation("cluster-b", 2, 4),
		},
		[]*template.Cluster{
			newRoutingCluster("cluster-a", 0),
			newRoutingCluster("cluster-b", 5),
		},
		&fakeRoutingReconciler{},
	)

	selected, _, selectedBy, err := server.selectClusterForTemplate(newRoutingContext(), "tmpl-a", "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected == nil || selected.ClusterID != "cluster-b" {
		t.Fatalf("selected cluster = %v, want cluster-b", clusterID(selected))
	}
	if selectedBy != "weight" {
		t.Fatalf("selectedBy = %q, want %q", selectedBy, "weight")
	}
}

func TestSelectClusterForTemplateFallsBackWhenWeightsAreUnavailable(t *testing.T) {
	tpl := newRoutingTemplate("tmpl-a")
	server := newRoutingTestServer(
		tpl,
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 2, 4),
			newRoutingAllocation("cluster-b", 2, 9),
		},
		[]*template.Cluster{
			newRoutingCluster("cluster-a", 0),
			newRoutingCluster("cluster-b", 0),
		},
		&fakeRoutingReconciler{},
	)

	selected, _, selectedBy, err := server.selectClusterForTemplate(newRoutingContext(), "tmpl-a", "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected == nil || selected.ClusterID != "cluster-b" {
		t.Fatalf("selected cluster = %v, want cluster-b", clusterID(selected))
	}
	if selectedBy != "fallback" {
		t.Fatalf("selectedBy = %q, want %q", selectedBy, "fallback")
	}
}

func TestSelectClusterForTemplateRecordsRoutingMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewScheduler(registry)

	tpl := newRoutingTemplate("tmpl-a")
	server := newRoutingTestServerWithMetrics(
		tpl,
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 2, 4),
			newRoutingAllocation("cluster-b", 2, 4),
		},
		[]*template.Cluster{
			newRoutingCluster("cluster-a", 1),
			newRoutingCluster("cluster-b", 1),
		},
		&fakeRoutingReconciler{
			clusterSummaries: map[string]*templreconciler.ClusterSummary{
				"cluster-a": {SandboxNodeCount: 2, TotalNodeCount: 2, TotalPodCount: 10},
				"cluster-b": {SandboxNodeCount: 3, TotalNodeCount: 3, TotalPodCount: 10},
			},
			clusterSummaryAge: map[string]time.Duration{
				"cluster-a": 2 * time.Second,
				"cluster-b": time.Second,
			},
		},
		metrics,
	)

	selected, _, selectedBy, err := server.selectClusterForTemplate(newRoutingContext(), "tmpl-a", "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected == nil || selected.ClusterID != "cluster-b" || selectedBy != "headroom" {
		t.Fatalf("selection = (%v,%q), want (cluster-b,headroom)", clusterID(selected), selectedBy)
	}

	if got := testutil.ToFloat64(metrics.RoutingDecisions.WithLabelValues("cluster-b", "headroom")); got != 1 {
		t.Fatalf("routing decision metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ClusterSummaryAge.WithLabelValues("cluster-a")); got != 2 {
		t.Fatalf("cluster-a summary age = %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.ClusterSummaryAge.WithLabelValues("cluster-b")); got != 1 {
		t.Fatalf("cluster-b summary age = %v, want 1", got)
	}
}

func TestSelectClusterForTemplateRecordsUnavailableRoutingMetric(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewScheduler(registry)

	server := newRoutingTestServerWithMetrics(
		newRoutingTemplate("tmpl-a"),
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 1, 1),
		},
		[]*template.Cluster{
			{ClusterID: "cluster-a", Enabled: false},
		},
		&fakeRoutingReconciler{},
		metrics,
	)

	selected, _, selectedBy, err := server.selectClusterForTemplate(newRoutingContext(), "tmpl-a", "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected != nil || selectedBy != "" {
		t.Fatalf("selection = (%v,%q), want (nil,\"\")", clusterID(selected), selectedBy)
	}
	if got := testutil.ToFloat64(metrics.RoutingDecisions.WithLabelValues("none", "unavailable")); got != 1 {
		t.Fatalf("unavailable routing metric = %v, want 1", got)
	}
}

func TestCreateSandboxRoutesRequestByHeadroom(t *testing.T) {
	gin.SetMode(gin.TestMode)
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewScheduler(registry)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	receivedA := 0
	clusterA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedA++
		w.Header().Set("Content-Type", "application/json")
		if got := r.Header.Get("X-Team-ID"); got != "team-a" {
			t.Fatalf("cluster-a X-Team-ID = %q, want team-a", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"sandbox_id":"sb-a"}}`))
	}))
	defer clusterA.Close()

	receivedB := 0
	clusterB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedB++
		w.Header().Set("Content-Type", "application/json")
		if got := r.Header.Get("X-Team-ID"); got != "team-a" {
			t.Fatalf("cluster-b X-Team-ID = %q, want team-a", got)
		}
		if got := r.Header.Get("X-Internal-Token"); got == "" {
			t.Fatal("expected internal token header")
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"sandbox_id":"sb-b"}}`))
	}))
	defer clusterB.Close()

	server := newRoutingTestServerWithMetrics(
		newRoutingTemplate("tmpl-a"),
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 2, 4),
			newRoutingAllocation("cluster-b", 2, 4),
		},
		[]*template.Cluster{
			{ClusterID: "cluster-a", ClusterGatewayURL: clusterA.URL, Enabled: true, Weight: 1},
			{ClusterID: "cluster-b", ClusterGatewayURL: clusterB.URL, Enabled: true, Weight: 1},
		},
		&fakeRoutingReconciler{
			clusterSummaries: map[string]*templreconciler.ClusterSummary{
				"cluster-a": {SandboxNodeCount: 1, TotalNodeCount: 1, TotalPodCount: 8},
				"cluster-b": {SandboxNodeCount: 3, TotalNodeCount: 3, TotalPodCount: 15},
			},
			clusterSummaryAge: map[string]time.Duration{
				"cluster-a": time.Second,
				"cluster-b": time.Second,
			},
		},
		metrics,
	)
	server.clusterGatewayProxies = make(map[string]*proxy.Router)
	server.authValidator = internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "scheduler",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"regional-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	server.internalAuthGen = internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "scheduler",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})

	router := gin.New()
	v1 := router.Group("/api/v1")
	v1.Use(server.authMiddleware())
	v1.POST("/sandboxes", server.createSandbox)
	httpServer := httptest.NewServer(router)
	defer httpServer.Close()

	requestBody, err := json.Marshal(map[string]any{"template": "tmpl-a"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/sandboxes", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", mustGenerateSchedulerTestToken(t, privateKey, "regional-gateway", "scheduler", "team-a", "user-a"))
	resp, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if receivedA != 0 {
		t.Fatalf("cluster-a requests = %d, want 0", receivedA)
	}
	if receivedB != 1 {
		t.Fatalf("cluster-b requests = %d, want 1", receivedB)
	}
	if got := testutil.ToFloat64(metrics.RoutingDecisions.WithLabelValues("cluster-b", "headroom")); got != 1 {
		t.Fatalf("routing metric = %v, want 1", got)
	}
}

func TestCreateSandboxFallsBackWhenNoFreshSignalsExist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	receivedA := 0
	clusterA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedA++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":{"sandbox_id":"sb-a"}}`))
	}))
	defer clusterA.Close()

	receivedB := 0
	clusterB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedB++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":{"sandbox_id":"sb-b"}}`))
	}))
	defer clusterB.Close()

	server := newRoutingTestServerWithMetrics(
		newRoutingTemplate("tmpl-a"),
		[]*template.TemplateAllocation{
			newRoutingAllocation("cluster-a", 2, 4),
			newRoutingAllocation("cluster-b", 2, 9),
		},
		[]*template.Cluster{
			{ClusterID: "cluster-a", ClusterGatewayURL: clusterA.URL, Enabled: true, Weight: 0},
			{ClusterID: "cluster-b", ClusterGatewayURL: clusterB.URL, Enabled: true, Weight: 0},
		},
		&fakeRoutingReconciler{},
		nil,
	)
	server.clusterGatewayProxies = make(map[string]*proxy.Router)
	server.authValidator = internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "scheduler",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"regional-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	server.internalAuthGen = internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "scheduler",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})

	router := gin.New()
	v1 := router.Group("/api/v1")
	v1.Use(server.authMiddleware())
	v1.POST("/sandboxes", server.createSandbox)
	httpServer := httptest.NewServer(router)
	defer httpServer.Close()

	requestBody, err := json.Marshal(map[string]any{"template": "tmpl-a"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/sandboxes", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", mustGenerateSchedulerTestToken(t, privateKey, "regional-gateway", "scheduler", "team-a", "user-a"))
	resp, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if receivedA != 0 {
		t.Fatalf("cluster-a requests = %d, want 0", receivedA)
	}
	if receivedB != 1 {
		t.Fatalf("cluster-b requests = %d, want 1", receivedB)
	}
}

type fakeRoutingTemplateStore struct {
	template *template.Template
}

func (s *fakeRoutingTemplateStore) CreateTemplate(ctx context.Context, tpl *template.Template) error {
	return errors.New("not implemented")
}

func (s *fakeRoutingTemplateStore) GetTemplate(ctx context.Context, scope, teamID, templateID string) (*template.Template, error) {
	return s.template, nil
}

func (s *fakeRoutingTemplateStore) GetTemplateForTeam(ctx context.Context, teamID, templateID string) (*template.Template, error) {
	if s.template == nil || s.template.TemplateID != templateID {
		return nil, nil
	}
	return s.template, nil
}

func (s *fakeRoutingTemplateStore) ListTemplates(ctx context.Context) ([]*template.Template, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeRoutingTemplateStore) ListVisibleTemplates(ctx context.Context, teamID string) ([]*template.Template, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeRoutingTemplateStore) UpdateTemplate(ctx context.Context, tpl *template.Template) error {
	return errors.New("not implemented")
}

func (s *fakeRoutingTemplateStore) DeleteTemplate(ctx context.Context, scope, teamID, templateID string) error {
	return errors.New("not implemented")
}

type fakeRoutingAllocationStore struct {
	allocations []*template.TemplateAllocation
}

func (s *fakeRoutingAllocationStore) UpsertAllocation(ctx context.Context, alloc *template.TemplateAllocation) error {
	return errors.New("not implemented")
}

func (s *fakeRoutingAllocationStore) ListAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) ([]*template.TemplateAllocation, error) {
	return s.allocations, nil
}

func (s *fakeRoutingAllocationStore) UpdateAllocationSyncStatus(ctx context.Context, scope, teamID, templateID, clusterID, status string, syncError *string) error {
	return errors.New("not implemented")
}

func (s *fakeRoutingAllocationStore) DeleteAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) error {
	return errors.New("not implemented")
}

type fakeRoutingClusterRepo struct {
	clusters []*template.Cluster
}

func (r *fakeRoutingClusterRepo) Ping(ctx context.Context) error { return nil }
func (r *fakeRoutingClusterRepo) CreateCluster(ctx context.Context, cluster *template.Cluster) error {
	return errors.New("not implemented")
}
func (r *fakeRoutingClusterRepo) GetCluster(ctx context.Context, clusterID string) (*template.Cluster, error) {
	for _, cluster := range r.clusters {
		if cluster.ClusterID == clusterID {
			return cluster, nil
		}
	}
	return nil, nil
}
func (r *fakeRoutingClusterRepo) ListClusters(ctx context.Context) ([]*template.Cluster, error) {
	return r.clusters, nil
}
func (r *fakeRoutingClusterRepo) ListEnabledClusters(ctx context.Context) ([]*template.Cluster, error) {
	var enabled []*template.Cluster
	for _, cluster := range r.clusters {
		if cluster.Enabled {
			enabled = append(enabled, cluster)
		}
	}
	return enabled, nil
}
func (r *fakeRoutingClusterRepo) UpdateCluster(ctx context.Context, cluster *template.Cluster) error {
	return errors.New("not implemented")
}
func (r *fakeRoutingClusterRepo) UpdateClusterLastSeen(ctx context.Context, clusterID string) error {
	return errors.New("not implemented")
}
func (r *fakeRoutingClusterRepo) DeleteCluster(ctx context.Context, clusterID string) error {
	return errors.New("not implemented")
}

type fakeRoutingReconciler struct {
	templateIdle      map[string]map[string]int32
	templateStatsAge  map[string]time.Duration
	clusterSummaries  map[string]*templreconciler.ClusterSummary
	clusterSummaryAge map[string]time.Duration
}

func (r *fakeRoutingReconciler) TriggerReconcile(ctx context.Context) {}

func (r *fakeRoutingReconciler) GetTemplateIdleCount(clusterID, templateID string) (int32, bool) {
	if r.templateIdle == nil {
		return 0, false
	}
	byTemplate, ok := r.templateIdle[clusterID]
	if !ok {
		return 0, false
	}
	idle, ok := byTemplate[templateID]
	return idle, ok
}

func (r *fakeRoutingReconciler) GetTemplateStatsAge(clusterID string) (time.Duration, bool) {
	age, ok := r.templateStatsAge[clusterID]
	return age, ok
}

func (r *fakeRoutingReconciler) GetClusterSummary(clusterID string) (*templreconciler.ClusterSummary, bool) {
	summary, ok := r.clusterSummaries[clusterID]
	return summary, ok
}

func (r *fakeRoutingReconciler) GetClusterSummaryAge(clusterID string) (time.Duration, bool) {
	age, ok := r.clusterSummaryAge[clusterID]
	return age, ok
}

func newRoutingTestServer(tpl *template.Template, allocations []*template.TemplateAllocation, clusters []*template.Cluster, reconciler *fakeRoutingReconciler) *Server {
	return newRoutingTestServerWithMetrics(tpl, allocations, clusters, reconciler, nil)
}

func newRoutingTestServerWithMetrics(tpl *template.Template, allocations []*template.TemplateAllocation, clusters []*template.Cluster, reconciler *fakeRoutingReconciler, metrics *obsmetrics.SchedulerMetrics) *Server {
	return &Server{
		cfg: &config.SchedulerConfig{
			ReconcileInterval: metav1.Duration{Duration: time.Second},
			PodsPerNode:       10,
		},
		repo:            &fakeRoutingClusterRepo{clusters: clusters},
		templateStore:   &fakeRoutingTemplateStore{template: tpl},
		allocationStore: &fakeRoutingAllocationStore{allocations: allocations},
		reconciler:      reconciler,
		logger:          zap.NewNop(),
		metrics:         metrics,
	}
}

func newRoutingContext() *gin.Context {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/api/v1/sandboxes", nil)
	return ctx
}

func newRoutingTemplate(templateID string) *template.Template {
	return &template.Template{
		TemplateID: templateID,
		Scope:      naming.ScopePublic,
	}
}

func newRoutingAllocation(clusterID string, minIdle, maxIdle int32) *template.TemplateAllocation {
	return &template.TemplateAllocation{
		TemplateID: "tmpl-a",
		ClusterID:  clusterID,
		MinIdle:    minIdle,
		MaxIdle:    maxIdle,
	}
}

func newRoutingCluster(clusterID string, weight int) *template.Cluster {
	return &template.Cluster{
		ClusterID: clusterID,
		Weight:    weight,
		Enabled:   true,
	}
}

func clusterID(cluster *template.Cluster) string {
	if cluster == nil {
		return "<nil>"
	}
	return cluster.ClusterID
}

func mustGenerateSchedulerTestToken(t *testing.T, privateKey ed25519.PrivateKey, caller, target, teamID, userID string) string {
	t.Helper()
	gen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     caller,
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	token, err := gen.Generate(target, teamID, userID, internalauth.GenerateOptions{})
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}
