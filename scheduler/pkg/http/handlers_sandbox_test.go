package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatehttp "github.com/sandbox0-ai/sandbox0/pkg/template/http"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/db"
	"go.uber.org/zap"
)

func TestSetupRoutesMountsTemplateFromSandboxEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := &Server{router: gin.New(), templateHandler: &templatehttp.Handler{}}
	server.setupRoutes()
	for _, route := range server.router.Routes() {
		if route.Method == http.MethodPost && route.Path == "/api/v1/templates/from-sandbox" {
			return
		}
	}
	t.Fatal("expected POST /api/v1/templates/from-sandbox route")
}

func TestSelectBestClusterUsesDeterministicResourceOrder(t *testing.T) {
	capacities := []*db.ClusterCapacity{
		capacity("cluster-weight", 20, 4, 4, 16<<30, 4000),
		capacity("cluster-slots", 1, 4, 5, 8<<30, 2000),
		capacity("cluster-claims", 1, 5, 1, 4<<30, 1000),
	}
	if got := selectBestCluster(capacities); got == nil || got.Cluster.ClusterID != "cluster-claims" {
		t.Fatalf("selected = %#v, want cluster-claims", got)
	}

	capacities = []*db.ClusterCapacity{
		capacity("cluster-b", 10, 2, 2, 8<<30, 2000),
		capacity("cluster-a", 10, 2, 2, 8<<30, 2000),
	}
	if got := selectBestCluster(capacities); got == nil || got.Cluster.ClusterID != "cluster-a" {
		t.Fatalf("tie selected = %#v, want lexical cluster-a", got)
	}
}

func TestSelectBestClusterRejectsUnclaimableRows(t *testing.T) {
	disabled := capacity("disabled", 100, 10, 10, 16<<30, 4000)
	disabled.Cluster.Enabled = false
	if got := selectBestCluster([]*db.ClusterCapacity{
		nil,
		disabled,
		capacity("no-claim-capacity", 1, 0, 10, 16<<30, 4000),
		capacity("no-ready-slots", 1, 10, 0, 16<<30, 4000),
	}); got != nil {
		t.Fatalf("selected = %#v, want nil", got)
	}
}

func TestSelectClusterForTemplateUsesExactMemoryOverride(t *testing.T) {
	tpl := routingTemplate("tmpl-a")
	memory := "2Gi"
	repo := &fakeCapacityRepository{capacities: []*db.ClusterCapacity{
		capacity("cluster-a", 100, 8, 8, 32<<30, 8000),
	}}
	server := routingServer(tpl, repo)
	req := &apispec.ClaimRequest{
		Template: "tmpl-a",
		Config: &apispec.SandboxConfig{
			Resources: &apispec.SandboxResourceConfig{Memory: &memory},
		},
	}

	selected, loaded, selectedBy, err := server.selectClusterForTemplate(routingContext(), req, "team-a")
	if err != nil {
		t.Fatalf("selectClusterForTemplate() error = %v", err)
	}
	if selected == nil || selected.ClusterID != "cluster-a" || loaded != tpl || selectedBy != "resource_capacity" {
		t.Fatalf("selection = cluster %#v template %#v by %q", selected, loaded, selectedBy)
	}
	if repo.cpuMillicores != 500 || repo.memoryBytes != 2<<30 {
		t.Fatalf("capacity query = cpu %dm memory %d, want 500m/2Gi", repo.cpuMillicores, repo.memoryBytes)
	}
}

func TestSelectClusterForTemplateRejectsInvalidOverrideBeforeCapacityQuery(t *testing.T) {
	tpl := routingTemplate("tmpl-a")
	empty := ""
	repo := &fakeCapacityRepository{}
	server := routingServer(tpl, repo)
	_, _, _, err := server.selectClusterForTemplate(routingContext(), &apispec.ClaimRequest{
		Template: "tmpl-a",
		Config: &apispec.SandboxConfig{
			Resources: &apispec.SandboxResourceConfig{Memory: &empty},
		},
	}, "team-a")
	if err == nil || repo.queries != 0 {
		t.Fatalf("error = %v, capacity queries = %d", err, repo.queries)
	}
}

func TestCreateSandboxRejectsCreatingTemplateBeforeCapacitySelection(t *testing.T) {
	tpl := routingTemplate("derived")
	tpl.Status = &sandboxspec.TemplateStatus{Creation: &sandboxspec.TemplateCreationStatus{
		State: sandboxspec.TemplateCreationStateCreating,
		Stage: sandboxspec.TemplateCreationStagePublishing,
	}}
	repo := &fakeCapacityRepository{}
	server := routingServer(tpl, repo)
	router := authenticatedClaimRouter(server)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes",
		bytes.NewBufferString(`{"template":"derived"}`),
	))
	if recorder.Code != http.StatusConflict || recorder.Header().Get("Retry-After") != "1" {
		t.Fatalf("status = %d retry-after = %q body = %s", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
	}
	if repo.queries != 0 {
		t.Fatalf("capacity queries = %d, want 0", repo.queries)
	}
}

func TestCreateSandboxRoutesUnmodifiedBodyToCapacitySelectedCluster(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var upstreamBody []byte
	var upstreamToken string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamBody, _ = io.ReadAll(r.Body)
		upstreamToken = r.Header.Get(internalauth.DefaultTokenHeader)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"success":true,"data":{"sandbox_id":"s0-cluster-a-tmpl-a-op","runtime_id":"alloc","status":"running","template":"tmpl-a","cluster_id":"cluster-a"}}`))
	}))
	defer upstream.Close()

	tpl := routingTemplate("tmpl-a")
	repo := &fakeCapacityRepository{capacities: []*db.ClusterCapacity{
		capacityWithURL("cluster-a", upstream.URL),
	}}
	server := routingServer(tpl, repo)
	server.internalAuthGen = internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: "scheduler", PrivateKey: privateKey, TTL: time.Minute,
	})
	server.httpClient = upstream.Client()
	server.clusterGatewayProxies = make(map[string]*proxy.Router)

	body := []byte(`{"template":"tmpl-a","config":{"resources":{"memory":"2Gi"}},"snapshot_id":"snap-a"}`)
	recorder := httptest.NewRecorder()
	authenticatedClaimRouter(server).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", bytes.NewReader(body)),
	)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", recorder.Code, recorder.Body.String())
	}
	if !bytes.Equal(upstreamBody, body) {
		t.Fatalf("upstream body = %s, want %s", upstreamBody, body)
	}
	if upstreamToken == "" {
		t.Fatal("upstream internal token is empty")
	}
	if repo.cpuMillicores != 500 || repo.memoryBytes != 2<<30 {
		t.Fatalf("capacity query = %dm/%d", repo.cpuMillicores, repo.memoryBytes)
	}
}

func TestCreateSandboxReturnsUnavailableWithoutClaimableCapacity(t *testing.T) {
	server := routingServer(routingTemplate("tmpl-a"), &fakeCapacityRepository{})
	recorder := httptest.NewRecorder()
	authenticatedClaimRouter(server).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", bytes.NewBufferString(`{"template":"tmpl-a"}`)),
	)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
	}
}

type fakeTemplateStore struct {
	templatestore.TemplateStore
	template *template.Template
}

func (s *fakeTemplateStore) GetTemplateForTeam(_ context.Context, _ string, templateID string) (*template.Template, error) {
	if s.template == nil || s.template.TemplateID != templateID {
		return nil, nil
	}
	return s.template, nil
}

type fakeCapacityRepository struct {
	ClusterRepository
	capacities    []*db.ClusterCapacity
	cpuMillicores int64
	memoryBytes   int64
	queries       int
	err           error
}

func (r *fakeCapacityRepository) ListSchedulableClusters(_ context.Context, cpuMillicores, memoryBytes int64) ([]*db.ClusterCapacity, error) {
	r.queries++
	r.cpuMillicores = cpuMillicores
	r.memoryBytes = memoryBytes
	return r.capacities, r.err
}

func routingServer(tpl *template.Template, repo *fakeCapacityRepository) *Server {
	return &Server{
		cfg: &config.SchedulerConfig{
			TeamTemplateMemoryPerCPU: "4Gi",
			SandboxMaxMemory:         "16Gi",
			ProxyTimeout:             config.Duration{Duration: time.Second},
		},
		repo:                  repo,
		templateStore:         &fakeTemplateStore{template: tpl},
		logger:                zap.NewNop(),
		httpClient:            http.DefaultClient,
		clusterGatewayProxies: make(map[string]*proxy.Router),
	}
}

func routingTemplate(templateID string) *template.Template {
	return &template.Template{
		TemplateID: templateID,
		Scope:      naming.ScopeTeam,
		TeamID:     "team-a",
		Spec: sandboxspec.TemplateSpec{
			MainContainer: sandboxspec.ContainerSpec{
				Image:     "ubuntu:24.04",
				Resources: sandboxspec.ResourceQuota{CPU: "1", Memory: "4Gi"},
			},
		},
	}
}

func capacity(clusterID string, weight int, claims, slots, memoryBytes, cpuMillicores int64) *db.ClusterCapacity {
	return &db.ClusterCapacity{
		Cluster:       template.Cluster{ClusterID: clusterID, Weight: weight, Enabled: true},
		ClaimCapacity: claims, ReadySlots: slots,
		FreeMemoryBytes: memoryBytes, FreeCPUMillicores: cpuMillicores,
	}
}

func capacityWithURL(clusterID, url string) *db.ClusterCapacity {
	result := capacity(clusterID, 100, 8, 8, 32<<30, 8000)
	result.Cluster.ClusterGatewayURL = url
	return result
}

func routingContext() *gin.Context {
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", nil)
	return ctx
}

func authenticatedClaimRouter(server *Server) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		claims := &internalauth.Claims{TeamID: "team-a", UserID: "user-a", Permissions: []string{"*:*"}}
		c.Request = c.Request.WithContext(internalauth.WithClaims(c.Request.Context(), claims))
		c.Next()
	})
	router.POST("/api/v1/sandboxes", server.createSandbox)
	return router
}

func stringPointer(value string) *string { return &value }
