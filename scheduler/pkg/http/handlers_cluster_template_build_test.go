package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

func TestDisableClusterFailsUncapturedTemplateBuildsBeforeMutation(t *testing.T) {
	t.Parallel()

	events := []string{}
	repo := &clusterLifecycleRepository{
		cluster: &template.Cluster{
			ClusterID:         "cluster-a",
			ClusterName:       "cluster-a",
			ClusterGatewayURL: "http://cluster-a.internal",
			Weight:            100,
			Enabled:           true,
		},
		events: &events,
	}
	buildStore := &clusterLifecycleTemplateStore{events: &events, failed: 2}
	server := &Server{
		repo:          repo,
		templateStore: buildStore,
		logger:        zap.NewNop(),
	}
	router := gin.New()
	router.PUT("/api/v1/clusters/:id", server.updateCluster)

	body, err := json.Marshal(ClusterRequest{
		ClusterName:       "cluster-a",
		ClusterGatewayURL: "http://cluster-a.internal",
		Weight:            100,
		Enabled:           false,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPut, "/api/v1/clusters/cluster-a", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got, want := events, []string{"fail:disabled", "update"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if buildStore.clusterID != "cluster-a" || buildStore.reason != "source_cluster_unavailable" {
		t.Fatalf("lifecycle call = cluster %q reason %q", buildStore.clusterID, buildStore.reason)
	}
	if repo.cluster.Enabled {
		t.Fatal("cluster remained enabled")
	}
}

func TestDeleteClusterFailsUncapturedTemplateBuildsBeforeMutation(t *testing.T) {
	t.Parallel()

	events := []string{}
	repo := &clusterLifecycleRepository{
		cluster: &template.Cluster{
			ClusterID:         "cluster-a",
			ClusterName:       "cluster-a",
			ClusterGatewayURL: "http://cluster-a.internal",
			Weight:            100,
			Enabled:           true,
		},
		events: &events,
	}
	buildStore := &clusterLifecycleTemplateStore{events: &events, failed: 1}
	server := &Server{
		repo:          repo,
		templateStore: buildStore,
		logger:        zap.NewNop(),
	}
	router := gin.New()
	router.DELETE("/api/v1/clusters/:id", server.deleteCluster)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/clusters/cluster-a", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got, want := events, []string{"fail:deleted", "delete"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if repo.cluster != nil {
		t.Fatalf("cluster = %#v, want deleted", repo.cluster)
	}
}

type clusterLifecycleTemplateStore struct {
	templatestore.TemplateStore
	events    *[]string
	clusterID string
	reason    string
	message   string
	failed    int64
	err       error
}

func (s *clusterLifecycleTemplateStore) FailCapturingTemplateBuildsForCluster(
	_ context.Context,
	clusterID, reason, message string,
) (int64, error) {
	s.clusterID = clusterID
	s.reason = reason
	s.message = message
	action := "unknown"
	if bytes.Contains([]byte(message), []byte("disabled")) {
		action = "disabled"
	}
	if bytes.Contains([]byte(message), []byte("deleted")) {
		action = "deleted"
	}
	*s.events = append(*s.events, "fail:"+action)
	return s.failed, s.err
}

type clusterLifecycleRepository struct {
	ClusterRepository
	cluster *template.Cluster
	events  *[]string
}

func (r *clusterLifecycleRepository) GetCluster(_ context.Context, clusterID string) (*template.Cluster, error) {
	if r.cluster == nil || r.cluster.ClusterID != clusterID {
		return nil, nil
	}
	copy := *r.cluster
	return &copy, nil
}

func (r *clusterLifecycleRepository) UpdateCluster(_ context.Context, cluster *template.Cluster) error {
	*r.events = append(*r.events, "update")
	copy := *cluster
	r.cluster = &copy
	return nil
}

func (r *clusterLifecycleRepository) DeleteCluster(_ context.Context, clusterID string) error {
	*r.events = append(*r.events, "delete")
	if r.cluster != nil && r.cluster.ClusterID == clusterID {
		r.cluster = nil
	}
	return nil
}
