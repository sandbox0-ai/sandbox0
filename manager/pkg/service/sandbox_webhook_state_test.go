package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestStorageProxyVolumeClientDefaultsClusterID(t *testing.T) {
	var gotClusterID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/sandboxvolumes/owned" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			ClusterID string `json:"cluster_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotClusterID = req.ClusterID
		_ = spec.WriteSuccess(w, http.StatusCreated, map[string]any{
			"volume": map[string]string{"id": "vol-1"},
		})
	}))
	defer server.Close()

	client := NewStorageProxyVolumeClient(StorageProxyVolumeClientConfig{
		BaseURL:        server.URL,
		TokenGenerator: staticTokenGenerator{},
	})
	volumeID, err := client.Create(t.Context(), "team-1", "user-1", "sandbox-1", webhookStateVolumeKind)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if volumeID != "vol-1" {
		t.Fatalf("volumeID = %q, want vol-1", volumeID)
	}
	if gotClusterID != naming.DefaultClusterID {
		t.Fatalf("clusterID = %q, want %q", gotClusterID, naming.DefaultClusterID)
	}
}

func TestStorageProxyVolumeClientPrepareForBind(t *testing.T) {
	var sawToken string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/sandboxvolumes/vol-1/prepare-bind" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		sawToken = r.Header.Get("X-Internal-Token")
		_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"prepared": true})
	}))
	defer server.Close()

	client := NewStorageProxyVolumeClient(StorageProxyVolumeClientConfig{
		BaseURL:        server.URL,
		TokenGenerator: staticTokenGenerator{},
	})
	if err := client.PrepareForBind(t.Context(), "team-1", "user-1", "vol-1"); err != nil {
		t.Fatalf("PrepareForBind returned error: %v", err)
	}
	if sawToken == "" {
		t.Fatal("PrepareForBind did not send an internal token")
	}
}
