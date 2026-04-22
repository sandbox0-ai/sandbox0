package service

import (
	"encoding/json"
	"errors"
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

func TestStorageProxyVolumeClientPrepareForPortalBind(t *testing.T) {
	var gotMethod, gotPath, gotToken string
	var gotReq PrepareVolumePortalBindRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotToken = r.Header.Get("X-Internal-Token")
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"prepared": true})
	}))
	defer server.Close()

	client := NewStorageProxyVolumeClient(StorageProxyVolumeClientConfig{
		BaseURL:        server.URL,
		TokenGenerator: staticTokenGenerator{},
	})
	if err := client.PrepareForVolumePortalBind(t.Context(), PrepareVolumePortalBindRequest{
		TeamID:         "team-1",
		UserID:         "user-1",
		VolumeID:       "vol-1",
		TargetCtldAddr: "http://10.0.0.1:8095",
		PodUID:         "pod-uid",
		MountPath:      "/workspace",
	}); err != nil {
		t.Fatalf("PrepareForVolumePortalBind() error = %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("method = %q, want %q", gotMethod, http.MethodPut)
	}
	if gotPath != "/internal/v1/sandboxvolumes/vol-1/prepare-portal-bind" {
		t.Fatalf("path = %q, want %q", gotPath, "/internal/v1/sandboxvolumes/vol-1/prepare-portal-bind")
	}
	if gotToken == "" {
		t.Fatal("expected internal token header to be set")
	}
	if gotReq.TargetCtldAddr != "http://10.0.0.1:8095" {
		t.Fatalf("targetCtldAddr = %q, want %q", gotReq.TargetCtldAddr, "http://10.0.0.1:8095")
	}
	if gotReq.PodUID != "pod-uid" {
		t.Fatalf("podUID = %q, want %q", gotReq.PodUID, "pod-uid")
	}
}

func TestStorageProxyVolumeClientPrepareForPortalBindConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "volume has active mounts")
	}))
	defer server.Close()

	client := NewStorageProxyVolumeClient(StorageProxyVolumeClientConfig{
		BaseURL:        server.URL,
		TokenGenerator: staticTokenGenerator{},
	})
	err := client.PrepareForVolumePortalBind(t.Context(), PrepareVolumePortalBindRequest{
		TeamID:   "team-1",
		UserID:   "user-1",
		VolumeID: "vol-1",
	})
	if err == nil {
		t.Fatal("PrepareForVolumePortalBind() error = nil, want conflict")
	}
	if !errors.Is(err, ErrVolumePortalBindConflict) {
		t.Fatalf("PrepareForVolumePortalBind() error = %v, want ErrVolumePortalBindConflict", err)
	}
}
