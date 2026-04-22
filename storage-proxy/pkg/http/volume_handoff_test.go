package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

type fakeCtldResolver struct {
	url string
	err error
}

func (f fakeCtldResolver) ResolveLocalCtldURL(context.Context) (string, error) {
	return f.url, f.err
}

func TestEnsureCtldVolumeOwnerAttachesLocalCtldForRWOWithoutOwner(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a", AccessMode: string(volume.AccessModeRWO)}

	var attachCalls int32
	var attachReq ctldapi.AttachVolumeOwnerRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/volume-portals/owners/attach" {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&attachCalls, 1)
		if err := json.NewDecoder(r.Body).Decode(&attachReq); err != nil {
			t.Fatalf("decode attach request: %v", err)
		}
		repo.activeMounts["vol-1"] = []*db.VolumeMount{{
			VolumeID:     "vol-1",
			ClusterID:    "cluster-a",
			PodID:        "sandbox0-system/ctld-a",
			MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld}),
		}}
		_ = json.NewEncoder(w).Encode(ctldapi.AttachVolumeOwnerResponse{Attached: true})
	}))
	defer ctld.Close()

	server := &Server{
		logger:       logrus.New(),
		repo:         repo,
		cfg:          &config.StorageProxyConfig{HeartbeatTimeout: 15},
		ctldResolver: fakeCtldResolver{url: ctld.URL},
	}

	if err := server.ensureCtldVolumeOwner(context.Background(), repo.volumes["vol-1"]); err != nil {
		t.Fatalf("ensureCtldVolumeOwner() error = %v", err)
	}
	if got := atomic.LoadInt32(&attachCalls); got != 1 {
		t.Fatalf("attach calls = %d, want 1", got)
	}
	if attachReq.TeamID != "team-a" || attachReq.SandboxVolumeID != "vol-1" {
		t.Fatalf("attach request = %+v, want team-a/vol-1", attachReq)
	}
}

func TestWaitForVolumeHandoffBlocksUntilDeleted(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.handoffs["vol-1"] = &db.VolumeHandoff{
		VolumeID:  "vol-1",
		Status:    "binding",
		ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	server := &Server{repo: repo}

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- server.waitForVolumeHandoff(context.Background(), "vol-1")
	}()

	time.Sleep(150 * time.Millisecond)
	if elapsed := time.Since(start); elapsed < volumeHandoffPollInterval {
		t.Fatalf("waitForVolumeHandoff() returned too quickly after %s", elapsed)
	}
	if _, ok := repo.handoffs["vol-1"]; !ok {
		t.Fatal("handoff removed before test cleanup")
	}
	if err := repo.DeleteVolumeHandoff(context.Background(), "vol-1"); err != nil {
		t.Fatalf("DeleteVolumeHandoff() error = %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForVolumeHandoff() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForVolumeHandoff() did not resume after handoff deletion")
	}
}

func TestExecutePortalBindHandoffCompletesAndCleansRow(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a", AccessMode: string(volume.AccessModeRWO)}

	var prepared int32
	var completed int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/volume-portals/handoffs/prepare":
			atomic.AddInt32(&prepared, 1)
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareVolumePortalHandoffResponse{Prepared: true})
		case "/api/v1/volume-portals/handoffs/complete":
			atomic.AddInt32(&completed, 1)
			_ = json.NewEncoder(w).Encode(ctldapi.CompleteVolumePortalHandoffResponse{Completed: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer source.Close()
	sourceURL, _ := url.Parse(source.URL)
	sourcePort, _ := strconv.Atoi(sourceURL.Port())

	var bindReq ctldapi.BindVolumePortalRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/volume-portals/bind" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&bindReq); err != nil {
			t.Fatalf("decode bind request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(ctldapi.BindVolumePortalResponse{SandboxVolumeID: "vol-1"})
	}))
	defer target.Close()

	server := &Server{
		logger:      logrus.New(),
		repo:        repo,
		podResolver: &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-source": source.URL}},
	}
	sourceMount := &db.VolumeMount{
		VolumeID:     "vol-1",
		ClusterID:    "cluster-a",
		PodID:        "sandbox0-system/ctld-source",
		MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld, OwnerPort: sourcePort}),
	}

	err := server.executePortalBindHandoff(context.Background(), repo.volumes["vol-1"], sourceMount, preparePortalBindRequest{
		TargetClusterID: "cluster-b",
		TargetCtldAddr:  target.URL,
		Namespace:       "default",
		PodName:         "sandbox-a",
		PodUID:          "pod-uid",
		PortalName:      "workspace",
		MountPath:       "/workspace",
		SandboxID:       "sandbox-1",
		OwnerTeamID:     "team-a",
	})
	if err != nil {
		t.Fatalf("executePortalBindHandoff() error = %v", err)
	}
	if got := atomic.LoadInt32(&prepared); got != 1 {
		t.Fatalf("prepare calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&completed); got != 1 {
		t.Fatalf("complete calls = %d, want 1", got)
	}
	if bindReq.TransferSourceClusterID != "cluster-a" || bindReq.TransferSourcePodID != "sandbox0-system/ctld-source" {
		t.Fatalf("bind transfer source = %q/%q, want cluster-a/sandbox0-system/ctld-source", bindReq.TransferSourceClusterID, bindReq.TransferSourcePodID)
	}
	if _, err := repo.GetVolumeHandoff(context.Background(), "vol-1"); err != db.ErrNotFound {
		t.Fatalf("GetVolumeHandoff() err = %v, want %v after cleanup", err, db.ErrNotFound)
	}
}

func TestExecutePortalBindHandoffAbortsOnBindFailure(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a", AccessMode: string(volume.AccessModeRWO)}

	var prepared int32
	var aborted int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/volume-portals/handoffs/prepare":
			atomic.AddInt32(&prepared, 1)
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareVolumePortalHandoffResponse{Prepared: true})
		case "/api/v1/volume-portals/handoffs/abort":
			atomic.AddInt32(&aborted, 1)
			_ = json.NewEncoder(w).Encode(ctldapi.AbortVolumePortalHandoffResponse{Aborted: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer source.Close()
	sourceURL, _ := url.Parse(source.URL)
	sourcePort, _ := strconv.Atoi(sourceURL.Port())

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/volume-portals/bind" {
			http.Error(w, "bind failed", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer target.Close()

	server := &Server{
		logger:      logrus.New(),
		repo:        repo,
		podResolver: &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-source": source.URL}},
	}
	sourceMount := &db.VolumeMount{
		VolumeID:     "vol-1",
		ClusterID:    "cluster-a",
		PodID:        "sandbox0-system/ctld-source",
		MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld, OwnerPort: sourcePort}),
	}

	err := server.executePortalBindHandoff(context.Background(), repo.volumes["vol-1"], sourceMount, preparePortalBindRequest{
		TargetClusterID: "cluster-b",
		TargetCtldAddr:  target.URL,
		Namespace:       "default",
		PodName:         "sandbox-a",
		PodUID:          "pod-uid",
		PortalName:      "workspace",
		MountPath:       "/workspace",
		SandboxID:       "sandbox-1",
		OwnerTeamID:     "team-a",
	})
	if err == nil {
		t.Fatal("executePortalBindHandoff() error = nil, want bind failure")
	}
	if got := atomic.LoadInt32(&prepared); got != 1 {
		t.Fatalf("prepare calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&aborted); got != 1 {
		t.Fatalf("abort calls = %d, want 1", got)
	}
	if _, err := repo.GetVolumeHandoff(context.Background(), "vol-1"); err != db.ErrNotFound {
		t.Fatalf("GetVolumeHandoff() err = %v, want %v after cleanup", err, db.ErrNotFound)
	}
}

func TestExecutePortalBindHandoffAbortsWhenPrepareDoesNotComplete(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a", AccessMode: string(volume.AccessModeRWO)}

	var prepared int32
	var aborted int32
	var bindCalls int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/volume-portals/handoffs/prepare":
			atomic.AddInt32(&prepared, 1)
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareVolumePortalHandoffResponse{
				Prepared: false,
				Error:    "volume is no longer owned by this ctld",
			})
		case "/api/v1/volume-portals/handoffs/abort":
			atomic.AddInt32(&aborted, 1)
			_ = json.NewEncoder(w).Encode(ctldapi.AbortVolumePortalHandoffResponse{Aborted: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer source.Close()
	sourceURL, _ := url.Parse(source.URL)
	sourcePort, _ := strconv.Atoi(sourceURL.Port())

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/volume-portals/bind" {
			atomic.AddInt32(&bindCalls, 1)
			_ = json.NewEncoder(w).Encode(ctldapi.BindVolumePortalResponse{SandboxVolumeID: "vol-1"})
			return
		}
		http.NotFound(w, r)
	}))
	defer target.Close()

	server := &Server{
		logger:      logrus.New(),
		repo:        repo,
		podResolver: &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-source": source.URL}},
	}
	sourceMount := &db.VolumeMount{
		VolumeID:     "vol-1",
		ClusterID:    "cluster-a",
		PodID:        "sandbox0-system/ctld-source",
		MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld, OwnerPort: sourcePort}),
	}

	err := server.executePortalBindHandoff(context.Background(), repo.volumes["vol-1"], sourceMount, preparePortalBindRequest{
		TargetClusterID: "cluster-b",
		TargetCtldAddr:  target.URL,
		Namespace:       "default",
		PodName:         "sandbox-a",
		PodUID:          "pod-uid",
		PortalName:      "workspace",
		MountPath:       "/workspace",
		SandboxID:       "sandbox-1",
		OwnerTeamID:     "team-a",
	})
	if err == nil {
		t.Fatal("executePortalBindHandoff() error = nil, want prepare failure")
	}
	if got := atomic.LoadInt32(&prepared); got != 1 {
		t.Fatalf("prepare calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&aborted); got != 1 {
		t.Fatalf("abort calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&bindCalls); got != 0 {
		t.Fatalf("bind calls = %d, want 0", got)
	}
	if _, err := repo.GetVolumeHandoff(context.Background(), "vol-1"); err != db.ErrNotFound {
		t.Fatalf("GetVolumeHandoff() err = %v, want %v after cleanup", err, db.ErrNotFound)
	}
}
