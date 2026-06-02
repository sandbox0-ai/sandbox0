package ctldapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPostJSONReturnsDecodedResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/api/v1/volume-portals/check" {
			t.Fatalf("path = %s, want /api/v1/volume-portals/check", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(CheckVolumePortalsResponse{Ready: true})
	}))
	defer server.Close()

	resp, err := PostJSON[CheckVolumePortalsResponse](context.Background(), server.Client(), server.URL, "/api/v1/volume-portals/check", CheckVolumePortalsRequest{PodUID: "pod-1"})
	if err != nil {
		t.Fatalf("PostJSON returned error: %v", err)
	}
	if resp == nil || !resp.Ready {
		t.Fatalf("response = %#v, want ready response", resp)
	}
}

func TestPostJSONReturnsStatusErrorWithTypedMessage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(PrepareVolumePortalHandoffResponse{Error: "volume is busy"})
	}))
	defer server.Close()

	resp, err := PostJSON[PrepareVolumePortalHandoffResponse](context.Background(), server.Client(), server.URL, "/api/v1/volume-portals/handoffs/prepare", PrepareVolumePortalHandoffRequest{SandboxVolumeID: "vol-1"})
	if err == nil {
		t.Fatal("expected error")
	}
	if resp == nil || resp.Error != "volume is busy" {
		t.Fatalf("response = %#v, want decoded error response", resp)
	}
	var reqErr *RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %T, want RequestError", err)
	}
	if reqErr.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", reqErr.StatusCode, http.StatusConflict)
	}
	if !IsConflictError(err) {
		t.Fatal("IsConflictError returned false")
	}
	want := "ctld request failed with status 409: volume is busy"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}

func TestClientBindVolumePortalUsesSharedPath(t *testing.T) {
	t.Parallel()

	var gotReq BindVolumePortalRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/api/v1/volume-portals/bind" {
			t.Fatalf("path = %s, want /api/v1/volume-portals/bind", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(BindVolumePortalResponse{SandboxVolumeID: gotReq.SandboxVolumeID})
	}))
	defer server.Close()

	resp, err := NewClient(server.Client()).BindVolumePortal(context.Background(), server.URL, BindVolumePortalRequest{
		SandboxVolumeID: "vol-1",
		PodUID:          "pod-1",
	})
	if err != nil {
		t.Fatalf("BindVolumePortal() error = %v", err)
	}
	if resp == nil || resp.SandboxVolumeID != "vol-1" {
		t.Fatalf("response = %#v, want vol-1", resp)
	}
	if gotReq.PodUID != "pod-1" {
		t.Fatalf("PodUID = %q, want pod-1", gotReq.PodUID)
	}
}

func TestClientPrepareRootFSUsesSharedPath(t *testing.T) {
	t.Parallel()

	var gotReq PrepareRootFSRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/api/v1/rootfs/prepare" {
			t.Fatalf("path = %s, want /api/v1/rootfs/prepare", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(PrepareRootFSResponse{
			Prepared:       true,
			SandboxID:      gotReq.SandboxID,
			RootFSVolumeID: gotReq.RootFSVolumeID,
			UpperDir:       "/var/lib/sandbox0/ctld/rootfs/team-a/sandbox-a/rootfs-a/s0fs/upper",
			WorkDir:        "/var/lib/sandbox0/ctld/rootfs/team-a/sandbox-a/rootfs-a/s0fs/work",
		})
	}))
	defer server.Close()

	resp, err := NewClient(server.Client()).PrepareRootFS(context.Background(), server.URL, PrepareRootFSRequest{
		SandboxID:      "sandbox-a",
		TeamID:         "team-a",
		RootFSVolumeID: "rootfs-a",
	})
	if err != nil {
		t.Fatalf("PrepareRootFS() error = %v", err)
	}
	if gotReq.TeamID != "team-a" || gotReq.RootFSVolumeID != "rootfs-a" {
		t.Fatalf("request = %+v, want team-a rootfs-a", gotReq)
	}
	if resp == nil || !resp.Prepared || resp.SandboxID != "sandbox-a" || resp.UpperDir == "" || resp.WorkDir == "" {
		t.Fatalf("response = %#v, want prepared rootfs response", resp)
	}
}

func TestPostJSONReturnsCheckVolumePortalErrorMessage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(CheckVolumePortalsResponse{Error: "portal data is not published"})
	}))
	defer server.Close()

	_, err := NewClient(server.Client()).CheckVolumePortals(context.Background(), server.URL, CheckVolumePortalsRequest{PodUID: "pod-1"})
	if err == nil {
		t.Fatal("expected error")
	}
	want := "ctld request failed with status 503: portal data is not published"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}
