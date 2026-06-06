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

func TestClientSandboxRootFSUsesSharedPaths(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.URL.Path] = true
		switch r.URL.Path {
		case "/api/v1/sandbox-rootfs/bind":
			var req BindSandboxRootFSRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode bind request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(BindSandboxRootFSResponse{FilesystemID: req.FilesystemID, MountPoint: req.TargetPath})
		case "/api/v1/sandbox-rootfs/flush":
			var req FlushSandboxRootFSRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode flush request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(FlushSandboxRootFSResponse{Flushed: true, FilesystemID: req.FilesystemID})
		case "/api/v1/sandbox-rootfs/release":
			var req ReleaseSandboxRootFSRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode release request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(ReleaseSandboxRootFSResponse{Released: true, FilesystemID: req.FilesystemID})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.Client())
	if _, err := client.BindSandboxRootFS(context.Background(), server.URL, BindSandboxRootFSRequest{FilesystemID: "fs-1", TargetPath: "/sandbox0/rootfs"}); err != nil {
		t.Fatalf("BindSandboxRootFS() error = %v", err)
	}
	if _, err := client.FlushSandboxRootFS(context.Background(), server.URL, FlushSandboxRootFSRequest{FilesystemID: "fs-1"}); err != nil {
		t.Fatalf("FlushSandboxRootFS() error = %v", err)
	}
	if _, err := client.ReleaseSandboxRootFS(context.Background(), server.URL, ReleaseSandboxRootFSRequest{FilesystemID: "fs-1"}); err != nil {
		t.Fatalf("ReleaseSandboxRootFS() error = %v", err)
	}
	for _, path := range []string{"/api/v1/sandbox-rootfs/bind", "/api/v1/sandbox-rootfs/flush", "/api/v1/sandbox-rootfs/release"} {
		if !seen[path] {
			t.Fatalf("path %s was not called", path)
		}
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

func TestPostJSONReturnsSandboxRootFSErrorMessage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(BindSandboxRootFSResponse{Error: "sandbox rootfs is busy"})
	}))
	defer server.Close()

	_, err := NewClient(server.Client()).BindSandboxRootFS(context.Background(), server.URL, BindSandboxRootFSRequest{FilesystemID: "fs-1"})
	if err == nil {
		t.Fatal("expected error")
	}
	want := "ctld request failed with status 409: sandbox rootfs is busy"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}
