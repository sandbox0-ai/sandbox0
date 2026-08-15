package ctldapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
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
		_ = json.NewEncoder(w).Encode(BindVolumePortalResponse{Error: "volume is busy"})
	}))
	defer server.Close()

	resp, err := PostJSON[BindVolumePortalResponse](context.Background(), server.Client(), server.URL, "/api/v1/volume-portals/bind", BindVolumePortalRequest{SandboxVolumeID: "vol-1"})
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

func TestClientRootFSMethodsUseSharedPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		call func(*Client, string) error
	}{
		{
			name: "inspect",
			path: "/api/v1/rootfs/inspect",
			call: func(client *Client, address string) error {
				_, err := client.InspectRootFS(context.Background(), address, InspectRootFSRequest{
					Target: RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "sandbox"},
				})
				return err
			},
		},
		{
			name: "save",
			path: "/api/v1/rootfs/save",
			call: func(client *Client, address string) error {
				_, err := client.SaveRootFS(context.Background(), address, SaveRootFSRequest{
					Target:    RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "sandbox"},
					SandboxID: "sandbox-1",
					TeamID:    "team-1",
				})
				return err
			},
		},
		{
			name: "apply",
			path: "/api/v1/rootfs/apply",
			call: func(client *Client, address string) error {
				_, err := client.ApplyRootFS(context.Background(), address, ApplyRootFSRequest{
					Target:     RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "sandbox"},
					Descriptor: RootFSDiffDescriptor{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: "sha256:abc", ObjectKey: "rootfs/diff.tar"},
				})
				return err
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
				}
				if r.URL.Path != tt.path {
					t.Fatalf("path = %s, want %s", r.URL.Path, tt.path)
				}
				switch tt.name {
				case "inspect":
					_ = json.NewEncoder(w).Encode(InspectRootFSResponse{Info: RootFSInfo{Runtime: "runc"}})
				case "save":
					_ = json.NewEncoder(w).Encode(SaveRootFSResponse{Descriptor: RootFSDiffDescriptor{ObjectKey: "rootfs/diff.tar"}})
				case "apply":
					_ = json.NewEncoder(w).Encode(ApplyRootFSResponse{Applied: true})
				}
			}))
			defer server.Close()

			if err := tt.call(NewClient(server.Client()), server.URL); err != nil {
				t.Fatalf("%s returned error: %v", tt.name, err)
			}
		})
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

func TestNewClientWithTimeoutUsesDefault(t *testing.T) {
	client := NewClientWithTimeout(0)
	if client == nil {
		t.Fatal("NewClientWithTimeout returned nil")
	}
	if got := client.RequestTimeout(); got != DefaultRequestTimeout {
		t.Fatalf("RequestTimeout() = %s, want %s", got, DefaultRequestTimeout)
	}
}

func TestClientProbeReturnsDecodedBodyOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/api/v1/sandboxes/sandbox-1/probes/liveness" {
			t.Fatalf("path = %s, want liveness probe path", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(sandboxprobe.Failed(sandboxprobe.KindLiveness, "ProcdProbeFailed", "connection refused", nil))
	}))
	defer server.Close()

	resp, err := NewClientWithTimeout(0).Probe(context.Background(), server.URL, "sandbox-1", sandboxprobe.KindLiveness)
	if err == nil {
		t.Fatal("Probe returned nil error")
	}
	if resp == nil {
		t.Fatal("Probe returned nil response")
	}
	if resp.Kind != sandboxprobe.KindLiveness || resp.Status != sandboxprobe.StatusFailed || resp.Reason != "ProcdProbeFailed" {
		t.Fatalf("response = %#v, want failed liveness response", resp)
	}
}

func TestClientProbePod(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/api/v1/pods/tpl-default/pod-1/probes/readiness" {
			t.Fatalf("path = %s, want readiness probe path", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindReadiness, "SandboxProbePassed", "sandbox probe passed", nil))
	}))
	defer server.Close()

	resp, err := NewClientWithTimeout(0).ProbePod(context.Background(), server.URL, "tpl-default", "pod-1", sandboxprobe.KindReadiness)
	if err != nil {
		t.Fatalf("ProbePod returned error: %v", err)
	}
	if resp == nil || resp.Kind != sandboxprobe.KindReadiness || resp.Status != sandboxprobe.StatusPassed {
		t.Fatalf("response = %#v, want passed readiness response", resp)
	}
}

func TestClientCheckVolumePortalsUsesSharedPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/api/v1/volume-portals/check" {
			t.Fatalf("path = %s, want /api/v1/volume-portals/check", r.URL.Path)
		}
		var req CheckVolumePortalsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.PodUID != "pod-uid" {
			t.Fatalf("PodUID = %q, want pod-uid", req.PodUID)
		}
		_ = json.NewEncoder(w).Encode(CheckVolumePortalsResponse{Ready: false, Missing: []string{"workspace"}})
	}))
	defer server.Close()

	resp, err := NewClientWithTimeout(0).CheckVolumePortals(context.Background(), server.URL, CheckVolumePortalsRequest{
		PodUID: "pod-uid",
		Portals: []VolumePortalRef{{
			PortalName: "workspace",
			MountPath:  "/workspace",
		}},
	})
	if err != nil {
		t.Fatalf("CheckVolumePortals returned error: %v", err)
	}
	if resp == nil || resp.Ready || len(resp.Missing) != 1 || resp.Missing[0] != "workspace" {
		t.Fatalf("response = %#v, want missing workspace", resp)
	}
}

func TestClientRootFSMethodsCanUseExtendedTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/rootfs/save" {
			t.Fatalf("path = %s, want /api/v1/rootfs/save", r.URL.Path)
		}
		time.Sleep(80 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(SaveRootFSResponse{Descriptor: RootFSDiffDescriptor{Digest: "sha256:diff"}})
	}))
	defer server.Close()

	client := NewClient(&http.Client{Timeout: 20 * time.Millisecond})
	req := SaveRootFSRequest{
		Target:    RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "procd"},
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
	}
	if _, err := client.SaveRootFS(context.Background(), server.URL, req); err == nil {
		t.Fatal("SaveRootFS returned nil error with a short timeout")
	}

	resp, err := NewClientWithTimeout(time.Second).SaveRootFS(context.Background(), server.URL, req)
	if err != nil {
		t.Fatalf("SaveRootFS with extended client timeout returned error: %v", err)
	}
	if resp == nil || resp.Descriptor.Digest != "sha256:diff" {
		t.Fatalf("response = %#v, want saved rootfs descriptor", resp)
	}
}
