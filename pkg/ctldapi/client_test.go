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
