package ctldapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestIsUnavailableErrorRecognizesWrappedRequestError(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("seal rootfs: %w", &RequestError{
		StatusCode: http.StatusServiceUnavailable,
		Message:    "object store unavailable",
	})

	if !IsUnavailableError(err) {
		t.Fatal("IsUnavailableError returned false")
	}
	if IsUnavailableError(&RequestError{StatusCode: http.StatusConflict}) {
		t.Fatal("IsUnavailableError returned true for a conflict")
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
		name   string
		method string
		path   string
		call   func(*Client, string) error
	}{
		{
			name:   "bind",
			method: http.MethodPut,
			path:   "/api/v1/rootfs/sync/bind",
			call: func(client *Client, address string) error {
				_, err := client.BindRootFSSync(context.Background(), address, BindRootFSSyncRequest{
					Target:    RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "sandbox"},
					SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1,
				})
				return err
			},
		},
		{
			name:   "status",
			method: http.MethodPost,
			path:   "/api/v1/rootfs/sync/status",
			call: func(client *Client, address string) error {
				_, err := client.GetRootFSSyncStatus(context.Background(), address, GetRootFSSyncStatusRequest{
					SandboxID: "sandbox-1", RuntimeGeneration: 1,
				})
				return err
			},
		},
		{
			name:   "seal",
			method: http.MethodPut,
			path:   "/api/v1/rootfs/heads/seal",
			call: func(client *Client, address string) error {
				_, err := client.SealRootFSHead(context.Background(), address, SealRootFSHeadRequest{
					SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "head-1", ExpectedRuntimeGeneration: 1,
				}, time.Second)
				return err
			},
		},
		{
			name:   "materialize",
			method: http.MethodPut,
			path:   "/api/v1/rootfs/heads/materialize",
			call: func(client *Client, address string) error {
				_, err := client.MaterializeRootFSHead(context.Background(), address, MaterializeRootFSHeadRequest{}, time.Second)
				return err
			},
		},
		{
			name:   "acknowledge",
			method: http.MethodPut,
			path:   "/api/v1/rootfs/heads/acknowledge",
			call: func(client *Client, address string) error {
				_, err := client.AcknowledgeRootFSHead(context.Background(), address, AcknowledgeRootFSHeadRequest{
					SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "head-1", RuntimeGeneration: 1, RuntimeContinues: true,
				}, time.Second)
				return err
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tt.method {
					t.Fatalf("method = %s, want %s", r.Method, tt.method)
				}
				if r.URL.Path != tt.path {
					t.Fatalf("path = %s, want %s", r.URL.Path, tt.path)
				}
				switch tt.name {
				case "bind":
					_ = json.NewEncoder(w).Encode(BindRootFSSyncResponse{})
				case "status":
					_ = json.NewEncoder(w).Encode(GetRootFSSyncStatusResponse{})
				case "seal":
					_ = json.NewEncoder(w).Encode(SealRootFSHeadResponse{})
				case "materialize":
					_ = json.NewEncoder(w).Encode(MaterializeRootFSHeadResponse{Materialized: true})
				case "acknowledge":
					_ = json.NewEncoder(w).Encode(AcknowledgeRootFSHeadResponse{Acknowledged: true})
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

func TestClientRootFSSealCanUseExtendedTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/rootfs/heads/seal" {
			t.Fatalf("path = %s, want /api/v1/rootfs/heads/seal", r.URL.Path)
		}
		time.Sleep(80 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(SealRootFSHeadResponse{})
	}))
	defer server.Close()

	client := NewClient(server.Client())
	req := SealRootFSHeadRequest{
		SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "head-1", ExpectedRuntimeGeneration: 1,
	}
	if _, err := client.SealRootFSHead(context.Background(), server.URL, req, 20*time.Millisecond); err == nil {
		t.Fatal("SealRootFSHead returned nil error with a short timeout")
	}

	resp, err := client.SealRootFSHead(context.Background(), server.URL, req, time.Second)
	if err != nil {
		t.Fatalf("SealRootFSHead with extended client timeout returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("SealRootFSHead response is nil")
	}
}
