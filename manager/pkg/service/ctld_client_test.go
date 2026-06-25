package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCtldClientUsesDefaultTimeout(t *testing.T) {
	client := NewCtldClient(CtldClientConfig{})

	require.NotNil(t, client.httpClient)
	assert.Equal(t, 15*time.Second, client.httpClient.Timeout)
}

func TestCtldClientProbeReturnsDecodedBodyOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/sandboxes/sandbox-1/probes/liveness", r.URL.Path)
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sandboxprobe.Failed(sandboxprobe.KindLiveness, "ProcdProbeFailed", "connection refused", nil))
	}))
	defer server.Close()

	client := NewCtldClient(CtldClientConfig{})
	resp, err := client.Probe(context.Background(), server.URL, "sandbox-1", sandboxprobe.KindLiveness)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, sandboxprobe.KindLiveness, resp.Kind)
	assert.Equal(t, sandboxprobe.StatusFailed, resp.Status)
	assert.Equal(t, "ProcdProbeFailed", resp.Reason)
}

func TestCtldClientProbePod(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/pods/tpl-default/pod-1/probes/readiness", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindReadiness, "SandboxProbePassed", "sandbox probe passed", nil))
	}))
	defer server.Close()

	client := NewCtldClient(CtldClientConfig{})
	resp, err := client.ProbePod(context.Background(), server.URL, "tpl-default", "pod-1", sandboxprobe.KindReadiness)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, sandboxprobe.KindReadiness, resp.Kind)
	assert.Equal(t, sandboxprobe.StatusPassed, resp.Status)
}

func TestCtldClientCheckVolumePortals(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/volume-portals/check", r.URL.Path)
		var req ctldapi.CheckVolumePortalsRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "pod-uid", req.PodUID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ctldapi.CheckVolumePortalsResponse{
			Ready:   false,
			Missing: []string{"workspace"},
		})
	}))
	defer server.Close()

	client := NewCtldClient(CtldClientConfig{})
	resp, err := client.CheckVolumePortals(context.Background(), server.URL, ctldapi.CheckVolumePortalsRequest{
		PodUID: "pod-uid",
		Portals: []ctldapi.VolumePortalRef{{
			PortalName: "workspace",
			MountPath:  "/workspace",
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Ready)
	assert.Equal(t, []string{"workspace"}, resp.Missing)
}

func TestCtldClientRootFSMethods(t *testing.T) {
	tests := []struct {
		name string
		path string
		call func(*CtldClient, string) error
	}{
		{
			name: "inspect",
			path: "/api/v1/rootfs/inspect",
			call: func(client *CtldClient, address string) error {
				_, err := client.InspectRootFS(context.Background(), address, ctldapi.InspectRootFSRequest{
					Target: ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "procd"},
				})
				return err
			},
		},
		{
			name: "save",
			path: "/api/v1/rootfs/save",
			call: func(client *CtldClient, address string) error {
				_, err := client.SaveRootFS(context.Background(), address, ctldapi.SaveRootFSRequest{
					Target:    ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "procd"},
					SandboxID: "sandbox-1",
					TeamID:    "team-1",
				})
				return err
			},
		},
		{
			name: "apply",
			path: "/api/v1/rootfs/apply",
			call: func(client *CtldClient, address string) error {
				_, err := client.ApplyRootFS(context.Background(), address, ctldapi.ApplyRootFSRequest{
					Target:     ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "procd"},
					Descriptor: ctldapi.RootFSDiffDescriptor{Digest: "sha256:abc", ObjectKey: "rootfs/diff.tar"},
				})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, tt.path, r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				switch tt.name {
				case "inspect":
					_ = json.NewEncoder(w).Encode(ctldapi.InspectRootFSResponse{Info: ctldapi.RootFSInfo{Runtime: "runc"}})
				case "save":
					_ = json.NewEncoder(w).Encode(ctldapi.SaveRootFSResponse{Descriptor: ctldapi.RootFSDiffDescriptor{ObjectKey: "rootfs/diff.tar"}})
				case "apply":
					_ = json.NewEncoder(w).Encode(ctldapi.ApplyRootFSResponse{Applied: true})
				}
			}))
			defer server.Close()

			client := NewCtldClient(CtldClientConfig{})
			require.NoError(t, tt.call(client, server.URL))
		})
	}
}

func TestCtldClientRootFSMethodsCanUseExtendedTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/rootfs/save", r.URL.Path)
		time.Sleep(80 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(ctldapi.SaveRootFSResponse{
			Descriptor: ctldapi.RootFSDiffDescriptor{Digest: "sha256:diff"},
		})
	}))
	defer server.Close()

	client := NewCtldClientWithHTTPClient(&http.Client{Timeout: 20 * time.Millisecond})
	req := ctldapi.SaveRootFSRequest{
		Target:    ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "procd"},
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
	}

	_, err := client.SaveRootFS(context.Background(), server.URL, req)
	require.Error(t, err)

	resp, err := client.SaveRootFSWithTimeout(context.Background(), server.URL, req, time.Second)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "sha256:diff", resp.Descriptor.Digest)
}
