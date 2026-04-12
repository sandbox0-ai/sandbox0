package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCtldClientPause(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/sandboxes/sandbox-1/pause", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"paused":true,"resource_usage":{"container_memory_working_set":123}}`))
	}))
	defer server.Close()

	client := NewCtldClient(CtldClientConfig{})
	resp, err := client.Pause(context.Background(), server.URL, "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Paused)
	assert.Equal(t, int64(123), resp.ResourceUsage.ContainerMemoryWorkingSet)
}

func TestCtldClientResumeReturnsDecodedBodyOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/sandboxes/sandbox-1/resume", r.URL.Path)
		w.WriteHeader(http.StatusNotImplemented)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ctldapi.ResumeResponse{Resumed: false, Error: "not implemented"})
	}))
	defer server.Close()

	client := NewCtldClient(CtldClientConfig{})
	resp, err := client.Resume(context.Background(), server.URL, "sandbox-1")
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.Resumed)
	assert.Equal(t, "not implemented", resp.Error)
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
