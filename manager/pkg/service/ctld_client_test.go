package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
