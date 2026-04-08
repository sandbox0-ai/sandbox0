package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
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
