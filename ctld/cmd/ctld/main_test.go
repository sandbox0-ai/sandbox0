package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	ctldserver "github.com/sandbox0-ai/sandbox0/internal/ctld/server"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCtldHealthEndpoints(t *testing.T) {
	server := newHTTPServer(":0", nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())

	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())
}

func TestCtldPauseResumeStubsReturnNotImplemented(t *testing.T) {
	server := newHTTPServer(":0", nil)

	t.Run("custom controller", func(t *testing.T) {
		server := newHTTPServer(":0", ctldserver.NotImplementedController{})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotImplemented, rec.Code)
	})

	t.Run("pause", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotImplemented, rec.Code)
		var resp ctldapi.PauseResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.False(t, resp.Paused)
		assert.Equal(t, "ctld pause not implemented", resp.Error)
	})

	t.Run("resume", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/resume", nil)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotImplemented, rec.Code)
		var resp ctldapi.ResumeResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.False(t, resp.Resumed)
		assert.Equal(t, "ctld resume not implemented", resp.Error)
	})
}
