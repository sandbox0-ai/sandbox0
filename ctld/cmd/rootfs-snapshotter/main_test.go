package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthHandlerSeparatesLivenessAndReadiness(t *testing.T) {
	handler := healthHandler(fakeReadiness{err: errors.New("not ready")})
	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, live.Code)
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, ready.Code)

	handler = healthHandler(fakeReadiness{})
	ready = httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusOK, ready.Code)
}

func TestParseBytes(t *testing.T) {
	value, err := parseBytes("128Mi")
	require.NoError(t, err)
	assert.Equal(t, int64(128<<20), value)
	value, err = parseBytes("off")
	require.NoError(t, err)
	assert.Zero(t, value)
	_, err = parseBytes("-1")
	assert.Error(t, err)
}

type fakeReadiness struct {
	err error
}

func (r fakeReadiness) Ready(context.Context) error {
	return r.err
}
