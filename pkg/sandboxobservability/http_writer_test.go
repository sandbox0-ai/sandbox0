package sandboxobservability

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPWriterPostsLogsAndMetricSamples(t *testing.T) {
	type recordedRequest struct {
		path  string
		token string
		body  []byte
	}
	var requests []recordedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		requests = append(requests, recordedRequest{
			path:  r.URL.Path,
			token: r.Header.Get(internalauth.DefaultTokenHeader),
			body:  body,
		})
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	writer := NewHTTPWriter(HTTPWriterOptions{
		LogsURL:    server.URL + "/logs",
		MetricsURL: server.URL + "/metrics",
		TokenProvider: func(context.Context) (string, error) {
			return "internal-token", nil
		},
		RequestTimeout: time.Second,
	})

	err := writer.InsertLogs(context.Background(), []LogEntry{{
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		OccurredAt: time.Unix(100, 0).UTC(),
		Stream:     LogStreamStdout,
		Message:    "hello",
		Cursor:     "log-cursor",
	}})
	require.NoError(t, err)

	err = writer.InsertMetricSamples(context.Background(), []MetricSample{{
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		OccurredAt: time.Unix(101, 0).UTC(),
		Name:       "process.cpu.percent",
		Unit:       "percent",
		Value:      12.5,
		Cursor:     "metric-cursor",
	}})
	require.NoError(t, err)

	require.Len(t, requests, 2)
	assert.Equal(t, "/logs", requests[0].path)
	assert.Equal(t, "internal-token", requests[0].token)
	var logsBody struct {
		Logs []LogEntry `json:"logs"`
	}
	require.NoError(t, json.Unmarshal(requests[0].body, &logsBody))
	require.Len(t, logsBody.Logs, 1)
	assert.Equal(t, "hello", logsBody.Logs[0].Message)

	assert.Equal(t, "/metrics", requests[1].path)
	assert.Equal(t, "internal-token", requests[1].token)
	var metricsBody struct {
		Samples []MetricSample `json:"samples"`
	}
	require.NoError(t, json.Unmarshal(requests[1].body, &metricsBody))
	require.Len(t, metricsBody.Samples, 1)
	assert.Equal(t, "process.cpu.percent", metricsBody.Samples[0].Name)
}

func TestHTTPWriterReturnsBackendDisabledWhenEndpointMissing(t *testing.T) {
	writer := NewHTTPWriter(HTTPWriterOptions{})

	err := writer.InsertLogs(context.Background(), []LogEntry{{Message: "hello"}})

	assert.True(t, errors.Is(err, ErrBackendDisabled))
}
