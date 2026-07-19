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

func TestHTTPWriterPostsLogsAndRuntimeSamples(t *testing.T) {
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
		LogsURL:           server.URL + "/logs",
		RuntimeSamplesURL: server.URL + "/runtime-samples",
		TeamTokenProvider: func(_ context.Context, teamID string) (string, error) {
			return "internal-token-" + teamID, nil
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

	cpuUsage := 0.5
	err = writer.InsertRuntimeSamples(context.Background(), []RuntimeSample{{
		TeamID:            "team-1",
		SandboxID:         "sandbox-1",
		RuntimeGeneration: 2,
		SeriesEpoch:       "epoch-1",
		ObservedAt:        time.Unix(101, 0).UTC(),
		SampleID:          "runtime-sample-1",
		CPU:               &RuntimeCPUValues{Usage: &cpuUsage},
	}})
	require.NoError(t, err)

	require.Len(t, requests, 2)
	assert.Equal(t, "/logs", requests[0].path)
	assert.Equal(t, "internal-token-team-1", requests[0].token)
	var logsBody struct {
		Logs []LogEntry `json:"logs"`
	}
	require.NoError(t, json.Unmarshal(requests[0].body, &logsBody))
	require.Len(t, logsBody.Logs, 1)
	assert.Equal(t, "hello", logsBody.Logs[0].Message)

	assert.Equal(t, "/runtime-samples", requests[1].path)
	assert.Equal(t, "internal-token-team-1", requests[1].token)
	var metricsBody struct {
		Samples []RuntimeSample `json:"samples"`
	}
	require.NoError(t, json.Unmarshal(requests[1].body, &metricsBody))
	require.Len(t, metricsBody.Samples, 1)
	assert.Equal(t, "runtime-sample-1", metricsBody.Samples[0].SampleID)
	require.NotNil(t, metricsBody.Samples[0].CPU)
	require.NotNil(t, metricsBody.Samples[0].CPU.Usage)
	assert.Equal(t, 0.5, *metricsBody.Samples[0].CPU.Usage)
}

func TestHTTPWriterReturnsBackendDisabledWhenEndpointMissing(t *testing.T) {
	writer := NewHTTPWriter(HTTPWriterOptions{})

	err := writer.InsertLogs(context.Background(), []LogEntry{{TeamID: "team-1", Message: "hello"}})

	assert.True(t, errors.Is(err, ErrBackendDisabled))
}

func TestHTTPWriterRejectsCrossTeamBatch(t *testing.T) {
	writer := NewHTTPWriter(HTTPWriterOptions{LogsURL: "http://unused.invalid/logs"})

	err := writer.InsertLogs(context.Background(), []LogEntry{
		{TeamID: "team-1", Message: "one"},
		{TeamID: "team-2", Message: "two"},
	})

	require.ErrorContains(t, err, "spans multiple teams")
}
