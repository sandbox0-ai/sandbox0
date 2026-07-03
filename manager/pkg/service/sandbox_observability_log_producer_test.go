package service

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	sandboxobsingest "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSandboxLogProducerProjectsProcessLogStream(t *testing.T) {
	writer := newRecordingSandboxObservabilityWriter()
	worker, err := sandboxobsingest.NewLogWorker(writer, sandboxobsingest.Config{
		QueueSize:     10,
		BatchSize:     1,
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	producer := NewSandboxLogProducer(nil, nil, worker, SandboxLogProducerConfig{
		RegionID:     "aws/us-east-1",
		ClusterID:    "cluster-a",
		PollInterval: time.Second,
	}, zap.NewNop(), nil)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-pod",
			Namespace: "sandbox-ns",
			UID:       types.UID("pod-uid"),
		},
	}
	input := strings.NewReader(`2026-04-17T12:34:56.789123456Z {"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"stdout","data":"hello","pid":123}`)

	err = producer.projectLogStream(ctx, pod, "sandbox-1", "team-1", input)
	require.NoError(t, err)

	logs := writer.waitForLogs(t)
	require.Len(t, logs, 1)
	entry := logs[0]
	assert.Equal(t, "team-1", entry.TeamID)
	assert.Equal(t, "sandbox-1", entry.SandboxID)
	assert.Equal(t, "aws/us-east-1", entry.RegionID)
	assert.Equal(t, "cluster-a", entry.ClusterID)
	assert.Equal(t, "ctx-1", entry.ContextID)
	assert.Equal(t, "ctx-1", entry.ProcessID)
	assert.Equal(t, sandboxobservability.LogStreamStdout, entry.Stream)
	assert.Equal(t, "hello", entry.Message)
	assert.Equal(t, "sandbox-pod", entry.Attributes["pod_name"])
	assert.Equal(t, "cmd", entry.Attributes["process_type"])
	assert.Equal(t, 123, entry.Attributes["pid"])
	assert.Contains(t, entry.Cursor, "procd-log:pod-uid:ctx-1:stdout:")
}

type recordingSandboxObservabilityWriter struct {
	logsCh chan []sandboxobservability.LogEntry
	mu     sync.Mutex
	logs   []sandboxobservability.LogEntry
}

func newRecordingSandboxObservabilityWriter() *recordingSandboxObservabilityWriter {
	return &recordingSandboxObservabilityWriter{
		logsCh: make(chan []sandboxobservability.LogEntry, 1),
	}
}

func (w *recordingSandboxObservabilityWriter) InsertEvents(context.Context, []sandboxobservability.Event) error {
	return nil
}

func (w *recordingSandboxObservabilityWriter) InsertLogs(_ context.Context, logs []sandboxobservability.LogEntry) error {
	w.mu.Lock()
	copied := append([]sandboxobservability.LogEntry(nil), logs...)
	w.logs = append(w.logs, copied...)
	w.mu.Unlock()
	w.logsCh <- copied
	return nil
}

func (w *recordingSandboxObservabilityWriter) InsertMetricSamples(context.Context, []sandboxobservability.MetricSample) error {
	return nil
}

func (w *recordingSandboxObservabilityWriter) waitForLogs(t *testing.T) []sandboxobservability.LogEntry {
	t.Helper()
	select {
	case logs := <-w.logsCh:
		return logs
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for logs")
		return nil
	}
}
