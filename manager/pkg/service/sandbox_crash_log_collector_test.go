package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestSandboxCrashLogCollectorEnqueuesUnexpectedProcdRestart(t *testing.T) {
	collector := NewSandboxCrashLogCollector(nil, zap.NewNop(), nil)
	t.Cleanup(collector.queue.ShutDown)

	oldPod := testProcdCrashPod(0, 0)
	newPod := testProcdCrashPod(1, 2)
	collector.ResourceEventHandler().UpdateFunc(oldPod, newPod)

	require.Equal(t, 1, collector.queue.Len())
	item, shutdown := collector.queue.Get()
	require.False(t, shutdown)
	collector.queue.Done(item)
	collector.queue.Forget(item)
	assert.Equal(t, "sandbox-1", item.SandboxID)
	assert.Equal(t, "team-1", item.TeamID)
	assert.Equal(t, int64(7), item.RuntimeGeneration)
	assert.Equal(t, int32(1), item.RestartCount)
	assert.Equal(t, int32(2), item.ExitCode)
	assert.Equal(t, "pod-uid:1", item.crashID())
}

func TestSandboxCrashLogCollectorIgnoresExpectedOrIrrelevantPodUpdates(t *testing.T) {
	tests := []struct {
		name   string
		oldPod *corev1.Pod
		newPod *corev1.Pod
	}{
		{
			name:   "restart count unchanged",
			oldPod: testProcdCrashPod(1, 2),
			newPod: testProcdCrashPod(1, 2),
		},
		{
			name:   "successful container restart",
			oldPod: testProcdCrashPod(0, 0),
			newPod: testProcdCrashPod(1, 0),
		},
		{
			name:   "idle pool pod",
			oldPod: testProcdCrashPod(0, 0),
			newPod: func() *corev1.Pod {
				pod := testProcdCrashPod(1, 2)
				pod.Labels[controller.LabelPoolType] = controller.PoolTypeIdle
				return pod
			}(),
		},
		{
			name:   "pod without team ownership",
			oldPod: testProcdCrashPod(0, 0),
			newPod: func() *corev1.Pod {
				pod := testProcdCrashPod(1, 2)
				delete(pod.Annotations, controller.AnnotationTeamID)
				return pod
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := NewSandboxCrashLogCollector(nil, zap.NewNop(), nil)
			t.Cleanup(collector.queue.ShutDown)
			collector.ResourceEventHandler().UpdateFunc(tt.oldPod, tt.newPod)
			assert.Equal(t, 0, collector.queue.Len())
		})
	}
}

func TestSandboxCrashLogCollectorAddRecoversLatestCrashAfterManagerRestart(t *testing.T) {
	collector := NewSandboxCrashLogCollector(nil, zap.NewNop(), nil)
	t.Cleanup(collector.queue.ShutDown)

	collector.ResourceEventHandler().AddFunc(testProcdCrashPod(3, 2))

	require.Equal(t, 1, collector.queue.Len())
	item, shutdown := collector.queue.Get()
	require.False(t, shutdown)
	collector.queue.Done(item)
	collector.queue.Forget(item)
	assert.Equal(t, int32(3), item.RestartCount)
}

func TestSandboxCrashLogCollectorCapturesInternalLogsAndDropsProcessOutput(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewManager(registry)
	reader := &recordingPreviousLogReader{logs: []byte(strings.Join([]string{
		`2026-07-23T03:26:46.900000000Z {"level":"info","msg":"request started","service":"procd"}`,
		`2026-07-23T03:26:46.950000000Z {"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"stderr","data":"TOP_SECRET"}`,
		`2026-07-23T03:26:46.978000000Z panic: test crash`,
		`2026-07-23T03:26:46.978100000Z goroutine 42 [running]:`,
	}, "\n"))}
	collector := NewSandboxCrashLogCollector(nil, zap.New(core), metrics)
	collector.reader = reader
	t.Cleanup(collector.queue.ShutDown)

	item := crashLogItemFromPod(testProcdCrashPod(1, 2))
	require.NoError(t, collector.capturePreviousLogs(context.Background(), item))

	assert.Equal(t, []string{"sandbox-ns/sandbox-pod"}, reader.Calls())
	entries := observed.FilterMessage("Captured previous procd container logs").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	assert.Equal(t, "sandbox_procd_crash", fields["event"])
	assert.Equal(t, "pod-uid:1", fields["crash_id"])
	logChunk, ok := fields["log_chunk"].(string)
	require.True(t, ok)
	assert.Contains(t, logChunk, "request started")
	assert.Contains(t, logChunk, "panic: test crash")
	assert.Contains(t, logChunk, "goroutine 42")
	assert.NotContains(t, logChunk, "TOP_SECRET")
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProcdCrashLogCapturesTotal.WithLabelValues("success")))
}

func TestSandboxCrashLogCollectorDoesNotEmitFilteredProcessOutput(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewManager(registry)
	reader := &recordingPreviousLogReader{logs: []byte(`2026-07-23T03:26:46.950000000Z {"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"stdout","data":"TOP_SECRET"}`)}
	collector := NewSandboxCrashLogCollector(nil, zap.New(core), metrics)
	collector.reader = reader
	t.Cleanup(collector.queue.ShutDown)

	require.NoError(t, collector.capturePreviousLogs(context.Background(), crashLogItemFromPod(testProcdCrashPod(1, 2))))

	assert.Equal(t, 0, observed.FilterMessage("Captured previous procd container logs").Len())
	emptyEntries := observed.FilterMessage("Previous procd container logs contained no internal log lines").All()
	require.Len(t, emptyEntries, 1)
	assert.NotContains(t, emptyEntries[0].ContextMap(), "log_chunk")
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProcdCrashLogCapturesTotal.WithLabelValues("empty")))
}

func TestKubernetesPreviousContainerLogReaderUsesPreviousProcdLogOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/namespaces/sandbox-ns/pods/sandbox-pod/log", r.URL.Path)
		query := r.URL.Query()
		assert.Equal(t, DefaultSandboxLogContainer, query.Get("container"))
		assert.Equal(t, "true", query.Get("previous"))
		assert.Equal(t, "true", query.Get("timestamps"))
		assert.Equal(t, "2048", query.Get("tailLines"))
		assert.Equal(t, "16777216", query.Get("limitBytes"))
		_, _ = w.Write([]byte("panic: captured\n"))
	}))
	defer server.Close()

	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	reader := &kubernetesPreviousContainerLogReader{client: client}

	logs, err := reader.ReadPreviousLogs(context.Background(), "sandbox-ns", "sandbox-pod")
	require.NoError(t, err)
	assert.Equal(t, "panic: captured\n", string(logs))
}

func TestSandboxCrashLogCollectorRetriesTransientReadFailure(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	reader := &recordingPreviousLogReader{
		logs:         []byte("panic: captured after retry\n"),
		failuresLeft: 2,
	}
	collector := NewSandboxCrashLogCollector(nil, zap.New(core), nil)
	collector.reader = reader

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx, 1) }()
	collector.queue.Add(crashLogItemFromPod(testProcdCrashPod(1, 2)))
	require.Eventually(t, func() bool {
		return observed.FilterMessage("Captured previous procd container logs").Len() == 1
	}, 3*time.Second, 20*time.Millisecond)
	assert.Equal(t, 3, len(reader.Calls()))

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

type recordingPreviousLogReader struct {
	mu           sync.Mutex
	logs         []byte
	failuresLeft int
	calls        []string
}

func (r *recordingPreviousLogReader) ReadPreviousLogs(_ context.Context, namespace, podName string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, namespace+"/"+podName)
	if r.failuresLeft > 0 {
		r.failuresLeft--
		return nil, errors.New("transient log read failure")
	}
	return append([]byte(nil), r.logs...), nil
}

func (r *recordingPreviousLogReader) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func testProcdCrashPod(restartCount, exitCode int32) *corev1.Pod {
	now := time.Now().UTC()
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-pod",
			Namespace: "sandbox-ns",
			UID:       types.UID("pod-uid"),
			Labels: map[string]string{
				controller.LabelPoolType:  controller.PoolTypeActive,
				controller.LabelSandboxID: "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:            "team-1",
				controller.AnnotationRuntimeGeneration: "7",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         DefaultSandboxLogContainer,
					RestartCount: restartCount,
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode:   exitCode,
							Reason:     "Error",
							StartedAt:  metav1.NewTime(now.Add(-time.Minute)),
							FinishedAt: metav1.NewTime(now),
						},
					},
				},
			},
		},
	}
}

func crashLogItemFromPod(pod *corev1.Pod) procdCrashLogItem {
	status := procdContainerStatus(pod)
	terminated := status.LastTerminationState.Terminated
	return procdCrashLogItem{
		Namespace:         pod.Namespace,
		PodName:           pod.Name,
		PodUID:            string(pod.UID),
		SandboxID:         sandboxIDFromPod(pod),
		TeamID:            pod.Annotations[controller.AnnotationTeamID],
		RuntimeGeneration: runtimeGenerationFromPod(pod),
		RestartCount:      status.RestartCount,
		ExitCode:          terminated.ExitCode,
		Signal:            terminated.Signal,
		Reason:            terminated.Reason,
		StartedAt:         terminated.StartedAt.Time,
		FinishedAt:        terminated.FinishedAt.Time,
	}
}
