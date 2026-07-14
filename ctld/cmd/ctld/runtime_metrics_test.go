package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	ctldruntimemetrics "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/runtimemetrics"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestCtldRuntimeMetricsProducerPostsAuthorizedRuntimeSample(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:         "cluster-gateway",
		PublicKey:      publicKey,
		AllowedCallers: []string{"ctld"},
	})
	received := make(chan sandboxobservability.RuntimeSample, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/internal/v1/sandbox-observability/runtime-samples", r.URL.Path)
		claims, validateErr := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader))
		if validateErr != nil {
			t.Errorf("validate ctld internal token: %v", validateErr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		assert.Equal(t, "ctld", claims.Caller)
		assert.Contains(t, claims.Permissions, authn.PermSandboxObservabilityWrite)
		var body struct {
			Samples []sandboxobservability.RuntimeSample `json:"samples"`
		}
		if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil {
			t.Errorf("decode runtime sample batch: %v", decodeErr)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(body.Samples) != 1 {
			t.Errorf("runtime sample count = %d, want 1", len(body.Samples))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- body.Samples[0]
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	originalNodeName := nodeName
	nodeName = "node-a"
	defer func() { nodeName = originalNodeName }()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns-a",
			Name:      "pod-a",
			UID:       types.UID("pod-uid-a"),
			Labels: map[string]string{
				controller.LabelPoolType:  controller.PoolTypeActive,
				controller.LabelSandboxID: "sandbox-a",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:            "team-a",
				controller.AnnotationRuntimeGeneration: "4",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-a",
			Containers: []corev1.Container{{Name: "procd"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "ctld", PrivateKey: privateKey, TTL: time.Minute})
	statsCalled := make(chan struct{}, 1)
	producer, err := newCtldRuntimeMetricsProducer(&config.CtldConfig{
		StorageProxyConfig:                          config.StorageProxyConfig{RegionID: "region-a", DefaultClusterId: "cluster-a"},
		SandboxObservabilityRuntimeSamplesIngestURL: server.URL + "/internal/v1/sandbox-observability/runtime-samples",
		SandboxObservabilityIngestQueueSize:         10,
		SandboxObservabilityIngestBatchSize:         100,
		SandboxObservabilityIngestFlushInterval:     metav1.Duration{Duration: time.Hour},
		SandboxObservabilityIngestRequestTimeout:    metav1.Duration{Duration: time.Second},
		SandboxObservabilityIngestMaxRetries:        1,
		SandboxObservabilityIngestRetryBackoff:      metav1.Duration{Duration: time.Millisecond},
		SandboxObservabilityRuntimeSampleInterval:   metav1.Duration{Duration: time.Minute},
		SandboxObservabilityRuntimeSampleJitter:     metav1.Duration{Duration: time.Second},
	}, staticStatsClient{onCall: statsCalled, stats: []*runtimeapi.PodSandboxStats{{
		Attributes: &runtimeapi.PodSandboxAttributes{
			Id:       "cri-sandbox-a",
			Metadata: &runtimeapi.PodSandboxMetadata{Namespace: "ns-a", Name: "pod-a", Uid: "pod-uid-a"},
		},
		Linux: &runtimeapi.LinuxPodSandboxStats{},
	}}}, testPodLister(t, pod), generator, nil, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handle := startCtldRuntimeMetricLoops(ctx, producer.worker.Run, producer.collector.Run)
	select {
	case <-statsCalled:
	case <-time.After(time.Second):
		t.Fatal("collector did not request CRI stats")
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, handle.Shutdown(shutdownCtx))
	shutdownCancel()

	select {
	case sample := <-received:
		assert.Equal(t, "team-a", sample.TeamID)
		assert.Equal(t, "sandbox-a", sample.SandboxID)
		assert.Equal(t, int64(4), sample.RuntimeGeneration)
		assert.Equal(t, "cri-sandbox-a", sample.SeriesEpoch)
		assert.NotEmpty(t, sample.SampleID)
	case <-time.After(2 * time.Second):
		t.Fatal("runtime sample was not posted")
	}
}

func TestCtldRuntimeMetricsShutdownStopsCollectorBeforeWorkerDrain(t *testing.T) {
	queue := make(chan string, 1)
	var mu sync.Mutex
	order := []string{}
	drained := make(chan string, 1)
	handle := startCtldRuntimeMetricLoops(context.Background(), func(ctx context.Context) {
		<-ctx.Done()
		for {
			select {
			case item := <-queue:
				drained <- item
			default:
				mu.Lock()
				order = append(order, "worker")
				mu.Unlock()
				return
			}
		}
	}, func(ctx context.Context) {
		<-ctx.Done()
		queue <- "final-sample"
		mu.Lock()
		order = append(order, "collector")
		mu.Unlock()
	})
	handle.statsClose = func() error {
		mu.Lock()
		order = append(order, "stats")
		mu.Unlock()
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, handle.Shutdown(shutdownCtx))

	select {
	case item := <-drained:
		assert.Equal(t, "final-sample", item)
	default:
		t.Fatal("worker did not drain the collector's final sample")
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"collector", "worker", "stats"}, order)
}

type staticStatsClient struct {
	stats  []*runtimeapi.PodSandboxStats
	onCall chan<- struct{}
}

func (c staticStatsClient) ListPodSandboxes(context.Context) ([]*runtimeapi.PodSandbox, error) {
	items := make([]*runtimeapi.PodSandbox, 0, len(c.stats))
	for _, stats := range c.stats {
		if stats == nil || stats.Attributes == nil {
			continue
		}
		items = append(items, &runtimeapi.PodSandbox{
			Id:       stats.Attributes.Id,
			Metadata: stats.Attributes.Metadata,
			State:    runtimeapi.PodSandboxState_SANDBOX_READY,
		})
	}
	return items, nil
}

func (c staticStatsClient) PodSandboxStats(_ context.Context, id string) (*runtimeapi.PodSandboxStats, error) {
	if c.onCall != nil {
		c.onCall <- struct{}{}
	}
	for _, stats := range c.stats {
		if stats != nil && stats.Attributes != nil && stats.Attributes.Id == id {
			return stats, nil
		}
	}
	return nil, nil
}

var _ ctldruntimemetrics.StatsClient = staticStatsClient{}

func testPodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		require.NoError(t, indexer.Add(pod))
	}
	return corelisters.NewPodLister(indexer)
}
