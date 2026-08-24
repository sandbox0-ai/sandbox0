package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCtldRuntimeMetricsProducerPostsAuthorizedNomadRuntimeSample(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target: "cluster-gateway", PublicKey: publicKey, AllowedCallers: []string{"ctld"},
	})
	received := make(chan sandboxobservability.RuntimeSample, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, http.MethodPost, request.Method)
		assert.Equal(t, "/internal/v1/sandbox-observability/runtime-samples", request.URL.Path)
		claims, validateErr := validator.Validate(request.Header.Get(internalauth.DefaultTokenHeader))
		if validateErr != nil {
			http.Error(writer, validateErr.Error(), http.StatusUnauthorized)
			return
		}
		assert.Equal(t, "ctld", claims.Caller)
		assert.Contains(t, claims.Permissions, authn.PermSandboxObservabilityWrite)
		var body struct {
			Samples []sandboxobservability.RuntimeSample `json:"samples"`
		}
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		require.Len(t, body.Samples, 1)
		received <- body.Samples[0]
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	target := nomadruntime.RuntimeMetricTarget{
		Version: nomadruntime.RuntimeMetricTargetVersion,
		TeamID:  "team-a", SandboxID: "sandbox-a", RuntimeGeneration: 4,
		CPUMillicpu: 500, MemoryMiB: 1024,
		AllocationID: "allocation-a", NodeBootID: "boot-a", LaunchAttempt: "launch-a",
		RunscContainerID: "runsc-a", BindingDigest: strings.Repeat("a", 64),
	}
	target.SeriesEpoch = nomadruntime.RuntimeMetricSeriesEpoch(
		target.AllocationID, target.NodeBootID, target.LaunchAttempt, target.RunscContainerID,
	)
	client := &staticRuntimeMetricClient{
		target: target,
		sample: nomadruntime.RuntimeMetricSample{
			Version: nomadruntime.RuntimeMetricSampleVersion, ObservedAt: time.Now().UTC(),
			Stats: gvisorcli.RunscStats{Type: "stats", ID: target.RunscContainerID},
		},
		called: make(chan struct{}, 1),
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "ctld", PrivateKey: privateKey, TTL: time.Minute})
	producer, err := newCtldRuntimeMetricsProducer(testRuntimeMetricConfig(server.URL), client, generator, nil, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handle := startCtldRuntimeMetricLoops(ctx, producer.worker.Run, producer.collector.Run)
	select {
	case <-client.called:
	case <-time.After(time.Second):
		t.Fatal("collector did not request runsc stats")
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, handle.Shutdown(shutdownCtx))
	shutdownCancel()

	select {
	case sample := <-received:
		assert.Equal(t, target.TeamID, sample.TeamID)
		assert.Equal(t, target.SandboxID, sample.SandboxID)
		assert.Equal(t, target.RuntimeGeneration, sample.RuntimeGeneration)
		assert.Equal(t, target.SeriesEpoch, sample.SeriesEpoch)
		assert.NotEmpty(t, sample.SampleID)
	case <-time.After(2 * time.Second):
		t.Fatal("runtime sample was not posted")
	}
}

func testRuntimeMetricConfig(baseURL string) *config.CtldConfig {
	return &config.CtldConfig{
		RegionID: "region-a", DefaultClusterId: "cluster-a",
		SandboxObservabilityRuntimeSamplesIngestURL: baseURL + "/internal/v1/sandbox-observability/runtime-samples",
		SandboxObservabilityIngestQueueSize:         10, SandboxObservabilityIngestBatchSize: 100,
		SandboxObservabilityIngestFlushInterval:   config.Duration{Duration: time.Hour},
		SandboxObservabilityIngestRequestTimeout:  config.Duration{Duration: time.Second},
		SandboxObservabilityIngestMaxRetries:      1,
		SandboxObservabilityIngestRetryBackoff:    config.Duration{Duration: time.Millisecond},
		SandboxObservabilityRuntimeSampleInterval: config.Duration{Duration: time.Minute},
		SandboxObservabilityRuntimeSampleJitter:   config.Duration{Duration: time.Second},
	}
}

type staticRuntimeMetricClient struct {
	target nomadruntime.RuntimeMetricTarget
	sample nomadruntime.RuntimeMetricSample
	called chan struct{}
}

func (c *staticRuntimeMetricClient) ListRuntimeMetricTargets(context.Context) ([]nomadruntime.RuntimeMetricTarget, error) {
	return []nomadruntime.RuntimeMetricTarget{c.target}, nil
}

func (c *staticRuntimeMetricClient) RuntimeMetricStats(context.Context, nomadruntime.RuntimeMetricTarget) (nomadruntime.RuntimeMetricSample, error) {
	select {
	case c.called <- struct{}{}:
	default:
	}
	return c.sample, nil
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
