package deletionwebhook

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type webhookRoundTripFunc func(*http.Request) (*http.Response, error)

func (f webhookRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type sandboxDeletionWebhookRetryRecord struct {
	eventID   string
	workerID  string
	status    int
	lastError string
	delay     time.Duration
}

type sandboxDeletionWebhookTerminalRecord struct {
	eventID   string
	workerID  string
	status    int
	reason    string
	lastError string
}

func TestSignWebhookPayload(t *testing.T) {
	assert.Equal(t, "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4", signWebhookPayload("secret", []byte("payload")))
}

type fakeSandboxDeletionWebhookOutbox struct {
	mu sync.Mutex

	deliveries []sandboxDeletionWebhookDelivery
	delivered  []sandboxDeletionWebhookTerminalRecord
	retries    []sandboxDeletionWebhookRetryRecord
	terminal   []sandboxDeletionWebhookTerminalRecord

	expiredCount int64
	purgedCount  int64
	purgeAfter   time.Duration
	claimLimit   int
	claimTTL     time.Duration
	claimWorker  string
	statsValue   sandboxDeletionWebhookOutboxStats
}

func (f *fakeSandboxDeletionWebhookOutbox) claim(_ context.Context, workerID string, limit int, claimTTL time.Duration) ([]sandboxDeletionWebhookDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimWorker = workerID
	f.claimLimit = limit
	f.claimTTL = claimTTL
	deliveries := append([]sandboxDeletionWebhookDelivery(nil), f.deliveries...)
	f.deliveries = nil
	return deliveries, nil
}

func (f *fakeSandboxDeletionWebhookOutbox) markDelivered(_ context.Context, eventID, workerID string, status int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, sandboxDeletionWebhookTerminalRecord{
		eventID:  eventID,
		workerID: workerID,
		status:   status,
	})
	return nil
}

func (f *fakeSandboxDeletionWebhookOutbox) markTerminal(_ context.Context, eventID, workerID string, status int, reason, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.terminal = append(f.terminal, sandboxDeletionWebhookTerminalRecord{
		eventID:   eventID,
		workerID:  workerID,
		status:    status,
		reason:    reason,
		lastError: lastError,
	})
	return nil
}

func (f *fakeSandboxDeletionWebhookOutbox) recordRetry(_ context.Context, eventID, workerID string, status int, lastError string, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries = append(f.retries, sandboxDeletionWebhookRetryRecord{
		eventID:   eventID,
		workerID:  workerID,
		status:    status,
		lastError: lastError,
		delay:     delay,
	})
	return nil
}

func (f *fakeSandboxDeletionWebhookOutbox) expire(context.Context) (int64, error) {
	return f.expiredCount, nil
}

func (f *fakeSandboxDeletionWebhookOutbox) purge(_ context.Context, retention time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purgeAfter = retention
	return f.purgedCount, nil
}

func (f *fakeSandboxDeletionWebhookOutbox) stats(context.Context) (*sandboxDeletionWebhookOutboxStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stats := f.statsValue
	return &stats, nil
}

func TestSandboxDeletionWebhookDispatcherDeliversPersistedRequest(t *testing.T) {
	payload := []byte(`{"event_id":"evt_sandbox_deleted_sandbox-1"}`)
	signature := signWebhookPayload("secret", payload)
	var receivedBody []byte
	var receivedSignature string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var err error
		receivedBody, err = io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("read webhook body: %v", err)
		}
		receivedSignature = req.Header.Get("X-Sandbox0-Signature")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
		EventID:   "evt_sandbox_deleted_sandbox-1",
		SandboxID: "sandbox-1",
		TargetURL: server.URL,
		Payload:   payload,
		Signature: signature,
		ExpiresAt: time.Now().Add(time.Hour),
	}}}
	dispatcher := newSandboxDeletionWebhookDispatcher(repo, server.Client(), SandboxDeletionWebhookDispatcherConfig{
		WorkerID:    "worker-1",
		BatchSize:   1,
		Concurrency: 1,
	}, zap.NewNop())

	require.NoError(t, dispatcher.RunOnce(t.Context()))
	require.Len(t, repo.delivered, 1)
	assert.Equal(t, "evt_sandbox_deleted_sandbox-1", repo.delivered[0].eventID)
	assert.Equal(t, "worker-1", repo.delivered[0].workerID)
	assert.Equal(t, http.StatusNoContent, repo.delivered[0].status)
	assert.Empty(t, repo.retries)
	assert.Empty(t, repo.terminal)
	assert.Equal(t, payload, receivedBody)
	assert.Equal(t, signature, receivedSignature)
	assert.Equal(t, defaultSandboxDeletionWebhookTerminalRetention, repo.purgeAfter)
}

func TestSandboxDeletionWebhookDispatcherDefersTransientResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		http.Error(w, "try again", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
		EventID:   "event-1",
		SandboxID: "sandbox-1",
		TargetURL: server.URL,
		Payload:   []byte(`{}`),
		ExpiresAt: time.Now().Add(time.Hour),
	}}}
	dispatcher := newSandboxDeletionWebhookDispatcher(repo, server.Client(), SandboxDeletionWebhookDispatcherConfig{WorkerID: "worker-1"}, zap.NewNop())

	require.NoError(t, dispatcher.RunOnce(t.Context()))
	require.Len(t, repo.retries, 1)
	assert.Equal(t, http.StatusServiceUnavailable, repo.retries[0].status)
	assert.Equal(t, 120*time.Second, repo.retries[0].delay)
	assert.Empty(t, repo.delivered)
	assert.Empty(t, repo.terminal)
}

func TestSandboxDeletionWebhookDispatcherRetriesEveryTransientStatus(t *testing.T) {
	for _, status := range []int{
		http.StatusRequestTimeout,
		http.StatusTooEarly,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &http.Client{Transport: webhookRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: status,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("try again")),
				}, nil
			})}
			repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
				EventID:   "event-1",
				SandboxID: "sandbox-1",
				TargetURL: "https://example.test/webhook",
				Payload:   []byte(`{}`),
				ExpiresAt: time.Now().Add(time.Hour),
			}}}
			dispatcher := newSandboxDeletionWebhookDispatcher(repo, client, SandboxDeletionWebhookDispatcherConfig{WorkerID: "worker-1"}, zap.NewNop())

			require.NoError(t, dispatcher.RunOnce(t.Context()))
			require.Len(t, repo.retries, 1)
			assert.Equal(t, status, repo.retries[0].status)
			assert.Empty(t, repo.terminal)
		})
	}
}

func TestSandboxDeletionWebhookDispatcherRetriesTransportFailures(t *testing.T) {
	for name, transportErr := range map[string]error{
		"dns":                &net.DNSError{Err: "no such host", Name: "retired.example.test"},
		"connection_refused": fmt.Errorf("dial webhook: %w", syscall.ECONNREFUSED),
		"timeout":            context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			client := &http.Client{Transport: webhookRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			})}
			repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
				EventID:   "event-1",
				SandboxID: "sandbox-1",
				TargetURL: "https://example.test/webhook",
				Payload:   []byte(`{}`),
				ExpiresAt: time.Now().Add(time.Hour),
			}}}
			dispatcher := newSandboxDeletionWebhookDispatcher(repo, client, SandboxDeletionWebhookDispatcherConfig{WorkerID: "worker-1"}, zap.NewNop())

			require.NoError(t, dispatcher.RunOnce(t.Context()))
			require.Len(t, repo.retries, 1)
			assert.Zero(t, repo.retries[0].status)
			assert.Contains(t, repo.retries[0].lastError, transportErr.Error())
			assert.Empty(t, repo.terminal)
		})
	}
}

func TestSandboxDeletionWebhookDispatcherStopsOnPermanentResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid", http.StatusBadRequest)
	}))
	defer server.Close()

	repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
		EventID:   "event-1",
		SandboxID: "sandbox-1",
		TargetURL: server.URL,
		Payload:   []byte(`{}`),
		ExpiresAt: time.Now().Add(time.Hour),
	}}}
	dispatcher := newSandboxDeletionWebhookDispatcher(repo, server.Client(), SandboxDeletionWebhookDispatcherConfig{WorkerID: "worker-1"}, zap.NewNop())

	require.NoError(t, dispatcher.RunOnce(t.Context()))
	require.Len(t, repo.terminal, 1)
	assert.Equal(t, http.StatusBadRequest, repo.terminal[0].status)
	assert.Equal(t, "permanent_http_status", repo.terminal[0].reason)
	assert.Empty(t, repo.retries)
}

func TestSandboxDeletionWebhookDispatcherBoundsEachRequest(t *testing.T) {
	client := &http.Client{Transport: webhookRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
		EventID:   "event-1",
		SandboxID: "sandbox-1",
		TargetURL: "https://example.test/webhook",
		Payload:   []byte(`{}`),
		ExpiresAt: time.Now().Add(time.Hour),
	}}}
	dispatcher := newSandboxDeletionWebhookDispatcher(repo, client, SandboxDeletionWebhookDispatcherConfig{
		WorkerID:       "worker-1",
		RequestTimeout: 20 * time.Millisecond,
	}, zap.NewNop())

	started := time.Now()
	require.NoError(t, dispatcher.RunOnce(t.Context()))
	assert.Less(t, time.Since(started), time.Second)
	require.Len(t, repo.retries, 1)
	assert.Zero(t, repo.retries[0].status)
	assert.Contains(t, repo.retries[0].lastError, context.DeadlineExceeded.Error())
}

func TestSandboxDeletionWebhookDispatcherDoesNotAttemptExpiredDelivery(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: webhookRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, assert.AnError
	})}
	repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
		EventID:   "event-1",
		SandboxID: "sandbox-1",
		TargetURL: "https://example.test/webhook",
		Payload:   []byte(`{}`),
		ExpiresAt: time.Now().Add(-time.Second),
	}}}
	dispatcher := newSandboxDeletionWebhookDispatcher(repo, client, SandboxDeletionWebhookDispatcherConfig{WorkerID: "worker-1"}, zap.NewNop())

	require.NoError(t, dispatcher.RunOnce(t.Context()))
	assert.Zero(t, calls)
	require.Len(t, repo.terminal, 1)
	assert.Equal(t, "delivery_deadline_exceeded", repo.terminal[0].reason)
	assert.Empty(t, repo.retries)
}

func TestSandboxDeletionWebhookDispatcherRejectsInvalidTargetWithoutRetry(t *testing.T) {
	repo := &fakeSandboxDeletionWebhookOutbox{deliveries: []sandboxDeletionWebhookDelivery{{
		EventID:   "event-1",
		SandboxID: "sandbox-1",
		TargetURL: "file:///tmp/webhook",
		Payload:   []byte(`{}`),
		ExpiresAt: time.Now().Add(time.Hour),
	}}}
	dispatcher := newSandboxDeletionWebhookDispatcher(repo, nil, SandboxDeletionWebhookDispatcherConfig{WorkerID: "worker-1"}, zap.NewNop())

	require.NoError(t, dispatcher.RunOnce(t.Context()))
	require.Len(t, repo.terminal, 1)
	assert.Equal(t, "invalid_target_url", repo.terminal[0].reason)
	assert.Empty(t, repo.retries)
}

func TestSandboxDeletionWebhookRetryPolicyIsBounded(t *testing.T) {
	assert.Equal(t, time.Second, sandboxDeletionWebhookBackoff(1, time.Second, 5*time.Minute))
	assert.Equal(t, 2*time.Second, sandboxDeletionWebhookBackoff(2, time.Second, 5*time.Minute))
	assert.Equal(t, 5*time.Minute, sandboxDeletionWebhookBackoff(100, time.Second, 5*time.Minute))

	now := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, 90*time.Second, parseWebhookRetryAfter("90", now))
	assert.Equal(t, 2*time.Minute, parseWebhookRetryAfter(now.Add(2*time.Minute).Format(http.TimeFormat), now))
	assert.Zero(t, parseWebhookRetryAfter("invalid", now))
	assert.Equal(t, 24*time.Hour, sandboxDeletionWebhookDeliveryWindow)

	cfg := normalizeSandboxDeletionWebhookDispatcherConfig(SandboxDeletionWebhookDispatcherConfig{
		BatchSize:      10_000,
		Concurrency:    10_000,
		RequestTimeout: time.Hour,
		WorkerID:       "worker-1",
	})
	assert.Equal(t, maxSandboxDeletionWebhookBatchSize, cfg.BatchSize)
	assert.Equal(t, maxSandboxDeletionWebhookConcurrency, cfg.Concurrency)
	assert.Equal(t, maxSandboxDeletionWebhookRequestTimeout, cfg.RequestTimeout)
}

func TestSandboxDeletionWebhookDispatcherExposesFailureAndQueueMetrics(t *testing.T) {
	now := time.Now().UTC()
	client := &http.Client{Transport: webhookRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("try again")),
		}, nil
	})}
	repo := &fakeSandboxDeletionWebhookOutbox{
		deliveries: []sandboxDeletionWebhookDelivery{{
			EventID:   "event-1",
			SandboxID: "sandbox-1",
			TargetURL: "https://example.test/webhook",
			Payload:   []byte(`{}`),
			ExpiresAt: now.Add(time.Hour),
		}},
		expiredCount: 2,
		statsValue: sandboxDeletionWebhookOutboxStats{
			Pending:       3,
			Due:           2,
			Claimed:       1,
			Delivered:     4,
			Terminal:      5,
			OldestPending: now.Add(-10 * time.Minute),
		},
	}
	dispatcher := newSandboxDeletionWebhookDispatcher(repo, client, SandboxDeletionWebhookDispatcherConfig{WorkerID: "worker-1"}, zap.NewNop())
	dispatcher.now = func() time.Time { return now }
	metrics := obsmetrics.NewManager(prometheus.NewRegistry())
	dispatcher.SetMetrics(metrics)

	require.NoError(t, dispatcher.RunOnce(t.Context()))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.SandboxDeletionWebhookAttemptsTotal.WithLabelValues("retry", "5xx")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.SandboxDeletionWebhookExpiredTotal))
	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("pending")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("due")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("claimed")))
	assert.Equal(t, float64(4), testutil.ToFloat64(metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("delivered")))
	assert.Equal(t, float64(5), testutil.ToFloat64(metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("terminal")))
	assert.Equal(t, float64(600), testutil.ToFloat64(metrics.SandboxDeletionWebhookOldestPendingAge))
}

var _ sandboxDeletionWebhookOutboxRepository = (*fakeSandboxDeletionWebhookOutbox)(nil)
