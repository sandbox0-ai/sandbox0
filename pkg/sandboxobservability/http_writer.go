package sandboxobservability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type HTTPWriterOptions struct {
	EventsURL      string
	LogsURL        string
	MetricsURL     string
	Client         *http.Client
	TokenProvider  func(context.Context) (string, error)
	RequestTimeout time.Duration
}

// HTTPWriter posts observability ingest batches to cluster-gateway internal APIs.
type HTTPWriter struct {
	eventsURL      string
	logsURL        string
	metricsURL     string
	client         *http.Client
	tokenProvider  func(context.Context) (string, error)
	requestTimeout time.Duration
}

func NewHTTPWriter(opts HTTPWriterOptions) *HTTPWriter {
	client := opts.Client
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPWriter{
		eventsURL:      strings.TrimSpace(opts.EventsURL),
		logsURL:        strings.TrimSpace(opts.LogsURL),
		metricsURL:     strings.TrimSpace(opts.MetricsURL),
		client:         client,
		tokenProvider:  opts.TokenProvider,
		requestTimeout: opts.RequestTimeout,
	}
}

func (w *HTTPWriter) InsertEvents(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	return w.post(ctx, w.eventsURL, struct {
		Events []Event `json:"events"`
	}{Events: events})
}

func (w *HTTPWriter) InsertLogs(ctx context.Context, logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	return w.post(ctx, w.logsURL, struct {
		Logs []LogEntry `json:"logs"`
	}{Logs: logs})
}

func (w *HTTPWriter) InsertMetricSamples(ctx context.Context, samples []MetricSample) error {
	if len(samples) == 0 {
		return nil
	}
	return w.post(ctx, w.metricsURL, struct {
		Samples []MetricSample `json:"samples"`
	}{Samples: samples})
}

func (w *HTTPWriter) post(ctx context.Context, endpoint string, body any) error {
	if w == nil || endpoint == "" {
		return ErrBackendDisabled
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal sandbox observability ingest body: %w", err)
	}
	requestCtx := ctx
	cancel := func() {}
	if w.requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, w.requestTimeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create sandbox observability ingest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if w.tokenProvider != nil {
		token, err := w.tokenProvider(ctx)
		if err != nil {
			return fmt.Errorf("generate sandbox observability ingest token: %w", err)
		}
		req.Header.Set(internalauth.DefaultTokenHeader, token)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: post sandbox observability ingest: %v", ErrBackendUnavailable, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: sandbox observability ingest returned status %d", ErrBackendUnavailable, resp.StatusCode)
	}
	return nil
}
