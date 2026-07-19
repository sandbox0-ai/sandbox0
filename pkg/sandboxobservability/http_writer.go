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
	EventsURL         string
	LogsURL           string
	RuntimeSamplesURL string
	Client            *http.Client
	TokenProvider     func(context.Context) (string, error)
	TeamTokenProvider func(context.Context, string) (string, error)
	RequestTimeout    time.Duration
}

// HTTPWriter posts observability ingest batches to cluster-gateway internal APIs.
type HTTPWriter struct {
	eventsURL         string
	logsURL           string
	runtimeSamplesURL string
	client            *http.Client
	tokenProvider     func(context.Context) (string, error)
	teamTokenProvider func(context.Context, string) (string, error)
	requestTimeout    time.Duration
}

func NewHTTPWriter(opts HTTPWriterOptions) *HTTPWriter {
	client := opts.Client
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPWriter{
		eventsURL:         strings.TrimSpace(opts.EventsURL),
		logsURL:           strings.TrimSpace(opts.LogsURL),
		runtimeSamplesURL: strings.TrimSpace(opts.RuntimeSamplesURL),
		client:            client,
		tokenProvider:     opts.TokenProvider,
		teamTokenProvider: opts.TeamTokenProvider,
		requestTimeout:    opts.RequestTimeout,
	}
}

func (w *HTTPWriter) InsertEvents(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	return w.post(ctx, w.eventsURL, struct {
		Events []Event `json:"events"`
	}{Events: events}, w.tokenProvider)
}

func (w *HTTPWriter) InsertLogs(ctx context.Context, logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	teamID, err := logBatchTeamID(logs)
	if err != nil {
		return err
	}
	return w.post(ctx, w.logsURL, struct {
		Logs []LogEntry `json:"logs"`
	}{Logs: logs}, w.teamToken(teamID))
}

func (w *HTTPWriter) InsertRuntimeSamples(ctx context.Context, samples []RuntimeSample) error {
	if len(samples) == 0 {
		return nil
	}
	teamID, err := runtimeSampleBatchTeamID(samples)
	if err != nil {
		return err
	}
	return w.post(ctx, w.runtimeSamplesURL, struct {
		Samples []RuntimeSample `json:"samples"`
	}{Samples: samples}, w.teamToken(teamID))
}

func (w *HTTPWriter) teamToken(teamID string) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		if w == nil || w.teamTokenProvider == nil {
			return "", fmt.Errorf("team token provider is not configured")
		}
		return w.teamTokenProvider(ctx, teamID)
	}
}

func (w *HTTPWriter) post(
	ctx context.Context,
	endpoint string,
	body any,
	tokenProvider func(context.Context) (string, error),
) error {
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
	if tokenProvider != nil {
		token, err := tokenProvider(requestCtx)
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

func logBatchTeamID(logs []LogEntry) (string, error) {
	teamID := strings.TrimSpace(logs[0].TeamID)
	if teamID == "" {
		return "", fmt.Errorf("sandbox observability log batch has empty team_id")
	}
	for i := 1; i < len(logs); i++ {
		if strings.TrimSpace(logs[i].TeamID) != teamID {
			return "", fmt.Errorf("sandbox observability log batch spans multiple teams")
		}
	}
	return teamID, nil
}

func runtimeSampleBatchTeamID(samples []RuntimeSample) (string, error) {
	teamID := strings.TrimSpace(samples[0].TeamID)
	if teamID == "" {
		return "", fmt.Errorf("sandbox runtime sample batch has empty team_id")
	}
	for i := 1; i < len(samples); i++ {
		if strings.TrimSpace(samples[i].TeamID) != teamID {
			return "", fmt.Errorf("sandbox runtime sample batch spans multiple teams")
		}
	}
	return teamID, nil
}
