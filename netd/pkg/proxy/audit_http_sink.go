package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type httpAuditSink struct {
	endpoint       string
	regionID       string
	clusterID      string
	client         *http.Client
	generator      *internalauth.Generator
	queue          chan auditEvent
	batchSize      int
	flushInterval  time.Duration
	requestTimeout time.Duration
	maxRetries     int
	retryBackoff   time.Duration
	cancel         context.CancelFunc
	done           chan struct{}
}

type httpAuditSinkOptions struct {
	Endpoint       string
	RegionID       string
	ClusterID      string
	Client         *http.Client
	Generator      *internalauth.Generator
	QueueSize      int
	BatchSize      int
	FlushInterval  time.Duration
	RequestTimeout time.Duration
	MaxRetries     int
	RetryBackoff   time.Duration
}

func newHTTPAuditSinkFromConfig(cfg *config.NetdConfig) (*httpAuditSink, error) {
	if cfg == nil || strings.TrimSpace(cfg.SandboxObservabilityIngestURL) == "" {
		return nil, nil
	}
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load netd internal jwt private key: %w", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "netd",
		PrivateKey: privateKey,
		TTL:        10 * time.Second,
	})
	return newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint:       cfg.SandboxObservabilityIngestURL,
		RegionID:       cfg.RegionID,
		ClusterID:      cfg.ClusterID,
		Client:         &http.Client{},
		Generator:      generator,
		QueueSize:      cfg.SandboxObservabilityIngestQueueSize,
		BatchSize:      cfg.SandboxObservabilityIngestBatchSize,
		FlushInterval:  cfg.SandboxObservabilityIngestFlushInterval.Duration,
		RequestTimeout: cfg.SandboxObservabilityIngestRequestTimeout.Duration,
		MaxRetries:     cfg.SandboxObservabilityIngestMaxRetries,
		RetryBackoff:   cfg.SandboxObservabilityIngestRetryBackoff.Duration,
	}), nil
}

func newHTTPAuditSink(opts httpAuditSinkOptions) *httpAuditSink {
	queueSize := opts.QueueSize
	if queueSize <= 0 {
		queueSize = 1024
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	flushInterval := opts.FlushInterval
	if flushInterval <= 0 {
		flushInterval = time.Second
	}
	requestTimeout := opts.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = 2 * time.Second
	}
	retryBackoff := opts.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = 100 * time.Millisecond
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	sink := &httpAuditSink{
		endpoint:       strings.TrimSpace(opts.Endpoint),
		regionID:       strings.TrimSpace(opts.RegionID),
		clusterID:      strings.TrimSpace(opts.ClusterID),
		client:         client,
		generator:      opts.Generator,
		queue:          make(chan auditEvent, queueSize),
		batchSize:      batchSize,
		flushInterval:  flushInterval,
		requestTimeout: requestTimeout,
		maxRetries:     opts.MaxRetries,
		retryBackoff:   retryBackoff,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
	go sink.run(ctx)
	return sink
}

func (s *httpAuditSink) WriteAuditEvent(event auditEvent) error {
	if s == nil || s.endpoint == "" {
		return nil
	}
	select {
	case s.queue <- event:
		proxyMetrics.RecordAuditIngestEvents("enqueued", 1)
	default:
		proxyMetrics.RecordAuditIngestEvents("dropped", 1)
	}
	return nil
}

func (s *httpAuditSink) Close() error {
	if s == nil || s.cancel == nil {
		return nil
	}
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
	}
	return nil
}

func (s *httpAuditSink) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	batch := make([]auditEvent, 0, s.batchSize)
	flush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			return
		}
		s.flushBatch(flushCtx, batch)
		batch = make([]auditEvent, 0, s.batchSize)
	}

	for {
		select {
		case event := <-s.queue:
			batch = append(batch, event)
			if len(batch) >= s.batchSize {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		case <-ctx.Done():
			for {
				select {
				case event := <-s.queue:
					batch = append(batch, event)
				default:
					shutdownCtx, cancel := context.WithTimeout(context.Background(), s.requestTimeout)
					flush(shutdownCtx)
					cancel()
					return
				}
			}
		}
	}
}

func (s *httpAuditSink) flushBatch(ctx context.Context, batch []auditEvent) {
	events := make([]sandboxobservability.Event, 0, len(batch))
	for _, event := range batch {
		projected, ok := s.toObservabilityEvent(event)
		if !ok {
			proxyMetrics.RecordAuditIngestEvents("skipped", 1)
			continue
		}
		events = append(events, projected)
	}
	if len(events) == 0 {
		return
	}

	body, err := json.Marshal(struct {
		Events []sandboxobservability.Event `json:"events"`
	}{Events: events})
	if err != nil {
		proxyMetrics.RecordAuditIngestBatch("failed")
		proxyMetrics.RecordAuditIngestEvents("dropped", len(events))
		return
	}

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if err := s.postEvents(ctx, body); err == nil {
			proxyMetrics.RecordAuditIngestBatch("sent")
			proxyMetrics.RecordAuditIngestEvents("sent", len(events))
			return
		}
		if attempt == s.maxRetries || !sleepWithContext(ctx, s.retryBackoff) {
			proxyMetrics.RecordAuditIngestBatch("failed")
			proxyMetrics.RecordAuditIngestEvents("dropped", len(events))
			return
		}
	}
}

func (s *httpAuditSink) postEvents(ctx context.Context, body []byte) error {
	requestCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.generator != nil {
		token, err := s.generator.GenerateSystem("cluster-gateway", internalauth.GenerateOptions{
			Permissions: []string{authn.PermSandboxObservabilityWrite},
		})
		if err != nil {
			return err
		}
		req.Header.Set(internalauth.DefaultTokenHeader, token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ingest returned status %d", resp.StatusCode)
	}
	return nil
}

func (s *httpAuditSink) toObservabilityEvent(event auditEvent) (sandboxobservability.Event, bool) {
	if event.TeamID == "" || event.SandboxID == "" {
		return sandboxobservability.Event{}, false
	}
	timestamp := event.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	timestamp = timestamp.UTC()
	cursorSuffix := event.FlowID
	if cursorSuffix == "" {
		cursorSuffix = "event"
	}
	cursor := fmt.Sprintf("netd:%s:%d", cursorSuffix, timestamp.UnixNano())
	outcome := sandboxobservability.Outcome(event.Outcome)
	if outcome == "" || !sandboxobservability.ValidOutcome(outcome) {
		outcome = sandboxobservability.OutcomeCompleted
	}
	return sandboxobservability.Event{
		TeamID:     event.TeamID,
		SandboxID:  event.SandboxID,
		RegionID:   s.regionID,
		ClusterID:  s.clusterID,
		OccurredAt: timestamp,
		Source:     sandboxobservability.SourceNetd,
		EventType:  sandboxobservability.EventTypeNetworkAudit,
		Outcome:    outcome,
		Cursor:     cursor,
		Watermark:  cursor,
		Attributes: auditEventAttributes(event),
	}, true
}

func auditEventAttributes(event auditEvent) map[string]any {
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil
	}
	var attributes map[string]any
	if err := json.Unmarshal(encoded, &attributes); err != nil {
		return nil
	}
	delete(attributes, "timestamp")
	delete(attributes, "team_id")
	delete(attributes, "sandbox_id")
	return attributes
}

func sleepWithContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
