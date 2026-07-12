package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

type httpAuditSink struct {
	endpoint         string
	regionID         string
	clusterID        string
	producerInstance string
	client           *http.Client
	generator        *internalauth.Generator
	logger           *zap.Logger
	queue            chan auditEvent
	spool            *auditSpool
	queuedMu         sync.Mutex
	queued           map[string]struct{}
	batchSize        int
	flushInterval    time.Duration
	requestTimeout   time.Duration
	maxRetries       int
	retryBackoff     time.Duration
	cancel           context.CancelFunc
	done             chan struct{}
	spoolCorrupt     atomic.Bool
}

type httpAuditSinkOptions struct {
	Endpoint         string
	RegionID         string
	ClusterID        string
	ProducerInstance string
	Spool            *auditSpool
	Client           *http.Client
	Generator        *internalauth.Generator
	Logger           *zap.Logger
	QueueSize        int
	BatchSize        int
	FlushInterval    time.Duration
	RequestTimeout   time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
}

func newHTTPAuditSinkFromConfig(cfg *config.NetdConfig, logger *zap.Logger) (*httpAuditSink, error) {
	if cfg == nil || strings.TrimSpace(cfg.SandboxObservabilityIngestURL) == "" {
		return nil, nil
	}
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultAuditJWTPrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load netd internal jwt private key: %w", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "netd",
		PrivateKey: privateKey,
		TTL:        10 * time.Second,
	})
	spool, err := newAuditSpool(cfg.SandboxObservabilityAuditSpoolDir)
	if err != nil {
		return nil, err
	}
	if spool == nil {
		return nil, fmt.Errorf("sandbox audit ingest requires a durable spool directory")
	}
	return newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint:         cfg.SandboxObservabilityIngestURL,
		RegionID:         cfg.RegionID,
		ClusterID:        cfg.ClusterID,
		ProducerInstance: cfg.NodeName,
		Spool:            spool,
		Client:           &http.Client{},
		Generator:        generator,
		Logger:           logger,
		QueueSize:        cfg.SandboxObservabilityIngestQueueSize,
		BatchSize:        cfg.SandboxObservabilityIngestBatchSize,
		FlushInterval:    cfg.SandboxObservabilityIngestFlushInterval.Duration,
		RequestTimeout:   cfg.SandboxObservabilityIngestRequestTimeout.Duration,
		MaxRetries:       cfg.SandboxObservabilityIngestMaxRetries,
		RetryBackoff:     cfg.SandboxObservabilityIngestRetryBackoff.Duration,
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
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	ctx, cancel := context.WithCancel(context.Background())
	sink := &httpAuditSink{
		endpoint:         strings.TrimSpace(opts.Endpoint),
		regionID:         strings.TrimSpace(opts.RegionID),
		clusterID:        strings.TrimSpace(opts.ClusterID),
		producerInstance: strings.TrimSpace(opts.ProducerInstance),
		client:           client,
		generator:        opts.Generator,
		logger:           logger,
		queue:            make(chan auditEvent, queueSize),
		spool:            opts.Spool,
		queued:           make(map[string]struct{}),
		batchSize:        batchSize,
		flushInterval:    flushInterval,
		requestTimeout:   requestTimeout,
		maxRetries:       opts.MaxRetries,
		retryBackoff:     retryBackoff,
		cancel:           cancel,
		done:             make(chan struct{}),
	}
	go sink.run(ctx)
	return sink
}

func (s *httpAuditSink) WriteAuditEvent(event auditEvent) error {
	if s == nil || s.endpoint == "" {
		return nil
	}
	if s.spoolCorrupt.Load() {
		return fmt.Errorf("audit spool contains an unreadable record")
	}
	if strings.TrimSpace(event.TeamID) == "" || strings.TrimSpace(event.SandboxID) == "" {
		proxyMetrics.RecordAuditIngestEvents("invalid", 1)
		return fmt.Errorf("audit event requires team_id and sandbox_id")
	}
	if event.EventID == "" {
		event.EventID = uuid.NewString()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.ProducerSequence == 0 {
		if s.spool != nil {
			sequence, err := s.spool.NextSequence()
			if err != nil {
				proxyMetrics.RecordAuditIngestEvents("persist_failed", 1)
				event.ProducerSequence = uint64(event.Timestamp.UnixNano())
				if event.ProducerSequence == 0 {
					event.ProducerSequence = 1
				}
				s.auditLog().Error("Failed to persist netd audit producer sequence; continuing with event timestamp sequence",
					zap.String("event_id", event.EventID),
					zap.String("sandbox_id", event.SandboxID),
					zap.Error(err),
				)
			} else {
				event.ProducerSequence = sequence
			}
		} else {
			event.ProducerSequence = uint64(event.Timestamp.UnixNano())
		}
	}
	if event.Phase == "" {
		event.Phase = string(sandboxobservability.EventPhaseResult)
	}
	if s.spool != nil {
		if spoolErr := s.spool.Put(event); spoolErr != nil {
			proxyMetrics.RecordAuditIngestEvents("persist_failed", 1)
			if !errors.Is(spoolErr, errAuditSpoolWrite) {
				return fmt.Errorf("persist audit event: %w", spoolErr)
			}
			if event.Phase != string(sandboxobservability.EventPhaseAttempt) && event.Phase != string(sandboxobservability.EventPhaseResult) {
				return fmt.Errorf("persist audit event: %w", spoolErr)
			}
			canonicalErr := s.deliverCanonical(event)
			if canonicalErr != nil {
				proxyMetrics.RecordAuditIngestBatch("failed")
				proxyMetrics.RecordAuditIngestEvents("unrecorded", 1)
				s.auditLog().Error("Netd audit event is unrecorded after spool and canonical delivery both failed",
					zap.String("event_id", event.EventID),
					zap.String("sandbox_id", event.SandboxID),
					zap.String("phase", event.Phase),
					zap.Error(spoolErr),
					zap.NamedError("canonical_error", canonicalErr),
				)
				return fmt.Errorf("audit %s is unrecorded: durable spool persistence failed: %v; canonical fallback failed: %v", event.Phase, spoolErr, canonicalErr)
			}
			proxyMetrics.RecordAuditIngestBatch("fallback_sent")
			proxyMetrics.RecordAuditIngestEvents("fallback_sent", 1)
			s.auditLog().Warn("Netd audit event reached the canonical store through the synchronous spool fallback",
				zap.String("event_id", event.EventID),
				zap.String("sandbox_id", event.SandboxID),
				zap.String("phase", event.Phase),
				zap.Error(spoolErr),
			)
			return nil
		}
	}
	if event.Phase == string(sandboxobservability.EventPhaseAttempt) {
		if s.spool == nil {
			return fmt.Errorf("audit attempts require a durable spool")
		}
		if err := s.deliverAttempt(event); err != nil {
			proxyMetrics.RecordAuditIngestBatch("failed")
			proxyMetrics.RecordAuditIngestEvents("retrying", 1)
			return err
		}
		proxyMetrics.RecordAuditIngestBatch("sent")
		proxyMetrics.RecordAuditIngestEvents("sent", 1)
		return nil
	}
	if s.tryEnqueue(event) {
		proxyMetrics.RecordAuditIngestEvents("enqueued", 1)
	} else if s.spool != nil {
		proxyMetrics.RecordAuditIngestEvents("spooled", 1)
	} else {
		proxyMetrics.RecordAuditIngestEvents("dropped", 1)
		return fmt.Errorf("audit ingest queue is full")
	}
	return nil
}

func (s *httpAuditSink) deliverAttempt(event auditEvent) error {
	if err := s.deliverCanonical(event); err != nil {
		return fmt.Errorf("persist audit attempt in canonical store: %w", err)
	}
	if err := s.spool.Remove(event.EventID); err != nil {
		// The canonical store already acknowledged the attempt. Keep the local
		// record for idempotent replay, but do not block the connection with a
		// false delivery failure.
		proxyMetrics.RecordAuditIngestEvents("ack_failed", 1)
		s.auditLog().Error("Canonical netd audit attempt was acknowledged but spool cleanup failed",
			zap.String("event_id", event.EventID),
			zap.String("sandbox_id", event.SandboxID),
			zap.Error(err),
		)
	}
	return nil
}

func (s *httpAuditSink) deliverCanonical(event auditEvent) error {
	projected, ok := s.toObservabilityEvent(event)
	if !ok {
		return fmt.Errorf("invalid audit event")
	}
	body, err := json.Marshal(struct {
		Events []sandboxobservability.Event `json:"events"`
	}{Events: []sandboxobservability.Event{projected}})
	if err != nil {
		return fmt.Errorf("marshal audit event: %w", err)
	}
	ctx := context.Background()
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		lastErr = s.postEvents(ctx, projected.TeamID, projected.SandboxID, body)
		if lastErr == nil {
			return nil
		}
		if attempt == s.maxRetries {
			break
		}
		if !sleepWithContext(ctx, s.retryBackoff) {
			return fmt.Errorf("canonical audit delivery canceled")
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("canonical audit delivery exhausted without an attempt")
	}
	return lastErr
}

func (s *httpAuditSink) auditLog() *zap.Logger {
	if s == nil || s.logger == nil {
		return zap.NewNop()
	}
	return s.logger
}

func (s *httpAuditSink) tryEnqueue(event auditEvent) bool {
	s.queuedMu.Lock()
	if _, exists := s.queued[event.EventID]; exists {
		s.queuedMu.Unlock()
		return true
	}
	s.queued[event.EventID] = struct{}{}
	s.queuedMu.Unlock()
	select {
	case s.queue <- event:
		return true
	default:
		s.unmarkQueued([]auditEvent{event})
		return false
	}
}

func (s *httpAuditSink) unmarkQueued(events []auditEvent) {
	s.queuedMu.Lock()
	defer s.queuedMu.Unlock()
	for _, event := range events {
		delete(s.queued, event.EventID)
	}
}

func (s *httpAuditSink) enqueueSpool() {
	if s.spool == nil {
		return
	}
	events, err := s.spool.Load(cap(s.queue))
	if err != nil {
		s.spoolCorrupt.Store(true)
		proxyMetrics.RecordAuditIngestEvents("replay_failed", 1)
		return
	}
	for _, event := range events {
		if !s.tryEnqueue(event) {
			break
		}
	}
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
	s.enqueueSpool()
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
			s.enqueueSpool()
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
	rawByID := make(map[string]auditEvent, len(batch))
	for _, event := range batch {
		projected, ok := s.toObservabilityEvent(event)
		if !ok {
			proxyMetrics.RecordAuditIngestEvents("skipped", 1)
			continue
		}
		events = append(events, projected)
		rawByID[projected.EventID] = event
	}
	if len(events) == 0 {
		return
	}

	groups := make(map[string][]sandboxobservability.Event)
	for _, event := range events {
		key := event.TeamID + "\x00" + event.SandboxID
		groups[key] = append(groups[key], event)
	}
	for _, group := range groups {
		rawGroup := make([]auditEvent, 0, len(group))
		for _, event := range group {
			rawGroup = append(rawGroup, rawByID[event.EventID])
		}
		body, err := json.Marshal(struct {
			Events []sandboxobservability.Event `json:"events"`
		}{Events: group})
		if err != nil {
			proxyMetrics.RecordAuditIngestBatch("failed")
			proxyMetrics.RecordAuditIngestEvents("dropped", len(group))
			s.completeAuditGroup(rawGroup, false)
			continue
		}
		sent := false
		for attempt := 0; attempt <= s.maxRetries; attempt++ {
			if err := s.postEvents(ctx, group[0].TeamID, group[0].SandboxID, body); err == nil {
				proxyMetrics.RecordAuditIngestBatch("sent")
				proxyMetrics.RecordAuditIngestEvents("sent", len(group))
				sent = true
				break
			}
			if attempt == s.maxRetries || !sleepWithContext(ctx, s.retryBackoff) {
				break
			}
		}
		if !sent {
			proxyMetrics.RecordAuditIngestBatch("failed")
			result := "dropped"
			if s.spool != nil {
				result = "retrying"
			}
			proxyMetrics.RecordAuditIngestEvents(result, len(group))
		}
		s.completeAuditGroup(rawGroup, sent)
	}
}

func (s *httpAuditSink) completeAuditGroup(events []auditEvent, delivered bool) {
	defer s.unmarkQueued(events)
	if !delivered || s.spool == nil {
		return
	}
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.EventID)
	}
	if err := s.spool.Remove(ids...); err != nil {
		proxyMetrics.RecordAuditIngestEvents("ack_failed", len(events))
		s.auditLog().Error("Canonical netd audit results were acknowledged but spool cleanup failed",
			zap.Int("event_count", len(events)),
			zap.Strings("event_ids", ids),
			zap.Error(err),
		)
	}
}

func (s *httpAuditSink) postEvents(ctx context.Context, teamID, sandboxID string, body []byte) error {
	requestCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.generator != nil {
		token, err := s.generator.Generate("cluster-gateway", teamID, "", internalauth.GenerateOptions{
			Permissions: []string{authn.PermSandboxObservabilityWrite},
			SandboxID:   sandboxID,
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
	eventID := event.EventID
	if eventID == "" {
		eventID = uuid.NewString()
	}
	outcome := sandboxobservability.Outcome(event.Outcome)
	if outcome == "" || !sandboxobservability.ValidOutcome(outcome) {
		outcome = sandboxobservability.OutcomeCompleted
	}
	return sandboxobservability.Event{
		EventID:       eventID,
		SchemaVersion: sandboxobservability.CurrentEventSchemaVersion,
		TeamID:        event.TeamID,
		SandboxID:     event.SandboxID,
		RegionID:      s.regionID,
		ClusterID:     s.clusterID,
		OccurredAt:    timestamp,
		Source:        sandboxobservability.SourceNetd,
		EventType:     sandboxobservability.EventTypeNetworkAudit,
		Phase:         sandboxobservability.EventPhase(event.Phase),
		Outcome:       outcome,
		OperationID:   "netd:" + event.FlowID,
		Producer: sandboxobservability.AuditProducer{
			Service:  "netd",
			Instance: s.producerInstance,
			Sequence: event.ProducerSequence,
		},
		Cursor:     eventID,
		Watermark:  eventID,
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
	delete(attributes, "event_id")
	delete(attributes, "producer_sequence")
	delete(attributes, "phase")
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
