package handlers

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

const (
	defaultSandboxObservabilityLimit   = 100
	maxSandboxObservabilityLimit       = 1000
	sandboxObservabilityWatchPoll      = time.Second
	sandboxObservabilityHeartbeat      = 15 * time.Second
	MaxSandboxObservabilityIngestBytes = 8 << 20
)

type sandboxObservabilityWatchLine struct {
	Type      string `json:"type"`
	Data      any    `json:"data,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
	Watermark string `json:"watermark,omitempty"`
	Time      string `json:"time,omitempty"`
	Error     string `json:"error,omitempty"`
}

// SandboxObservabilityHandler serves historical per-sandbox observability queries.
type SandboxObservabilityHandler struct {
	repo   sandboxobservability.Repository
	writer sandboxobservability.Writer
	logger *zap.Logger
	audit  *AuditIntegrityPolicy
	ingest *SandboxObservabilityIngestPolicy
}

// AuditIntegrityPolicy defines cluster-gateway-owned audit identity, signing,
// and query-time verification. Producer-controlled identity fields are
// replaced before ClickHouse insert.
type AuditIntegrityPolicy struct {
	RegionID        string
	ClusterID       string
	SigningKey      ed25519.PrivateKey
	VerificationKey ed25519.PublicKey
	Now             func() time.Time
}

// SandboxObservabilityIngestPolicy defines gateway-owned identity for trusted
// log and runtime-sample producers.
type SandboxObservabilityIngestPolicy struct {
	RegionID  string
	ClusterID string
	Now       func() time.Time
}

type SandboxObservabilityHandlerOption func(*SandboxObservabilityHandler)

func WithSandboxObservabilityWriter(writer sandboxobservability.Writer) SandboxObservabilityHandlerOption {
	return func(h *SandboxObservabilityHandler) {
		if writer == nil {
			return
		}
		h.writer = writer
	}
}

func WithAuditIntegrityPolicy(policy AuditIntegrityPolicy) SandboxObservabilityHandlerOption {
	return func(h *SandboxObservabilityHandler) {
		copyPolicy := policy
		if len(copyPolicy.VerificationKey) != ed25519.PublicKeySize && len(copyPolicy.SigningKey) == ed25519.PrivateKeySize {
			copyPolicy.VerificationKey = copyPolicy.SigningKey.Public().(ed25519.PublicKey)
		}
		if copyPolicy.Now == nil {
			copyPolicy.Now = func() time.Time { return time.Now().UTC() }
		}
		h.audit = &copyPolicy
	}
}

func WithSandboxObservabilityIngestPolicy(policy SandboxObservabilityIngestPolicy) SandboxObservabilityHandlerOption {
	return func(h *SandboxObservabilityHandler) {
		copyPolicy := policy
		if copyPolicy.Now == nil {
			copyPolicy.Now = func() time.Time { return time.Now().UTC() }
		}
		h.ingest = &copyPolicy
	}
}

func NewSandboxObservabilityHandler(repo sandboxobservability.Repository, logger *zap.Logger, opts ...SandboxObservabilityHandlerOption) *SandboxObservabilityHandler {
	if repo == nil {
		repo = sandboxobservability.NewDisabledRepository()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	handler := &SandboxObservabilityHandler{
		repo:   repo,
		logger: logger,
	}
	if writer, ok := repo.(sandboxobservability.Writer); ok {
		handler.writer = writer
	}
	for _, opt := range opts {
		if opt != nil {
			opt(handler)
		}
	}
	return handler
}

func (h *SandboxObservabilityHandler) ListEvents(c *gin.Context) {
	query, ok := parseSandboxObservabilityQuery(c)
	if !ok {
		return
	}
	watch, ok := parseSandboxObservabilityWatch(c)
	if !ok {
		return
	}
	if watch && query.EventID != "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "event_id cannot be combined with watch")
		return
	}
	if watch {
		h.watchEvents(c, query)
		return
	}

	result, err := h.repo.ListEvents(c.Request.Context(), query)
	if err != nil {
		h.writeQueryError(c, err, "failed to list sandbox observability events",
			zap.String("sandbox_id", query.SandboxID),
			zap.String("team_id", query.TeamID))
		return
	}
	if result == nil {
		result = &sandboxobservability.EventListResult{Events: []sandboxobservability.Event{}}
	}
	if result.Events == nil {
		result.Events = []sandboxobservability.Event{}
	}
	if query.EventID != "" {
		result.NextCursor = ""
	}
	for i := range result.Events {
		h.verifyEventStatus(&result.Events[i])
	}
	markEventConflicts(result.Events)
	spec.JSONSuccess(c, http.StatusOK, result)
}

func (h *SandboxObservabilityHandler) verifyEventStatus(event *sandboxobservability.Event) {
	if event == nil {
		return
	}
	if h.audit == nil || len(h.audit.VerificationKey) != ed25519.PublicKeySize {
		event.Integrity.SignatureStatus = sandboxobservability.AuditSignatureStatusUnavailable
		return
	}
	if err := sandboxobservability.ValidateSignedEvent(*event); err != nil {
		event.Integrity.SignatureStatus = sandboxobservability.AuditSignatureStatusInvalid
		h.logger.Error("Sandbox audit integrity envelope is invalid", zap.String("event_id", event.EventID), zap.Error(err))
		return
	}
	keyID, err := sandboxobservability.AuditSigningKeyID(h.audit.VerificationKey)
	if err != nil || event.Integrity.SigningKeyID != keyID {
		event.Integrity.SignatureStatus = sandboxobservability.AuditSignatureStatusUnavailable
		return
	}
	if err := sandboxobservability.VerifyEventIntegrity(*event, h.audit.VerificationKey); err != nil {
		event.Integrity.SignatureStatus = sandboxobservability.AuditSignatureStatusInvalid
		h.logger.Error("Sandbox audit integrity verification failed", zap.String("event_id", event.EventID), zap.Error(err))
		return
	}
	event.Integrity.SignatureStatus = sandboxobservability.AuditSignatureStatusVerified
}

func markEventConflicts(events []sandboxobservability.Event) {
	hashes := make(map[string]string, len(events))
	conflicts := make(map[string]struct{})
	for _, event := range events {
		if previous, ok := hashes[event.EventID]; ok && previous != event.Integrity.PayloadHash {
			conflicts[event.EventID] = struct{}{}
			continue
		}
		hashes[event.EventID] = event.Integrity.PayloadHash
	}
	for i := range events {
		if _, ok := conflicts[events[i].EventID]; ok {
			events[i].Integrity.EventIDConflict = true
		}
	}
}

func (h *SandboxObservabilityHandler) ListLogs(c *gin.Context) {
	query, ok := parseSandboxLogQuery(c)
	if !ok {
		return
	}
	watch, ok := parseSandboxObservabilityWatch(c)
	if !ok {
		return
	}
	if watch {
		h.watchLogs(c, query)
		return
	}
	result, err := h.repo.ListLogs(c.Request.Context(), query)
	if err != nil {
		h.writeQueryError(c, err, "failed to list sandbox observability logs", zap.String("sandbox_id", query.SandboxID), zap.String("team_id", query.TeamID))
		return
	}
	if result == nil {
		result = &sandboxobservability.LogListResult{Logs: []sandboxobservability.LogEntry{}}
	}
	if result.Logs == nil {
		result.Logs = []sandboxobservability.LogEntry{}
	}
	spec.JSONSuccess(c, http.StatusOK, result)
}

func (h *SandboxObservabilityHandler) GetRuntimeMetrics(c *gin.Context) {
	query, ok := parseSandboxRuntimeSeriesQuery(c)
	if !ok {
		return
	}
	result, err := h.repo.ListRuntimeSeries(c.Request.Context(), query)
	if err != nil {
		h.writeQueryError(c, err, "failed to query sandbox runtime metrics", zap.String("sandbox_id", query.SandboxID), zap.String("team_id", query.TeamID))
		return
	}
	if result == nil {
		result = &sandboxobservability.RuntimeSeriesResult{Series: []sandboxobservability.RuntimeSeries{}, Gaps: []sandboxobservability.RuntimeSeriesGap{}, Freshness: sandboxobservability.RuntimeSeriesFreshness{Status: sandboxobservability.RuntimeSeriesFreshnessMissing}, Partial: true}
	}
	if result.Series == nil {
		result.Series = []sandboxobservability.RuntimeSeries{}
	}
	if result.Gaps == nil {
		result.Gaps = []sandboxobservability.RuntimeSeriesGap{}
	}
	for i := range result.Series {
		if result.Series[i].Segments == nil {
			result.Series[i].Segments = []sandboxobservability.RuntimeSeriesSegment{}
		}
		for j := range result.Series[i].Segments {
			if result.Series[i].Segments[j].Points == nil {
				result.Series[i].Segments[j].Points = []sandboxobservability.RuntimeSeriesPoint{}
			}
		}
	}
	spec.JSONSuccess(c, http.StatusOK, result)
}

func (h *SandboxObservabilityHandler) GetRuntimeMetricsCatalog(c *gin.Context) {
	if _, _, ok := parseSandboxAndTeam(c); !ok {
		return
	}
	catalog := sandboxobservability.RuntimeMetricCatalogSnapshot()
	spec.JSONSuccess(c, http.StatusOK, catalog)
}

func (h *SandboxObservabilityHandler) IngestEvents(c *gin.Context) {
	if h.writer == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability ingest backend is disabled")
		return
	}
	var req struct {
		Events []sandboxobservability.Event `json:"events"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, MaxSandboxObservabilityIngestBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if len(req.Events) > maxSandboxObservabilityLimit {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "too many events")
		return
	}
	if len(req.Events) > 0 {
		if err := h.normalizeAuditEvents(c.Request.Context(), req.Events); err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
	}
	if err := h.writer.InsertEvents(c.Request.Context(), req.Events); err != nil {
		if errors.Is(err, sandboxobservability.ErrBackendDisabled) {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability backend is disabled")
			return
		}
		if errors.Is(err, sandboxobservability.ErrBackendUnavailable) {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability backend is unavailable")
			return
		}
		h.logger.Error("Failed to ingest sandbox observability events",
			zap.Int("event_count", len(req.Events)),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to ingest sandbox observability events")
		return
	}
	spec.JSONSuccess(c, http.StatusAccepted, gin.H{"inserted": len(req.Events)})
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func (h *SandboxObservabilityHandler) normalizeAuditEvents(ctx context.Context, events []sandboxobservability.Event) error {
	if h.audit == nil || len(h.audit.SigningKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("audit ingest policy is not configured")
	}
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.IsSystem || strings.TrimSpace(claims.TeamID) == "" || strings.TrimSpace(claims.SandboxID) == "" {
		return fmt.Errorf("audit ingest requires a team and sandbox scoped token")
	}
	if strings.TrimSpace(claims.Caller) != "netd" {
		return fmt.Errorf("audit producer is not allowed")
	}
	now := h.audit.Now().UTC()
	for i := range events {
		event := &events[i]
		if event.TeamID != claims.TeamID || event.SandboxID != claims.SandboxID {
			return fmt.Errorf("event %d is outside the authenticated team or sandbox", i)
		}
		if _, err := uuid.Parse(event.EventID); err != nil {
			return fmt.Errorf("event %d has invalid event_id", i)
		}
		if event.EventType != sandboxobservability.EventTypeNetworkAudit {
			return fmt.Errorf("event %d type is not allowed for producer %s", i, claims.Caller)
		}
		if event.OccurredAt.IsZero() || event.OccurredAt.After(now.Add(5*time.Minute)) {
			return fmt.Errorf("event %d has invalid occurred_at", i)
		}
		if !sandboxobservability.ValidEventPhase(event.Phase) {
			return fmt.Errorf("event %d has invalid phase", i)
		}
		event.SchemaVersion = sandboxobservability.CurrentEventSchemaVersion
		event.RegionID = strings.TrimSpace(h.audit.RegionID)
		event.ClusterID = strings.TrimSpace(h.audit.ClusterID)
		event.IngestedAt = now
		event.Source = sandboxobservability.SourceNetd
		event.Producer.Service = claims.Caller
		event.Actor = sandboxobservability.AuditActor{
			Kind:       sandboxobservability.ActorKindSandboxWorkload,
			ID:         event.SandboxID,
			AuthMethod: "workload_token",
		}
		event.EventType = sandboxobservability.EventTypeNetworkAudit
		// The ctld network runtime observes a network flow, not an HTTP API response. Request
		// metadata is gateway-owned and must not be accepted from this producer.
		event.Request = sandboxobservability.AuditRequest{}
		attributes := sandboxobservability.SanitizeNetworkAuditAttributes(event.Attributes)
		event.Action = sandboxobservability.NetworkAuditAction(attributes)
		event.Resource = sandboxobservability.AuditResource{Type: "sandbox_network", ID: event.SandboxID}
		event.Attributes = attributes.CanonicalMap()
		if err := sandboxobservability.SignEvent(event, h.audit.SigningKey); err != nil {
			return fmt.Errorf("event %d integrity: %w", i, err)
		}
	}
	return nil
}

func (h *SandboxObservabilityHandler) IngestLogs(c *gin.Context) {
	if h.writer == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability ingest backend is disabled")
		return
	}
	var req struct {
		Logs []sandboxobservability.LogEntry `json:"logs"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, MaxSandboxObservabilityIngestBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if len(req.Logs) > maxSandboxObservabilityLimit {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "too many logs")
		return
	}
	if err := h.normalizeLogs(c.Request.Context(), req.Logs); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if err := h.writer.InsertLogs(c.Request.Context(), req.Logs); err != nil {
		h.writeIngestError(c, err, "failed to ingest sandbox observability logs", zap.Int("log_count", len(req.Logs)))
		return
	}
	spec.JSONSuccess(c, http.StatusAccepted, gin.H{"inserted": len(req.Logs)})
}

func (h *SandboxObservabilityHandler) IngestRuntimeSamples(c *gin.Context) {
	if h.writer == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability ingest backend is disabled")
		return
	}
	var req struct {
		Samples []sandboxobservability.RuntimeSample `json:"samples"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, MaxSandboxObservabilityIngestBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if len(req.Samples) > maxSandboxObservabilityLimit {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "too many runtime samples")
		return
	}
	if err := h.normalizeRuntimeSamples(c.Request.Context(), req.Samples); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if err := h.writer.InsertRuntimeSamples(c.Request.Context(), req.Samples); err != nil {
		h.writeIngestError(c, err, "failed to ingest sandbox runtime samples", zap.Int("sample_count", len(req.Samples)))
		return
	}
	spec.JSONSuccess(c, http.StatusAccepted, gin.H{"inserted": len(req.Samples)})
}

func (h *SandboxObservabilityHandler) normalizeLogs(ctx context.Context, logs []sandboxobservability.LogEntry) error {
	claims, now, err := h.observabilityProducer(ctx, internalauth.ServiceManager)
	if err != nil {
		return err
	}
	for i := range logs {
		entry := &logs[i]
		if entry.TeamID != claims.TeamID || strings.TrimSpace(entry.SandboxID) == "" {
			return fmt.Errorf("log %d is outside the authenticated team or has no sandbox", i)
		}
		if entry.OccurredAt.IsZero() || entry.OccurredAt.After(now.Add(5*time.Minute)) {
			return fmt.Errorf("log %d has invalid occurred_at", i)
		}
		if strings.TrimSpace(entry.Cursor) == "" {
			return fmt.Errorf("log %d has no cursor", i)
		}
		entry.RegionID = strings.TrimSpace(h.ingest.RegionID)
		entry.ClusterID = strings.TrimSpace(h.ingest.ClusterID)
		entry.IngestedAt = now
	}
	return nil
}

func (h *SandboxObservabilityHandler) normalizeRuntimeSamples(
	ctx context.Context,
	samples []sandboxobservability.RuntimeSample,
) error {
	claims, now, err := h.observabilityProducer(ctx, internalauth.ServiceCtld)
	if err != nil {
		return err
	}
	for i := range samples {
		sample := &samples[i]
		if sample.TeamID != claims.TeamID || strings.TrimSpace(sample.SandboxID) == "" {
			return fmt.Errorf("runtime sample %d is outside the authenticated team or has no sandbox", i)
		}
		if sample.ObservedAt.IsZero() || sample.ObservedAt.After(now.Add(5*time.Minute)) {
			return fmt.Errorf("runtime sample %d has invalid observed_at", i)
		}
		if strings.TrimSpace(sample.SeriesEpoch) == "" || strings.TrimSpace(sample.SampleID) == "" {
			return fmt.Errorf("runtime sample %d has no series_epoch or sample_id", i)
		}
		sample.RegionID = strings.TrimSpace(h.ingest.RegionID)
		sample.ClusterID = strings.TrimSpace(h.ingest.ClusterID)
		sample.IngestedAt = now
	}
	return nil
}

func (h *SandboxObservabilityHandler) observabilityProducer(
	ctx context.Context,
	allowedCaller string,
) (*internalauth.Claims, time.Time, error) {
	if h.ingest == nil {
		return nil, time.Time{}, fmt.Errorf("sandbox observability ingest policy is not configured")
	}
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.IsSystem || strings.TrimSpace(claims.TeamID) == "" {
		return nil, time.Time{}, fmt.Errorf("sandbox observability ingest requires a team-scoped token")
	}
	if strings.TrimSpace(claims.Caller) != allowedCaller {
		return nil, time.Time{}, fmt.Errorf("sandbox observability producer is not allowed")
	}
	return claims, h.ingest.Now().UTC(), nil
}

func (h *SandboxObservabilityHandler) watchEvents(c *gin.Context, query sandboxobservability.EventQuery) {
	if !validateSandboxObservabilityWatch(c, query.EndTime) {
		return
	}
	watchRepo, ok := h.watchRepository(c)
	if !ok {
		return
	}

	opts := buildSandboxObservabilityWatchOptions(query.Cursor, query.Limit, query.StartTime)
	fetch := func() (*sandboxobservability.EventListResult, error) {
		return watchRepo.WatchEvents(c.Request.Context(), query, opts)
	}
	result, err := fetch()
	if err != nil {
		h.writeQueryError(c, err, "failed to watch sandbox observability events",
			zap.String("sandbox_id", query.SandboxID),
			zap.String("team_id", query.TeamID))
		return
	}

	encoder, flusher, ok := h.startSandboxObservabilityWatch(c)
	if !ok {
		return
	}
	lastHeartbeat := time.Now().UTC()
	for {
		fullBatch := h.writeWatchEvents(c, encoder, flusher, result, &opts)
		if result == nil || !fullBatch {
			if !h.waitForNextWatchPoll(c, encoder, flusher, &lastHeartbeat) {
				return
			}
		}
		result, err = fetch()
		if err != nil {
			h.writeWatchErrorLine(c, encoder, flusher, err, "failed to watch sandbox observability events")
			return
		}
	}
}

func (h *SandboxObservabilityHandler) watchLogs(c *gin.Context, query sandboxobservability.LogQuery) {
	if !validateSandboxObservabilityWatch(c, query.EndTime) {
		return
	}
	watchRepo, ok := h.watchRepository(c)
	if !ok {
		return
	}

	opts := buildSandboxObservabilityWatchOptions(query.Cursor, query.Limit, query.StartTime)
	fetch := func() (*sandboxobservability.LogListResult, error) {
		return watchRepo.WatchLogs(c.Request.Context(), query, opts)
	}
	result, err := fetch()
	if err != nil {
		h.writeQueryError(c, err, "failed to watch sandbox observability logs",
			zap.String("sandbox_id", query.SandboxID),
			zap.String("team_id", query.TeamID))
		return
	}

	encoder, flusher, ok := h.startSandboxObservabilityWatch(c)
	if !ok {
		return
	}
	lastHeartbeat := time.Now().UTC()
	for {
		fullBatch := h.writeWatchLogs(c, encoder, flusher, result, &opts)
		if result == nil || !fullBatch {
			if !h.waitForNextWatchPoll(c, encoder, flusher, &lastHeartbeat) {
				return
			}
		}
		result, err = fetch()
		if err != nil {
			h.writeWatchErrorLine(c, encoder, flusher, err, "failed to watch sandbox observability logs")
			return
		}
	}
}

func (h *SandboxObservabilityHandler) watchRepository(c *gin.Context) (sandboxobservability.WatchRepository, bool) {
	watchRepo, ok := h.repo.(sandboxobservability.WatchRepository)
	if !ok {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability watch backend is disabled")
		return nil, false
	}
	return watchRepo, true
}

func (h *SandboxObservabilityHandler) startSandboxObservabilityWatch(c *gin.Context) (*json.Encoder, http.Flusher, bool) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "streaming response is not supported")
		return nil, nil, false
	}
	if err := proxy.DisableResponseWriteDeadline(c.Writer); err != nil {
		h.logger.Debug("Failed to disable sandbox observability watch write deadline", zap.Error(err))
	}
	c.Header("Content-Type", "application/x-ndjson")
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	return json.NewEncoder(c.Writer), flusher, true
}

func (h *SandboxObservabilityHandler) writeWatchEvents(c *gin.Context, encoder *json.Encoder, flusher http.Flusher, result *sandboxobservability.EventListResult, opts *sandboxobservability.WatchOptions) bool {
	if result == nil {
		return false
	}
	for i := range result.Events {
		h.verifyEventStatus(&result.Events[i])
	}
	markEventConflicts(result.Events)
	for _, event := range result.Events {
		if !h.writeWatchLine(c, encoder, flusher, sandboxObservabilityWatchLine{Type: "event", Data: event}) {
			return false
		}
	}
	h.writeWatchWatermark(c, encoder, flusher, result.NextCursor, result.Watermark, opts)
	return len(result.Events) >= opts.Limit && opts.Limit > 0
}

func (h *SandboxObservabilityHandler) writeWatchLogs(c *gin.Context, encoder *json.Encoder, flusher http.Flusher, result *sandboxobservability.LogListResult, opts *sandboxobservability.WatchOptions) bool {
	if result == nil {
		return false
	}
	for _, entry := range result.Logs {
		if !h.writeWatchLine(c, encoder, flusher, sandboxObservabilityWatchLine{Type: "log", Data: entry}) {
			return false
		}
	}
	h.writeWatchWatermark(c, encoder, flusher, result.NextCursor, result.Watermark, opts)
	return len(result.Logs) >= opts.Limit && opts.Limit > 0
}

func (h *SandboxObservabilityHandler) writeWatchWatermark(c *gin.Context, encoder *json.Encoder, flusher http.Flusher, cursor, watermark string, opts *sandboxobservability.WatchOptions) {
	if cursor == "" {
		return
	}
	opts.Cursor = cursor
	_ = h.writeWatchLine(c, encoder, flusher, sandboxObservabilityWatchLine{
		Type:      "watermark",
		Cursor:    cursor,
		Watermark: watermark,
	})
}

func (h *SandboxObservabilityHandler) writeWatchErrorLine(c *gin.Context, encoder *json.Encoder, flusher http.Flusher, err error, message string) {
	h.logger.Error(message, zap.Error(err))
	_ = h.writeWatchLine(c, encoder, flusher, sandboxObservabilityWatchLine{
		Type:  "error",
		Error: message,
	})
}

func (h *SandboxObservabilityHandler) waitForNextWatchPoll(c *gin.Context, encoder *json.Encoder, flusher http.Flusher, lastHeartbeat *time.Time) bool {
	poll := time.NewTimer(sandboxObservabilityWatchPoll)
	defer poll.Stop()

	select {
	case <-c.Request.Context().Done():
		return false
	case <-poll.C:
		now := time.Now().UTC()
		if lastHeartbeat != nil && now.Sub(*lastHeartbeat) >= sandboxObservabilityHeartbeat {
			*lastHeartbeat = now
			if !h.writeWatchLine(c, encoder, flusher, sandboxObservabilityWatchLine{
				Type: "heartbeat",
				Time: now.Format(time.RFC3339Nano),
			}) {
				return false
			}
		}
		return true
	}
}

func (h *SandboxObservabilityHandler) writeWatchLine(c *gin.Context, encoder *json.Encoder, flusher http.Flusher, line sandboxObservabilityWatchLine) bool {
	if err := encoder.Encode(line); err != nil {
		h.logger.Debug("Failed to write sandbox observability watch line", zap.Error(err))
		return false
	}
	flusher.Flush()
	return c.Request.Context().Err() == nil
}

func (h *SandboxObservabilityHandler) writeQueryError(c *gin.Context, err error, message string, fields ...zap.Field) {
	if errors.Is(err, sandboxobservability.ErrBackendDisabled) {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability backend is disabled")
		return
	}
	if errors.Is(err, sandboxobservability.ErrBackendUnavailable) {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability backend is unavailable")
		return
	}
	if errors.Is(err, sandboxobservability.ErrInvalidCursor) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid cursor")
		return
	}
	if errors.Is(err, sandboxobservability.ErrInvalidQuery) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	fields = append(fields, zap.Error(err))
	h.logger.Error(message, fields...)
	spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, message)
}

func (h *SandboxObservabilityHandler) writeIngestError(c *gin.Context, err error, message string, fields ...zap.Field) {
	if errors.Is(err, sandboxobservability.ErrBackendDisabled) {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability backend is disabled")
		return
	}
	if errors.Is(err, sandboxobservability.ErrBackendUnavailable) {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability backend is unavailable")
		return
	}
	fields = append(fields, zap.Error(err))
	h.logger.Error(message, fields...)
	spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, message)
}

func parseSandboxObservabilityQuery(c *gin.Context) (sandboxobservability.EventQuery, bool) {
	sandboxID := strings.TrimSpace(c.Param("id"))
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return sandboxobservability.EventQuery{}, false
	}

	authCtx := authn.FromContext(c.Request.Context())
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return sandboxobservability.EventQuery{}, false
	}
	teamID := strings.TrimSpace(authCtx.TeamID)
	if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return sandboxobservability.EventQuery{}, false
	}

	startTime, ok := parseOptionalTimeQuery(c, "start_time")
	if !ok {
		return sandboxobservability.EventQuery{}, false
	}
	endTime, ok := parseOptionalTimeQuery(c, "end_time")
	if !ok {
		return sandboxobservability.EventQuery{}, false
	}
	if startTime != nil && endTime != nil && endTime.Before(*startTime) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "end_time must be greater than or equal to start_time")
		return sandboxobservability.EventQuery{}, false
	}

	limit, ok := parseSandboxObservabilityLimit(c)
	if !ok {
		return sandboxobservability.EventQuery{}, false
	}

	source, ok := parseOptionalSourceQuery(c)
	if !ok {
		return sandboxobservability.EventQuery{}, false
	}
	eventType, ok := parseOptionalEventTypeQuery(c)
	if !ok {
		return sandboxobservability.EventQuery{}, false
	}
	outcome, ok := parseOptionalOutcomeQuery(c)
	if !ok {
		return sandboxobservability.EventQuery{}, false
	}
	actorKind := sandboxobservability.ActorKind(strings.TrimSpace(c.Query("actor_kind")))
	if actorKind != "" && !sandboxobservability.ValidActorKind(actorKind) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid actor_kind")
		return sandboxobservability.EventQuery{}, false
	}
	cursor := strings.TrimSpace(c.Query("cursor"))
	actorID := strings.TrimSpace(c.Query("actor_id"))
	action := strings.TrimSpace(c.Query("action"))
	resourceType := strings.TrimSpace(c.Query("resource_type"))
	operationID := strings.TrimSpace(c.Query("operation_id"))
	eventID := strings.TrimSpace(c.Query("event_id"))
	if eventID != "" {
		if _, err := uuid.Parse(eventID); err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid event_id")
			return sandboxobservability.EventQuery{}, false
		}
		if startTime != nil || endTime != nil || cursor != "" || source != "" || eventType != "" || outcome != "" || actorKind != "" || actorID != "" || action != "" || resourceType != "" || operationID != "" {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "event_id cannot be combined with other event filters or cursor")
			return sandboxobservability.EventQuery{}, false
		}
		// An exact lookup returns at most two payload variants. Two are enough
		// to prove that the stable ID has conflicting canonical facts.
		limit = 2
	}

	return sandboxobservability.EventQuery{
		TeamID:       teamID,
		SandboxID:    sandboxID,
		StartTime:    startTime,
		EndTime:      endTime,
		Limit:        limit,
		Cursor:       cursor,
		Source:       source,
		EventType:    eventType,
		Outcome:      outcome,
		ActorKind:    actorKind,
		ActorID:      actorID,
		Action:       action,
		ResourceType: resourceType,
		OperationID:  operationID,
		EventID:      eventID,
	}, true
}

func parseSandboxLogQuery(c *gin.Context) (sandboxobservability.LogQuery, bool) {
	sandboxID, teamID, ok := parseSandboxAndTeam(c)
	if !ok {
		return sandboxobservability.LogQuery{}, false
	}
	startTime, ok := parseOptionalTimeQuery(c, "start_time")
	if !ok {
		return sandboxobservability.LogQuery{}, false
	}
	endTime, ok := parseOptionalTimeQuery(c, "end_time")
	if !ok {
		return sandboxobservability.LogQuery{}, false
	}
	if startTime != nil && endTime != nil && endTime.Before(*startTime) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "end_time must be greater than or equal to start_time")
		return sandboxobservability.LogQuery{}, false
	}
	limit, ok := parseSandboxObservabilityLimit(c)
	if !ok {
		return sandboxobservability.LogQuery{}, false
	}
	stream, ok := parseOptionalLogStreamQuery(c)
	if !ok {
		return sandboxobservability.LogQuery{}, false
	}
	return sandboxobservability.LogQuery{
		TeamID:    teamID,
		SandboxID: sandboxID,
		StartTime: startTime,
		EndTime:   endTime,
		Limit:     limit,
		Cursor:    strings.TrimSpace(c.Query("cursor")),
		ContextID: strings.TrimSpace(c.Query("context_id")),
		Stream:    stream,
	}, true
}

func parseSandboxRuntimeSeriesQuery(c *gin.Context) (sandboxobservability.RuntimeSeriesQuery, bool) {
	sandboxID, teamID, ok := parseSandboxAndTeam(c)
	if !ok {
		return sandboxobservability.RuntimeSeriesQuery{}, false
	}
	startTime, ok := parseOptionalTimeQuery(c, "start_time")
	if !ok {
		return sandboxobservability.RuntimeSeriesQuery{}, false
	}
	endTime, ok := parseOptionalTimeQuery(c, "end_time")
	if !ok {
		return sandboxobservability.RuntimeSeriesQuery{}, false
	}
	if startTime != nil && endTime != nil && !endTime.After(*startTime) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "end_time must be greater than start_time")
		return sandboxobservability.RuntimeSeriesQuery{}, false
	}
	step, ok := parseOptionalPositiveIntegerQuery(c, "step_seconds", 86400)
	if !ok {
		return sandboxobservability.RuntimeSeriesQuery{}, false
	}
	maxPoints, ok := parseOptionalPositiveIntegerQuery(c, "max_points", 1000)
	if !ok {
		return sandboxobservability.RuntimeSeriesQuery{}, false
	}
	statistic := sandboxobservability.RuntimeMetricStatistic(strings.TrimSpace(c.Query("statistic")))
	if statistic == "" {
		statistic = sandboxobservability.RuntimeMetricStatisticAuto
	}
	if !sandboxobservability.ValidRuntimeMetricStatistic(statistic) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid statistic")
		return sandboxobservability.RuntimeSeriesQuery{}, false
	}
	query := sandboxobservability.RuntimeSeriesQuery{
		TeamID:    teamID,
		SandboxID: sandboxID,
		Metrics:   parseRuntimeMetricNames(c.Query("metrics")),
		Step:      time.Duration(step) * time.Second,
		Statistic: statistic,
		MaxPoints: maxPoints,
	}
	if startTime != nil {
		query.StartTime = *startTime
	}
	if endTime != nil {
		query.EndTime = *endTime
	}
	for _, name := range query.Metrics {
		if !sandboxobservability.ValidRuntimeMetricName(name) {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "unknown metric "+string(name))
			return sandboxobservability.RuntimeSeriesQuery{}, false
		}
	}
	return query, true
}

func parseSandboxAndTeam(c *gin.Context) (string, string, bool) {
	sandboxID := strings.TrimSpace(c.Param("id"))
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return "", "", false
	}
	authCtx := authn.FromContext(c.Request.Context())
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return "", "", false
	}
	teamID := strings.TrimSpace(authCtx.TeamID)
	if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return "", "", false
	}
	return sandboxID, teamID, true
}

func parseOptionalTimeQuery(c *gin.Context, name string) (*time.Time, bool) {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return nil, true
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid "+name)
		return nil, false
	}
	parsed = parsed.UTC()
	return &parsed, true
}

func parseSandboxObservabilityLimit(c *gin.Context) (int, bool) {
	value := strings.TrimSpace(c.Query("limit"))
	if value == "" {
		return defaultSandboxObservabilityLimit, true
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid limit")
		return 0, false
	}
	if parsed > maxSandboxObservabilityLimit {
		parsed = maxSandboxObservabilityLimit
	}
	return parsed, true
}

func parseSandboxObservabilityWatch(c *gin.Context) (bool, bool) {
	value := strings.TrimSpace(c.Query("watch"))
	if value == "" {
		return false, true
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid watch")
		return false, false
	}
	return parsed, true
}

func validateSandboxObservabilityWatch(c *gin.Context, endTime *time.Time) bool {
	if endTime != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "end_time is not supported when watch=true")
		return false
	}
	return true
}

func buildSandboxObservabilityWatchOptions(cursor string, limit int, startTime *time.Time) sandboxobservability.WatchOptions {
	opts := sandboxobservability.WatchOptions{
		Cursor: strings.TrimSpace(cursor),
		Limit:  limit,
	}
	if opts.Cursor == "" && startTime == nil {
		after := time.Now().UTC()
		opts.AfterIngestedAt = &after
	}
	return opts
}

func parseOptionalSourceQuery(c *gin.Context) (sandboxobservability.Source, bool) {
	value := sandboxobservability.Source(strings.TrimSpace(c.Query("source")))
	if value == "" {
		return "", true
	}
	if !sandboxobservability.ValidSource(value) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid source")
		return "", false
	}
	return value, true
}

func parseOptionalEventTypeQuery(c *gin.Context) (sandboxobservability.EventType, bool) {
	value := sandboxobservability.EventType(strings.TrimSpace(c.Query("event_type")))
	if value == "" {
		return "", true
	}
	if !sandboxobservability.ValidEventType(value) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid event_type")
		return "", false
	}
	return value, true
}

func parseOptionalOutcomeQuery(c *gin.Context) (sandboxobservability.Outcome, bool) {
	value := sandboxobservability.Outcome(strings.TrimSpace(c.Query("outcome")))
	if value == "" {
		return "", true
	}
	if !sandboxobservability.ValidOutcome(value) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid outcome")
		return "", false
	}
	return value, true
}

func parseOptionalLogStreamQuery(c *gin.Context) (sandboxobservability.LogStream, bool) {
	value := sandboxobservability.LogStream(strings.TrimSpace(c.Query("stream")))
	if value == "" {
		return "", true
	}
	if !sandboxobservability.ValidLogStream(value) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid stream")
		return "", false
	}
	return value, true
}

func parseRuntimeMetricNames(value string) []sandboxobservability.RuntimeMetricName {
	var names []sandboxobservability.RuntimeMetricName
	seen := map[sandboxobservability.RuntimeMetricName]struct{}{}
	for _, part := range strings.Split(value, ",") {
		name := sandboxobservability.RuntimeMetricName(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func parseOptionalPositiveIntegerQuery(c *gin.Context, name string, maximum int) (int, bool) {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return 0, true
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 || (maximum > 0 && parsed > maximum) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid "+name)
		return 0, false
	}
	return parsed, true
}
