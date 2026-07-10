package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

const (
	defaultSandboxObservabilityLimit = 100
	maxSandboxObservabilityLimit     = 1000
	sandboxObservabilityWatchPoll    = time.Second
	sandboxObservabilityHeartbeat    = 15 * time.Second
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
	spec.JSONSuccess(c, http.StatusOK, result)
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
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if len(req.Events) > maxSandboxObservabilityLimit {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "too many events")
		return
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

func (h *SandboxObservabilityHandler) IngestLogs(c *gin.Context) {
	if h.writer == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox observability ingest backend is disabled")
		return
	}
	var req struct {
		Logs []sandboxobservability.LogEntry `json:"logs"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if len(req.Logs) > maxSandboxObservabilityLimit {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "too many logs")
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
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if len(req.Samples) > maxSandboxObservabilityLimit {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "too many runtime samples")
		return
	}
	if err := h.writer.InsertRuntimeSamples(c.Request.Context(), req.Samples); err != nil {
		h.writeIngestError(c, err, "failed to ingest sandbox runtime samples", zap.Int("sample_count", len(req.Samples)))
		return
	}
	spec.JSONSuccess(c, http.StatusAccepted, gin.H{"inserted": len(req.Samples)})
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

	return sandboxobservability.EventQuery{
		TeamID:    teamID,
		SandboxID: sandboxID,
		StartTime: startTime,
		EndTime:   endTime,
		Limit:     limit,
		Cursor:    strings.TrimSpace(c.Query("cursor")),
		Source:    source,
		EventType: eventType,
		Outcome:   outcome,
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
