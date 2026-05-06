package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	obsquery "github.com/sandbox0-ai/sandbox0/pkg/observability/query"
	"go.uber.org/zap"
)

type ObservabilityReader interface {
	ListTraceSpans(ctx context.Context, opts obsquery.ListOptions) ([]obsquery.TraceSpan, error)
	ListLogs(ctx context.Context, opts obsquery.ListOptions) ([]obsquery.LogRecord, error)
}

type ObservabilityHandler struct {
	reader ObservabilityReader
	logger *zap.Logger
}

func NewObservabilityHandler(reader ObservabilityReader, logger *zap.Logger) *ObservabilityHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ObservabilityHandler{reader: reader, logger: logger}
}

func (h *ObservabilityHandler) ListTraceSpans(c *gin.Context) {
	if h.reader == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "observability is unavailable")
		return
	}
	opts, ok := parseObservabilityListOptions(c)
	if !ok {
		return
	}
	spans, err := h.reader.ListTraceSpans(c.Request.Context(), opts)
	if err != nil {
		h.logger.Error("Failed to list trace spans", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list trace spans")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"spans": spans})
}

func (h *ObservabilityHandler) ListLogs(c *gin.Context) {
	if h.reader == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "observability is unavailable")
		return
	}
	opts, ok := parseObservabilityListOptions(c)
	if !ok {
		return
	}
	records, err := h.reader.ListLogs(c.Request.Context(), opts)
	if err != nil {
		h.logger.Error("Failed to list observability logs", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list observability logs")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"logs": records})
}

func parseObservabilityListOptions(c *gin.Context) (obsquery.ListOptions, bool) {
	opts := obsquery.ListOptions{
		SandboxID: strings.TrimSpace(c.Query("sandbox_id")),
		TraceID:   strings.TrimSpace(c.Query("trace_id")),
		Limit:     100,
	}
	if authCtx := authn.FromContext(c.Request.Context()); authCtx != nil && !authCtx.IsSystemAdmin {
		opts.TeamID = strings.TrimSpace(authCtx.TeamID)
	}
	if value := strings.TrimSpace(c.Query("limit")); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit <= 0 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid limit")
			return obsquery.ListOptions{}, false
		}
		opts.Limit = limit
	}
	var ok bool
	if opts.StartTime, ok = parseQueryTime(c, "start_time"); !ok {
		return obsquery.ListOptions{}, false
	}
	if opts.EndTime, ok = parseQueryTime(c, "end_time"); !ok {
		return obsquery.ListOptions{}, false
	}
	if !opts.StartTime.IsZero() && !opts.EndTime.IsZero() && opts.StartTime.After(opts.EndTime) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "start_time must be before end_time")
		return obsquery.ListOptions{}, false
	}
	return opts, true
}

func parseQueryTime(c *gin.Context, key string) (time.Time, bool) {
	value := strings.TrimSpace(c.Query(key))
	if value == "" {
		return time.Time{}, true
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid "+key)
		return time.Time{}, false
	}
	return parsed, true
}
