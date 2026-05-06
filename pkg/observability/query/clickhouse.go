package query

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTracesTable = "otel_traces"
	defaultLogsTable   = "otel_logs"
)

type ClickHouseConfig struct {
	HTTPURL  string
	Database string
	Username string
	Password string
	Client   *http.Client
}

type Client struct {
	cfg        ClickHouseConfig
	httpClient *http.Client
}

type ListOptions struct {
	TeamID    string
	SandboxID string
	TraceID   string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
}

type TraceSpan struct {
	Timestamp          string            `json:"timestamp"`
	TraceID            string            `json:"trace_id"`
	SpanID             string            `json:"span_id"`
	ParentSpanID       string            `json:"parent_span_id,omitempty"`
	ServiceName        string            `json:"service_name,omitempty"`
	Name               string            `json:"name"`
	Kind               string            `json:"kind,omitempty"`
	DurationNano       uint64            `json:"duration_nano,omitempty"`
	StatusCode         string            `json:"status_code,omitempty"`
	StatusMessage      string            `json:"status_message,omitempty"`
	ResourceAttributes map[string]string `json:"resource_attributes,omitempty"`
	Attributes         map[string]string `json:"attributes,omitempty"`
}

type LogRecord struct {
	Timestamp          string            `json:"timestamp"`
	TraceID            string            `json:"trace_id,omitempty"`
	SpanID             string            `json:"span_id,omitempty"`
	SeverityText       string            `json:"severity_text,omitempty"`
	SeverityNumber     int               `json:"severity_number,omitempty"`
	Body               string            `json:"body"`
	ResourceAttributes map[string]string `json:"resource_attributes,omitempty"`
	Attributes         map[string]string `json:"attributes,omitempty"`
}

func NewClickHouseClient(cfg ClickHouseConfig) (*Client, error) {
	if strings.TrimSpace(cfg.HTTPURL) == "" {
		return nil, fmt.Errorf("clickhouse http url is required")
	}
	if strings.TrimSpace(cfg.Database) == "" {
		return nil, fmt.Errorf("clickhouse database is required")
	}
	httpClient := cfg.Client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{cfg: cfg, httpClient: httpClient}, nil
}

func (c *Client) ListTraceSpans(ctx context.Context, opts ListOptions) ([]TraceSpan, error) {
	limit := normalizeLimit(opts.Limit)
	where := buildWhere(opts, "SpanAttributes")
	sql := fmt.Sprintf(`SELECT
  toString(Timestamp) AS timestamp,
  TraceId AS trace_id,
  SpanId AS span_id,
  ParentSpanId AS parent_span_id,
  ServiceName AS service_name,
  SpanName AS name,
  SpanKind AS kind,
  Duration AS duration_nano,
  StatusCode AS status_code,
  StatusMessage AS status_message,
  ResourceAttributes AS resource_attributes,
  SpanAttributes AS attributes
FROM %s.%s
%s
ORDER BY Timestamp DESC
LIMIT %d
FORMAT JSONEachRow`, ident(c.cfg.Database), ident(defaultTracesTable), where, limit)

	var rows []traceSpanRow
	if err := c.queryJSONEachRow(ctx, sql, func(data []byte) error {
		var row traceSpanRow
		if err := json.Unmarshal(data, &row); err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	}); err != nil {
		return nil, err
	}
	out := make([]TraceSpan, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.toTraceSpan())
	}
	return out, nil
}

func (c *Client) ListLogs(ctx context.Context, opts ListOptions) ([]LogRecord, error) {
	limit := normalizeLimit(opts.Limit)
	where := buildWhere(opts, "LogAttributes")
	sql := fmt.Sprintf(`SELECT
  toString(Timestamp) AS timestamp,
  TraceId AS trace_id,
  SpanId AS span_id,
  SeverityText AS severity_text,
  SeverityNumber AS severity_number,
  Body AS body,
  ResourceAttributes AS resource_attributes,
  LogAttributes AS attributes
FROM %s.%s
%s
ORDER BY Timestamp DESC
LIMIT %d
FORMAT JSONEachRow`, ident(c.cfg.Database), ident(defaultLogsTable), where, limit)

	var rows []logRecordRow
	if err := c.queryJSONEachRow(ctx, sql, func(data []byte) error {
		var row logRecordRow
		if err := json.Unmarshal(data, &row); err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	}); err != nil {
		return nil, err
	}
	out := make([]LogRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.toLogRecord())
	}
	return out, nil
}

func (c *Client) queryJSONEachRow(ctx context.Context, sql string, consume func([]byte) error) error {
	endpoint, err := url.Parse(c.cfg.HTTPURL)
	if err != nil {
		return fmt.Errorf("parse clickhouse http url: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewBufferString(sql))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.cfg.Username != "" || c.cfg.Password != "" {
		req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("query clickhouse: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse query failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := consume(line); err != nil {
			return fmt.Errorf("decode clickhouse row: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read clickhouse response: %w", err)
	}
	return nil
}

func buildWhere(opts ListOptions, signalAttributesColumn string) string {
	clauses := make([]string, 0, 5)
	if opts.TeamID != "" {
		clauses = append(clauses, attributeFilter(signalAttributesColumn, "sandbox0.team_id", opts.TeamID))
	}
	if opts.SandboxID != "" {
		clauses = append(clauses, attributeFilter(signalAttributesColumn, "sandbox0.sandbox_id", opts.SandboxID))
	}
	if opts.TraceID != "" {
		clauses = append(clauses, "TraceId = "+quote(opts.TraceID))
	}
	if !opts.StartTime.IsZero() {
		clauses = append(clauses, "Timestamp >= parseDateTime64BestEffort("+quote(opts.StartTime.UTC().Format(time.RFC3339Nano))+")")
	}
	if !opts.EndTime.IsZero() {
		clauses = append(clauses, "Timestamp <= parseDateTime64BestEffort("+quote(opts.EndTime.UTC().Format(time.RFC3339Nano))+")")
	}
	if len(clauses) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(clauses, " AND ")
}

func attributeFilter(signalAttributesColumn, key, value string) string {
	quotedKey := quote(key)
	quotedValue := quote(value)
	column := ident(strings.TrimSpace(signalAttributesColumn))
	return fmt.Sprintf("(ResourceAttributes[%s] = %s OR %s[%s] = %s)", quotedKey, quotedValue, column, quotedKey, quotedValue)
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func ident(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "default"
	}
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func quote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

type traceSpanRow struct {
	Timestamp          string          `json:"timestamp"`
	TraceID            string          `json:"trace_id"`
	SpanID             string          `json:"span_id"`
	ParentSpanID       string          `json:"parent_span_id"`
	ServiceName        string          `json:"service_name"`
	Name               string          `json:"name"`
	Kind               string          `json:"kind"`
	DurationNano       json.RawMessage `json:"duration_nano"`
	StatusCode         string          `json:"status_code"`
	StatusMessage      string          `json:"status_message"`
	ResourceAttributes json.RawMessage `json:"resource_attributes"`
	Attributes         json.RawMessage `json:"attributes"`
}

func (r traceSpanRow) toTraceSpan() TraceSpan {
	return TraceSpan{
		Timestamp:          r.Timestamp,
		TraceID:            r.TraceID,
		SpanID:             r.SpanID,
		ParentSpanID:       r.ParentSpanID,
		ServiceName:        r.ServiceName,
		Name:               r.Name,
		Kind:               r.Kind,
		DurationNano:       parseUint(r.DurationNano),
		StatusCode:         r.StatusCode,
		StatusMessage:      r.StatusMessage,
		ResourceAttributes: decodeStringMap(r.ResourceAttributes),
		Attributes:         decodeStringMap(r.Attributes),
	}
}

type logRecordRow struct {
	Timestamp          string          `json:"timestamp"`
	TraceID            string          `json:"trace_id"`
	SpanID             string          `json:"span_id"`
	SeverityText       string          `json:"severity_text"`
	SeverityNumber     int             `json:"severity_number"`
	Body               string          `json:"body"`
	ResourceAttributes json.RawMessage `json:"resource_attributes"`
	Attributes         json.RawMessage `json:"attributes"`
}

func (r logRecordRow) toLogRecord() LogRecord {
	return LogRecord{
		Timestamp:          r.Timestamp,
		TraceID:            r.TraceID,
		SpanID:             r.SpanID,
		SeverityText:       r.SeverityText,
		SeverityNumber:     r.SeverityNumber,
		Body:               r.Body,
		ResourceAttributes: decodeStringMap(r.ResourceAttributes),
		Attributes:         decodeStringMap(r.Attributes),
	}
}

func parseUint(raw json.RawMessage) uint64 {
	if len(raw) == 0 {
		return 0
	}
	var n uint64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0
	}
	n, _ = strconv.ParseUint(s, 10, 64)
	return n
}

func decodeStringMap(raw json.RawMessage) map[string]string {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	out := map[string]string{}
	if err := json.Unmarshal(raw, &out); err == nil {
		return out
	}
	var anyMap map[string]any
	if err := json.Unmarshal(raw, &anyMap); err != nil {
		return nil
	}
	for k, v := range anyMap {
		out[k] = fmt.Sprint(v)
	}
	return out
}
