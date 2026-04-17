package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// EventType defines the webhook event type.
type EventType string

const (
	// Sandbox events
	EventTypeSandboxReady   EventType = "sandbox.ready"
	EventTypeSandboxKilled  EventType = "sandbox.killed"
	EventTypeSandboxPaused  EventType = "sandbox.paused"
	EventTypeSandboxResumed EventType = "sandbox.resumed"

	// Process events
	EventTypeProcessStarted EventType = "process.started"
	EventTypeProcessExited  EventType = "process.exited"
	EventTypeProcessCrashed EventType = "process.crashed"

	// File system events
	EventTypeFileModified EventType = "file.modified"

	// Agent events
	EventTypeAgentEvent EventType = "agent.event"
)

// Event represents a webhook event payload.
type Event struct {
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	SandboxID string    `json:"sandbox_id"`
	TeamID    string    `json:"team_id"`
	Payload   any       `json:"payload"`
}

// Config holds webhook dispatch configuration.
type Config struct {
	URL    string
	Secret string
}

// Options controls dispatcher behavior.
type Options struct {
	QueueSize      int
	MaxRetries     int
	BaseBackoff    time.Duration
	RequestTimeout time.Duration
	OutboxDir      string
}

// Dispatcher sends webhook events asynchronously. When OutboxDir is configured,
// Enqueue only succeeds after the signed delivery record is durably written.
type Dispatcher struct {
	logger *zap.Logger
	client *http.Client

	queue chan Event
	wake  chan struct{}

	mu         sync.RWMutex
	config     Config
	sandbox    string
	teamID     string
	options    Options
	randSeed   *rand.Rand
	enqueueMu  sync.Mutex
	closed     bool
	workerDone chan struct{}
}

var (
	ErrDispatcherClosed = errors.New("webhook dispatcher closed")
	ErrQueueFull        = errors.New("webhook queue full")
)

type deliveryRecord struct {
	Event         Event           `json:"event"`
	TargetURL     string          `json:"target_url"`
	Body          json.RawMessage `json:"body"`
	Signature     string          `json:"signature,omitempty"`
	Attempts      int             `json:"attempts"`
	NextAttemptAt time.Time       `json:"next_attempt_at"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	LastError     string          `json:"last_error,omitempty"`
}

// NewDispatcher creates a new dispatcher.
func NewDispatcher(options Options, logger *zap.Logger) *Dispatcher {
	if options.QueueSize <= 0 {
		options.QueueSize = 256
	}
	if options.MaxRetries < 0 {
		options.MaxRetries = 0
	}
	if options.BaseBackoff <= 0 {
		options.BaseBackoff = 500 * time.Millisecond
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 5 * time.Second
	}

	d := &Dispatcher{
		logger:     logger,
		client:     &http.Client{Timeout: options.RequestTimeout},
		queue:      make(chan Event, options.QueueSize),
		wake:       make(chan struct{}, 1),
		options:    options,
		randSeed:   rand.New(rand.NewSource(time.Now().UnixNano())),
		workerDone: make(chan struct{}),
	}

	go d.worker()
	return d
}

// SetConfig updates the webhook target configuration.
func (d *Dispatcher) SetConfig(url, secret string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.config = Config{
		URL:    url,
		Secret: secret,
	}
}

// SetIdentity sets the sandbox and team identifiers.
func (d *Dispatcher) SetIdentity(sandboxID, teamID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sandbox = sandboxID
	d.teamID = teamID
}

// Identity returns the current identity context.
func (d *Dispatcher) Identity() (string, string) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sandbox, d.teamID
}

// Enqueue sends an event to the dispatcher queue and returns its event ID.
func (d *Dispatcher) Enqueue(event Event) (string, error) {
	if event.EventID == "" {
		event.EventID = "evt_" + uuid.NewString()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	d.fillIdentity(&event)

	if !d.isConfigured() {
		return event.EventID, nil
	}

	d.enqueueMu.Lock()
	defer d.enqueueMu.Unlock()
	if d.closed {
		return event.EventID, ErrDispatcherClosed
	}
	if d.options.OutboxDir != "" {
		if err := d.enqueueDurable(event); err != nil {
			return event.EventID, err
		}
		return event.EventID, nil
	}

	select {
	case d.queue <- event:
		return event.EventID, nil
	default:
		if d.logger != nil {
			d.logger.Warn("Webhook queue full, dropping event",
				zap.String("event_id", event.EventID),
				zap.String("event_type", string(event.EventType)),
			)
		}
		return event.EventID, ErrQueueFull
	}
}

func (d *Dispatcher) worker() {
	defer close(d.workerDone)
	if d.options.OutboxDir != "" {
		d.durableWorker()
		return
	}
	for event := range d.queue {
		d.sendWithRetry(event)
	}
}

func (d *Dispatcher) durableWorker() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		_ = d.drainOutbox()
		select {
		case <-d.wake:
			continue
		case <-ticker.C:
			continue
		case <-d.queue:
			return
		}
	}
}

// Shutdown drains the queue and waits for the worker to finish.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	d.enqueueMu.Lock()
	if d.closed {
		d.enqueueMu.Unlock()
	} else {
		d.closed = true
		close(d.queue)
		d.enqueueMu.Unlock()
	}

	select {
	case <-d.workerDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Dispatcher) sendWithRetry(event Event) {
	cfg := d.getConfig()
	if cfg.URL == "" {
		return
	}

	var lastErr error
	for attempt := 0; attempt <= d.options.MaxRetries; attempt++ {
		status, err := d.sendOnce(cfg, event)
		if err == nil && status >= 200 && status < 300 {
			return
		}
		if !shouldRetry(status, err) || attempt == d.options.MaxRetries {
			lastErr = err
			break
		}
		lastErr = err
		time.Sleep(d.backoffForAttempt(attempt))
	}

	if d.logger != nil && lastErr != nil {
		d.logger.Warn("Webhook delivery failed",
			zap.String("event_id", event.EventID),
			zap.String("event_type", string(event.EventType)),
			zap.Error(lastErr),
		)
	}
}

func (d *Dispatcher) sendOnce(cfg Config, event Event) (int, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest(http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	if cfg.Secret != "" {
		req.Header.Set("X-Sandbox0-Signature", signPayload(cfg.Secret, body))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func (d *Dispatcher) enqueueDurable(event Event) error {
	cfg := d.getConfig()
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	record := deliveryRecord{
		Event:         event,
		TargetURL:     cfg.URL,
		Body:          append([]byte(nil), body...),
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
		NextAttemptAt: time.Time{},
	}
	if cfg.Secret != "" {
		record.Signature = signPayload(cfg.Secret, body)
	}
	if err := d.writeRecord(record); err != nil {
		return err
	}
	d.wakeWorker()
	return nil
}

func (d *Dispatcher) writeRecord(record deliveryRecord) error {
	if record.Event.EventID == "" {
		return fmt.Errorf("missing event id")
	}
	if err := os.MkdirAll(d.options.OutboxDir, 0o700); err != nil {
		return fmt.Errorf("create webhook outbox: %w", err)
	}
	path := d.recordPath(record.Event.EventID)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat webhook outbox record: %w", err)
	}

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(d.options.OutboxDir, "."+record.Event.EventID+".*.tmp")
	if err != nil {
		return fmt.Errorf("create webhook outbox temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write webhook outbox temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync webhook outbox temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close webhook outbox temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("commit webhook outbox record: %w", err)
	}
	cleanup = false
	if dir, err := os.Open(d.options.OutboxDir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (d *Dispatcher) drainOutbox() error {
	if d.options.OutboxDir == "" {
		return nil
	}
	entries, err := filepath.Glob(filepath.Join(d.options.OutboxDir, "*.json"))
	if err != nil {
		return err
	}
	sort.Strings(entries)
	now := time.Now().UTC()
	for _, path := range entries {
		record, err := readRecord(path)
		if err != nil {
			d.moveBadRecord(path, err)
			continue
		}
		if !record.NextAttemptAt.IsZero() && record.NextAttemptAt.After(now) {
			continue
		}
		status, err := d.sendRecord(record)
		if err == nil && status >= 200 && status < 300 {
			if removeErr := os.Remove(path); removeErr != nil && d.logger != nil {
				d.logger.Warn("Failed to remove delivered webhook outbox record",
					zap.String("path", path),
					zap.Error(removeErr),
				)
			}
			continue
		}
		if !shouldRetry(status, err) {
			d.moveFailedRecord(path, record, status, err)
			continue
		}
		record.Attempts++
		record.UpdatedAt = now
		record.NextAttemptAt = now.Add(d.backoffForAttempt(record.Attempts))
		if err != nil {
			record.LastError = err.Error()
		} else {
			record.LastError = fmt.Sprintf("http status %d", status)
		}
		if saveErr := writeRecordFile(path, record); saveErr != nil && d.logger != nil {
			d.logger.Warn("Failed to update webhook outbox retry state",
				zap.String("event_id", record.Event.EventID),
				zap.Error(saveErr),
			)
		}
	}
	return nil
}

func readRecord(path string) (deliveryRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return deliveryRecord{}, err
	}
	var record deliveryRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return deliveryRecord{}, err
	}
	if record.TargetURL == "" || len(record.Body) == 0 || record.Event.EventID == "" {
		return deliveryRecord{}, fmt.Errorf("invalid webhook outbox record")
	}
	return record, nil
}

func writeRecordFile(path string, record deliveryRecord) error {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *Dispatcher) sendRecord(record deliveryRecord) (int, error) {
	req, err := http.NewRequest(http.MethodPost, record.TargetURL, bytes.NewReader(record.Body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if record.Signature != "" {
		req.Header.Set("X-Sandbox0-Signature", record.Signature)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func (d *Dispatcher) wakeWorker() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *Dispatcher) moveBadRecord(path string, cause error) {
	if d.logger != nil {
		d.logger.Warn("Invalid webhook outbox record",
			zap.String("path", path),
			zap.Error(cause),
		)
	}
	_ = os.MkdirAll(filepath.Join(d.options.OutboxDir, "bad"), 0o700)
	_ = os.Rename(path, filepath.Join(d.options.OutboxDir, "bad", filepath.Base(path)))
}

func (d *Dispatcher) moveFailedRecord(path string, record deliveryRecord, status int, cause error) {
	if d.logger != nil {
		fields := []zap.Field{
			zap.String("event_id", record.Event.EventID),
			zap.String("event_type", string(record.Event.EventType)),
			zap.Int("status", status),
		}
		if cause != nil {
			fields = append(fields, zap.Error(cause))
		}
		d.logger.Warn("Webhook delivery permanently failed", fields...)
	}
	_ = os.MkdirAll(filepath.Join(d.options.OutboxDir, "failed"), 0o700)
	_ = os.Rename(path, filepath.Join(d.options.OutboxDir, "failed", filepath.Base(path)))
}

func (d *Dispatcher) recordPath(eventID string) string {
	return filepath.Join(d.options.OutboxDir, eventID+".json")
}

func (d *Dispatcher) fillIdentity(event *Event) {
	if event == nil {
		return
	}
	d.mu.RLock()
	sandboxID := d.sandbox
	teamID := d.teamID
	d.mu.RUnlock()

	if event.SandboxID == "" {
		event.SandboxID = sandboxID
	}
	if event.TeamID == "" {
		event.TeamID = teamID
	}
}

func (d *Dispatcher) getConfig() Config {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.config
}

func (d *Dispatcher) isConfigured() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.config.URL != ""
}

func (d *Dispatcher) backoffForAttempt(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	maxJitter := int64(d.options.BaseBackoff / 2)
	jitter := time.Duration(d.randSeed.Int63n(maxJitter + 1))
	backoff := float64(d.options.BaseBackoff) * math.Pow(2, float64(attempt-1))
	return time.Duration(backoff) + jitter
}

func shouldRetry(status int, err error) bool {
	if err != nil {
		return true
	}
	if status == http.StatusTooManyRequests {
		return true
	}
	return status >= 500
}

func signPayload(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}
