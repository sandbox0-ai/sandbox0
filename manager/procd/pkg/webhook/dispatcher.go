package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/rand"
	"net/http"
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
}

// Dispatcher sends webhook events asynchronously.
type Dispatcher struct {
	logger *zap.Logger
	client *http.Client

	queue chan Event

	mu       sync.RWMutex
	config   Config
	sandbox  string
	teamID   string
	options  Options
	randSeed *rand.Rand
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
		logger:   logger,
		client:   &http.Client{Timeout: options.RequestTimeout},
		queue:    make(chan Event, options.QueueSize),
		options:  options,
		randSeed: rand.New(rand.NewSource(time.Now().UnixNano())),
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

// Enqueue sends an event to the dispatcher queue.
func (d *Dispatcher) Enqueue(event Event) {
	if event.EventID == "" {
		event.EventID = "evt_" + uuid.NewString()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	d.fillIdentity(&event)

	if !d.isConfigured() {
		return
	}

	select {
	case d.queue <- event:
	default:
		if d.logger != nil {
			d.logger.Warn("Webhook queue full, dropping event",
				zap.String("event_id", event.EventID),
				zap.String("event_type", string(event.EventType)),
			)
		}
	}
}

func (d *Dispatcher) worker() {
	for event := range d.queue {
		d.sendWithRetry(event)
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
