package deletionwebhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/retryqueue"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	sandboxDeletionWebhookDeliveryWindow = 24 * time.Hour

	defaultSandboxDeletionWebhookDispatchInterval    = time.Second
	defaultSandboxDeletionWebhookMaintenanceInterval = time.Minute
	defaultSandboxDeletionWebhookBatchSize           = 32
	defaultSandboxDeletionWebhookConcurrency         = 8
	defaultSandboxDeletionWebhookClaimTTL            = 30 * time.Second
	defaultSandboxDeletionWebhookBackoffBase         = time.Second
	defaultSandboxDeletionWebhookBackoffMax          = 5 * time.Minute
	defaultSandboxDeletionWebhookRequestTimeout      = 5 * time.Second
	defaultSandboxDeletionWebhookTerminalRetention   = 24 * time.Hour
	maxSandboxDeletionWebhookBatchSize               = 256
	maxSandboxDeletionWebhookConcurrency             = 32
	maxSandboxDeletionWebhookRequestTimeout          = 30 * time.Second

	maxSandboxDeletionWebhookErrorLength = 2048
)

// DeliveryWindow is the immutable retry deadline stored with each event.
const DeliveryWindow = sandboxDeletionWebhookDeliveryWindow

type sandboxDeletedWebhookEvent struct {
	EventID   string                       `json:"event_id"`
	EventType string                       `json:"event_type"`
	Timestamp time.Time                    `json:"timestamp"`
	SandboxID string                       `json:"sandbox_id"`
	TeamID    string                       `json:"team_id"`
	Payload   sandboxDeletedWebhookPayload `json:"payload"`
}

type sandboxDeletedWebhookPayload struct {
	Reason string `json:"reason"`
}

// Enqueue persists the exact signed request that will be
// retried after sandbox deletion commits. The event ID conflict keeps both the
// payload and its original delivery deadline immutable across reconciles.
func Enqueue(ctx context.Context, tx pgx.Tx, sandboxID, teamID, targetURL, secret string, deletedAt time.Time) error {
	if tx == nil || strings.TrimSpace(targetURL) == "" {
		return nil
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	targetURL = strings.TrimSpace(targetURL)
	eventID := "evt_sandbox_deleted_" + sandboxID
	event := sandboxDeletedWebhookEvent{
		EventID:   eventID,
		EventType: "sandbox.deleted",
		Timestamp: deletedAt.UTC(),
		SandboxID: sandboxID,
		TeamID:    teamID,
		Payload: sandboxDeletedWebhookPayload{
			Reason: "pod_deleted",
		},
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal sandbox deletion webhook: %w", err)
	}
	signature := ""
	if secret != "" {
		signature = signWebhookPayload(secret, payload)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO manager.sandbox_deletion_webhook_outbox (
			event_id, sandbox_id, team_id, target_url, payload, signature,
			next_attempt_at, expires_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW() + ($7::bigint * INTERVAL '1 second'), NOW(), NOW())
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, sandboxID, teamID, targetURL, payload, signature, int64(sandboxDeletionWebhookDeliveryWindow/time.Second))
	if err != nil {
		return fmt.Errorf("enqueue sandbox deletion webhook: %w", err)
	}
	return nil
}

func signWebhookPayload(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// SandboxDeletionWebhookOutbox stores manager-owned sandbox.deleted deliveries.
type SandboxDeletionWebhookOutbox struct {
	pool *pgxpool.Pool
}

// NewSandboxDeletionWebhookOutbox creates the PostgreSQL delivery repository.
func NewSandboxDeletionWebhookOutbox(pool *pgxpool.Pool) *SandboxDeletionWebhookOutbox {
	if pool == nil {
		return nil
	}
	return &SandboxDeletionWebhookOutbox{pool: pool}
}

type sandboxDeletionWebhookDelivery struct {
	EventID   string
	SandboxID string
	TargetURL string
	Payload   []byte
	Signature string
	Attempts  int
	ExpiresAt time.Time
}

type sandboxDeletionWebhookOutboxStats struct {
	Pending       int64
	Due           int64
	Claimed       int64
	Delivered     int64
	Terminal      int64
	OldestPending time.Time
}

type sandboxDeletionWebhookOutboxRepository interface {
	claim(context.Context, string, int, time.Duration) ([]sandboxDeletionWebhookDelivery, error)
	markDelivered(context.Context, string, string, int) error
	markTerminal(context.Context, string, string, int, string, string) error
	recordRetry(context.Context, string, string, int, string, time.Duration) error
	expire(context.Context) (int64, error)
	purge(context.Context, time.Duration) (int64, error)
	stats(context.Context) (*sandboxDeletionWebhookOutboxStats, error)
}

func (o *SandboxDeletionWebhookOutbox) claim(ctx context.Context, workerID string, limit int, claimTTL time.Duration) ([]sandboxDeletionWebhookDelivery, error) {
	if o == nil || o.pool == nil {
		return nil, nil
	}
	rows, err := o.pool.Query(ctx, `
		WITH due AS (
			SELECT event_id
			FROM manager.sandbox_deletion_webhook_outbox
			WHERE delivered_at IS NULL
				AND terminal_at IS NULL
				AND expires_at > NOW()
				AND next_attempt_at <= NOW()
				AND (claimed_until IS NULL OR claimed_until <= NOW())
			ORDER BY next_attempt_at ASC, created_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE manager.sandbox_deletion_webhook_outbox o
			SET claimed_by = $1,
				claimed_until = NOW() + ($3::int * INTERVAL '1 second'),
				updated_at = NOW()
			FROM due
			WHERE o.event_id = due.event_id
			RETURNING o.event_id, o.sandbox_id, o.target_url, o.payload,
				o.signature, o.attempts, o.expires_at
		)
		SELECT event_id, sandbox_id, target_url, payload, signature, attempts, expires_at
		FROM claimed
	`, workerID, limit, retryqueue.DurationSeconds(claimTTL))
	if err != nil {
		return nil, fmt.Errorf("claim sandbox deletion webhooks: %w", err)
	}
	defer rows.Close()

	var deliveries []sandboxDeletionWebhookDelivery
	for rows.Next() {
		var delivery sandboxDeletionWebhookDelivery
		if err := rows.Scan(
			&delivery.EventID,
			&delivery.SandboxID,
			&delivery.TargetURL,
			&delivery.Payload,
			&delivery.Signature,
			&delivery.Attempts,
			&delivery.ExpiresAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimed sandbox deletion webhook: %w", err)
		}
		deliveries = append(deliveries, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed sandbox deletion webhooks: %w", err)
	}
	return deliveries, nil
}

func (o *SandboxDeletionWebhookOutbox) markDelivered(ctx context.Context, eventID, workerID string, status int) error {
	return o.updateClaim(ctx, eventID, workerID, `
		UPDATE manager.sandbox_deletion_webhook_outbox
		SET attempts = attempts + 1,
			delivered_at = NOW(),
			last_status = $3,
			last_error = '',
			claimed_by = '',
			claimed_until = NULL,
			updated_at = NOW()
		WHERE event_id = $1 AND claimed_by = $2
	`, status)
}

func (o *SandboxDeletionWebhookOutbox) markTerminal(ctx context.Context, eventID, workerID string, status int, reason, lastError string) error {
	return o.updateClaim(ctx, eventID, workerID, `
		UPDATE manager.sandbox_deletion_webhook_outbox
		SET attempts = attempts + 1,
			terminal_at = NOW(),
			terminal_reason = $4,
			last_status = $3,
			last_error = $5,
			claimed_by = '',
			claimed_until = NULL,
			updated_at = NOW()
		WHERE event_id = $1 AND claimed_by = $2
	`, nullableWebhookStatus(status), strings.TrimSpace(reason), truncateSandboxDeletionWebhookError(lastError))
}

func (o *SandboxDeletionWebhookOutbox) recordRetry(ctx context.Context, eventID, workerID string, status int, lastError string, delay time.Duration) error {
	return o.updateClaim(ctx, eventID, workerID, `
		UPDATE manager.sandbox_deletion_webhook_outbox
		SET attempts = attempts + 1,
			next_attempt_at = LEAST(NOW() + ($4::int * INTERVAL '1 second'), expires_at),
			last_status = $3,
			last_error = $5,
			claimed_by = '',
			claimed_until = NULL,
			updated_at = NOW()
		WHERE event_id = $1 AND claimed_by = $2
	`, nullableWebhookStatus(status), retryqueue.DurationSeconds(delay), truncateSandboxDeletionWebhookError(lastError))
}

func (o *SandboxDeletionWebhookOutbox) updateClaim(ctx context.Context, eventID, workerID, query string, args ...any) error {
	if o == nil || o.pool == nil {
		return nil
	}
	arguments := append([]any{eventID, workerID}, args...)
	tag, err := o.pool.Exec(ctx, query, arguments...)
	if err != nil {
		return fmt.Errorf("update sandbox deletion webhook %s: %w", eventID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("sandbox deletion webhook %s claim is no longer owned by %s", eventID, workerID)
	}
	return nil
}

func (o *SandboxDeletionWebhookOutbox) expire(ctx context.Context) (int64, error) {
	if o == nil || o.pool == nil {
		return 0, nil
	}
	tag, err := o.pool.Exec(ctx, `
		UPDATE manager.sandbox_deletion_webhook_outbox
		SET terminal_at = NOW(),
			terminal_reason = 'delivery_deadline_exceeded',
			last_error = 'sandbox.deleted webhook delivery window expired',
			claimed_by = '',
			claimed_until = NULL,
			updated_at = NOW()
		WHERE delivered_at IS NULL
			AND terminal_at IS NULL
			AND expires_at <= NOW()
			AND (claimed_until IS NULL OR claimed_until <= NOW())
	`)
	if err != nil {
		return 0, fmt.Errorf("expire sandbox deletion webhooks: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (o *SandboxDeletionWebhookOutbox) purge(ctx context.Context, retention time.Duration) (int64, error) {
	if o == nil || o.pool == nil {
		return 0, nil
	}
	tag, err := o.pool.Exec(ctx, `
		DELETE FROM manager.sandbox_deletion_webhook_outbox
		WHERE (delivered_at IS NOT NULL AND delivered_at <= NOW() - ($1::int * INTERVAL '1 second'))
			OR (terminal_at IS NOT NULL AND terminal_at <= NOW() - ($1::int * INTERVAL '1 second'))
	`, retryqueue.DurationSeconds(retention))
	if err != nil {
		return 0, fmt.Errorf("purge terminal sandbox deletion webhooks: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (o *SandboxDeletionWebhookOutbox) stats(ctx context.Context) (*sandboxDeletionWebhookOutboxStats, error) {
	if o == nil || o.pool == nil {
		return nil, nil
	}
	var stats sandboxDeletionWebhookOutboxStats
	var oldestPending *time.Time
	err := o.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (
				WHERE delivered_at IS NULL AND terminal_at IS NULL AND expires_at > NOW()
			) AS pending,
			COUNT(*) FILTER (
				WHERE delivered_at IS NULL AND terminal_at IS NULL AND expires_at > NOW()
					AND next_attempt_at <= NOW()
					AND (claimed_until IS NULL OR claimed_until <= NOW())
			) AS due,
			COUNT(*) FILTER (
				WHERE delivered_at IS NULL AND terminal_at IS NULL AND expires_at > NOW()
					AND claimed_until > NOW()
			) AS claimed,
			COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered,
			COUNT(*) FILTER (WHERE terminal_at IS NOT NULL) AS terminal,
			MIN(created_at) FILTER (
				WHERE delivered_at IS NULL AND terminal_at IS NULL AND expires_at > NOW()
			) AS oldest_pending
		FROM manager.sandbox_deletion_webhook_outbox
	`).Scan(&stats.Pending, &stats.Due, &stats.Claimed, &stats.Delivered, &stats.Terminal, &oldestPending)
	if err != nil {
		return nil, fmt.Errorf("load sandbox deletion webhook outbox stats: %w", err)
	}
	if oldestPending != nil {
		stats.OldestPending = *oldestPending
	}
	return &stats, nil
}

func nullableWebhookStatus(status int) any {
	if status <= 0 {
		return nil
	}
	return status
}

func truncateSandboxDeletionWebhookError(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= maxSandboxDeletionWebhookErrorLength {
		return message
	}
	return message[:maxSandboxDeletionWebhookErrorLength]
}

func sandboxDeletionWebhookErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Sprintf("%s webhook request: %v", urlErr.Op, urlErr.Err)
	}
	return err.Error()
}

// SandboxDeletionWebhookDispatcherConfig bounds manager resource use while an
// external webhook endpoint is unavailable.
type SandboxDeletionWebhookDispatcherConfig struct {
	Interval            time.Duration
	MaintenanceInterval time.Duration
	BatchSize           int
	Concurrency         int
	ClaimTTL            time.Duration
	BackoffBase         time.Duration
	BackoffMax          time.Duration
	RequestTimeout      time.Duration
	TerminalRetention   time.Duration
	WorkerID            string
}

// SandboxDeletionWebhookDispatcher delivers manager-owned sandbox.deleted
// events without coupling sandbox resource cleanup to external HTTP health.
type SandboxDeletionWebhookDispatcher struct {
	repo            sandboxDeletionWebhookOutboxRepository
	httpClient      *http.Client
	cfg             SandboxDeletionWebhookDispatcherConfig
	logger          *zap.Logger
	metrics         *obsmetrics.ManagerMetrics
	now             func() time.Time
	nextMaintenance time.Time
}

// NewSandboxDeletionWebhookDispatcher creates the bounded asynchronous worker.
func NewSandboxDeletionWebhookDispatcher(outbox *SandboxDeletionWebhookOutbox, httpClient *http.Client, cfg SandboxDeletionWebhookDispatcherConfig, logger *zap.Logger) *SandboxDeletionWebhookDispatcher {
	return newSandboxDeletionWebhookDispatcher(outbox, httpClient, cfg, logger)
}

func newSandboxDeletionWebhookDispatcher(repo sandboxDeletionWebhookOutboxRepository, httpClient *http.Client, cfg SandboxDeletionWebhookDispatcherConfig, logger *zap.Logger) *SandboxDeletionWebhookDispatcher {
	cfg = normalizeSandboxDeletionWebhookDispatcherConfig(cfg)
	if httpClient == nil {
		httpClient = &http.Client{Timeout: cfg.RequestTimeout}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SandboxDeletionWebhookDispatcher{
		repo:       repo,
		httpClient: httpClient,
		cfg:        cfg,
		logger:     logger,
		now:        time.Now,
	}
}

// SetMetrics enables delivery and queue metrics.
func (d *SandboxDeletionWebhookDispatcher) SetMetrics(metrics *obsmetrics.ManagerMetrics) {
	if d == nil {
		return
	}
	d.metrics = metrics
}

// Run dispatches due records until the context is canceled.
func (d *SandboxDeletionWebhookDispatcher) Run(ctx context.Context) error {
	if d == nil || d.repo == nil {
		return nil
	}
	ticker := time.NewTicker(d.cfg.Interval)
	defer ticker.Stop()
	d.logger.Info("Starting sandbox deletion webhook dispatcher",
		zap.String("workerID", d.cfg.WorkerID),
		zap.Duration("interval", d.cfg.Interval),
		zap.Duration("maintenanceInterval", d.cfg.MaintenanceInterval),
		zap.Int("batchSize", d.cfg.BatchSize),
		zap.Int("concurrency", d.cfg.Concurrency),
		zap.Duration("requestTimeout", d.cfg.RequestTimeout),
		zap.Duration("deliveryWindow", sandboxDeletionWebhookDeliveryWindow),
	)
	for {
		if err := d.RunOnce(ctx); err != nil && ctx.Err() == nil {
			d.logger.Warn("Sandbox deletion webhook dispatch cycle failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			d.logger.Info("Sandbox deletion webhook dispatcher stopped")
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// RunOnce expires old records, claims one bounded batch, and dispatches it.
func (d *SandboxDeletionWebhookDispatcher) RunOnce(ctx context.Context) error {
	if d == nil || d.repo == nil {
		return nil
	}
	var runErr error
	runMaintenance := d.nextMaintenance.IsZero() || !d.nextMaintenance.After(d.now())
	if runMaintenance {
		expired, err := d.repo.expire(ctx)
		if err == nil {
			d.observeExpired(expired)
			if expired > 0 {
				d.logger.Warn("Sandbox deletion webhook delivery windows expired",
					zap.Int64("count", expired),
				)
			}
		} else {
			runErr = errors.Join(runErr, err)
		}
		purged, err := d.repo.purge(ctx, d.cfg.TerminalRetention)
		if err != nil {
			runErr = errors.Join(runErr, err)
		} else if purged > 0 {
			d.logger.Debug("Purged terminal sandbox deletion webhook records",
				zap.Int64("count", purged),
			)
		}
		d.nextMaintenance = d.now().Add(d.cfg.MaintenanceInterval)
	}
	deliveries, err := d.repo.claim(ctx, d.cfg.WorkerID, d.cfg.BatchSize, d.cfg.ClaimTTL)
	if err != nil {
		return errors.Join(runErr, err)
	}

	var group errgroup.Group
	group.SetLimit(d.cfg.Concurrency)
	for i := range deliveries {
		delivery := deliveries[i]
		group.Go(func() error {
			return d.deliver(ctx, delivery)
		})
	}
	deliveryErr := group.Wait()
	var statsErr error
	if runMaintenance {
		statsErr = d.observeQueueStats(ctx)
	}
	return errors.Join(runErr, deliveryErr, statsErr)
}

func (d *SandboxDeletionWebhookDispatcher) deliver(ctx context.Context, delivery sandboxDeletionWebhookDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := d.now().UTC()
	if !delivery.ExpiresAt.After(now) {
		return d.markExpired(ctx, delivery)
	}

	attemptDeadline := now.Add(d.cfg.RequestTimeout)
	if delivery.ExpiresAt.Before(attemptDeadline) {
		attemptDeadline = delivery.ExpiresAt
	}
	attemptCtx, cancel := context.WithDeadline(ctx, attemptDeadline)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, delivery.TargetURL, bytes.NewReader(delivery.Payload))
	if err != nil {
		started := d.now()
		persistErr := d.repo.markTerminal(ctx, delivery.EventID, d.cfg.WorkerID, 0, "invalid_target_url", "invalid webhook target URL")
		d.observeAttempt("terminal", "invalid_request", d.now().Sub(started))
		return persistErr
	}
	if (req.URL.Scheme != "http" && req.URL.Scheme != "https") || req.URL.Host == "" {
		started := d.now()
		persistErr := d.repo.markTerminal(ctx, delivery.EventID, d.cfg.WorkerID, 0, "invalid_target_url", "webhook target must use http or https and include a host")
		d.observeAttempt("terminal", "invalid_request", d.now().Sub(started))
		return persistErr
	}
	req.Header.Set("Content-Type", "application/json")
	if delivery.Signature != "" {
		req.Header.Set("X-Sandbox0-Signature", delivery.Signature)
	}

	started := d.now()
	resp, requestErr := d.httpClient.Do(req)
	duration := d.now().Sub(started)
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32*1024))
		_ = resp.Body.Close()
	}
	if requestErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !delivery.ExpiresAt.After(d.now().UTC()) {
			d.observeAttempt("expired", "transport", duration)
			return d.markExpired(ctx, delivery)
		}
		delay := d.retryDelay(delivery, "")
		errorMessage := sandboxDeletionWebhookErrorMessage(requestErr)
		if err := d.repo.recordRetry(ctx, delivery.EventID, d.cfg.WorkerID, 0, errorMessage, delay); err != nil {
			return err
		}
		d.observeAttempt("retry", "transport", duration)
		d.logger.Debug("Sandbox deletion webhook delivery deferred",
			zap.String("eventID", delivery.EventID),
			zap.String("sandboxID", delivery.SandboxID),
			zap.Duration("retryAfter", delay),
			zap.String("error", errorMessage),
		)
		return nil
	}

	status := resp.StatusCode
	statusClass := webhookStatusClass(status)
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		if err := d.repo.markDelivered(ctx, delivery.EventID, d.cfg.WorkerID, status); err != nil {
			return err
		}
		d.observeAttempt("delivered", statusClass, duration)
		return nil
	}
	if !retryableSandboxDeletionWebhookStatus(status) {
		message := fmt.Sprintf("sandbox.deleted webhook failed with status %d", status)
		if err := d.repo.markTerminal(ctx, delivery.EventID, d.cfg.WorkerID, status, "permanent_http_status", message); err != nil {
			return err
		}
		d.observeAttempt("terminal", statusClass, duration)
		d.logger.Warn("Sandbox deletion webhook permanently rejected",
			zap.String("eventID", delivery.EventID),
			zap.String("sandboxID", delivery.SandboxID),
			zap.Int("status", status),
		)
		return nil
	}

	delay := d.retryDelay(delivery, resp.Header.Get("Retry-After"))
	message := fmt.Sprintf("sandbox.deleted webhook failed with status %d", status)
	if err := d.repo.recordRetry(ctx, delivery.EventID, d.cfg.WorkerID, status, message, delay); err != nil {
		return err
	}
	d.observeAttempt("retry", statusClass, duration)
	d.logger.Debug("Sandbox deletion webhook delivery deferred",
		zap.String("eventID", delivery.EventID),
		zap.String("sandboxID", delivery.SandboxID),
		zap.Int("status", status),
		zap.Duration("retryAfter", delay),
	)
	return nil
}

func (d *SandboxDeletionWebhookDispatcher) markExpired(ctx context.Context, delivery sandboxDeletionWebhookDelivery) error {
	if err := d.repo.markTerminal(ctx, delivery.EventID, d.cfg.WorkerID, 0, "delivery_deadline_exceeded", "sandbox.deleted webhook delivery window expired"); err != nil {
		return err
	}
	d.observeExpired(1)
	d.logger.Warn("Sandbox deletion webhook delivery window expired",
		zap.String("eventID", delivery.EventID),
		zap.String("sandboxID", delivery.SandboxID),
		zap.Int("attempts", delivery.Attempts),
	)
	return nil
}

func (d *SandboxDeletionWebhookDispatcher) retryDelay(delivery sandboxDeletionWebhookDelivery, retryAfterHeader string) time.Duration {
	delay := sandboxDeletionWebhookBackoff(delivery.Attempts+1, d.cfg.BackoffBase, d.cfg.BackoffMax)
	delay = jitterSandboxDeletionWebhookBackoff(delay, delivery.EventID, delivery.Attempts+1)
	if delay > d.cfg.BackoffMax {
		delay = d.cfg.BackoffMax
	}
	if retryAfter := parseWebhookRetryAfter(retryAfterHeader, d.now().UTC()); retryAfter > delay {
		delay = retryAfter
	}
	remaining := delivery.ExpiresAt.Sub(d.now().UTC())
	if remaining > 0 && delay > remaining {
		delay = remaining
	}
	if delay <= 0 {
		return time.Second
	}
	return delay
}

func normalizeSandboxDeletionWebhookDispatcherConfig(cfg SandboxDeletionWebhookDispatcherConfig) SandboxDeletionWebhookDispatcherConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultSandboxDeletionWebhookDispatchInterval
	}
	if cfg.MaintenanceInterval <= 0 {
		cfg.MaintenanceInterval = defaultSandboxDeletionWebhookMaintenanceInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultSandboxDeletionWebhookBatchSize
	}
	if cfg.BatchSize > maxSandboxDeletionWebhookBatchSize {
		cfg.BatchSize = maxSandboxDeletionWebhookBatchSize
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultSandboxDeletionWebhookConcurrency
	}
	if cfg.Concurrency > maxSandboxDeletionWebhookConcurrency {
		cfg.Concurrency = maxSandboxDeletionWebhookConcurrency
	}
	if cfg.Concurrency > cfg.BatchSize {
		cfg.Concurrency = cfg.BatchSize
	}
	if cfg.ClaimTTL <= 0 {
		cfg.ClaimTTL = defaultSandboxDeletionWebhookClaimTTL
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = defaultSandboxDeletionWebhookBackoffBase
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = defaultSandboxDeletionWebhookBackoffMax
	}
	if cfg.BackoffMax < cfg.BackoffBase {
		cfg.BackoffMax = cfg.BackoffBase
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultSandboxDeletionWebhookRequestTimeout
	}
	if cfg.RequestTimeout > maxSandboxDeletionWebhookRequestTimeout {
		cfg.RequestTimeout = maxSandboxDeletionWebhookRequestTimeout
	}
	waves := (cfg.BatchSize + cfg.Concurrency - 1) / cfg.Concurrency
	minimumClaimTTL := time.Duration(waves)*cfg.RequestTimeout + 5*time.Second
	if cfg.ClaimTTL < minimumClaimTTL {
		cfg.ClaimTTL = minimumClaimTTL
	}
	if cfg.TerminalRetention <= 0 {
		cfg.TerminalRetention = defaultSandboxDeletionWebhookTerminalRetention
	}
	cfg.WorkerID = strings.TrimSpace(cfg.WorkerID)
	if cfg.WorkerID == "" {
		cfg.WorkerID = "manager-webhook-" + uuid.NewString()
	}
	return cfg
}

func sandboxDeletionWebhookBackoff(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = defaultSandboxDeletionWebhookBackoffBase
	}
	if max <= 0 {
		max = defaultSandboxDeletionWebhookBackoffMax
	}
	if max < base {
		max = base
	}
	return retryqueue.ExponentialBackoff(attempt, base, max)
}

func jitterSandboxDeletionWebhookBackoff(delay time.Duration, eventID string, attempt int) time.Duration {
	if delay <= 0 {
		return delay
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(eventID))
	_, _ = hash.Write([]byte(strconv.Itoa(attempt)))
	// Spread retries deterministically over 80%-120% of the base delay.
	factor := 0.8 + (float64(hash.Sum32()%401) / 1000)
	return time.Duration(float64(delay) * factor)
}

func parseWebhookRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds >= int64(sandboxDeletionWebhookDeliveryWindow/time.Second) {
			return sandboxDeletionWebhookDeliveryWindow
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	delay := when.Sub(now)
	if delay > sandboxDeletionWebhookDeliveryWindow {
		return sandboxDeletionWebhookDeliveryWindow
	}
	return delay
}

func retryableSandboxDeletionWebhookStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		status >= http.StatusInternalServerError
}

func webhookStatusClass(status int) string {
	if status <= 0 {
		return "none"
	}
	return fmt.Sprintf("%dxx", status/100)
}

func (d *SandboxDeletionWebhookDispatcher) observeAttempt(result, statusClass string, duration time.Duration) {
	if d == nil || d.metrics == nil {
		return
	}
	if d.metrics.SandboxDeletionWebhookAttemptsTotal != nil {
		d.metrics.SandboxDeletionWebhookAttemptsTotal.WithLabelValues(result, statusClass).Inc()
	}
	if d.metrics.SandboxDeletionWebhookDeliveryDuration != nil {
		d.metrics.SandboxDeletionWebhookDeliveryDuration.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func (d *SandboxDeletionWebhookDispatcher) observeExpired(count int64) {
	if d == nil || d.metrics == nil || d.metrics.SandboxDeletionWebhookExpiredTotal == nil || count <= 0 {
		return
	}
	d.metrics.SandboxDeletionWebhookExpiredTotal.Add(float64(count))
}

func (d *SandboxDeletionWebhookDispatcher) observeQueueStats(ctx context.Context) error {
	if d == nil || d.metrics == nil || d.repo == nil {
		return nil
	}
	stats, err := d.repo.stats(ctx)
	if err != nil {
		return err
	}
	if stats == nil {
		return nil
	}
	if d.metrics.SandboxDeletionWebhookQueueDepth != nil {
		d.metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("pending").Set(float64(stats.Pending))
		d.metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("due").Set(float64(stats.Due))
		d.metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("claimed").Set(float64(stats.Claimed))
		d.metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("delivered").Set(float64(stats.Delivered))
		d.metrics.SandboxDeletionWebhookQueueDepth.WithLabelValues("terminal").Set(float64(stats.Terminal))
	}
	if d.metrics.SandboxDeletionWebhookOldestPendingAge != nil {
		age := 0.0
		if !stats.OldestPending.IsZero() {
			age = d.now().Sub(stats.OldestPending).Seconds()
			if age < 0 {
				age = 0
			}
		}
		d.metrics.SandboxDeletionWebhookOldestPendingAge.Set(age)
	}
	return nil
}

var _ sandboxDeletionWebhookOutboxRepository = (*SandboxDeletionWebhookOutbox)(nil)
