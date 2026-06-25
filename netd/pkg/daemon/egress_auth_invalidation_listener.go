package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
	"go.uber.org/zap"
)

func startCredentialSourceRotationListener(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger, proxyServer *proxy.Server) {
	if pool == nil || proxyServer == nil {
		return
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	go func() {
		backoff := time.Second
		for {
			if ctx.Err() != nil {
				return
			}

			if err := listenCredentialSourceRotationsOnce(ctx, pool, logger, proxyServer); err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Warn("Credential source rotation listener stopped, retrying",
					zap.Error(err),
					zap.Duration("backoff", backoff),
				)
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				if backoff < 10*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second
		}
	}()
}

func listenCredentialSourceRotationsOnce(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger, proxyServer *proxy.Server) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire listen connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+pubsub.CredentialSourceRotationChannel); err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	logger.Info("Credential source rotation listener started")
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("wait for notification: %w", err)
		}
		if notification.Channel != pubsub.CredentialSourceRotationChannel {
			continue
		}

		var event pubsub.CredentialSourceRotatedEvent
		if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
			logger.Warn("Failed to decode credential source rotation event", zap.Error(err))
			continue
		}
		if !validCredentialSourceRotationEvent(event) {
			logger.Warn("Invalid credential source rotation event payload",
				zap.String("team_id", event.TeamID),
				zap.Int64("source_id", event.SourceID),
				zap.String("source_ref", event.SourceRef),
				zap.Int64("source_version", event.SourceVersion),
			)
			continue
		}

		removed := proxyServer.InvalidateEgressAuthSource(event.TeamID, event.SourceID, event.SourceRef, event.SourceVersion)
		logger.Info("Invalidated egress auth cache for credential source rotation",
			zap.String("team_id", event.TeamID),
			zap.Int64("source_id", event.SourceID),
			zap.String("source_ref", event.SourceRef),
			zap.Int64("source_version", event.SourceVersion),
			zap.Int("removed_entries", removed),
		)
	}
}

func validCredentialSourceRotationEvent(event pubsub.CredentialSourceRotatedEvent) bool {
	return strings.TrimSpace(event.TeamID) != "" &&
		(event.SourceID > 0 || strings.TrimSpace(event.SourceRef) != "") &&
		event.SourceVersion > 0
}
