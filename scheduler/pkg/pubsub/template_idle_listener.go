package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
	"go.uber.org/zap"
)

// TemplateIdleHandler handles template idle events.
type TemplateIdleHandler func(event pubsub.TemplateIdleEvent)

// StartTemplateIdleListener starts a LISTEN loop for template idle events.
func StartTemplateIdleListener(ctx context.Context, databaseURL string, logger *zap.Logger, handler TemplateIdleHandler) {
	go func() {
		backoff := time.Second
		for {
			if ctx.Err() != nil {
				return
			}

			if err := listenOnce(ctx, databaseURL, logger, handler); err != nil {
				logger.Warn("Template idle listener stopped, retrying",
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

func listenOnce(ctx context.Context, databaseURL string, logger *zap.Logger, handler TemplateIdleHandler) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	_, err = conn.Exec(ctx, "LISTEN "+pubsub.TemplateIdleChannel)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	logger.Info("Template idle listener started")

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("wait for notification: %w", err)
		}

		var event pubsub.TemplateIdleEvent
		if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
			logger.Warn("Failed to decode template idle event", zap.Error(err))
			continue
		}

		if event.ClusterID == "" || event.TemplateID == "" {
			logger.Warn("Invalid template idle event payload",
				zap.String("cluster_id", event.ClusterID),
				zap.String("template_id", event.TemplateID),
			)
			continue
		}

		if handler != nil {
			handler(event)
		}
	}
}
