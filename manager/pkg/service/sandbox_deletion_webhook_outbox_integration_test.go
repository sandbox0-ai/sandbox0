package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSandboxDeletionWebhookOutboxEnqueueIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	var deliveryCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deliveryCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	deletedAt := time.Now().UTC().Truncate(time.Microsecond)
	record := rootFSTestSandboxRecord("sandbox-webhook", "team-1")
	record.Status = SandboxStatusTerminating
	record.Config.Webhook = &WebhookConfig{
		URL:    server.URL,
		Secret: "secret",
	}
	require.NoError(t, store.UpsertSandbox(ctx, record))

	require.NoError(t, store.MarkSandboxDeleted(ctx, record.ID, deletedAt))

	var eventID, sandboxID, teamID, targetURL, signature string
	var payload []byte
	var createdAt, expiresAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT event_id, sandbox_id, team_id, target_url, payload, signature, created_at, expires_at
		FROM manager.sandbox_deletion_webhook_outbox
		WHERE event_id = $1
	`, "evt_sandbox_deleted_"+record.ID).Scan(
		&eventID,
		&sandboxID,
		&teamID,
		&targetURL,
		&payload,
		&signature,
		&createdAt,
		&expiresAt,
	))
	assert.Equal(t, "evt_sandbox_deleted_"+record.ID, eventID)
	assert.Equal(t, record.ID, sandboxID)
	assert.Equal(t, record.TeamID, teamID)
	assert.Equal(t, record.Config.Webhook.URL, targetURL)
	assert.Equal(t, signWebhookPayload("secret", payload), signature)
	assert.Equal(t, sandboxDeletionWebhookDeliveryWindow, expiresAt.Sub(createdAt))

	var event sandboxDeletedWebhookEvent
	require.NoError(t, json.Unmarshal(payload, &event))
	assert.Equal(t, eventID, event.EventID)
	assert.Equal(t, "sandbox.deleted", event.EventType)
	assert.Equal(t, deletedAt, event.Timestamp)
	assert.Equal(t, record.ID, event.SandboxID)
	assert.Equal(t, record.TeamID, event.TeamID)
	assert.Equal(t, "pod_deleted", event.Payload.Reason)

	firstExpiresAt := expiresAt
	require.NoError(t, store.MarkSandboxDeleted(ctx, record.ID, deletedAt.Add(time.Hour)))
	var count int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*), MAX(expires_at)
		FROM manager.sandbox_deletion_webhook_outbox
		WHERE event_id = $1
	`, eventID).Scan(&count, &expiresAt))
	assert.Equal(t, 1, count)
	assert.Equal(t, firstExpiresAt, expiresAt, "a repeated reconcile must not extend the delivery deadline")

	// A fresh dispatcher instance represents a manager restart after the
	// deletion transaction committed but before delivery completed.
	dispatcher := NewSandboxDeletionWebhookDispatcher(
		NewSandboxDeletionWebhookOutbox(pool),
		server.Client(),
		SandboxDeletionWebhookDispatcherConfig{WorkerID: "restarted-manager"},
		nil,
	)
	require.NoError(t, dispatcher.RunOnce(ctx))
	assert.Equal(t, int32(1), deliveryCalls.Load())
	var deliveredAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT delivered_at
		FROM manager.sandbox_deletion_webhook_outbox
		WHERE event_id = $1
	`, eventID).Scan(&deliveredAt))
	assert.NotNil(t, deliveredAt)
}

func TestMarkSandboxDeletedDoesNotQueueInternalCleanupFailure(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-claim-failure", "team-1")
	record.Config.Webhook = &WebhookConfig{URL: "https://example.test/webhook"}
	require.NoError(t, store.UpsertSandbox(ctx, record))

	require.NoError(t, store.MarkSandboxDeleted(ctx, record.ID, time.Now().UTC()))

	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.sandbox_deletion_webhook_outbox`).Scan(&count))
	assert.Zero(t, count)
}

func TestSandboxDeletionWebhookEnqueueIsAtomicWithDeletedState(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-atomic", "team-1")
	record.Status = SandboxStatusTerminating
	record.Config.Webhook = &WebhookConfig{URL: "https://example.test/webhook"}
	require.NoError(t, store.UpsertSandbox(ctx, record))
	_, err := pool.Exec(ctx, `DROP TABLE manager.sandbox_deletion_webhook_outbox`)
	require.NoError(t, err)

	err = store.MarkSandboxDeleted(ctx, record.ID, time.Now().UTC())
	require.Error(t, err)

	loaded, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, SandboxStatusTerminating, loaded.Status)
	assert.True(t, loaded.DeletedAt.IsZero())
}
