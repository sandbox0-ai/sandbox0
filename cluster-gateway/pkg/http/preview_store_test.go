package http

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func TestMemoryPreviewGrantStoreBootstrapIsOneTime(t *testing.T) {
	now := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	store := newMemoryPreviewGrantStore(func() time.Time { return now })
	record := previewGrantRecord{
		ID:            "preview-1",
		SandboxID:     "sandbox-1",
		BootstrapHash: "bootstrap-hash",
		ExpiresAt:     now.Add(time.Minute),
	}
	if err := store.Put(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	activated, err := store.ConsumeBootstrap(context.Background(), record.ID, "bootstrap-hash", "session-hash")
	if err != nil {
		t.Fatalf("consume bootstrap: %v", err)
	}
	if activated.BootstrapHash != "" || activated.SessionHash != "session-hash" {
		t.Fatalf("activated record = %#v", activated)
	}
	if _, err := store.ConsumeBootstrap(context.Background(), record.ID, "bootstrap-hash", "other-session"); !errors.Is(err, errPreviewBootstrapInvalid) {
		t.Fatalf("bootstrap replay error = %v, want %v", err, errPreviewBootstrapInvalid)
	}
}

func TestMemoryPreviewGrantStoreExpiresAndRevokes(t *testing.T) {
	now := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	store := newMemoryPreviewGrantStore(func() time.Time { return now })
	record := previewGrantRecord{ID: "preview-1", ExpiresAt: now.Add(time.Minute)}
	if err := store.Put(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), record.ID); err != nil {
		t.Fatalf("get live grant: %v", err)
	}
	if err := store.Delete(context.Background(), record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), record.ID); !errors.Is(err, errPreviewGrantNotFound) {
		t.Fatalf("get revoked grant error = %v", err)
	}

	record.ID = "preview-2"
	if err := store.Put(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := store.Get(context.Background(), record.ID); !errors.Is(err, errPreviewGrantNotFound) {
		t.Fatalf("get expired grant error = %v", err)
	}
}

func TestMemoryPreviewGrantStoreRenewsOnlyLiveGrants(t *testing.T) {
	now := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	store := newMemoryPreviewGrantStore(func() time.Time { return now })
	record := previewGrantRecord{ID: "preview-1", ExpiresAt: now.Add(time.Minute)}
	if err := store.Put(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	renewed, err := store.Renew(context.Background(), record.ID, now.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("renew live grant: %v", err)
	}
	if want := now.Add(10 * time.Minute); !renewed.ExpiresAt.Equal(want) {
		t.Fatalf("renewed expiration = %s, want %s", renewed.ExpiresAt, want)
	}
	if err := store.Delete(context.Background(), record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Renew(context.Background(), record.ID, now.Add(20*time.Minute)); !errors.Is(err, errPreviewGrantNotFound) {
		t.Fatalf("renew revoked grant error = %v", err)
	}

	record.ID = "preview-2"
	record.ExpiresAt = now.Add(time.Minute)
	if err := store.Put(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := store.Renew(context.Background(), record.ID, now.Add(time.Minute)); !errors.Is(err, errPreviewGrantNotFound) {
		t.Fatalf("renew expired grant error = %v", err)
	}
}

func TestRedisPreviewGrantStoreSharesAndAtomicallyUpdatesGrants(t *testing.T) {
	redisServer := miniredis.RunT(t)
	cfg := config.GatewayConfig{
		RedisURL:       "redis://" + redisServer.Addr() + "/0",
		RedisKeyPrefix: "sandbox0:test",
		RedisTimeout:   config.Duration{Duration: time.Second},
	}
	first, err := newPreviewGrantStore(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newPreviewGrantStore(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range []previewGrantStore{first, second} {
		redisStore := store.(*redisPreviewGrantStore)
		t.Cleanup(func() { _ = redisStore.client.Close() })
	}

	record := previewGrantRecord{
		ID:            "preview-shared",
		SandboxID:     "sandbox-1",
		BootstrapHash: "bootstrap-hash",
		ExpiresAt:     time.Now().Add(time.Minute).UTC(),
	}
	if err := first.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	activated, err := second.ConsumeBootstrap(t.Context(), record.ID, "bootstrap-hash", "session-hash")
	if err != nil {
		t.Fatalf("consume shared bootstrap: %v", err)
	}
	if activated.SessionHash != "session-hash" {
		t.Fatalf("activated record = %#v", activated)
	}
	if _, err := first.ConsumeBootstrap(t.Context(), record.ID, "bootstrap-hash", "other-session"); !errors.Is(err, errPreviewBootstrapInvalid) {
		t.Fatalf("bootstrap replay error = %v", err)
	}

	expiresAt := time.Now().Add(10 * time.Minute).UTC()
	renewed, err := first.Renew(t.Context(), record.ID, expiresAt)
	if err != nil {
		t.Fatalf("renew shared grant: %v", err)
	}
	if !renewed.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("renewed expiration = %s, want %s", renewed.ExpiresAt, expiresAt)
	}
	if err := second.Delete(t.Context(), record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Renew(t.Context(), record.ID, time.Now().Add(time.Minute)); !errors.Is(err, errPreviewGrantNotFound) {
		t.Fatalf("renew revoked shared grant error = %v", err)
	}
}

func TestNormalizePreviewRequest(t *testing.T) {
	protocol, path, ttl, err := normalizePreviewRequest("", "/dashboard?q=1#logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	if protocol != "http" || path != "/dashboard?q=1#logs" || ttl != defaultPreviewTTL {
		t.Fatalf("normalized request = %q %q %s", protocol, path, ttl)
	}
	for _, invalidPath := range []string{"https://example.com", "//example.com/path", "/\\example.com/path", "relative"} {
		if _, _, _, err := normalizePreviewRequest("http", invalidPath, 60); err == nil {
			t.Fatalf("path %q should be rejected", invalidPath)
		}
	}
	if _, _, _, err := normalizePreviewRequest("ftp", "/", 60); err == nil {
		t.Fatal("unsupported protocol should be rejected")
	}
	if _, _, _, err := normalizePreviewRequest("http", "/", 10); err == nil {
		t.Fatal("short TTL should be rejected")
	}
}
