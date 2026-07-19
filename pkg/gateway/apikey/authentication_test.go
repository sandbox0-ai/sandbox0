package apikey

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidateAPIKeySingleflightForConcurrentValidKey(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	keyValue := testAuthenticationKey(1)
	var lookups atomic.Int64
	lookupStarted := make(chan struct{})
	releaseLookup := make(chan struct{})
	repository := NewRepository(
		nil,
		WithAuthenticationClock(func() time.Time { return now }),
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			if lookups.Add(1) == 1 {
				close(lookupStarted)
			}
			<-releaseLookup
			return testValidatedAPIKey(now), nil
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	const concurrent = 100
	results := make(chan *APIKey, concurrent)
	errs := make(chan error, concurrent)
	var wait sync.WaitGroup
	wait.Add(concurrent)
	for range concurrent {
		go func() {
			defer wait.Done()
			key, err := repository.ValidateAPIKey(context.Background(), keyValue)
			results <- key
			errs <- err
		}()
	}
	<-lookupStarted
	close(releaseLookup)
	wait.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ValidateAPIKey() error = %v", err)
		}
	}
	for key := range results {
		if key == nil || key.ID == "" {
			t.Fatalf("ValidateAPIKey() key = %#v", key)
		}
	}
	if got := lookups.Load(); got != 1 {
		t.Fatalf("lookup count = %d, want 1", got)
	}
}

func TestValidateAPIKeySingleflightForConcurrentInvalidKey(t *testing.T) {
	keyValue := testAuthenticationKey(2)
	var lookups atomic.Int64
	releaseLookup := make(chan struct{})
	repository := NewRepository(
		nil,
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			lookups.Add(1)
			<-releaseLookup
			return nil, ErrInvalidKey
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	const concurrent = 100
	errs := make(chan error, concurrent)
	var wait sync.WaitGroup
	wait.Add(concurrent)
	for range concurrent {
		go func() {
			defer wait.Done()
			_, err := repository.ValidateAPIKey(context.Background(), keyValue)
			errs <- err
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(releaseLookup)
	wait.Wait()
	close(errs)

	for err := range errs {
		if !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("ValidateAPIKey() error = %v, want ErrInvalidKey", err)
		}
	}
	if got := lookups.Load(); got != 1 {
		t.Fatalf("lookup count = %d, want 1", got)
	}
}

func TestAuthenticationCacheBoundsUniqueInvalidKeys(t *testing.T) {
	var lookups atomic.Int64
	repository := NewRepository(
		nil,
		WithAuthenticationCacheConfig(AuthenticationCacheConfig{
			PositiveTTL: time.Minute,
			NegativeTTL: time.Minute,
			MaxEntries:  5,
		}),
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			lookups.Add(1)
			return nil, ErrInvalidKey
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	for index := 0; index < 100; index++ {
		if _, err := repository.ValidateAPIKey(
			context.Background(),
			testAuthenticationKey(index),
		); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("ValidateAPIKey(%d) error = %v, want ErrInvalidKey", index, err)
		}
	}
	repository.authenticationMu.Lock()
	cacheEntries := repository.authenticationCache.len()
	repository.authenticationMu.Unlock()
	if cacheEntries != 5 {
		t.Fatalf("authentication cache entries = %d, want 5", cacheEntries)
	}
	if got := lookups.Load(); got != 100 {
		t.Fatalf("lookup count = %d, want 100", got)
	}
}

func TestValidateAPIKeyReturnsDeepClones(t *testing.T) {
	now := time.Now().UTC()
	repository := NewRepository(
		nil,
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			return testValidatedAPIKey(now), nil
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	first, err := repository.ValidateAPIKey(context.Background(), testAuthenticationKey(3))
	if err != nil {
		t.Fatalf("first ValidateAPIKey() error = %v", err)
	}
	*first.UserID = "mutated-user"
	*first.LastUsed = first.LastUsed.Add(time.Hour)
	first.Roles[0] = "mutated-role"
	first.KeyValue = "mutated-secret"

	second, err := repository.ValidateAPIKey(context.Background(), testAuthenticationKey(3))
	if err != nil {
		t.Fatalf("second ValidateAPIKey() error = %v", err)
	}
	if second.UserID == nil || *second.UserID != "user-1" {
		t.Fatalf("cached UserID = %#v, want user-1", second.UserID)
	}
	if second.LastUsed == nil || !second.LastUsed.Equal(now.Add(-time.Minute)) {
		t.Fatalf("cached LastUsed = %#v", second.LastUsed)
	}
	if len(second.Roles) != 1 || second.Roles[0] != "developer" {
		t.Fatalf("cached Roles = %#v", second.Roles)
	}
	if second.KeyValue != "" {
		t.Fatalf("validated KeyValue retained raw secret: %q", second.KeyValue)
	}
	repository.authenticationMu.Lock()
	for _, element := range repository.authenticationCache.entries {
		entry := element.Value.(*authenticationCacheEntry)
		if entry.key != nil && entry.key.KeyValue != "" {
			repository.authenticationMu.Unlock()
			t.Fatalf("cached API key retained raw secret: %q", entry.key.KeyValue)
		}
	}
	repository.authenticationMu.Unlock()
}

func TestAuthenticationCacheDoesNotCrossExpiration(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	current := now
	var lookups atomic.Int64
	key := testValidatedAPIKey(now)
	key.ExpiresAt = now.Add(500 * time.Millisecond)
	repository := NewRepository(
		nil,
		WithAuthenticationClock(func() time.Time { return current }),
		WithAuthenticationCacheConfig(AuthenticationCacheConfig{
			PositiveTTL: time.Hour,
			NegativeTTL: time.Minute,
			MaxEntries:  10,
		}),
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			lookups.Add(1)
			return cloneAPIKey(key), nil
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	if _, err := repository.ValidateAPIKey(
		context.Background(),
		testAuthenticationKey(4),
	); err != nil {
		t.Fatalf("ValidateAPIKey(before expiration) error = %v", err)
	}
	current = key.ExpiresAt
	if _, err := repository.ValidateAPIKey(
		context.Background(),
		testAuthenticationKey(4),
	); !errors.Is(err, ErrExpiredKey) {
		t.Fatalf("ValidateAPIKey(at expiration) error = %v, want ErrExpiredKey", err)
	}
	if _, err := repository.ValidateAPIKey(
		context.Background(),
		testAuthenticationKey(4),
	); !errors.Is(err, ErrExpiredKey) {
		t.Fatalf("ValidateAPIKey(cached expiration) error = %v, want ErrExpiredKey", err)
	}
	if got := lookups.Load(); got != 2 {
		t.Fatalf("lookup count = %d, want 2", got)
	}
}

func TestNegativeAuthenticationCacheExpiresAtConfiguredTTL(t *testing.T) {
	current := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	var lookups atomic.Int64
	repository := NewRepository(
		nil,
		WithAuthenticationClock(func() time.Time { return current }),
		WithAuthenticationCacheConfig(AuthenticationCacheConfig{
			PositiveTTL: time.Minute,
			NegativeTTL: 250 * time.Millisecond,
			MaxEntries:  10,
		}),
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			lookups.Add(1)
			return nil, ErrInvalidKey
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	keyValue := testAuthenticationKey(7)
	for range 2 {
		if _, err := repository.ValidateAPIKey(
			context.Background(),
			keyValue,
		); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("ValidateAPIKey() error = %v, want ErrInvalidKey", err)
		}
	}
	if got := lookups.Load(); got != 1 {
		t.Fatalf("lookup count before TTL = %d, want 1", got)
	}
	current = current.Add(250 * time.Millisecond)
	if _, err := repository.ValidateAPIKey(
		context.Background(),
		keyValue,
	); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("ValidateAPIKey(after TTL) error = %v, want ErrInvalidKey", err)
	}
	if got := lookups.Load(); got != 2 {
		t.Fatalf("lookup count after TTL = %d, want 2", got)
	}
}

func TestTransientAuthenticationErrorsAreNotNegativeCached(t *testing.T) {
	transient := errors.New("database unavailable")
	var lookups atomic.Int64
	repository := NewRepository(
		nil,
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			lookups.Add(1)
			return nil, transient
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	for range 2 {
		if _, err := repository.ValidateAPIKey(
			context.Background(),
			testAuthenticationKey(5),
		); !errors.Is(err, transient) {
			t.Fatalf("ValidateAPIKey() error = %v, want transient error", err)
		}
	}
	if got := lookups.Load(); got != 2 {
		t.Fatalf("lookup count = %d, want 2", got)
	}
}

func TestValidateAPIKeyRejectsMalformedOrOversizedBeforeLookup(t *testing.T) {
	var lookups atomic.Int64
	repository := NewRepository(
		nil,
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			lookups.Add(1)
			return nil, nil
		}),
	)
	t.Cleanup(func() { _ = repository.Close() })

	for _, value := range []string{
		"not-an-api-key",
		"s0_aws-us-east-1_short",
		strings.Repeat("x", MaxAPIKeyValueBytes+1),
	} {
		if _, err := repository.ValidateAPIKey(
			context.Background(),
			value,
		); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("ValidateAPIKey(%q) error = %v, want ErrInvalidKey", value, err)
		}
	}
	if got := lookups.Load(); got != 0 {
		t.Fatalf("lookup count = %d, want 0", got)
	}
}

func TestTenThousandValidationsUseOneRecorderWorkerAndOneBatch(t *testing.T) {
	now := time.Now().UTC()
	writer := &recordingUsageBatchWriter{}
	repository := NewRepository(
		nil,
		WithAuthenticationCacheConfig(AuthenticationCacheConfig{
			PositiveTTL: time.Hour,
			NegativeTTL: time.Minute,
			MaxEntries:  10,
		}),
		WithAuthenticationLookup(func(context.Context, string) (*APIKey, error) {
			return testValidatedAPIKey(now), nil
		}),
		WithUsageRecorderConfig(UsageRecorderConfig{
			FlushInterval: time.Hour,
			FlushTimeout:  time.Second,
			CloseTimeout:  2 * time.Second,
			QueueSize:     10_001,
			MaxPending:    10,
		}),
		WithUsageBatchWriter(writer),
	)

	baseline := runtime.NumGoroutine()
	for index := 0; index < 10_000; index++ {
		if _, err := repository.ValidateAPIKey(
			context.Background(),
			testAuthenticationKey(6),
		); err != nil {
			t.Fatalf("ValidateAPIKey(%d) error = %v", index, err)
		}
	}
	if delta := runtime.NumGoroutine() - baseline; delta > 3 {
		t.Fatalf("goroutine growth = %d, want at most 3", delta)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	batches, total := writer.snapshot()
	if batches != 1 {
		t.Fatalf("usage batches = %d, want 1", batches)
	}
	if total != 10_000 {
		t.Fatalf("recorded usage = %d, want 10000", total)
	}
}

func testAuthenticationKey(index int) string {
	return fmt.Sprintf("s0_aws-us-east-1_%048x", index)
}

func testValidatedAPIKey(now time.Time) *APIKey {
	userID := "user-1"
	lastUsed := now.Add(-time.Minute)
	return &APIKey{
		ID:        "11111111-1111-4111-8111-111111111111",
		KeyValue:  "must-not-be-cached",
		TeamID:    "team-1",
		UserID:    &userID,
		CreatedBy: "creator-1",
		Scope:     ScopeTeam,
		Roles:     []string{"developer"},
		IsActive:  true,
		ExpiresAt: now.Add(time.Hour),
		LastUsed:  &lastUsed,
	}
}

type recordingUsageBatchWriter struct {
	mu      sync.Mutex
	batches [][]APIKeyUsage
}

func (w *recordingUsageBatchWriter) WriteAPIKeyUsageBatch(
	_ context.Context,
	batch []APIKeyUsage,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batches = append(w.batches, append([]APIKeyUsage(nil), batch...))
	return nil
}

func (w *recordingUsageBatchWriter) snapshot() (int, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var total int64
	for _, batch := range w.batches {
		for _, usage := range batch {
			total += usage.Count
		}
	}
	return len(w.batches), total
}
