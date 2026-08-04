package clock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockRow implements Row interface for testing
type mockRow struct {
	pgTime time.Time
	err    error
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if t, ok := dest[0].(*time.Time); ok {
			*t = r.pgTime
		}
	}
	return nil
}

// mockDB implements DB interface for testing
type mockDB struct {
	mu        sync.Mutex
	pgTime    time.Time
	offset    time.Duration // offset from local time
	delay     time.Duration // simulated network delay
	err       error
	callCount atomic.Int64
}

func (m *mockDB) QueryRow(ctx context.Context, sql string, args ...any) Row {
	m.callCount.Add(1)

	// Simulate network delay
	m.mu.Lock()
	delay := m.delay
	offset := m.offset
	err := m.err
	pgTime := m.pgTime
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if err != nil {
		return &mockRow{err: err}
	}

	// If pgTime is zero, use current time + offset
	if pgTime.IsZero() {
		pgTime = time.Now().Add(offset)
	}

	return &mockRow{pgTime: pgTime}
}

func (m *mockDB) setOffset(d time.Duration) {
	m.mu.Lock()
	m.offset = d
	m.mu.Unlock()
}

func (m *mockDB) setError(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

// testLogger implements Logger for testing
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Info(msg string, keysAndValues ...any) {
	l.t.Logf("INFO: %s %v", msg, keysAndValues)
}

func (l *testLogger) Warn(msg string, keysAndValues ...any) {
	l.t.Logf("WARN: %s %v", msg, keysAndValues)
}

func (l *testLogger) Error(msg string, keysAndValues ...any) {
	l.t.Logf("ERROR: %s %v", msg, keysAndValues)
}

func TestNew(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db, WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	if c.SyncCount() != 1 {
		t.Errorf("SyncCount() = %d, want 1", c.SyncCount())
	}

	if c.LastSyncError() != nil {
		t.Errorf("LastSyncError() = %v, want nil", c.LastSyncError())
	}
}

func TestNew_Error(t *testing.T) {
	expectedErr := errors.New("connection failed")
	db := &mockDB{err: expectedErr}
	ctx := context.Background()

	_, err := New(ctx, db)
	if err == nil {
		t.Fatal("New() expected error, got nil")
	}
	if err != expectedErr {
		t.Errorf("New() error = %v, want %v", err, expectedErr)
	}
}

func TestClock_Now(t *testing.T) {
	// DB returns time 100ms ahead of local time
	offset := 100 * time.Millisecond
	db := &mockDB{offset: offset}
	ctx := context.Background()

	c, err := New(ctx, db, WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	// Clock.Now() should be approximately offset ahead of local time
	clockNow := c.Now()
	localNow := time.Now()
	diff := clockNow.Sub(localNow)

	// Allow some tolerance for test execution time
	tolerance := 50 * time.Millisecond
	if diff < offset-tolerance || diff > offset+tolerance {
		t.Errorf("Now() diff from local = %v, want ~%v", diff, offset)
	}
}

func TestClock_NowMonotonic(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db, WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	// Time should never go backward
	prev := c.Now()
	for i := 0; i < 100; i++ {
		time.Sleep(time.Millisecond)
		curr := c.Now()
		if curr.Before(prev) {
			t.Errorf("Time went backward: prev=%v, curr=%v", prev, curr)
		}
		prev = curr
	}
}

func TestClock_Since(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db, WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	past := c.Now()
	time.Sleep(50 * time.Millisecond)

	elapsed := c.Since(past)
	if elapsed < 40*time.Millisecond || elapsed > 100*time.Millisecond {
		t.Errorf("Since() = %v, want ~50ms", elapsed)
	}
}

func TestClock_Until(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db, WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	future := c.Now().Add(100 * time.Millisecond)
	remaining := c.Until(future)

	if remaining < 80*time.Millisecond || remaining > 120*time.Millisecond {
		t.Errorf("Until() = %v, want ~100ms", remaining)
	}
}

func TestClock_Offset(t *testing.T) {
	offset := 500 * time.Millisecond
	db := &mockDB{offset: offset}
	ctx := context.Background()

	c, err := New(ctx, db, WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	tolerance := 50 * time.Millisecond
	gotOffset := c.Offset()
	if gotOffset < offset-tolerance || gotOffset > offset+tolerance {
		t.Errorf("Offset() = %v, want ~%v", gotOffset, offset)
	}
}

func TestClock_BackgroundSync(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	// Use short sync interval for testing
	c, err := New(ctx, db,
		WithSyncInterval(50*time.Millisecond),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	initialCount := c.SyncCount()

	// Wait for a few sync cycles
	time.Sleep(150 * time.Millisecond)

	if c.SyncCount() <= initialCount {
		t.Errorf("SyncCount() = %d, expected > %d after background sync", c.SyncCount(), initialCount)
	}
}

func TestClock_BackgroundSyncError(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db,
		WithSyncInterval(50*time.Millisecond),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	// Simulate DB error
	expectedErr := errors.New("db error")
	db.setError(expectedErr)

	// Wait for sync attempt
	time.Sleep(100 * time.Millisecond)

	lastErr := c.LastSyncError()
	if lastErr != expectedErr {
		t.Errorf("LastSyncError() = %v, want %v", lastErr, expectedErr)
	}
}

func TestClock_Close(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db,
		WithSyncInterval(10*time.Millisecond),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	countBefore := db.callCount.Load()
	c.Close()
	time.Sleep(50 * time.Millisecond)

	countAfter := db.callCount.Load()
	if countAfter > countBefore+1 {
		t.Errorf("Background sync continued after Close(): before=%d, after=%d", countBefore, countAfter)
	}
}

func TestClock_Options(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	syncInterval := 100 * time.Millisecond
	c, err := New(ctx, db,
		WithSyncInterval(syncInterval),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	if c.syncInterval != syncInterval {
		t.Errorf("syncInterval = %v, want %v", c.syncInterval, syncInterval)
	}
}

func TestClock_LargeOffsetChange(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db,
		WithSyncInterval(30*time.Millisecond),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	// Simulate large offset change
	db.setOffset(2 * time.Second)

	// Wait for sync to detect the change
	time.Sleep(100 * time.Millisecond)

	// The test logger should have logged a warning (check test output)
	// We just verify sync continues working
	if c.SyncCount() < 2 {
		t.Errorf("SyncCount() = %d, expected >= 2", c.SyncCount())
	}
}

func TestClock_NetworkDelay(t *testing.T) {
	// Simulate 50ms network delay
	db := &mockDB{delay: 50 * time.Millisecond}
	ctx := context.Background()

	c, err := New(ctx, db, WithLogger(&testLogger{t}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	// RTT should be approximately 50ms
	rtt := c.LastRTT()
	if rtt < 40*time.Millisecond || rtt > 100*time.Millisecond {
		t.Errorf("LastRTT() = %v, want ~50ms", rtt)
	}
}

func TestClock_TimeSinceLastSync(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db,
		WithSyncInterval(1*time.Hour), // Long interval so no background sync
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	time.Sleep(50 * time.Millisecond)

	timeSince := c.TimeSinceLastSync()
	if timeSince < 40*time.Millisecond || timeSince > 100*time.Millisecond {
		t.Errorf("TimeSinceLastSync() = %v, want ~50ms", timeSince)
	}
}

func TestClock_ConcurrentAccess(t *testing.T) {
	db := &mockDB{}
	ctx := context.Background()

	c, err := New(ctx, db,
		WithSyncInterval(10*time.Millisecond),
		WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	// Concurrent reads
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = c.Now()
				_ = c.Offset()
				_ = c.LastRTT()
				_ = c.SyncCount()
				_ = c.LastSyncError()
				_ = c.TimeSinceLastSync()
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()
}
