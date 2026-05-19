package pglock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type fakeConn struct {
	execs    []fakeExec
	released bool
}

type fakeExec struct {
	sql  string
	args []any
}

func (c *fakeConn) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	args := append([]any(nil), arguments...)
	c.execs = append(c.execs, fakeExec{sql: sql, args: args})
	return pgconn.CommandTag{}, nil
}

func (c *fakeConn) Release() {
	c.released = true
}

func TestLockerWithExclusiveLocksAndUnlocks(t *testing.T) {
	conn := &fakeConn{}
	locker := newLocker(func(context.Context) (advisoryConn, error) {
		return conn, nil
	}, WithUnlockTimeout(time.Second))

	called := false
	if err := locker.WithExclusive(context.Background(), "volume-a", func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("WithExclusive() error = %v", err)
	}

	if !called {
		t.Fatal("callback was not called")
	}
	if !conn.released {
		t.Fatal("connection was not released")
	}
	if len(conn.execs) != 2 {
		t.Fatalf("exec count = %d, want 2", len(conn.execs))
	}
	if conn.execs[0].sql != "SELECT pg_advisory_lock($1)" {
		t.Fatalf("lock sql = %q", conn.execs[0].sql)
	}
	if conn.execs[1].sql != "SELECT pg_advisory_unlock($1)" {
		t.Fatalf("unlock sql = %q", conn.execs[1].sql)
	}
	if conn.execs[0].args[0] != Key("volume-a") || conn.execs[1].args[0] != Key("volume-a") {
		t.Fatalf("lock args = %#v, %#v; want resource key", conn.execs[0].args, conn.execs[1].args)
	}
}

func TestLockerWithSharedUsesSharedStatements(t *testing.T) {
	conn := &fakeConn{}
	locker := newLocker(func(context.Context) (advisoryConn, error) {
		return conn, nil
	})

	if err := locker.WithShared(context.Background(), "volume-a", func(context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("WithShared() error = %v", err)
	}

	if len(conn.execs) != 2 {
		t.Fatalf("exec count = %d, want 2", len(conn.execs))
	}
	if conn.execs[0].sql != "SELECT pg_advisory_lock_shared($1)" {
		t.Fatalf("lock sql = %q", conn.execs[0].sql)
	}
	if conn.execs[1].sql != "SELECT pg_advisory_unlock_shared($1)" {
		t.Fatalf("unlock sql = %q", conn.execs[1].sql)
	}
}

func TestLockerUnlocksWhenCallbackFails(t *testing.T) {
	conn := &fakeConn{}
	locker := newLocker(func(context.Context) (advisoryConn, error) {
		return conn, nil
	})
	wantErr := errors.New("callback failed")

	err := locker.WithExclusive(context.Background(), "volume-a", func(context.Context) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithExclusive() error = %v, want %v", err, wantErr)
	}
	if len(conn.execs) != 2 || conn.execs[1].sql != "SELECT pg_advisory_unlock($1)" {
		t.Fatalf("execs = %#v, want unlock after callback failure", conn.execs)
	}
}

func TestLockerWithoutPoolRunsCallback(t *testing.T) {
	called := false
	locker := New(nil)
	if err := locker.WithExclusive(context.Background(), "volume-a", func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("WithExclusive() error = %v", err)
	}
	if !called {
		t.Fatal("callback was not called")
	}
}
