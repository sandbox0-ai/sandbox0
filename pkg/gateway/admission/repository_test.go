package admission

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeQuerier struct {
	rows  []pgx.Row
	calls int
}

func (q *fakeQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	row := q.rows[q.calls]
	q.calls++
	return row
}

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for index, value := range r.values {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *int64:
			*target = value.(int64)
		case *State:
			*target = value.(State)
		case *time.Time:
			*target = value.(time.Time)
		}
	}
	return nil
}

func admissionRow(version int64, state State, source, reason string) pgx.Row {
	return fakeRow{values: []any{
		"11111111-1111-4111-8111-111111111111",
		version,
		state,
		source,
		reason,
		time.Unix(10, 0).UTC(),
	}}
}

func TestRepositoryGet(t *testing.T) {
	if repository := NewRepository(&pgxpool.Pool{}); repository.db == nil {
		t.Fatal("NewRepository(pool) did not retain the pool")
	}
	var nilRepository *Repository
	if _, _, err := nilRepository.Get(context.Background(), "team"); err == nil {
		t.Fatal("nil Repository.Get() error = nil")
	}
	if _, _, err := NewRepository(nil).Get(context.Background(), "team"); err == nil {
		t.Fatal("NewRepository(nil).Get() error = nil")
	}

	t.Run("not found", func(t *testing.T) {
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{fakeRow{err: pgx.ErrNoRows}}}}
		_, found, err := repository.Get(context.Background(), "team")
		if err != nil || found {
			t.Fatalf("Get() found = %v, error = %v", found, err)
		}
	})

	t.Run("database error", func(t *testing.T) {
		wantErr := errors.New("database failure")
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{fakeRow{err: wantErr}}}}
		_, _, err := repository.Get(context.Background(), "team")
		if !errors.Is(err, wantErr) {
			t.Fatalf("Get() error = %v, want %v", err, wantErr)
		}
	})

	t.Run("record", func(t *testing.T) {
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{
			admissionRow(3, StateRestricted, "source", "reason"),
		}}}
		record, found, err := repository.Get(context.Background(), "team")
		if err != nil || !found {
			t.Fatalf("Get() found = %v, error = %v", found, err)
		}
		if record.Version != 3 || record.State != StateRestricted || record.Source != "source" || record.Reason != "reason" {
			t.Fatalf("Get() = %#v", record)
		}
	})
}

func TestRepositoryPut(t *testing.T) {
	ctx := context.Background()
	valid := Update{Version: 2, State: StateAllowed, Source: "source", Reason: "reason"}

	var nilRepository *Repository
	if _, err := nilRepository.Put(ctx, "team", valid); err == nil {
		t.Fatal("nil Repository.Put() error = nil")
	}
	if _, err := NewRepository(nil).Put(ctx, "team", valid); err == nil {
		t.Fatal("NewRepository(nil).Put() error = nil")
	}

	repository := &Repository{db: &fakeQuerier{}}
	if _, err := repository.Put(ctx, "team", Update{}); !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("Put(invalid) error = %v", err)
	}

	t.Run("applied", func(t *testing.T) {
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{
			admissionRow(2, StateAllowed, "source", "reason"),
		}}}
		result, err := repository.Put(ctx, "team", valid)
		if err != nil || !result.Applied || result.Record.Version != 2 {
			t.Fatalf("Put() result = %#v, error = %v", result, err)
		}
	})

	t.Run("insert error", func(t *testing.T) {
		wantErr := errors.New("insert failure")
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{fakeRow{err: wantErr}}}}
		_, err := repository.Put(ctx, "team", valid)
		if !errors.Is(err, wantErr) {
			t.Fatalf("Put() error = %v, want %v", err, wantErr)
		}
	})

	t.Run("read after race error", func(t *testing.T) {
		wantErr := errors.New("read failure")
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{
			fakeRow{err: pgx.ErrNoRows},
			fakeRow{err: wantErr},
		}}}
		_, err := repository.Put(ctx, "team", valid)
		if !errors.Is(err, wantErr) {
			t.Fatalf("Put() error = %v, want %v", err, wantErr)
		}
	})

	t.Run("row disappears after race", func(t *testing.T) {
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{
			fakeRow{err: pgx.ErrNoRows},
			fakeRow{err: pgx.ErrNoRows},
		}}}
		if _, err := repository.Put(ctx, "team", valid); err == nil {
			t.Fatal("Put() error = nil")
		}
	})

	t.Run("same version conflict", func(t *testing.T) {
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{
			fakeRow{err: pgx.ErrNoRows},
			admissionRow(2, StateRestricted, "source", "reason"),
		}}}
		_, err := repository.Put(ctx, "team", valid)
		if !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("Put() error = %v, want ErrVersionConflict", err)
		}
	})

	t.Run("idempotent replay", func(t *testing.T) {
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{
			fakeRow{err: pgx.ErrNoRows},
			admissionRow(2, StateAllowed, "source", "reason"),
		}}}
		result, err := repository.Put(ctx, "team", valid)
		if err != nil || result.Applied {
			t.Fatalf("Put() result = %#v, error = %v", result, err)
		}
	})

	t.Run("stale version", func(t *testing.T) {
		repository := &Repository{db: &fakeQuerier{rows: []pgx.Row{
			fakeRow{err: pgx.ErrNoRows},
			admissionRow(3, StateRestricted, "newer", "newer"),
		}}}
		result, err := repository.Put(ctx, "team", valid)
		if err != nil || result.Applied || result.Record.Version != 3 {
			t.Fatalf("Put() result = %#v, error = %v", result, err)
		}
	})
}
