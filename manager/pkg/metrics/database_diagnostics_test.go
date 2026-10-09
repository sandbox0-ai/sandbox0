package metrics

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
)

func TestDatabaseDiagnosticsLabelsAndCanceledCheckout(t *testing.T) {
	d := NewDatabaseDiagnostics(prometheus.NewRegistry())
	ctx := dbpool.WithOperation(context.Background(), "unbounded-request-id")
	ctx = d.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
	if n := testutil.ToFloat64(d.waiting.WithLabelValues("other")); n != 1 {
		t.Fatalf("pending = %v", n)
	}
	d.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{Err: context.Canceled})
	if n := testutil.ToFloat64(d.waiting.WithLabelValues("other")); n != 0 {
		t.Fatalf("pending after cancel = %v", n)
	}
	if len(d.holds) != 0 {
		t.Fatal("canceled checkout retained a connection")
	}
	d.RuntimeClaimAdmissionStarted()
	d.RuntimeClaimAdmissionFinished(time.Millisecond, true)
	if n := testutil.ToFloat64(d.admissionWaiting); n != 0 {
		t.Fatalf("admission pending = %v", n)
	}
	var disabled *DatabaseDiagnostics
	disabled.ConfigurePool(nil)
	disabled.RuntimeClaimAdmissionStarted()
	disabled.RuntimeClaimAdmissionFinished(0, false)
}

func TestDatabaseDiagnosticsBatchIsCountedOnce(t *testing.T) {
	d := NewDatabaseDiagnostics(prometheus.NewRegistry())
	conn := &pgx.Conn{}
	ctx := dbpool.WithOperation(context.Background(), "slot_acquire")
	a := d.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
	d.TraceAcquireEnd(a, nil, pgxpool.TraceAcquireEndData{Conn: conn})
	h := d.holds[conn]
	b := d.TraceBatchStart(ctx, conn, pgx.TraceBatchStartData{})
	d.TraceBatchQuery(b, conn, pgx.TraceBatchQueryData{SQL: "SELECT secret FROM customer"})
	d.TraceBatchQuery(b, conn, pgx.TraceBatchQueryData{SQL: "SELECT 2"})
	d.TraceBatchEnd(b, conn, pgx.TraceBatchEndData{})
	if h.sql <= 0 || h.active != 0 {
		t.Fatalf("batch accounting: %+v", h)
	}
	if n := testutil.ToFloat64(d.statementCount.WithLabelValues("slot_acquire", "batch", "SELECT", "held")); n != 2 {
		t.Fatalf("batch statements = %v", n)
	}
	if n := testutil.ToFloat64(d.held.WithLabelValues("slot_acquire", "client_gap")); n != 1 {
		t.Fatalf("held = %v", n)
	}
	d.TraceRelease(nil, pgxpool.TraceReleaseData{Conn: conn})
	if len(d.holds) != 0 {
		t.Fatal("release retained connection")
	}
	if n := testutil.ToFloat64(d.held.WithLabelValues("slot_acquire", "client_gap")); n != 0 {
		t.Fatalf("held after release = %v", n)
	}
}

// Exercise actual pgxpool callback order, row consumption, batch closure,
// cancellation and primary fencing rather than only calling trace methods.
func TestDatabaseDiagnosticsPostgres(t *testing.T) {
	url := os.Getenv("SANDBOX0_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires SANDBOX0_TEST_DATABASE_URL")
	}
	registry := prometheus.NewRegistry()
	d := NewDatabaseDiagnostics(registry)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: url, MaxConns: 1, RequirePrimary: true, ConfigModifier: func(c *pgxpool.Config) error { d.ConfigurePool(c); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, ok := pool.Config().ConnConfig.Tracer.(*DatabaseDiagnostics); !ok {
		t.Fatal("missing diagnostic tracer")
	}
	operation := dbpool.WithOperation(ctx, "slot_acquire")
	tx, err := pool.BeginTx(operation, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var one int
	if err := tx.QueryRow(operation, "SELECT 1").Scan(&one); err != nil {
		t.Fatal(err)
	}
	batch := &pgx.Batch{}
	batch.Queue("SELECT 1")
	batch.Queue("SELECT 2")
	results := tx.SendBatch(operation, batch)
	for i := 1; i <= 2; i++ {
		if err := results.QueryRow().Scan(&one); err != nil || one != i {
			t.Fatalf("batch result %d: %d %v", i, one, err)
		}
	}
	if err := results.Close(); err != nil {
		t.Fatal(err)
	}
	waitCtx, stop := context.WithTimeout(operation, 20*time.Millisecond)
	_, err = pool.Acquire(waitCtx)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued checkout: %v", err)
	}
	if err := tx.Commit(operation); err != nil {
		t.Fatal(err)
	}
	if n := testutil.ToFloat64(d.held.WithLabelValues("slot_acquire", "client_gap")); n != 0 {
		t.Fatalf("connection leaked: %v", n)
	}
	if n := testutil.ToFloat64(d.waiting.WithLabelValues("slot_acquire")); n != 0 {
		t.Fatalf("waiter leaked: %v", n)
	}
	if n := testutil.ToFloat64(d.statementCount.WithLabelValues("slot_acquire", "batch", "SELECT", "held")); n != 2 {
		t.Fatalf("batch count = %v", n)
	}
	if n := testutil.ToFloat64(d.statementCount.WithLabelValues("slot_acquire", "query", "COMMIT", "held")); n != 1 {
		t.Fatalf("commit count = %v", n)
	}
	rows, err := pool.Query(operation, "SELECT generate_series(1,3)")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(d.holds) != 0 {
		t.Fatal("early row close leaked hold")
	}
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, err := pool.Exec(operation, "SELECT pg_sleep(0.001)"); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if len(d.holds) != 0 || pool.Stat().AcquiredConns() != 0 {
		t.Fatal("concurrent calls leaked a connection")
	}
	// Preserve existing query, batch, acquire and release tracers together.
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	c.ConnConfig.Tracer = d
	second := NewDatabaseDiagnostics(prometheus.NewRegistry())
	second.ConfigurePool(c)
	combined, ok := c.ConnConfig.Tracer.(*multitracer.Tracer)
	if !ok || len(combined.QueryTracers) != 2 || len(combined.BatchTracers) != 2 || len(combined.PoolAcquireTracers) != 2 || len(combined.PoolReleaseTracers) != 2 {
		t.Fatal("existing tracing interfaces replaced")
	}
}

func BenchmarkDatabaseDiagnostics(b *testing.B) {
	d := NewDatabaseDiagnostics(prometheus.NewRegistry())
	ctx := dbpool.WithOperation(context.Background(), "slot_acquire")
	conn := &pgx.Conn{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := d.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
		d.TraceAcquireEnd(a, nil, pgxpool.TraceAcquireEndData{Conn: conn})
		q := d.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{SQL: "SELECT 1"})
		d.TraceQueryEnd(q, conn, pgx.TraceQueryEndData{})
		d.TraceRelease(nil, pgxpool.TraceReleaseData{Conn: conn})
	}
}
