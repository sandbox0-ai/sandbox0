package metrics

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
)

// DatabaseDiagnostics separates checkout, connection holding and local slot
// admission. SQL durations include server waits, transport and result consumption;
// hold time outside traced SQL is a client gap, not a measurement of CPU time.
// Only fixed operation labels are exported, never SQL, parameters or identities.
type DatabaseDiagnostics struct {
	mu               sync.Mutex
	holds            map[*pgx.Conn]*databaseHold
	checkout         *prometheus.HistogramVec
	hold             *prometheus.HistogramVec
	statements       *prometheus.HistogramVec
	statementCount   *prometheus.CounterVec
	waiting          *prometheus.GaugeVec
	held             *prometheus.GaugeVec
	admission        *prometheus.HistogramVec
	admissionWaiting prometheus.Gauge
}

type databaseHold struct {
	operation string
	started   time.Time
	sql       time.Duration
	active    int
}

type databaseAcquire struct {
	started   time.Time
	operation string
}
type databaseQuery struct {
	started                           time.Time
	lastResult                        time.Time
	results                           int
	operation, statement, mode, phase string
	hold                              *databaseHold
}
type databaseAcquireKey struct{}
type databaseQueryKey struct{}

func NewDatabaseDiagnostics(registry prometheus.Registerer) *DatabaseDiagnostics {
	if registry == nil {
		return nil
	}
	buckets := []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	d := &DatabaseDiagnostics{holds: make(map[*pgx.Conn]*databaseHold)}
	d.checkout = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "manager_pg_checkout_duration_seconds", Help: "Complete pgx checkout including pool queue, construction, ping and primary checks", Buckets: buckets}, []string{"operation", "status"})
	d.hold = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "manager_pg_hold_duration_seconds", Help: "Released connection hold split into total, traced SQL and client gap time", Buckets: buckets}, []string{"operation", "phase"})
	d.statements = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "manager_pg_statement_duration_seconds", Help: "Client SQL call or entire SendBatch duration including waits and result consumption", Buckets: buckets}, []string{"operation", "mode", "statement", "phase"})
	d.statementCount = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "manager_pg_statements_total", Help: "SQL statements including each consumed batch result"}, []string{"operation", "mode", "statement", "phase"})
	d.waiting = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "manager_pg_checkout_pending", Help: "Calls currently waiting for checkout completion"}, []string{"operation"})
	d.held = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "manager_pg_held_connections", Help: "Checked out connections currently in traced SQL or between SQL calls; listener waits are client gaps"}, []string{"operation", "phase"})
	d.admission = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "manager_runtime_slot_admission_wait_seconds", Help: "Local slot admission wait before any pool checkout", Buckets: buckets}, []string{"status"})
	d.admissionWaiting = prometheus.NewGauge(prometheus.GaugeOpts{Name: "manager_runtime_slot_admission_pending", Help: "Local slot admission calls waiting for the bounded channel"})
	registry.MustRegister(d.checkout, d.hold, d.statements, d.statementCount, d.waiting, d.held, d.admission, d.admissionWaiting)
	return d
}

// ConfigurePool preserves all existing pgx tracing interfaces.
func (d *DatabaseDiagnostics) ConfigurePool(config *pgxpool.Config) {
	if d == nil || config == nil {
		return
	}
	if config.ConnConfig.Tracer == nil {
		config.ConnConfig.Tracer = d
	} else {
		config.ConnConfig.Tracer = multitracer.New(config.ConnConfig.Tracer, d)
	}
}

func databaseOperation(ctx context.Context) string {
	switch operation := dbpool.Operation(ctx); operation {
	case "slot_acquire", "writer_bind", "command_ready", "claim_reserve", "get_sandbox", "slot_start", "migration_listener":
		return operation
	default:
		return "other"
	}
}

func databaseStatement(sql string) string {
	// Inspect only a short prefix; retain no SQL and create no fingerprint labels.
	if len(sql) > 64 {
		sql = sql[:64]
	}
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "OTHER"
	}
	switch operation := strings.ToUpper(fields[0]); operation {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT", "ROLLBACK", "SHOW", "SET", "LISTEN":
		return operation
	default:
		return "OTHER"
	}
}

func (d *DatabaseDiagnostics) RuntimeClaimAdmissionStarted() {
	if d != nil {
		d.admissionWaiting.Inc()
	}
}
func (d *DatabaseDiagnostics) RuntimeClaimAdmissionFinished(wait time.Duration, canceled bool) {
	if d == nil {
		return
	}
	d.admissionWaiting.Dec()
	status := "acquired"
	if canceled {
		status = "canceled"
	}
	d.admission.WithLabelValues(status).Observe(wait.Seconds())
}

func (d *DatabaseDiagnostics) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	f := &databaseAcquire{started: time.Now(), operation: databaseOperation(ctx)}
	d.waiting.WithLabelValues(f.operation).Inc()
	return context.WithValue(ctx, databaseAcquireKey{}, f)
}
func (d *DatabaseDiagnostics) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	f, _ := ctx.Value(databaseAcquireKey{}).(*databaseAcquire)
	if f == nil {
		return
	}
	d.waiting.WithLabelValues(f.operation).Dec()
	status := "acquired"
	if data.Err != nil {
		status = "error"
	}
	d.checkout.WithLabelValues(f.operation, status).Observe(time.Since(f.started).Seconds())
	if data.Err != nil || data.Conn == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.holds[data.Conn] = &databaseHold{operation: f.operation, started: time.Now()}
	d.held.WithLabelValues(f.operation, "client_gap").Inc()
}
func (d *DatabaseDiagnostics) TraceRelease(_ *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h := d.holds[data.Conn]
	if h == nil {
		return
	}
	delete(d.holds, data.Conn)
	phase := "client_gap"
	if h.active > 0 {
		phase = "sql"
	}
	d.held.WithLabelValues(h.operation, phase).Dec()
	total := time.Since(h.started)
	gap := max(time.Duration(0), total-h.sql)
	d.hold.WithLabelValues(h.operation, "total").Observe(total.Seconds())
	d.hold.WithLabelValues(h.operation, "sql").Observe(h.sql.Seconds())
	d.hold.WithLabelValues(h.operation, "client_gap").Observe(gap.Seconds())
}

func (d *DatabaseDiagnostics) start(ctx context.Context, conn *pgx.Conn, mode, statement string) context.Context {
	f := &databaseQuery{started: time.Now(), operation: databaseOperation(ctx), statement: statement, mode: mode, phase: "untracked"}
	d.mu.Lock()
	if h := d.holds[conn]; h != nil {
		f.hold = h
		f.operation = h.operation
		f.phase = "held"
		if h.active == 0 {
			d.held.WithLabelValues(h.operation, "client_gap").Dec()
			d.held.WithLabelValues(h.operation, "sql").Inc()
		}
		h.active++
	} else if acquire, _ := ctx.Value(databaseAcquireKey{}).(*databaseAcquire); acquire != nil {
		f.operation = acquire.operation
		f.phase = "checkout"
	}
	d.mu.Unlock()
	return context.WithValue(ctx, databaseQueryKey{}, f)
}
func (d *DatabaseDiagnostics) end(ctx context.Context, conn *pgx.Conn) {
	f, _ := ctx.Value(databaseQueryKey{}).(*databaseQuery)
	if f == nil {
		return
	}
	elapsed := time.Since(f.started)
	d.statements.WithLabelValues(f.operation, f.mode, f.statement, f.phase).Observe(elapsed.Seconds())
	d.mu.Lock()
	defer d.mu.Unlock()
	if h := f.hold; h != nil && d.holds[conn] == h {
		h.sql += elapsed
		h.active--
		if h.active == 0 {
			d.held.WithLabelValues(h.operation, "sql").Dec()
			d.held.WithLabelValues(h.operation, "client_gap").Inc()
		}
	}
}
func (d *DatabaseDiagnostics) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return d.start(ctx, conn, "query", databaseStatement(data.SQL))
}
func (d *DatabaseDiagnostics) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, _ pgx.TraceQueryEndData) {
	if f, _ := ctx.Value(databaseQueryKey{}).(*databaseQuery); f != nil {
		d.statementCount.WithLabelValues(f.operation, f.mode, f.statement, f.phase).Inc()
	}
	d.end(ctx, conn)
}
func (d *DatabaseDiagnostics) TraceBatchStart(ctx context.Context, conn *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return d.start(ctx, conn, "batch", "BATCH")
}
func (d *DatabaseDiagnostics) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	if f, _ := ctx.Value(databaseQueryKey{}).(*databaseQuery); f != nil {
		now := time.Now()
		previous, mode := f.lastResult, "batch_later_result"
		if f.results == 0 {
			previous, mode = f.started, "batch_first_result"
		}
		d.statements.WithLabelValues(f.operation, mode, databaseStatement(data.SQL), f.phase).Observe(now.Sub(previous).Seconds())
		f.lastResult, f.results = now, f.results+1
		d.statementCount.WithLabelValues(f.operation, "batch", databaseStatement(data.SQL), f.phase).Inc()
	}
}
func (d *DatabaseDiagnostics) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, _ pgx.TraceBatchEndData) {
	d.end(ctx, conn)
}
