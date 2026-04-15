package pgx

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/internal/promutil"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Adapter provides observable PostgreSQL clients
type Adapter struct {
	config  AdapterConfig
	metrics *metrics
}

// AdapterConfig configures the Pgx adapter
type AdapterConfig struct {
	ServiceName    string
	Tracer         trace.Tracer
	Logger         *zap.Logger
	Registry       prometheus.Registerer
	DisableMetrics bool
	DisableLogging bool
	Disabled       bool
}

// Config holds configuration for creating an observable pgx pool
type Config struct {
	// Database connection URL
	DatabaseURL string

	// Connection pool settings
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime string // e.g., "1h"
	MaxConnIdleTime string // e.g., "30m"

	// Schema to use
	Schema string

	// Optional: custom pool config modifier
	ConfigModifier func(*pgxpool.Config) error
}

// NewAdapter creates a new Pgx adapter
func NewAdapter(cfg AdapterConfig) Adapter {
	var m *metrics
	if !cfg.Disabled && !cfg.DisableMetrics && cfg.Registry != nil {
		m = newMetrics(cfg.ServiceName, cfg.Registry)
	}

	return Adapter{
		config:  cfg,
		metrics: m,
	}
}

// NewPool creates a fully observable pgx connection pool
func (a Adapter) NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("database URL is required")
	}

	// Parse pool config
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	// Set schema if specified
	if cfg.Schema != "" {
		if poolConfig.ConnConfig.RuntimeParams == nil {
			poolConfig.ConnConfig.RuntimeParams = map[string]string{}
		}
		poolConfig.ConnConfig.RuntimeParams["search_path"] = cfg.Schema
	}

	// Set pool size
	if cfg.MaxConns > 0 {
		poolConfig.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolConfig.MinConns = cfg.MinConns
	}

	// Apply custom config modifier if provided
	if cfg.ConfigModifier != nil {
		if err := cfg.ConfigModifier(poolConfig); err != nil {
			return nil, fmt.Errorf("apply config modifier: %w", err)
		}
	}

	a.ConfigurePool(poolConfig)

	// Create pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	// Ping to verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	a.config.Logger.Info("PostgreSQL pool created",
		zap.Int32("max_conns", poolConfig.MaxConns),
		zap.Int32("min_conns", poolConfig.MinConns),
		zap.String("schema", cfg.Schema),
	)

	return pool, nil
}

// ConfigurePool attaches query tracing to a pgx pool config before the pool is created.
func (a Adapter) ConfigurePool(poolConfig *pgxpool.Config) {
	if poolConfig == nil || a.config.Disabled {
		return
	}
	poolConfig.ConnConfig.Tracer = &observableTracer{
		config:  a.config,
		metrics: a.metrics,
	}
}

// ConfigModifier returns a dbpool.Options-compatible modifier.
func (a Adapter) ConfigModifier() func(*pgxpool.Config) error {
	return func(poolConfig *pgxpool.Config) error {
		a.ConfigurePool(poolConfig)
		return nil
	}
}

// WrapPool cannot retrofit pgx query tracing onto an already-created pool.
// Use ConfigurePool or ConfigModifier before pgxpool.NewWithConfig instead.
func (a Adapter) WrapPool(pool *pgxpool.Pool) {
	if pool == nil || a.config.Disabled {
		return
	}
	if !a.config.DisableLogging && a.config.Logger != nil {
		a.config.Logger.Warn("PostgreSQL pool was already created; query tracing was not attached")
	}
}

// metrics holds Prometheus metrics for pgx
type metrics struct {
	queriesTotal    *prometheus.CounterVec
	queryDuration   *prometheus.HistogramVec
	activeQueries   *prometheus.GaugeVec
	rowsAffected    *prometheus.CounterVec
	poolConnections *prometheus.GaugeVec
}

func newMetrics(serviceName string, registry prometheus.Registerer) *metrics {
	prefix := promutil.MetricPrefix(serviceName)

	return &metrics{
		queriesTotal: promutil.RegisterCounterVec(
			registry,
			prometheus.CounterOpts{
				Name: prefix + "_pgx_queries_total",
				Help: "Total number of PostgreSQL queries",
			},
			[]string{"operation", "status"},
		),
		queryDuration: promutil.RegisterHistogramVec(
			registry,
			prometheus.HistogramOpts{
				Name:    prefix + "_pgx_query_duration_seconds",
				Help:    "PostgreSQL query duration in seconds",
				Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
			},
			[]string{"operation"},
		),
		activeQueries: promutil.RegisterGaugeVec(
			registry,
			prometheus.GaugeOpts{
				Name: prefix + "_pgx_active_queries",
				Help: "Number of active PostgreSQL queries",
			},
			[]string{"operation"},
		),
		rowsAffected: promutil.RegisterCounterVec(
			registry,
			prometheus.CounterOpts{
				Name: prefix + "_pgx_rows_affected_total",
				Help: "Total number of rows affected by PostgreSQL queries",
			},
			[]string{"operation"},
		),
		poolConnections: promutil.RegisterGaugeVec(
			registry,
			prometheus.GaugeOpts{
				Name: prefix + "_pgx_pool_connections",
				Help: "Number of connections in the pool",
			},
			[]string{"state"}, // idle, acquired, constructing
		),
	}
}
