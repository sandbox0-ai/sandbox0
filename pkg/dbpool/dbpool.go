package dbpool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultPrimaryConnectTimeout = 3 * time.Second
	defaultPrimaryCheckInterval  = time.Second
	defaultPrimaryCheckTimeout   = time.Second
	defaultHAHealthCheckPeriod   = time.Second
)

// Options configures a pgx pool setup.
type Options struct {
	DatabaseURL     string
	MaxConns        int32
	MinConns        int32
	DefaultMaxConns int32
	DefaultMinConns int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	Schema          string
	ConfigModifier  func(*pgxpool.Config) error

	// RequirePrimary makes every new connection select a read-write PostgreSQL
	// server and periodically revalidates pooled connections before checkout.
	// This is intended for state-mutating services using a multi-host DSN or a
	// managed writer endpoint.
	RequirePrimary       bool
	ConnectTimeout       time.Duration
	PrimaryCheckInterval time.Duration
	PrimaryCheckTimeout  time.Duration
	HealthCheckPeriod    time.Duration
}

// New creates a pgx pool and validates connectivity.
func New(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	poolConfig, err := buildConfig(opts)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}

func buildConfig(opts Options) (*pgxpool.Config, error) {
	if opts.DatabaseURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}
	if opts.ConnectTimeout < 0 {
		return nil, fmt.Errorf("connect timeout must not be negative")
	}
	if opts.PrimaryCheckInterval < 0 {
		return nil, fmt.Errorf("primary check interval must not be negative")
	}
	if opts.PrimaryCheckTimeout < 0 {
		return nil, fmt.Errorf("primary check timeout must not be negative")
	}
	if opts.HealthCheckPeriod < 0 {
		return nil, fmt.Errorf("health check period must not be negative")
	}

	poolConfig, err := pgxpool.ParseConfig(opts.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	if opts.Schema != "" {
		if poolConfig.ConnConfig.RuntimeParams == nil {
			poolConfig.ConnConfig.RuntimeParams = map[string]string{}
		}
		poolConfig.ConnConfig.RuntimeParams["search_path"] = opts.Schema
	}

	if opts.MaxConns == 0 && opts.DefaultMaxConns > 0 {
		opts.MaxConns = opts.DefaultMaxConns
	}
	if opts.MinConns == 0 && opts.DefaultMinConns > 0 {
		opts.MinConns = opts.DefaultMinConns
	}

	poolConfig.MaxConns = opts.MaxConns
	poolConfig.MinConns = opts.MinConns
	if opts.MaxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = opts.MaxConnLifetime
	}
	if opts.MaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = opts.MaxConnIdleTime
	}
	if opts.HealthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = opts.HealthCheckPeriod
	}
	if opts.ConfigModifier != nil {
		if err := opts.ConfigModifier(poolConfig); err != nil {
			return nil, fmt.Errorf("apply pool config modifier: %w", err)
		}
	}

	// Install correctness hooks after the caller modifier so instrumentation can
	// add its own hooks without replacing schema or primary fencing.
	if opts.Schema != "" {
		setSearchPathSQL := buildSetSearchPathSQL(opts.Schema)
		existingAfterConnect := poolConfig.AfterConnect
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			if existingAfterConnect != nil {
				if err := existingAfterConnect(ctx, conn); err != nil {
					return err
				}
			}

			if _, err := conn.Exec(ctx, setSearchPathSQL); err != nil {
				return fmt.Errorf("set search_path via after connect: %w", err)
			}
			return nil
		}
	}

	if opts.RequirePrimary {
		applyPrimaryPolicy(poolConfig, opts)
	}

	return poolConfig, nil
}

func buildSetSearchPathSQL(schema string) string {
	return fmt.Sprintf("SET search_path TO %s, public", pgx.Identifier{schema}.Sanitize())
}

type primaryValidationState struct {
	mu        sync.Mutex
	validated map[*pgx.Conn]time.Time
	interval  time.Duration
	timeout   time.Duration
}

func applyPrimaryPolicy(poolConfig *pgxpool.Config, opts Options) {
	if opts.ConnectTimeout > 0 && (poolConfig.ConnConfig.ConnectTimeout == 0 || opts.ConnectTimeout < poolConfig.ConnConfig.ConnectTimeout) {
		poolConfig.ConnConfig.ConnectTimeout = opts.ConnectTimeout
	} else if poolConfig.ConnConfig.ConnectTimeout == 0 {
		poolConfig.ConnConfig.ConnectTimeout = defaultPrimaryConnectTimeout
	}

	if opts.HealthCheckPeriod == 0 {
		poolConfig.HealthCheckPeriod = defaultHAHealthCheckPeriod
	}

	interval := opts.PrimaryCheckInterval
	if interval == 0 {
		interval = defaultPrimaryCheckInterval
	}
	timeout := opts.PrimaryCheckTimeout
	if timeout == 0 {
		timeout = defaultPrimaryCheckTimeout
	}
	state := &primaryValidationState{
		validated: make(map[*pgx.Conn]time.Time),
		interval:  interval,
		timeout:   timeout,
	}

	// A caller may have selected another target_session_attrs mode in the DSN
	// or installed custom validation. Read-write validation runs first and can
	// never be bypassed by pgconn's prefer-* fallback behavior.
	existingValidateConnect := poolConfig.ConnConfig.ValidateConnect
	poolConfig.ConnConfig.ValidateConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		if err := pgconn.ValidateConnectTargetSessionAttrsReadWrite(ctx, conn); err != nil {
			return fmt.Errorf("require read-write PostgreSQL primary: %w", err)
		}
		if existingValidateConnect != nil {
			return existingValidateConnect(ctx, conn)
		}
		return nil
	}

	existingAfterConnect := poolConfig.AfterConnect
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if existingAfterConnect != nil {
			if err := existingAfterConnect(ctx, conn); err != nil {
				return err
			}
		}
		state.markValidated(conn, time.Now())
		return nil
	}

	existingBeforeAcquire := poolConfig.BeforeAcquire
	poolConfig.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		if existingBeforeAcquire != nil && !existingBeforeAcquire(ctx, conn) {
			return false
		}
		return state.isPrimary(ctx, conn, time.Now())
	}

	existingBeforeClose := poolConfig.BeforeClose
	poolConfig.BeforeClose = func(conn *pgx.Conn) {
		state.forget(conn)
		if existingBeforeClose != nil {
			existingBeforeClose(conn)
		}
	}
}

func (s *primaryValidationState) markValidated(conn *pgx.Conn, now time.Time) {
	s.mu.Lock()
	s.validated[conn] = now
	s.mu.Unlock()
}

func (s *primaryValidationState) forget(conn *pgx.Conn) {
	s.mu.Lock()
	delete(s.validated, conn)
	s.mu.Unlock()
}

func (s *primaryValidationState) isPrimary(ctx context.Context, conn *pgx.Conn, now time.Time) bool {
	s.mu.Lock()
	lastValidated, ok := s.validated[conn]
	s.mu.Unlock()
	if ok && now.Sub(lastValidated) < s.interval {
		return true
	}

	checkCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var readOnly string
	if err := conn.QueryRow(checkCtx, "SHOW transaction_read_only").Scan(&readOnly); err != nil || readOnly != "off" {
		s.forget(conn)
		return false
	}

	s.markValidated(conn, now)
	return true
}
