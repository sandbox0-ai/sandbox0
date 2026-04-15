package dbpool

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
}

// New creates a pgx pool and validates connectivity.
func New(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	if opts.DatabaseURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}

	poolConfig, err := pgxpool.ParseConfig(opts.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	if opts.Schema != "" {
		setSearchPathSQL := buildSetSearchPathSQL(opts.Schema)
		if poolConfig.ConnConfig.RuntimeParams == nil {
			poolConfig.ConnConfig.RuntimeParams = map[string]string{}
		}
		poolConfig.ConnConfig.RuntimeParams["search_path"] = opts.Schema

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
	if opts.ConfigModifier != nil {
		if err := opts.ConfigModifier(poolConfig); err != nil {
			return nil, fmt.Errorf("apply pool config modifier: %w", err)
		}
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

func buildSetSearchPathSQL(schema string) string {
	return fmt.Sprintf("SET search_path TO %s, public", pgx.Identifier{schema}.Sanitize())
}
