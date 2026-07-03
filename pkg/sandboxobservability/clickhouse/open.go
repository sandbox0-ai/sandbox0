package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type OpenConfig struct {
	DSN     string
	Schema  Config
	Migrate bool
}

func Open(ctx context.Context, cfg OpenConfig) (*sql.DB, *Repository, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, nil, fmt.Errorf("clickhouse dsn is required")
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open clickhouse: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = db.Close()
		}
	}()

	if err := db.PingContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	if cfg.Migrate {
		if err := EnsureSchema(ctx, db, cfg.Schema); err != nil {
			return nil, nil, err
		}
	}
	repo, err := NewRepository(db, cfg.Schema)
	if err != nil {
		return nil, nil, err
	}
	cleanup = false
	return db, repo, nil
}
