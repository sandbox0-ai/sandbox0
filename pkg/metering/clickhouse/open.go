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
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	normalized, err := normalizeConfig(cfg.Schema)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if cfg.Migrate {
		if err := ApplySchema(ctx, db, normalized); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
	}
	return db, NewRepository(db, normalized), nil
}
