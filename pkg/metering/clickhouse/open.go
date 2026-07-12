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
	db, normalized, err := openDB(cfg)
	if err != nil {
		return nil, nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	if cfg.Migrate {
		if err := ApplySchema(ctx, db, normalized); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
	}
	return db, NewRepository(db, normalized), nil
}

// OpenDeferred validates the configuration without requiring ClickHouse to be
// reachable. database/sql reconnects when the outbox projector retries later.
func OpenDeferred(cfg OpenConfig) (*sql.DB, *Repository, error) {
	db, normalized, err := openDB(cfg)
	if err != nil {
		return nil, nil, err
	}
	return db, NewRepository(db, normalized), nil
}

func openDB(cfg OpenConfig) (*sql.DB, Config, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, Config{}, fmt.Errorf("clickhouse dsn is required")
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, Config{}, fmt.Errorf("open clickhouse: %w", err)
	}
	normalized, err := normalizeConfig(cfg.Schema)
	if err != nil {
		_ = db.Close()
		return nil, Config{}, err
	}
	return db, normalized, nil
}
