// Package migrate provides a universal database migration solution for sandbox0 services.
//
// It uses goose as the underlying migration engine and supports:
//   - Running migrations programmatically (embedded in Go code)
//   - PostgreSQL with pgx v5
//   - Idempotent migrations via version tracking
//   - SQL and Go-based migrations
//
// Usage:
//
//	import "github.com/sandbox0-ai/sandbox0/pkg/migrate"
//
//	// Simple auto-migration on startup
//	if err := migrate.Up(ctx, db, "migrations"); err != nil {
//	    log.Fatal(err)
//	}
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const dialect = "pgx"

// Options configures the migrator behavior.
type Options struct {
	// Logger is an optional logger for migration output.
	Logger Logger
	// BaseFS is an optional filesystem for embedded migrations.
	// When set, migrationsDir is resolved within this filesystem.
	BaseFS fs.FS
	// TableName is the name of the migration tracking table.
	// Defaults to "goose_db_version".
	TableName string
	// Schema is the database schema to use for migrations.
	// If specified, sets search_path to this schema before running migrations.
	Schema string
}

// Option is a functional option for configuring the migrator.
type Option func(*Options)

// WithLogger sets the logger for migration output.
func WithLogger(l Logger) Option {
	return func(o *Options) { o.Logger = l }
}

// WithBaseFS sets the filesystem that holds embedded migrations.
// Use migrationsDir as the path within this filesystem.
func WithBaseFS(baseFS fs.FS) Option {
	return func(o *Options) { o.BaseFS = baseFS }
}

// WithTableName sets the migration tracking table name.
func WithTableName(name string) Option {
	return func(o *Options) { o.TableName = name }
}

// WithSchema sets the database schema for migrations.
func WithSchema(schema string) Option {
	return func(o *Options) { o.Schema = schema }
}

// Logger defines the interface for logging migration progress.
type Logger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// defaultLogger is a silent logger that discards output.
type defaultLogger struct{}

func (defaultLogger) Printf(string, ...any) {}
func (defaultLogger) Fatalf(string, ...any) {}

// Up runs all pending migrations in the specified directory.
//
// The migrations directory can be:
//   - An absolute path
//   - A relative path from the working directory
//   - A path relative to the service's binary location
//
// This function is idempotent - it tracks which migrations have been applied
// and only runs new ones.
func Up(ctx context.Context, pool *pgxpool.Pool, migrationsDir string, opts ...Option) error {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.Logger == nil {
		options.Logger = defaultLogger{}
	}

	// Resolve the migrations directory
	resolvedDir, err := resolveDirWithBaseFS(migrationsDir, options.BaseFS)
	if err != nil {
		return fmt.Errorf("resolve migrations directory: %w", err)
	}

	// Convert pgx pool to sql.DB
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	// Set goose base filesystem for embedded migrations
	if options.BaseFS != nil {
		goose.SetBaseFS(options.BaseFS)
	} else {
		goose.SetBaseFS(nil)
	}

	// Set goose logger
	if options.Logger != nil {
		goose.SetLogger(options.Logger)
	}

	// Set custom table name if provided
	if options.TableName != "" {
		goose.SetTableName(options.TableName)
	}

	// Set schema search_path if specified
	if options.Schema != "" {
		// Create schema if not exists
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", options.Schema)); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		// Set search_path to the schema
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET search_path TO %s", options.Schema)); err != nil {
			return fmt.Errorf("set search_path: %w", err)
		}
	}

	// Run migrations
	if err := goose.UpContext(ctx, db, resolvedDir); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	return nil
}

// Status prints the current migration status to stdout.
func Status(ctx context.Context, pool *pgxpool.Pool, migrationsDir string, opts ...Option) error {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	resolvedDir, err := resolveDirWithBaseFS(migrationsDir, options.BaseFS)
	if err != nil {
		return err
	}

	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	// Set goose base filesystem for embedded migrations
	if options.BaseFS != nil {
		goose.SetBaseFS(options.BaseFS)
	} else {
		goose.SetBaseFS(nil)
	}

	// Set schema search_path if specified
	if options.Schema != "" {
		// Create schema if not exists
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", options.Schema)); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		// Set search_path to the schema
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET search_path TO %s", options.Schema)); err != nil {
			return fmt.Errorf("set search_path: %w", err)
		}
	}

	if options.TableName != "" {
		goose.SetTableName(options.TableName)
	}

	return goose.StatusContext(ctx, db, resolvedDir)
}

// Down rolls back the most recently applied migration.
func Down(ctx context.Context, pool *pgxpool.Pool, migrationsDir string, opts ...Option) error {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.Logger == nil {
		options.Logger = defaultLogger{}
	}

	resolvedDir, err := resolveDirWithBaseFS(migrationsDir, options.BaseFS)
	if err != nil {
		return fmt.Errorf("resolve migrations directory: %w", err)
	}

	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	if options.BaseFS != nil {
		goose.SetBaseFS(options.BaseFS)
	} else {
		goose.SetBaseFS(nil)
	}

	if options.Logger != nil {
		goose.SetLogger(options.Logger)
	}

	// Set schema search_path if specified
	if options.Schema != "" {
		// Create schema if not exists
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", options.Schema)); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		// Set search_path to the schema
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET search_path TO %s", options.Schema)); err != nil {
			return fmt.Errorf("set search_path: %w", err)
		}
	}

	if options.TableName != "" {
		goose.SetTableName(options.TableName)
	}

	if err := goose.DownContext(ctx, db, resolvedDir); err != nil {
		return fmt.Errorf("run down migration: %w", err)
	}

	return nil
}

// Create creates a new migration file in the specified directory.
//
// name: The name of the migration (e.g., "add_users_table")
// migrationType: The type of migration ("sql" or "go")
func Create(migrationsDir, name, migrationType string) error {
	resolvedDir, err := resolveDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("resolve migrations directory: %w", err)
	}

	db, err := sql.Open("pgx", "postgres:///")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	return goose.Create(db, resolvedDir, name, migrationType)
}

// resolveDirWithBaseFS resolves the migrations directory path with an optional base FS.
func resolveDirWithBaseFS(migrationsDir string, baseFS fs.FS) (string, error) {
	if baseFS == nil {
		return resolveDir(migrationsDir)
	}
	if migrationsDir == "" {
		return "", fmt.Errorf("migrations directory is required")
	}
	if filepath.IsAbs(migrationsDir) {
		return "", fmt.Errorf("embedded migrations path must be relative: %s", migrationsDir)
	}
	if !fs.ValidPath(migrationsDir) {
		return "", fmt.Errorf("invalid embedded migrations path: %s", migrationsDir)
	}
	if _, err := fs.Stat(baseFS, migrationsDir); err != nil {
		return "", fmt.Errorf("migrations directory not found in embedded filesystem: %s", migrationsDir)
	}
	return migrationsDir, nil
}

// resolveDir resolves the migrations directory to an absolute path.
func resolveDir(migrationsDir string) (string, error) {
	dir := migrationsDir

	// If absolute, return as-is
	if filepath.IsAbs(dir) {
		if _, err := os.Stat(dir); err != nil {
			return "", fmt.Errorf("migrations directory not found: %s", dir)
		}
		return dir, nil
	}

	// Try relative to current working directory
	if _, err := os.Stat(dir); err == nil {
		return filepath.Abs(dir)
	}

	// Try relative to the binary location
	if exePath, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exePath)
		resolved := filepath.Join(exeDir, dir)
		if _, err := os.Stat(resolved); err == nil {
			return resolved, nil
		}
	}

	return "", fmt.Errorf("migrations directory not found: %s", dir)
}
