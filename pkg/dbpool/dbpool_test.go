package dbpool

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestBuildSetSearchPathSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		schema string
		want   string
	}{
		{
			name:   "simple schema",
			schema: "global_gateway",
			want:   `SET search_path TO "global_gateway", public`,
		},
		{
			name:   "quotes schema safely",
			schema: `schema"withquote`,
			want:   `SET search_path TO "schema""withquote", public`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := buildSetSearchPathSQL(tt.schema); got != tt.want {
				t.Fatalf("buildSetSearchPathSQL(%q) = %q, want %q", tt.schema, got, tt.want)
			}
		})
	}
}

func TestBuildConfigAppliesPrimaryPolicyAfterModifier(t *testing.T) {
	t.Parallel()

	modifierAfterConnect := func(context.Context, *pgx.Conn) error { return nil }
	modifierBeforeAcquire := func(context.Context, *pgx.Conn) bool { return true }
	modifierBeforeClose := func(*pgx.Conn) {}
	config, err := buildConfig(Options{
		DatabaseURL:    "postgres://user:password@primary:5432,standby:5433/database?sslmode=disable",
		RequirePrimary: true,
		ConfigModifier: func(config *pgxpool.Config) error {
			config.AfterConnect = modifierAfterConnect
			config.BeforeAcquire = modifierBeforeAcquire
			config.BeforeClose = modifierBeforeClose
			return nil
		},
	})
	if err != nil {
		t.Fatalf("build config: %v", err)
	}

	if config.ConnConfig.ValidateConnect == nil {
		t.Fatal("expected read-write connect validation")
	}
	if config.AfterConnect == nil || config.BeforeAcquire == nil || config.BeforeClose == nil {
		t.Fatal("expected primary policy pool hooks")
	}
	if config.ConnConfig.ConnectTimeout != defaultPrimaryConnectTimeout {
		t.Fatalf("connect timeout = %s, want %s", config.ConnConfig.ConnectTimeout, defaultPrimaryConnectTimeout)
	}
	if config.HealthCheckPeriod != defaultHAHealthCheckPeriod {
		t.Fatalf("health check period = %s, want %s", config.HealthCheckPeriod, defaultHAHealthCheckPeriod)
	}
	if len(config.ConnConfig.Fallbacks) != 1 {
		t.Fatalf("fallbacks = %d, want 1", len(config.ConnConfig.Fallbacks))
	}
}

func TestBuildConfigPrimaryPolicyHonorsTimeouts(t *testing.T) {
	t.Parallel()

	config, err := buildConfig(Options{
		DatabaseURL:         "postgres://user:password@localhost/database?sslmode=disable&connect_timeout=7",
		RequirePrimary:      true,
		ConnectTimeout:      5 * time.Second,
		HealthCheckPeriod:   2 * time.Second,
		PrimaryCheckTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("build config: %v", err)
	}
	if config.ConnConfig.ConnectTimeout != 5*time.Second {
		t.Fatalf("connect timeout = %s, want 5s", config.ConnConfig.ConnectTimeout)
	}
	if config.HealthCheckPeriod != 2*time.Second {
		t.Fatalf("health check period = %s, want 2s", config.HealthCheckPeriod)
	}
}

func TestBuildConfigRejectsNegativeHATimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options Options
	}{
		{name: "connect", options: Options{ConnectTimeout: -time.Second}},
		{name: "primary interval", options: Options{PrimaryCheckInterval: -time.Second}},
		{name: "primary timeout", options: Options{PrimaryCheckTimeout: -time.Second}},
		{name: "health", options: Options{HealthCheckPeriod: -time.Second}},
	}
	for _, tt := range tests {
		test := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			test.options.DatabaseURL = "postgres://user:password@localhost/database?sslmode=disable"
			if _, err := buildConfig(test.options); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestPrimaryPolicyEvictsConnectionThatBecomesReadOnly(t *testing.T) {
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := New(ctx, Options{
		DatabaseURL:          databaseURL,
		MaxConns:             1,
		RequirePrimary:       true,
		PrimaryCheckInterval: time.Millisecond,
		PrimaryCheckTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire original connection: %v", err)
	}
	original := conn.Conn()
	if _, err := conn.Exec(ctx, "SET default_transaction_read_only = on"); err != nil {
		conn.Release()
		t.Fatalf("make connection read only: %v", err)
	}
	conn.Release()
	time.Sleep(2 * time.Millisecond)

	replacement, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire replacement connection: %v", err)
	}
	defer replacement.Release()
	if replacement.Conn() == original {
		t.Fatal("read-only connection was returned to the caller")
	}
	var readOnly string
	if err := replacement.QueryRow(ctx, "SHOW transaction_read_only").Scan(&readOnly); err != nil {
		t.Fatalf("inspect replacement connection: %v", err)
	}
	if readOnly != "off" {
		t.Fatalf("replacement transaction_read_only = %q, want off", readOnly)
	}
}
