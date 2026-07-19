package dbpool

import (
	"context"
	"errors"
	"testing"

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

func TestNewConnectionBoundsPrecedence(t *testing.T) {
	t.Parallel()

	inspectComplete := errors.New("inspection complete")
	tests := []struct {
		name         string
		maxConns     int32
		minConns     int32
		defaultMax   int32
		defaultMin   int32
		wantMaxConns int32
		wantMinConns int32
	}{
		{
			name:         "URL values survive omitted options",
			wantMaxConns: 7,
			wantMinConns: 2,
		},
		{
			name:         "explicit options override URL values",
			maxConns:     11,
			minConns:     3,
			wantMaxConns: 11,
			wantMinConns: 3,
		},
		{
			name:         "service defaults override URL values",
			defaultMax:   13,
			defaultMin:   4,
			wantMaxConns: 13,
			wantMinConns: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(context.Background(), Options{
				DatabaseURL:     "postgres://postgres@127.0.0.1:1/postgres?pool_max_conns=7&pool_min_conns=2",
				MaxConns:        tt.maxConns,
				MinConns:        tt.minConns,
				DefaultMaxConns: tt.defaultMax,
				DefaultMinConns: tt.defaultMin,
				ConfigModifier: func(config *pgxpool.Config) error {
					if config.MaxConns != tt.wantMaxConns {
						t.Fatalf("MaxConns = %d, want %d", config.MaxConns, tt.wantMaxConns)
					}
					if config.MinConns != tt.wantMinConns {
						t.Fatalf("MinConns = %d, want %d", config.MinConns, tt.wantMinConns)
					}
					return inspectComplete
				},
			})
			if !errors.Is(err, inspectComplete) {
				t.Fatalf("New() error = %v, want inspection sentinel", err)
			}
		})
	}
}
