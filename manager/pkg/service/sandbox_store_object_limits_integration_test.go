package service

import (
	"context"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestSandboxStoreRejectsOversizedRecordBeforePostgresWrite(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	ctx := context.Background()

	var before int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.sandboxes`).Scan(&before); err != nil {
		t.Fatalf("count sandboxes before rejected upsert: %v", err)
	}
	err := store.UpsertSandbox(ctx, &SandboxRecord{
		ID:     "sandbox-oversized",
		TeamID: "team-1",
		Config: SandboxConfig{
			EnvVars: map[string]string{
				"SECRET": strings.Repeat("s", int(templatepkg.MaxMapValueBytes)+1),
			},
		},
	})
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("UpsertSandbox() error = %v, want TooLargeError", err)
	}

	var after int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.sandboxes`).Scan(&after); err != nil {
		t.Fatalf("count sandboxes after rejected upsert: %v", err)
	}
	if after != before {
		t.Fatalf("sandbox count = %d, want unchanged %d", after, before)
	}
}
