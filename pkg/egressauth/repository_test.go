package egressauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
)

type testMigrateLogger struct{}

func (testMigrateLogger) Printf(string, ...any) {}
func (testMigrateLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func TestRepositoryPutSourceAdvancesExistingBindings(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)

	first, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "old-token"))
	if err != nil {
		t.Fatalf("put first source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, "team-1", "github-source")
	if err != nil {
		t.Fatalf("get source by ref: %v", err)
	}
	if source == nil {
		t.Fatal("source is nil")
	}
	if err := repo.UpsertBindings(ctx, &BindingRecord{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		Bindings: []CredentialBinding{{
			Ref:           "github-api",
			SourceRef:     "github-source",
			SourceID:      source.ID,
			SourceVersion: first.CurrentVersion,
			Projection: ProjectionSpec{
				Type: CredentialProjectionTypeHTTPHeaders,
				HTTPHeaders: &HTTPHeadersProjection{
					Headers: []ProjectedHeader{{
						Name:          "Authorization",
						ValueTemplate: "Bearer {{ .token }}",
					}},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("upsert bindings: %v", err)
	}

	rotated, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "new-token"))
	if err != nil {
		t.Fatalf("rotate source: %v", err)
	}
	if rotated.CurrentVersion != 2 {
		t.Fatalf("rotated version = %d, want 2", rotated.CurrentVersion)
	}

	record, err := repo.GetBindings(ctx, "team-1", "sbx-1")
	if err != nil {
		t.Fatalf("get bindings: %v", err)
	}
	if record == nil || len(record.Bindings) != 1 {
		t.Fatalf("bindings = %#v, want one binding", record)
	}
	if got := record.Bindings[0].SourceVersion; got != rotated.CurrentVersion {
		t.Fatalf("binding source version = %d, want %d", got, rotated.CurrentVersion)
	}
	version, err := repo.GetSourceVersion(ctx, source.ID, record.Bindings[0].SourceVersion)
	if err != nil {
		t.Fatalf("get source version: %v", err)
	}
	if version == nil || version.Spec.StaticHeaders == nil {
		t.Fatalf("source version = %#v, want static headers", version)
	}
	if got := version.Spec.StaticHeaders.Values["token"]; got != "new-token" {
		t.Fatalf("resolved token = %q, want new-token", got)
	}
}

func TestEmptyObjectIfJSONNullDefaultsNilCachePolicy(t *testing.T) {
	raw, err := json.Marshal((*CachePolicySpec)(nil))
	if err != nil {
		t.Fatalf("marshal nil cache policy: %v", err)
	}
	if got := string(emptyObjectIfJSONNull(raw)); got != "{}" {
		t.Fatalf("normalized cache policy = %s, want {}", got)
	}
}

func TestRepositoryPutSourcePublishesRotationEvent(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)

	listenConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire listen connection: %v", err)
	}
	defer listenConn.Release()
	if _, err := listenConn.Exec(ctx, "LISTEN "+pubsub.CredentialSourceRotationChannel); err != nil {
		t.Fatalf("listen: %v", err)
	}

	first, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "old-token"))
	if err != nil {
		t.Fatalf("put first source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, "team-1", "github-source")
	if err != nil {
		t.Fatalf("get source by ref: %v", err)
	}
	if source == nil {
		t.Fatal("source is nil")
	}
	if first.CurrentVersion != 1 {
		t.Fatalf("first version = %d, want 1", first.CurrentVersion)
	}

	rotated, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "new-token"))
	if err != nil {
		t.Fatalf("rotate source: %v", err)
	}
	if rotated.CurrentVersion != 2 {
		t.Fatalf("rotated version = %d, want 2", rotated.CurrentVersion)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	notification, err := listenConn.Conn().WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("wait for notification: %v", err)
	}
	if notification.Channel != pubsub.CredentialSourceRotationChannel {
		t.Fatalf("notification channel = %q, want %q", notification.Channel, pubsub.CredentialSourceRotationChannel)
	}
	var event pubsub.CredentialSourceRotatedEvent
	if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if event.TeamID != "team-1" || event.SourceID != source.ID || event.SourceRef != "github-source" || event.SourceVersion != 2 {
		t.Fatalf("rotation event = %#v", event)
	}
	if event.Timestamp.IsZero() {
		t.Fatal("rotation event timestamp is zero")
	}
}

func TestRepositoryPutSourceRejectsResolverKindChange(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)

	if _, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "token")); err != nil {
		t.Fatalf("put first source: %v", err)
	}
	_, err := repo.PutSource(ctx, "team-1", &CredentialSourceWriteRequest{
		Name:         "github-source",
		ResolverKind: "static_username_password",
		Spec: CredentialSourceSecretSpec{
			StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
				Username: "alice",
				Password: "secret",
			},
		},
	})
	if !errors.Is(err, ErrCredentialSourceResolverKindImmutable) {
		t.Fatalf("resolver kind change error = %v, want ErrCredentialSourceResolverKindImmutable", err)
	}
}

func newRepositoryTestStore(t *testing.T) (*Repository, *pgxpool.Pool) {
	t.Helper()

	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
		return nil, nil
	}

	ctx := context.Background()
	schema := fmt.Sprintf("egressauth_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	adminPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(adminPool.Close)
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL,
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("connect schema-scoped pool: %v", err)
	}
	t.Cleanup(pool.Close)
	t.Cleanup(func() {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	if _, err := pool.Exec(ctx, `
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';
`); err != nil {
		t.Fatalf("create updated_at trigger helper: %v", err)
	}
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrations.FS),
		migrate.WithSchema(schema),
		migrate.WithTableName("goose_egressauth_test"),
		migrate.WithLogger(testMigrateLogger{}),
	); err != nil {
		t.Fatalf("migrate egressauth schema: %v", err)
	}

	return NewRepository(pool, WithDefaultStorageKind(CredentialSourceStorageKindPlaintextPG)), pool
}

func staticHeadersSourceWriteRequest(name, token string) *CredentialSourceWriteRequest {
	return &CredentialSourceWriteRequest{
		Name:         name,
		ResolverKind: "static_headers",
		Spec: CredentialSourceSecretSpec{
			StaticHeaders: &StaticHeadersSourceSpec{
				Values: map[string]string{"token": token},
			},
		},
	}
}
