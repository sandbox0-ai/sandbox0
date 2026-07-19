package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	storagemigrations "github.com/sandbox0-ai/sandbox0/storage-proxy/migrations"
)

func TestDeleteExpiredCoordinationsRetainsRecentAndInFlightRows(t *testing.T) {
	repo := newCoordinationRetentionTestRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	volumeID := "volume-" + uuid.NewString()
	if _, err := repo.Pool().Exec(ctx, `
		INSERT INTO sandbox_volumes (id, team_id, user_id)
		VALUES ($1, 'team-1', 'user-1')
	`, volumeID); err != nil {
		t.Fatalf("create sandbox volume: %v", err)
	}

	testCases := []struct {
		id        string
		status    string
		expiresAt time.Time
		deleted   bool
	}{
		{
			id:        "expired-beyond-retention",
			status:    CoordStatusFlushing,
			expiresAt: now.Add(-25 * time.Hour),
			deleted:   true,
		},
		{
			id:        "expired-within-retention",
			status:    CoordStatusTimeout,
			expiresAt: now.Add(-23 * time.Hour),
		},
		{
			id:        "in-flight",
			status:    CoordStatusFlushing,
			expiresAt: now.Add(time.Hour),
		},
		{
			id:        "terminal-before-deadline",
			status:    CoordStatusCompleted,
			expiresAt: now.Add(time.Hour),
		},
	}
	for _, testCase := range testCases {
		coordination := &SnapshotCoordination{
			ID:            testCase.id,
			VolumeID:      volumeID,
			Status:        testCase.status,
			ExpectedNodes: 1,
			CreatedAt:     now,
			UpdatedAt:     now,
			ExpiresAt:     testCase.expiresAt,
		}
		if err := repo.CreateCoordination(ctx, coordination); err != nil {
			t.Fatalf("create coordination %s: %v", testCase.id, err)
		}
		flushedAt := now
		if err := repo.CreateFlushResponse(ctx, &FlushResponse{
			ID:        "response-" + testCase.id,
			CoordID:   testCase.id,
			ClusterID: "cluster-1",
			PodID:     "pod-1",
			Success:   true,
			FlushedAt: &flushedAt,
		}); err != nil {
			t.Fatalf("create flush response %s: %v", testCase.id, err)
		}
	}

	deleted, err := repo.DeleteExpiredCoordinations(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteExpiredCoordinations() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteExpiredCoordinations() deleted = %d, want 1", deleted)
	}

	for _, testCase := range testCases {
		_, err := repo.GetCoordination(ctx, testCase.id)
		if testCase.deleted {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("GetCoordination(%s) error = %v, want ErrNotFound", testCase.id, err)
			}
		} else if err != nil {
			t.Fatalf("GetCoordination(%s) error = %v", testCase.id, err)
		}

		var responses int
		if err := repo.Pool().QueryRow(ctx, `
			SELECT COUNT(*)
			FROM snapshot_flush_responses
			WHERE coord_id = $1
		`, testCase.id).Scan(&responses); err != nil {
			t.Fatalf("count flush responses for %s: %v", testCase.id, err)
		}
		wantResponses := 1
		if testCase.deleted {
			wantResponses = 0
		}
		if responses != wantResponses {
			t.Fatalf(
				"flush responses for %s = %d, want %d",
				testCase.id,
				responses,
				wantResponses,
			)
		}
	}
}

func TestDeleteExpiredCoordinationsRejectsNonPositiveRetention(t *testing.T) {
	repo := newCoordinationRetentionTestRepository(t)
	if _, err := repo.DeleteExpiredCoordinations(context.Background(), 0); err == nil {
		t.Fatal("DeleteExpiredCoordinations() error = nil, want invalid retention")
	}
}

func newCoordinationRetentionTestRepository(t *testing.T) *Repository {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx := context.Background()
	schema := fmt.Sprintf(
		"storage_proxy_coord_retention_%s",
		strings.ReplaceAll(uuid.NewString(), "-", ""),
	)
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     databaseURL,
		Schema:          schema,
		DefaultMaxConns: 10,
		DefaultMinConns: 1,
	})
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(
			"DROP SCHEMA IF EXISTS %s CASCADE",
			schema,
		))
		pool.Close()
	})
	if err := migrate.Up(
		ctx,
		pool,
		".",
		migrate.WithBaseFS(storagemigrations.FS),
		migrate.WithSchema(schema),
	); err != nil {
		t.Fatalf("migrate storage-proxy schema: %v", err)
	}
	return NewRepository(pool)
}
