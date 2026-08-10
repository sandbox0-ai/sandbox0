package portal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	storagemigrations "github.com/sandbox0-ai/sandbox0/storage-proxy/migrations"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestS0FSBackgroundCompactionDoesNotDeleteActiveVolumeObjects(t *testing.T) {
	repo := newActiveS0FSTestRepository(t)
	if repo == nil {
		return
	}

	ctx := context.Background()
	const teamID = "team-active-gc"
	volumeID := activeS0FSRegressionVolumeID()
	createActiveS0FSTestVolume(t, repo, volumeID, teamID)

	bucket := "s0fs-active-gc-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	storageConfig := &apiconfig.StorageProxyConfig{
		ObjectStorageType:            objectstore.TypeMem,
		S3Bucket:                     bucket,
		S0FSCompactionInterval:       "10ms",
		S0FSSegmentTargetSize:        "1Ki",
		S0FSCompactionMinDeadRatio:   "0.001",
		S0FSCompactionMinReclaimSize: "1",
	}
	prefix, err := naming.S3VolumePrefix(teamID, volumeID)
	if err != nil {
		t.Fatalf("S3VolumePrefix() error = %v", err)
	}
	store := objectstore.Prefix(objectstore.NewMemoryStore(bucket), prefix+"/s0fs/")
	cacheDir := t.TempDir()
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          volumeID,
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:       store,
		HeadStore:         db.NewS0FSHeadStore(repo),
		SegmentTargetSize: 1024,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	observer := newBlockingMaterializationObserver()
	bound := &boundVolume{
		volumeID: volumeID,
		teamID:   teamID,
		access:   volume.AccessModeRWO,
		volCtx: &volume.VolumeContext{
			VolumeID: volumeID,
			TeamID:   teamID,
			Backend:  volume.BackendS0FS,
			S0FS:     engine,
			Access:   volume.AccessModeRWO,
			CacheDir: cacheDir,
			Observer: observer,
		},
	}
	t.Cleanup(func() {
		observer.Unblock()
		if bound.materializeCancel != nil {
			bound.materializeCancel()
		}
		if bound.materializeDone != nil {
			<-bound.materializeDone
		}
		if err := engine.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	a, err := engine.CreateFile(s0fs.RootInode, "a.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(a.txt) error = %v", err)
	}
	b, err := engine.CreateFile(s0fs.RootInode, "b.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(b.txt) error = %v", err)
	}
	if _, err := engine.Write(a.Inode, 0, bytes.Repeat([]byte("a"), 64)); err != nil {
		t.Fatalf("Write(a.txt) error = %v", err)
	}
	if _, err := engine.Write(b.Inode, 0, bytes.Repeat([]byte("b"), 64)); err != nil {
		t.Fatalf("Write(b.txt) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(initial) error = %v", err)
	}
	if _, err := engine.Write(a.Inode, 10, []byte("Z")); err != nil {
		t.Fatalf("Write(a.txt overwrite) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(fragmented) error = %v", err)
	}

	const orphanKey = "segments/orphan-before-active-gc.bin"
	if err := store.Put(orphanKey, bytes.NewReader([]byte("orphan"))); err != nil {
		t.Fatalf("Put(%q) error = %v", orphanKey, err)
	}

	manager := NewManager(Config{
		RootDir:                 t.TempDir(),
		StorageConfig:           storageConfig,
		Repository:              repo,
		MaterializerConcurrency: 1,
	})
	manager.volumes.add(bound.volCtx)
	manager.startMaterializer(bound)

	select {
	case <-observer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("background compaction did not reach its materialization observer")
	}
	observer.Unblock()

	deadline := time.NewTimer(500 * time.Millisecond)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-deadline.C:
			return
		case <-poll.C:
			if _, err := store.Head(orphanKey); err != nil {
				t.Fatalf("active background compaction deleted %q: %v", orphanKey, err)
			}
		}
	}
}

func activeS0FSRegressionVolumeID() string {
	for index := 0; ; index++ {
		volumeID := fmt.Sprintf("s0fs-active-gc-%d", index)
		if materializerInitialJitter(volumeID) == 0 {
			return volumeID
		}
	}
}

type blockingMaterializationObserver struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingMaterializationObserver() *blockingMaterializationObserver {
	return &blockingMaterializationObserver{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (o *blockingMaterializationObserver) ObserveVolumeState(_ context.Context, _ string, _ string, state *s0fs.SnapshotState, _ time.Time) error {
	if state == nil {
		return nil
	}
	o.enteredOnce.Do(func() {
		close(o.entered)
	})
	<-o.release
	return nil
}

func (o *blockingMaterializationObserver) Unblock() {
	if o == nil {
		return
	}
	o.releaseOnce.Do(func() {
		close(o.release)
	})
}

func newActiveS0FSTestRepository(t *testing.T) *db.Repository {
	t.Helper()

	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
		return nil
	}

	ctx := context.Background()
	schema := fmt.Sprintf("ctld_s0fs_active_gc_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: databaseURL,
		MaxConns:    4,
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		pool.Close()
	})
	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(storagemigrations.FS), migrate.WithSchema(schema)); err != nil {
		t.Fatalf("migrate storage-proxy schema: %v", err)
	}
	return db.NewRepository(pool)
}

func createActiveS0FSTestVolume(t *testing.T, repo *db.Repository, volumeID, teamID string) {
	t.Helper()
	now := time.Now().UTC()
	if err := repo.CreateSandboxVolume(context.Background(), &db.SandboxVolume{
		ID:         volumeID,
		TeamID:     teamID,
		UserID:     "user-active-gc",
		AccessMode: string(volume.AccessModeRWO),
		CreatedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		t.Fatalf("CreateSandboxVolume(%q) error = %v", volumeID, err)
	}
}
