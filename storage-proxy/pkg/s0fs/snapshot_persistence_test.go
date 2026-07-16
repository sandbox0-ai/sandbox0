package s0fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

func TestPersistedSnapshotLoadsFromFreshCache(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	cfg := Config{
		VolumeID:    "vol-durable",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
		Encryption:  testEncryptionConfig(16),
	}
	state := snapshotStateWithFile(t, "durable.txt", "durable snapshot payload")

	if err := PersistSnapshot(ctx, cfg, "snap-durable", state); err != nil {
		t.Fatalf("PersistSnapshot() error = %v", err)
	}
	assertObjectDoesNotContain(t, store, snapshotObjectKey("snap-durable"), []byte("durable snapshot payload"))

	freshCfg := cfg
	freshCfg.WALPath = filepath.Join(t.TempDir(), "engine.wal")
	loaded, err := LoadSnapshot(ctx, freshCfg, "snap-durable")
	if err != nil {
		t.Fatalf("LoadSnapshot() from fresh cache error = %v", err)
	}
	assertSnapshotFilePayload(t, loaded, "durable.txt", "durable snapshot payload")
	if _, err := os.Stat(snapshotFilePath(freshCfg.WALPath, "snap-durable")); err != nil {
		t.Fatalf("local snapshot cache stat error = %v", err)
	}
}

func TestPersistSnapshotDoesNotFailWhenCanonicalObjectOutlivesLocalCache(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	blockedParent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedParent, []byte("block local cache"), 0o600); err != nil {
		t.Fatalf("WriteFile(blocked parent) error = %v", err)
	}
	cfg := Config{
		VolumeID:    "vol-cache-failure",
		WALPath:     filepath.Join(blockedParent, "engine.wal"),
		ObjectStore: store,
	}
	state := snapshotStateWithFile(t, "durable.txt", "canonical")

	if err := PersistSnapshot(ctx, cfg, "snap-cache-failure", state); err != nil {
		t.Fatalf("PersistSnapshot() local cache failure error = %v", err)
	}
	freshCfg := cfg
	freshCfg.WALPath = filepath.Join(t.TempDir(), "engine.wal")
	loaded, err := LoadSnapshot(ctx, freshCfg, "snap-cache-failure")
	if err != nil {
		t.Fatalf("LoadSnapshot() canonical object error = %v", err)
	}
	assertSnapshotFilePayload(t, loaded, "durable.txt", "canonical")
}

func TestLoadSnapshotBackfillsLegacyLocalState(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	cfg := Config{
		VolumeID:    "vol-legacy",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
	}
	state := snapshotStateWithFile(t, "legacy.txt", "legacy payload")
	if err := saveSnapshotState(snapshotFilePath(cfg.WALPath, "snap-legacy"), cfg.VolumeID, "snapshot:snap-legacy", state, nil); err != nil {
		t.Fatalf("saveSnapshotState() error = %v", err)
	}
	if _, err := store.Head(snapshotObjectKey("snap-legacy")); !objectstore.IsNotFound(err) {
		t.Fatalf("legacy snapshot object Head() error = %v, want not found", err)
	}

	loaded, err := LoadSnapshot(ctx, cfg, "snap-legacy")
	if err != nil {
		t.Fatalf("LoadSnapshot() legacy error = %v", err)
	}
	assertSnapshotFilePayload(t, loaded, "legacy.txt", "legacy payload")
	if _, err := store.Head(snapshotObjectKey("snap-legacy")); err != nil {
		t.Fatalf("backfilled snapshot object Head() error = %v", err)
	}

	freshCfg := cfg
	freshCfg.WALPath = filepath.Join(t.TempDir(), "engine.wal")
	loaded, err = LoadSnapshot(ctx, freshCfg, "snap-legacy")
	if err != nil {
		t.Fatalf("LoadSnapshot() backfilled snapshot from fresh cache error = %v", err)
	}
	assertSnapshotFilePayload(t, loaded, "legacy.txt", "legacy payload")
}

func TestLoadSnapshotRequiresSuccessfulLegacyBackfill(t *testing.T) {
	ctx := context.Background()
	store := &snapshotPutFailingStore{Store: objectstore.NewMemoryStore(t.Name())}
	cfg := Config{
		VolumeID:    "vol-backfill-failure",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
	}
	state := snapshotStateWithFile(t, "legacy.txt", "legacy payload")
	if err := saveSnapshotState(snapshotFilePath(cfg.WALPath, "snap-legacy"), cfg.VolumeID, "snapshot:snap-legacy", state, nil); err != nil {
		t.Fatalf("saveSnapshotState() error = %v", err)
	}

	_, err := LoadSnapshot(ctx, cfg, "snap-legacy")
	if err == nil || !errors.Is(err, errSnapshotPutFailed) {
		t.Fatalf("LoadSnapshot() error = %v, want snapshot put failure", err)
	}
}

func TestLoadSnapshotPrefersCanonicalObject(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	cfg := Config{
		VolumeID:    "vol-canonical",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
	}
	canonical := snapshotStateWithFile(t, "state.txt", "canonical")
	if err := PersistSnapshot(ctx, cfg, "snap-canonical", canonical); err != nil {
		t.Fatalf("PersistSnapshot() error = %v", err)
	}
	stale := snapshotStateWithFile(t, "state.txt", "stale-local")
	if err := saveSnapshotState(snapshotFilePath(cfg.WALPath, "snap-canonical"), cfg.VolumeID, "snapshot:snap-canonical", stale, nil); err != nil {
		t.Fatalf("save stale local snapshot error = %v", err)
	}

	loaded, err := LoadSnapshot(ctx, cfg, "snap-canonical")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	assertSnapshotFilePayload(t, loaded, "state.txt", "canonical")
}

func TestDeleteSnapshotRemovesCanonicalAndLocalCopies(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	cfg := Config{
		VolumeID:    "vol-delete",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
	}
	if err := PersistSnapshot(ctx, cfg, "snap-delete", snapshotStateWithFile(t, "delete.txt", "payload")); err != nil {
		t.Fatalf("PersistSnapshot() error = %v", err)
	}

	if err := DeleteSnapshot(ctx, cfg, "snap-delete"); err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	if _, err := store.Head(snapshotObjectKey("snap-delete")); !objectstore.IsNotFound(err) {
		t.Fatalf("deleted snapshot object Head() error = %v, want not found", err)
	}
	if _, err := os.Stat(snapshotFilePath(cfg.WALPath, "snap-delete")); !os.IsNotExist(err) {
		t.Fatalf("deleted local snapshot stat error = %v, want not exist", err)
	}
	if err := DeleteSnapshot(ctx, cfg, "snap-delete"); err != nil {
		t.Fatalf("DeleteSnapshot() repeated error = %v", err)
	}
}

func TestRecoverSnapshotFromManifestSelectsNewestBeforeCutoff(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	cfg := Config{
		VolumeID:    "vol-recover",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
	}
	materializer := snapshotMaterializer(cfg)
	base := time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)
	manifests := []*Manifest{
		recoveryManifest(t, cfg.VolumeID, 2, base.Add(time.Minute), "first"),
		recoveryManifest(t, cfg.VolumeID, 4, base, "selected"),
		recoveryManifest(t, cfg.VolumeID, 6, base.Add(2*time.Minute), "future"),
	}
	for _, manifest := range manifests {
		if err := materializer.putJSON(ctx, manifestKey(manifest.ManifestSeq), manifest); err != nil {
			t.Fatalf("put manifest %d error = %v", manifest.ManifestSeq, err)
		}
	}
	legacyLatest := recoveryManifest(t, cfg.VolumeID, 100, base.Add(3*time.Minute), "legacy-latest")
	if err := materializer.putJSON(ctx, manifestLatestKey, legacyLatest); err != nil {
		t.Fatalf("put legacy latest manifest error = %v", err)
	}
	if err := store.Put("manifests/not-an-immutable-manifest.json", bytes.NewBufferString("not-json")); err != nil {
		t.Fatalf("put non-immutable manifest object error = %v", err)
	}

	expectedSize := StateStorageBytes(manifests[1].State)
	state, manifest, err := RecoverSnapshotFromManifest(ctx, cfg, base.Add(90*time.Second), expectedSize)
	if err != nil {
		t.Fatalf("RecoverSnapshotFromManifest() error = %v", err)
	}
	if manifest.ManifestSeq != 4 {
		t.Fatalf("recovered manifest seq = %d, want 4", manifest.ManifestSeq)
	}
	assertSnapshotFilePayload(t, state, "state.txt", "selected")

	_, _, err = RecoverSnapshotFromManifest(ctx, cfg, base.Add(-time.Second), expectedSize)
	if !errors.Is(err, ErrMaterializedManifestNotFound) {
		t.Fatalf("RecoverSnapshotFromManifest() before first error = %v, want ErrMaterializedManifestNotFound", err)
	}
}

func TestRecoverSnapshotFromManifestRejectsInvalidCandidates(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)

	t.Run("corrupt object", func(t *testing.T) {
		store := objectstore.NewMemoryStore(t.Name())
		cfg := Config{VolumeID: "vol-corrupt", WALPath: filepath.Join(t.TempDir(), "engine.wal"), ObjectStore: store}
		if err := snapshotMaterializer(cfg).putJSON(ctx, manifestLatestKey, recoveryManifest(t, cfg.VolumeID, 2, base, "head")); err != nil {
			t.Fatalf("put recovery head error = %v", err)
		}
		if err := store.Put(manifestKey(2), bytes.NewBufferString("not-json")); err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		if _, _, err := RecoverSnapshotFromManifest(ctx, cfg, base, 0); err == nil {
			t.Fatal("RecoverSnapshotFromManifest() corrupt object returned nil error")
		}
	})

	t.Run("encrypted object without key", func(t *testing.T) {
		store := objectstore.NewMemoryStore(t.Name())
		cfg := Config{
			VolumeID: "vol-encrypted", WALPath: filepath.Join(t.TempDir(), "engine.wal"),
			ObjectStore: store, Encryption: testEncryptionConfig(16),
		}
		manifest := recoveryManifest(t, cfg.VolumeID, 2, base, "encrypted")
		if err := snapshotMaterializer(cfg).putJSON(ctx, manifestKey(2), manifest); err != nil {
			t.Fatalf("put encrypted manifest error = %v", err)
		}
		if err := snapshotMaterializer(cfg).putJSON(ctx, manifestLatestKey, manifest); err != nil {
			t.Fatalf("put encrypted recovery head error = %v", err)
		}
		cfg.Encryption = nil
		if _, _, err := RecoverSnapshotFromManifest(ctx, cfg, base, StateStorageBytes(manifest.State)); err == nil {
			t.Fatal("RecoverSnapshotFromManifest() encrypted object without key returned nil error")
		}
	})

	tests := []struct {
		name     string
		volumeID string
		state    *SnapshotState
	}{
		{name: "wrong volume", volumeID: "another-volume", state: snapshotStateWithFile(t, "state.txt", "wrong-volume")},
		{name: "nil state", volumeID: "vol-invalid", state: nil},
		{name: "missing root", volumeID: "vol-invalid", state: &SnapshotState{NextSeq: 3, Nodes: map[uint64]*Node{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := objectstore.NewMemoryStore(t.Name())
			cfg := Config{VolumeID: "vol-invalid", WALPath: filepath.Join(t.TempDir(), "engine.wal"), ObjectStore: store}
			if tt.state != nil && tt.state.NextSeq == 0 {
				tt.state.NextSeq = 3
			}
			manifest := &Manifest{Version: 1, VolumeID: tt.volumeID, ManifestSeq: 2, CheckpointSeq: 2, CreatedAt: base, State: tt.state}
			if err := snapshotMaterializer(cfg).putJSON(ctx, manifestLatestKey, recoveryManifest(t, cfg.VolumeID, 2, base, "head")); err != nil {
				t.Fatalf("put recovery head error = %v", err)
			}
			if err := snapshotMaterializer(cfg).putJSON(ctx, manifestKey(2), manifest); err != nil {
				t.Fatalf("put invalid manifest error = %v", err)
			}
			if _, _, err := RecoverSnapshotFromManifest(ctx, cfg, base, 0); err == nil {
				t.Fatal("RecoverSnapshotFromManifest() invalid candidate returned nil error")
			}
		})
	}
}

func TestRecoverSnapshotFromManifestBoundsHeadAndRejectsAmbiguity(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)

	t.Run("ignores manifest above committed head", func(t *testing.T) {
		store := objectstore.NewMemoryStore(t.Name())
		cfg := Config{VolumeID: "vol-bounded", WALPath: filepath.Join(t.TempDir(), "engine.wal"), ObjectStore: store}
		materializer := snapshotMaterializer(cfg)
		committed := recoveryManifest(t, cfg.VolumeID, 4, base, "committed")
		orphan := recoveryManifest(t, cfg.VolumeID, 6, base.Add(time.Second), "orphaned!")
		for _, manifest := range []*Manifest{committed, orphan} {
			if err := materializer.putJSON(ctx, manifestKey(manifest.ManifestSeq), manifest); err != nil {
				t.Fatalf("put manifest %d error = %v", manifest.ManifestSeq, err)
			}
		}
		if err := materializer.putJSON(ctx, manifestLatestKey, committed); err != nil {
			t.Fatalf("put recovery head error = %v", err)
		}
		state, manifest, err := RecoverSnapshotFromManifest(ctx, cfg, base.Add(2*time.Second), StateStorageBytes(committed.State))
		if err != nil {
			t.Fatalf("RecoverSnapshotFromManifest() error = %v", err)
		}
		if manifest.ManifestSeq != committed.ManifestSeq {
			t.Fatalf("manifest seq = %d, want %d", manifest.ManifestSeq, committed.ManifestSeq)
		}
		assertSnapshotFilePayload(t, state, "state.txt", "committed")
	})

	t.Run("rejects multiple size matches", func(t *testing.T) {
		store := objectstore.NewMemoryStore(t.Name())
		cfg := Config{VolumeID: "vol-ambiguous", WALPath: filepath.Join(t.TempDir(), "engine.wal"), ObjectStore: store}
		materializer := snapshotMaterializer(cfg)
		first := recoveryManifest(t, cfg.VolumeID, 2, base, "first")
		second := recoveryManifest(t, cfg.VolumeID, 4, base.Add(time.Second), "other")
		for _, manifest := range []*Manifest{first, second} {
			if err := materializer.putJSON(ctx, manifestKey(manifest.ManifestSeq), manifest); err != nil {
				t.Fatalf("put manifest %d error = %v", manifest.ManifestSeq, err)
			}
		}
		if err := materializer.putJSON(ctx, manifestLatestKey, second); err != nil {
			t.Fatalf("put recovery head error = %v", err)
		}
		_, _, err := RecoverSnapshotFromManifest(ctx, cfg, base.Add(2*time.Second), StateStorageBytes(first.State))
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("RecoverSnapshotFromManifest() error = %v, want ErrInvalidInput", err)
		}
	})

	t.Run("rejects committed head changes", func(t *testing.T) {
		store := objectstore.NewMemoryStore(t.Name())
		committed := recoveryManifest(t, "vol-head-change", 2, base, "committed")
		advanced := recoveryManifest(t, "vol-head-change", 4, base.Add(time.Second), "advanced!")
		heads := &changingRecoveryHeadStore{heads: []*CommittedHead{
			recoveryHeadForManifest(committed),
			recoveryHeadForManifest(advanced),
		}}
		cfg := Config{
			VolumeID: "vol-head-change", WALPath: filepath.Join(t.TempDir(), "engine.wal"),
			ObjectStore: store, HeadStore: heads,
		}
		if err := snapshotMaterializer(cfg).putJSON(ctx, manifestKey(committed.ManifestSeq), committed); err != nil {
			t.Fatalf("put committed manifest error = %v", err)
		}
		_, _, err := RecoverSnapshotFromManifest(ctx, cfg, base.Add(2*time.Second), StateStorageBytes(committed.State))
		if !errors.Is(err, ErrCommittedHeadConflict) {
			t.Fatalf("RecoverSnapshotFromManifest() error = %v, want ErrCommittedHeadConflict", err)
		}
	})
}

func TestSnapshotIDMustBePathSafe(t *testing.T) {
	cfg := Config{
		VolumeID:    "vol-safe-id",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: objectstore.NewMemoryStore(t.Name()),
	}
	state := snapshotStateWithFile(t, "state.txt", "payload")
	for _, snapshotID := range []string{"../escape", "nested/id", `nested\id`, " padded "} {
		t.Run(snapshotID, func(t *testing.T) {
			if err := PersistSnapshot(context.Background(), cfg, snapshotID, state); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("PersistSnapshot(%q) error = %v, want ErrInvalidInput", snapshotID, err)
			}
		})
	}
}

func TestEngineRestoreStateDoesNotMutateInput(t *testing.T) {
	ctx := context.Background()
	engine, err := Open(ctx, Config{
		VolumeID: "vol-restore-state",
		WALPath:  filepath.Join(t.TempDir(), "engine.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	current, err := engine.CreateFile(RootInode, "current.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(current.Inode, 0, []byte("current")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	state := snapshotStateWithFile(t, "restored.txt", "restored")
	originalNextSeq := state.NextSeq

	if err := engine.RestoreState(state); err != nil {
		t.Fatalf("RestoreState() error = %v", err)
	}
	if state.NextSeq != originalNextSeq {
		t.Fatalf("RestoreState() mutated input next seq = %d, want %d", state.NextSeq, originalNextSeq)
	}
	if _, err := engine.Lookup(RootInode, "current.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(current.txt) error = %v, want ErrNotFound", err)
	}
	restored, err := engine.Lookup(RootInode, "restored.txt")
	if err != nil {
		t.Fatalf("Lookup(restored.txt) error = %v", err)
	}
	payload, err := engine.Read(restored.Inode, 0, restored.Size)
	if err != nil {
		t.Fatalf("Read(restored.txt) error = %v", err)
	}
	if !bytes.Equal(payload, []byte("restored")) {
		t.Fatalf("restored payload = %q, want restored", payload)
	}
}

func snapshotStateWithFile(t *testing.T, name, payload string) *SnapshotState {
	t.Helper()
	engine, err := Open(context.Background(), Config{
		VolumeID: "state-builder",
		WALPath:  filepath.Join(t.TempDir(), "engine.wal"),
	})
	if err != nil {
		t.Fatalf("Open(state builder) error = %v", err)
	}
	defer engine.Close()
	node, err := engine.CreateFile(RootInode, name, 0o644)
	if err != nil {
		t.Fatalf("CreateFile(%q) error = %v", name, err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte(payload)); err != nil {
		t.Fatalf("Write(%q) error = %v", name, err)
	}
	return engine.SnapshotState()
}

func recoveryManifest(t *testing.T, volumeID string, sequence uint64, createdAt time.Time, payload string) *Manifest {
	t.Helper()
	state := snapshotStateWithFile(t, "state.txt", payload)
	state.NextSeq = sequence + 1
	return &Manifest{
		Version:       1,
		VolumeID:      volumeID,
		ManifestSeq:   sequence,
		CheckpointSeq: sequence,
		CreatedAt:     createdAt,
		State:         state,
	}
}

func recoveryHeadForManifest(manifest *Manifest) *CommittedHead {
	return &CommittedHead{
		VolumeID:      manifest.VolumeID,
		ManifestSeq:   manifest.ManifestSeq,
		CheckpointSeq: manifest.CheckpointSeq,
		ManifestKey:   manifestKey(manifest.ManifestSeq),
		UpdatedAt:     manifest.CreatedAt,
	}
}

type changingRecoveryHeadStore struct {
	heads []*CommittedHead
	loads int
}

func (s *changingRecoveryHeadStore) LoadCommittedHead(context.Context, string) (*CommittedHead, error) {
	if len(s.heads) == 0 {
		return nil, ErrCommittedHeadNotFound
	}
	index := s.loads
	if index >= len(s.heads) {
		index = len(s.heads) - 1
	}
	s.loads++
	copy := *s.heads[index]
	return &copy, nil
}

func (s *changingRecoveryHeadStore) CompareAndSwapCommittedHead(context.Context, string, uint64, *CommittedHead) error {
	return nil
}

func assertSnapshotFilePayload(t *testing.T, state *SnapshotState, name, want string) {
	t.Helper()
	node, err := state.Lookup(RootInode, name)
	if err != nil {
		t.Fatalf("snapshot Lookup(%q) error = %v", name, err)
	}
	if got := string(state.Data[node.Inode]); got != want {
		t.Fatalf("snapshot payload for %q = %q, want %q", name, got, want)
	}
}

var errSnapshotPutFailed = errors.New("snapshot put failed")

type snapshotPutFailingStore struct {
	objectstore.Store
}

func (s *snapshotPutFailingStore) Put(_ string, _ io.Reader) error {
	return errSnapshotPutFailed
}
