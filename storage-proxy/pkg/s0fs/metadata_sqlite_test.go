package s0fs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteMetadataEngineRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		VolumeID:           "vol-indexed",
		WALPath:            filepath.Join(dir, "engine.wal"),
		MetadataPath:       filepath.Join(dir, "metadata.sqlite"),
		MetadataCacheBytes: 1 << 20,
		StateFormatVersion: StateFormatV2,
	}
	engine, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	directory, err := engine.Mkdir(RootInode, "dir", 0o755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	for index := range 5 {
		name := fmt.Sprintf("file-%d", index)
		node, err := engine.CreateFile(directory.Inode, name, 0o644)
		if err != nil {
			t.Fatalf("CreateFile(%s) error = %v", name, err)
		}
		if _, err := engine.Write(node.Inode, 0, []byte(name)); err != nil {
			t.Fatalf("Write(%s) error = %v", name, err)
		}
	}
	if !engine.metadata.NeedsMaterialization() {
		t.Fatal("NeedsMaterialization() = false with inline file data")
	}
	page, eof, err := engine.ReadDirPage(directory.Inode, 1, 2)
	if err != nil {
		t.Fatalf("ReadDirPage() error = %v", err)
	}
	if eof || len(page) != 2 || page[0].Name != "file-1" || page[1].Name != "file-2" {
		t.Fatalf("ReadDirPage() = %+v, eof %v", page, eof)
	}
	if path, ok := engine.Path(directory.Inode); !ok || path != "/dir" {
		t.Fatalf("Path() = %q, %v", path, ok)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer reopened.Close()
	directory, err = reopened.Lookup(RootInode, "dir")
	if err != nil {
		t.Fatalf("Lookup(dir) error = %v", err)
	}
	node, err := reopened.Lookup(directory.Inode, "file-4")
	if err != nil {
		t.Fatalf("Lookup(file-4) error = %v", err)
	}
	payload, err := reopened.Read(node.Inode, 0, 32)
	if err != nil || string(payload) != "file-4" {
		t.Fatalf("Read(file-4) = %q, %v", payload, err)
	}
}

func TestSQLiteMetadataMutationRollsBackAtomically(t *testing.T) {
	store, err := newSQLiteMetadataStore(context.Background(), filepath.Join(t.TempDir(), "metadata.sqlite"), sqliteMetadataFixture(0), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	injected := errors.New("injected mutation failure")
	err = store.ApplyMutation(func() error {
		store.PutNode(42, &Node{Inode: 42, Type: TypeFile, Mode: 0o644, Nlink: 1})
		store.PutChild(RootInode, "rolled-back", 42)
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("ApplyMutation() error = %v, want injected failure", err)
	}
	if _, ok := store.Node(42); ok {
		t.Fatal("rolled-back inode remains visible")
	}
	if _, ok := store.Child(RootInode, "rolled-back"); ok {
		t.Fatal("rolled-back directory entry remains visible")
	}
}

func TestSQLiteMetadataRebuildHonorsCancellation(t *testing.T) {
	state := sqliteMetadataFixture(10_000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newSQLiteMetadataStore(ctx, filepath.Join(t.TempDir(), "metadata.sqlite"), state, 1<<20)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newSQLiteMetadataStore() error = %v, want context.Canceled", err)
	}
}

func TestSQLiteMetadataStateV2RebuildHonorsMidStreamCancellation(t *testing.T) {
	state := sqliteMetadataFixture(20_000)
	payload, err := encodeStateV2("vol-cancel-v2", stateBlobAAD("vol-cancel-v2", "head"), state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil)
	if err != nil {
		t.Fatalf("encodeStateV2() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelingStateReader{reader: bytes.NewReader(payload), remaining: len(payload) / 2, cancel: cancel}
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	_, _, err = newSQLiteMetadataStoreFromStateV2(ctx, path, reader, "vol-cancel-v2", stateBlobAAD("vol-cancel-v2", "head"), StateV2Role_STATE_V2_ROLE_HEAD, nil, 1<<20)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newSQLiteMetadataStoreFromStateV2() error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(path + ".rebuild"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("metadata rebuild file remains after cancellation: %v", err)
	}
}

func TestSQLiteMetadataMemoryChargeDoesNotScaleWithNamespace(t *testing.T) {
	const cacheBytes = 2 << 20
	store, err := newSQLiteMetadataStore(context.Background(), filepath.Join(t.TempDir(), "metadata.sqlite"), sqliteMetadataFixture(20_000), cacheBytes)
	if err != nil {
		t.Fatalf("newSQLiteMetadataStore() error = %v", err)
	}
	defer store.Close()
	if got, want := store.EstimatedMemoryBytes(), int64(cacheBytes+1<<20); got != want {
		t.Fatalf("EstimatedMemoryBytes() = %d, want %d", got, want)
	}
	if got := store.NodeCount(); got != 20_001 {
		t.Fatalf("NodeCount() = %d, want 20001", got)
	}
	if store.NeedsMaterialization() {
		t.Fatal("NeedsMaterialization() = true for metadata-only namespace")
	}
	info, err := os.Stat(store.path)
	if err != nil {
		t.Fatalf("stat metadata index: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("metadata index mode = %o, want 600", got)
	}
}

func TestEngineMemoryReservationIncludesBoundedCaches(t *testing.T) {
	const metadataCacheBytes = 2 << 20
	want := int64(metadataCacheBytes + (1 << 20) + defaultSegmentCacheMaxBytes + (1 << 20))
	if got := EngineMemoryReservationBytes(metadataCacheBytes); got != want {
		t.Fatalf("EngineMemoryReservationBytes() = %d, want %d", got, want)
	}
}

func TestSQLiteMetadataPruneUnlinkedUsesBoundedPages(t *testing.T) {
	state := sqliteMetadataFixture(600)
	for inode, node := range state.Nodes {
		if inode != RootInode {
			node.Nlink = 0
		}
	}
	state.Children[RootInode] = map[string]uint64{}
	store, err := newSQLiteMetadataStore(context.Background(), filepath.Join(t.TempDir(), "metadata.sqlite"), state, 1<<20)
	if err != nil {
		t.Fatalf("newSQLiteMetadataStore() error = %v", err)
	}
	defer store.Close()
	if err := store.PruneUnlinked(context.Background(), map[uint64]struct{}{300: {}}); err != nil {
		t.Fatalf("PruneUnlinked() error = %v", err)
	}
	if err := store.Err(); err != nil {
		t.Fatalf("PruneUnlinked() error = %v", err)
	}
	if got, want := store.NodeCount(), 2; got != want {
		t.Fatalf("NodeCount() = %d, want %d", got, want)
	}
	if _, ok := store.Node(300); !ok {
		t.Fatal("PruneUnlinked() removed retained inode")
	}
}

func TestSQLiteMetadataPruneUnlinkedHonorsCancellation(t *testing.T) {
	state := sqliteMetadataFixture(10)
	store, err := newSQLiteMetadataStore(context.Background(), filepath.Join(t.TempDir(), "metadata.sqlite"), state, 1<<20)
	if err != nil {
		t.Fatalf("newSQLiteMetadataStore() error = %v", err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.PruneUnlinked(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("PruneUnlinked() error = %v, want context.Canceled", err)
	}
}

func TestEncryptedSQLiteMetadataDoesNotPersistPlaintext(t *testing.T) {
	dir := t.TempDir()
	const secretName = "S0FS_METADATA_SECRET_NAME_805"
	const secretPayload = "S0FS_METADATA_SECRET_PAYLOAD_805"
	cfg := Config{
		VolumeID: "vol-protected-index", WALPath: filepath.Join(dir, "engine.wal"),
		MetadataPath: filepath.Join(dir, "metadata.sqlite"), StateFormatVersion: StateFormatV2,
		Encryption: testEncryptionConfig(64 << 10),
	}
	engine, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, secretName, 0o600)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte(secretPayload)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	raw, err := os.ReadFile(cfg.MetadataPath)
	if err != nil {
		t.Fatalf("read metadata index: %v", err)
	}
	for _, secret := range []string{secretName, secretPayload} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("encrypted metadata index contains plaintext %q", secret)
		}
	}
	reopened, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(rebuilt) error = %v", err)
	}
	defer reopened.Close()
	node, err = reopened.Lookup(RootInode, secretName)
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	payload, err := reopened.Read(node.Inode, 0, uint64(len(secretPayload)))
	if err != nil || string(payload) != secretPayload {
		t.Fatalf("Read() = %q, %v", payload, err)
	}
}

func TestOpenStreamsStateV2DirectlyIntoSQLiteMetadata(t *testing.T) {
	dir := t.TempDir()
	state := sqliteMetadataFixture(2_000)
	cfg := Config{
		VolumeID: "vol-v2-index", WALPath: filepath.Join(dir, "engine.wal"),
		MetadataPath: filepath.Join(dir, "metadata.sqlite"), StateFormatVersion: StateFormatV2,
	}
	if err := saveSnapshotState(headStatePath(cfg.WALPath), cfg.VolumeID, "head", state, nil, StateFormatV2); err != nil {
		t.Fatalf("saveSnapshotState() error = %v", err)
	}
	engine, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	if _, ok := engine.metadata.(*sqliteMetadataStore); !ok {
		t.Fatalf("metadata store = %T, want *sqliteMetadataStore", engine.metadata)
	}
	if got := engine.metadata.NodeCount(); got != len(state.Nodes) {
		t.Fatalf("NodeCount() = %d, want %d", got, len(state.Nodes))
	}
}

func TestOpenMigratesEncryptedStateV1ToPagedV2(t *testing.T) {
	dir := t.TempDir()
	state := sqliteMetadataFixture(200)
	encryption := testEncryptionConfig(64 << 10)
	cfg := Config{
		VolumeID: "vol-encrypted-v1-migration", WALPath: filepath.Join(dir, "engine.wal"),
		MetadataPath: filepath.Join(dir, "metadata.sqlite"), StateFormatVersion: StateFormatV2, Encryption: encryption,
	}
	if err := saveSnapshotState(headStatePath(cfg.WALPath), cfg.VolumeID, "head", state, encryption, StateFormatV1); err != nil {
		t.Fatalf("saveSnapshotState(v1) error = %v", err)
	}
	engine, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	file, err := os.Open(headStatePath(cfg.WALPath))
	if err != nil {
		t.Fatalf("open migrated head error = %v", err)
	}
	defer file.Close()
	v2, err := hasStateV2Magic(bufio.NewReader(file))
	if err != nil || !v2 {
		t.Fatalf("migrated head v2 = %v, error = %v", v2, err)
	}
	if _, ok := engine.metadata.(*sqliteMetadataStore); !ok {
		t.Fatalf("metadata store = %T, want *sqliteMetadataStore", engine.metadata)
	}
}

func sqliteMetadataFixture(files int) *SnapshotState {
	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	state := &SnapshotState{
		NextSeq: uint64(files + 1), NextInode: uint64(files + 2),
		Nodes:    map[uint64]*Node{RootInode: {Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 1, Atime: now, Mtime: now, Ctime: now}},
		Children: map[uint64]map[string]uint64{RootInode: {}}, Data: map[uint64][]byte{},
		ColdFiles: map[uint64][]FileExtent{}, Segments: map[string]*Segment{},
	}
	for index := range files {
		inode := uint64(index + 2)
		name := fmt.Sprintf("file-%08d", index)
		state.Nodes[inode] = &Node{Inode: inode, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: uint64(len(name)), Atime: now, Mtime: now, Ctime: now}
		state.Children[RootInode][name] = inode
	}
	return state
}

type cancelingStateReader struct {
	reader    *bytes.Reader
	remaining int
	cancel    context.CancelFunc
}

func (r *cancelingStateReader) Read(payload []byte) (int, error) {
	if r.remaining <= 0 {
		r.cancel()
	}
	if len(payload) > r.remaining && r.remaining > 0 {
		payload = payload[:r.remaining]
	}
	n, err := r.reader.Read(payload)
	r.remaining -= n
	if r.remaining <= 0 {
		r.cancel()
	}
	return n, err
}
