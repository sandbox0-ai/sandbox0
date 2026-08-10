package s0fs

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"google.golang.org/protobuf/proto"
)

func TestStateV2RoundTripDeterministic(t *testing.T) {
	state := richStateV2Fixture()
	metadata := stateV2Metadata{
		Role:          StateV2Role_STATE_V2_ROLE_MANIFEST,
		ManifestSeq:   state.NextSeq - 1,
		CheckpointSeq: state.NextSeq - 1,
		CreatedAt:     time.Date(2026, 8, 2, 3, 4, 5, 6, time.UTC),
	}
	binding := stateBlobAAD("vol-v2", "object:manifests/00000000000000000010.json")
	first, err := encodeStateV2("vol-v2", binding, state, metadata, nil)
	if err != nil {
		t.Fatalf("encodeStateV2() error = %v", err)
	}
	second, err := encodeStateV2("vol-v2", binding, state, metadata, nil)
	if err != nil {
		t.Fatalf("encodeStateV2(second) error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("unencrypted state v2 encoding is not deterministic")
	}
	result, err := decodeStateV2(bytes.NewReader(first), "vol-v2", binding, metadata.Role, nil)
	if err != nil {
		t.Fatalf("decodeStateV2() error = %v", err)
	}
	if result.Metadata != metadata {
		t.Fatalf("metadata = %+v, want %+v", result.Metadata, metadata)
	}
	if !reflect.DeepEqual(result.State, state) {
		t.Fatalf("decoded state mismatch\n got: %#v\nwant: %#v", result.State, state)
	}
}

func TestMetadataStateV2StreamingWriterMatchesEagerEncoding(t *testing.T) {
	state := richStateV2Fixture()
	metadata := stateV2Metadata{
		Role:          StateV2Role_STATE_V2_ROLE_MANIFEST,
		ManifestSeq:   state.NextSeq - 1,
		CheckpointSeq: state.NextSeq - 1,
		CreatedAt:     time.Date(2026, 8, 2, 3, 4, 5, 6, time.UTC),
	}
	binding := stateBlobAAD("vol-stream-v2", "object:manifests/00000000000000000010.json")
	want, err := encodeStateV2("vol-stream-v2", binding, state, metadata, nil)
	if err != nil {
		t.Fatalf("encodeStateV2() error = %v", err)
	}
	var got bytes.Buffer
	if err := writeMetadataStateV2(context.Background(), &got, "vol-stream-v2", binding, newEagerMetadataStore(state), state.NextSeq, state.NextInode, metadata, nil); err != nil {
		t.Fatalf("writeMetadataStateV2() error = %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatal("streaming state v2 encoding differs from eager encoding")
	}
}

func TestMetadataStateV2StreamingWriterReadsSQLiteIndex(t *testing.T) {
	state := richStateV2Fixture()
	store, err := newSQLiteMetadataStore(context.Background(), filepath.Join(t.TempDir(), "metadata.sqlite"), state, 1<<20)
	if err != nil {
		t.Fatalf("newSQLiteMetadataStore() error = %v", err)
	}
	defer store.Close()
	binding := stateBlobAAD("vol-stream-sqlite", "head")
	var payload bytes.Buffer
	if err := writeMetadataStateV2(context.Background(), &payload, "vol-stream-sqlite", binding, store, state.NextSeq, state.NextInode, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil); err != nil {
		t.Fatalf("writeMetadataStateV2() error = %v", err)
	}
	decoded, err := decodeStateV2(bytes.NewReader(payload.Bytes()), "vol-stream-sqlite", binding, StateV2Role_STATE_V2_ROLE_HEAD, nil)
	if err != nil {
		t.Fatalf("decodeStateV2() error = %v", err)
	}
	if !reflect.DeepEqual(decoded.State, state) {
		t.Fatal("SQLite streaming state v2 round trip mismatch")
	}
}

func TestStateV2EncryptedChunksBindRoleAndObject(t *testing.T) {
	state := richStateV2Fixture()
	encryption := testEncryptionConfig(64 << 10)
	binding := stateBlobAAD("vol-encrypted-v2", "object:snapshots/snap-1.json")
	payload, err := encodeStateV2("vol-encrypted-v2", binding, state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_SNAPSHOT}, encryption)
	if err != nil {
		t.Fatalf("encodeStateV2() error = %v", err)
	}
	for _, secret := range [][]byte{[]byte("secret-name.txt"), []byte("inline-secret-payload")} {
		if bytes.Contains(payload, secret) {
			t.Fatalf("encrypted state v2 contains plaintext %q", secret)
		}
	}
	result, err := decodeStateV2(bytes.NewReader(payload), "vol-encrypted-v2", binding, StateV2Role_STATE_V2_ROLE_SNAPSHOT, encryption)
	if err != nil {
		t.Fatalf("decodeStateV2() error = %v", err)
	}
	if !reflect.DeepEqual(result.State, state) {
		t.Fatal("encrypted state v2 round trip mismatch")
	}
	if _, err := decodeStateV2(bytes.NewReader(payload), "vol-encrypted-v2", stateBlobAAD("vol-encrypted-v2", "object:snapshots/other.json"), StateV2Role_STATE_V2_ROLE_SNAPSHOT, encryption); err == nil {
		t.Fatal("decodeStateV2() accepted a different object binding")
	}
	if _, err := decodeStateV2(bytes.NewReader(payload), "vol-encrypted-v2", binding, StateV2Role_STATE_V2_ROLE_HEAD, encryption); err == nil {
		t.Fatal("decodeStateV2() accepted a different role")
	}
	if _, err := decodeStateV2(bytes.NewReader(payload), "other-volume", binding, StateV2Role_STATE_V2_ROLE_SNAPSHOT, encryption); err == nil {
		t.Fatal("decodeStateV2() accepted a different volume")
	}
}

func TestStateV2DetectsChunkCorruption(t *testing.T) {
	state := richStateV2Fixture()
	binding := stateBlobAAD("vol-corrupt-v2", "head")
	payload, err := encodeStateV2("vol-corrupt-v2", binding, state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil)
	if err != nil {
		t.Fatalf("encodeStateV2() error = %v", err)
	}
	payload[len(payload)-1] ^= 0xff
	if _, err := decodeStateV2(bytes.NewReader(payload), "vol-corrupt-v2", binding, StateV2Role_STATE_V2_ROLE_HEAD, nil); err == nil {
		t.Fatal("decodeStateV2() accepted corrupted payload")
	}
}

func TestStateV2RejectsMismatchedMapIdentity(t *testing.T) {
	state := richStateV2Fixture()
	state.Nodes[2].Inode = 99
	if _, err := encodeStateV2("vol-mismatch", stateBlobAAD("vol-mismatch", "head"), state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil); err == nil {
		t.Fatal("encodeStateV2() accepted a node whose map key and inode differ")
	}

	state = richStateV2Fixture()
	state.Segments["segment-a"].ID = "segment-b"
	if _, err := encodeStateV2("vol-mismatch", stateBlobAAD("vol-mismatch", "head"), state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil); err == nil {
		t.Fatal("encodeStateV2() accepted a segment whose map key and id differ")
	}
}

func TestStateV2RejectsDuplicateEmptyMapRecords(t *testing.T) {
	tests := []struct {
		name       string
		descriptor *StateV2ChunkDescriptor
		chunk      *StateV2Chunk
	}{
		{
			name:       "directory",
			descriptor: &StateV2ChunkDescriptor{Kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_DIRECTORIES, RecordCount: 1, FirstInode: 2, LastInode: 2},
			chunk:      &StateV2Chunk{Directories: []*StateV2Directory{{ParentInode: 2}}},
		},
		{
			name:       "data",
			descriptor: &StateV2ChunkDescriptor{Kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_DATA, RecordCount: 1, FirstInode: 2, LastInode: 2},
			chunk:      &StateV2Chunk{Data: []*StateV2Data{{Inode: 2}}},
		},
		{
			name:       "cold file",
			descriptor: &StateV2ChunkDescriptor{Kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_COLD_FILES, RecordCount: 1, FirstInode: 2, LastInode: 2},
			chunk:      &StateV2Chunk{ColdFiles: []*StateV2ColdFile{{Inode: 2}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &SnapshotState{
				Nodes:     map[uint64]*Node{},
				Children:  map[uint64]map[string]uint64{},
				Data:      map[uint64][]byte{},
				ColdFiles: map[uint64][]FileExtent{},
				Segments:  map[string]*Segment{},
			}
			if err := applyStateV2Chunk(state, test.descriptor, test.chunk); err != nil {
				t.Fatalf("first applyStateV2Chunk() error = %v", err)
			}
			if err := applyStateV2Chunk(state, test.descriptor, test.chunk); err == nil {
				t.Fatal("second applyStateV2Chunk() accepted a duplicate empty record")
			}
		})
	}
}

func TestStateV2RecordCapacityDoesNotTrustUnboundedHeaderCounts(t *testing.T) {
	descriptors := []*StateV2ChunkDescriptor{{
		Kind:        StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES,
		RecordCount: ^uint64(0),
	}}
	if got := stateV2RecordCapacity(descriptors, StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES); got != 0 {
		t.Fatalf("stateV2RecordCapacity() = %d, want 0 for an unbounded count", got)
	}
}

func TestLocalStateDualReadsV1AndV2(t *testing.T) {
	state := richStateV2Fixture()
	for _, format := range []int{StateFormatV1, StateFormatV2} {
		t.Run(fmt.Sprintf("v%d", format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "head.json")
			if err := saveSnapshotState(path, "vol-local-v2", "head", state, nil, format); err != nil {
				t.Fatalf("saveSnapshotState() error = %v", err)
			}
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := bytes.HasPrefix(payload, stateV2Magic[:]); got != (format == StateFormatV2) {
				t.Fatalf("v2 magic = %v for format %d", got, format)
			}
			loaded, err := loadSnapshotState(path, "vol-local-v2", "head", nil)
			if err != nil {
				t.Fatalf("loadSnapshotState() error = %v", err)
			}
			if !reflect.DeepEqual(loaded, state) {
				t.Fatal("local state round trip mismatch")
			}
		})
	}
}

func TestStateFormatConfigRejectsUnsupportedWriterVersion(t *testing.T) {
	cfg := Config{VolumeID: "vol-invalid-format", WALPath: filepath.Join(t.TempDir(), "engine.wal"), StateFormatVersion: 3}
	if _, err := Open(context.Background(), cfg); err == nil {
		t.Fatal("Open() accepted unsupported state writer format")
	}
	if err := PersistSnapshot(context.Background(), cfg, "snap-invalid", richStateV2Fixture()); err == nil {
		t.Fatal("PersistSnapshot() accepted unsupported state writer format")
	}
}

func TestMaterializerDualReadsAndWritesStateFormats(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	heads := newMemoryHeadStore()
	state := richStateV2Fixture()
	state.NextSeq = 10

	v1 := NewMaterializer("vol-format", store, heads)
	manifestV1, err := v1.Materialize(ctx, state, 0)
	if err != nil {
		t.Fatalf("Materialize(v1) error = %v", err)
	}
	if manifestV1.Version != StateFormatV1 {
		t.Fatalf("v1 manifest version = %d", manifestV1.Version)
	}

	loadedV1, err := v1.LoadLatestManifest(ctx)
	if err != nil {
		t.Fatalf("LoadLatestManifest(v1) error = %v", err)
	}
	if loadedV1.Version != StateFormatV1 {
		t.Fatalf("loaded v1 version = %d", loadedV1.Version)
	}

	state.NextSeq = manifestV1.ManifestSeq + 2
	v2 := NewMaterializer("vol-format", store, heads)
	v2.SetStateFormatVersion(StateFormatV2)
	manifestV2, err := v2.Materialize(ctx, state, manifestV1.ManifestSeq)
	if err != nil {
		t.Fatalf("Materialize(v2) error = %v", err)
	}
	if manifestV2.Version != StateFormatV2 {
		t.Fatalf("v2 manifest version = %d", manifestV2.Version)
	}
	reader, err := store.Get(manifestKey(manifestV2.ManifestSeq), 0, int64(len(stateV2Magic)))
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, len(stateV2Magic))
	_, _ = reader.Read(prefix)
	_ = reader.Close()
	if !bytes.Equal(prefix, stateV2Magic[:]) {
		t.Fatal("v2 manifest does not use state v2 container")
	}

	// Reader compatibility is independent from the writer feature gate.
	legacyWriterModeReader := NewMaterializer("vol-format", store, heads)
	loadedV2, err := legacyWriterModeReader.LoadLatestManifest(ctx)
	if err != nil {
		t.Fatalf("LoadLatestManifest(v2) error = %v", err)
	}
	if loadedV2.Version != StateFormatV2 || loadedV2.ManifestSeq != manifestV2.ManifestSeq {
		t.Fatalf("loaded v2 manifest = %+v", loadedV2)
	}
}

func TestStateV2EncryptedSnapshotObjectLoadsWithV1WriterMode(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	state := snapshotStateWithFile(t, "secret-snapshot.txt", "secret snapshot payload")
	writeCfg := Config{
		VolumeID:           "vol-snapshot-v2",
		WALPath:            filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore:        store,
		Encryption:         testEncryptionConfig(64 << 10),
		StateFormatVersion: StateFormatV2,
	}
	if err := PersistSnapshot(ctx, writeCfg, "snap-v2", state); err != nil {
		t.Fatalf("PersistSnapshot(v2) error = %v", err)
	}
	reader, err := store.Get(snapshotObjectKey("snap-v2"), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(payload, stateV2Magic[:]) {
		t.Fatal("snapshot object does not use the state v2 container")
	}
	for _, secret := range [][]byte{[]byte("secret-snapshot.txt"), []byte("secret snapshot payload")} {
		if bytes.Contains(payload, secret) {
			t.Fatalf("encrypted v2 snapshot contains plaintext %q", secret)
		}
	}

	readCfg := writeCfg
	readCfg.StateFormatVersion = StateFormatV1
	readCfg.WALPath = filepath.Join(t.TempDir(), "engine.wal")
	loaded, err := LoadSnapshot(ctx, readCfg, "snap-v2")
	if err != nil {
		t.Fatalf("LoadSnapshot(v1 writer mode) error = %v", err)
	}
	assertSnapshotFilePayload(t, loaded, "secret-snapshot.txt", "secret snapshot payload")
}

func TestOpenReusesCommittedLocalHeadWithoutRemoteManifestGet(t *testing.T) {
	ctx := context.Background()
	store := &recordingStore{Store: objectstore.NewMemoryStore(t.Name())}
	heads := newMemoryHeadStore()
	cfg := Config{
		VolumeID:           "vol-local-reuse",
		WALPath:            filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore:        store,
		HeadStore:          heads,
		StateFormatVersion: StateFormatV2,
	}
	engine, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open(first) error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "reuse.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}
	store.mu.Lock()
	store.gets = nil
	store.mu.Unlock()

	reopened, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open(second) error = %v", err)
	}
	defer reopened.Close()
	if calls := store.calls(); len(calls) != 0 {
		t.Fatalf("remote object GET calls = %#v, want none for current local head", calls)
	}
	entries, err := reopened.ReadDir(RootInode)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "reuse.txt" {
		t.Fatalf("reopened directory entries = %#v", entries)
	}
}

func TestOpenRefreshesStaleLocalHeadFromCommittedManifest(t *testing.T) {
	ctx := context.Background()
	store := &recordingStore{Store: objectstore.NewMemoryStore(t.Name())}
	heads := newMemoryHeadStore()
	localCfg := Config{
		VolumeID:           "vol-local-stale",
		WALPath:            filepath.Join(t.TempDir(), "engine.wal"),
		MetadataPath:       filepath.Join(t.TempDir(), "metadata.sqlite"),
		ObjectStore:        store,
		HeadStore:          heads,
		StateFormatVersion: StateFormatV2,
	}
	local, err := Open(ctx, localCfg)
	if err != nil {
		t.Fatal(err)
	}
	first, err := local.CreateFile(RootInode, "first.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Write(first.Inode, 0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := local.SyncMaterialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}

	remoteCfg := localCfg
	remoteCfg.WALPath = filepath.Join(t.TempDir(), "engine.wal")
	remoteCfg.MetadataPath = filepath.Join(t.TempDir(), "metadata.sqlite")
	remote, err := Open(ctx, remoteCfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := remote.metadata.(*sqliteMetadataStore); !ok {
		t.Fatalf("remote metadata store = %T, want *sqliteMetadataStore", remote.metadata)
	}
	second, err := remote.CreateFile(RootInode, "second.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Write(second.Inode, 0, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.SyncMaterialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
	store.clearGets()

	reopened, err := Open(ctx, localCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(store.snapshotGets()) == 0 {
		t.Fatal("stale local head did not load the newer committed manifest")
	}
	if _, err := reopened.Lookup(RootInode, "second.txt"); err != nil {
		t.Fatalf("newer committed file is missing: %v", err)
	}
}

func TestStateV2CompressesLargeMetadata(t *testing.T) {
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	state := &SnapshotState{
		NextSeq:   2,
		NextInode: 20_002,
		Nodes:     make(map[uint64]*Node, 20_001),
		Children:  map[uint64]map[string]uint64{RootInode: {}},
		Data:      map[uint64][]byte{},
		ColdFiles: map[uint64][]FileExtent{},
		Segments:  map[string]*Segment{},
	}
	state.Nodes[RootInode] = &Node{Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 1, Atime: now, Mtime: now, Ctime: now}
	for i := uint64(2); i < 20_002; i++ {
		name := fmt.Sprintf("generated-source-file-%08d.go", i)
		state.Nodes[i] = &Node{Inode: i, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 1234, Atime: now, Mtime: now, Ctime: now}
		state.Children[RootInode][name] = i
	}
	legacy, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := encodeStateV2("vol-large", stateBlobAAD("vol-large", "head"), state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(v2) >= len(legacy)/3 {
		t.Fatalf("state v2 size = %d, legacy JSON = %d; expected at least 3x reduction", len(v2), len(legacy))
	}
}

func TestStateV2GroupsSmallInlineDataRecords(t *testing.T) {
	state := &SnapshotState{
		NextSeq:   2,
		NextInode: 10_002,
		Nodes:     map[uint64]*Node{},
		Children:  map[uint64]map[string]uint64{},
		Data:      make(map[uint64][]byte, 10_000),
		ColdFiles: map[uint64][]FileExtent{},
		Segments:  map[string]*Segment{},
	}
	for inode := uint64(2); inode < 10_002; inode++ {
		state.Data[inode] = []byte("payload")
	}
	payload, err := encodeStateV2("vol-small-data", stateBlobAAD("vol-small-data", "head"), state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil)
	if err != nil {
		t.Fatal(err)
	}
	headerSize := binary.LittleEndian.Uint32(payload[len(stateV2Magic) : len(stateV2Magic)+4])
	var header StateV2Header
	if err := proto.Unmarshal(payload[len(stateV2Magic)+4:len(stateV2Magic)+4+int(headerSize)], &header); err != nil {
		t.Fatal(err)
	}
	dataChunks := 0
	for _, descriptor := range header.Chunks {
		if descriptor.Kind == StateV2ChunkKind_STATE_V2_CHUNK_KIND_DATA {
			dataChunks++
		}
	}
	if dataChunks > 2 {
		t.Fatalf("small inline data chunks = %d, want at most 2", dataChunks)
	}
	decoded, err := decodeStateV2(bytes.NewReader(payload), "vol-small-data", stateBlobAAD("vol-small-data", "head"), StateV2Role_STATE_V2_ROLE_HEAD, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.State.Data, state.Data) {
		t.Fatal("grouped inline data did not round trip")
	}
}

func BenchmarkStateV2DecodeLargeMetadata(b *testing.B) {
	state := benchmarkStateV2Fixture(100_000)
	payload, err := encodeStateV2("vol-benchmark", stateBlobAAD("vol-benchmark", "head"), state, stateV2Metadata{Role: StateV2Role_STATE_V2_ROLE_HEAD}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportMetric(float64(len(payload)), "encoded-bytes")
	for i := 0; i < b.N; i++ {
		if _, err := decodeStateV2(bytes.NewReader(payload), "vol-benchmark", stateBlobAAD("vol-benchmark", "head"), StateV2Role_STATE_V2_ROLE_HEAD, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStateV1JSONDecodeLargeMetadata(b *testing.B) {
	state := benchmarkStateV2Fixture(100_000)
	payload, err := json.Marshal(state)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportMetric(float64(len(payload)), "encoded-bytes")
	for i := 0; i < b.N; i++ {
		var decoded SnapshotState
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func richStateV2Fixture() *SnapshotState {
	now := time.Date(2026, 8, 2, 1, 2, 3, 4, time.UTC)
	return &SnapshotState{
		NextSeq:   11,
		NextInode: 6,
		Nodes: map[uint64]*Node{
			1: {Inode: 1, Type: TypeDirectory, Mode: 0o755, Nlink: 2, Atime: now, Mtime: now, Ctime: now},
			2: {Inode: 2, Type: TypeFile, Mode: 0o640, UID: 1000, GID: 1000, Nlink: 1, Size: 21, Atime: now, Mtime: now.Add(time.Second), Ctime: now.Add(2 * time.Second)},
			3: {Inode: 3, Type: TypeDirectory, Mode: 0o700, Nlink: 1, Atime: now, Mtime: now, Ctime: now},
			4: {Inode: 4, Type: TypeFile, Mode: 0o600, Nlink: 1, Size: 12, Atime: now, Mtime: now, Ctime: now},
			5: {Inode: 5, Type: TypeSymlink, Mode: 0o777, Nlink: 1, Size: 15, Target: "secret-name.txt", Atime: now, Mtime: now, Ctime: now},
		},
		Children: map[uint64]map[string]uint64{
			1: {"secret-name.txt": 2, "cold": 3, "link": 5},
			3: {"data.bin": 4},
		},
		Data: map[uint64][]byte{
			2: []byte("inline-secret-payload"),
		},
		ColdFiles: map[uint64][]FileExtent{
			4: {{SegmentID: "segment-a", Offset: 1, Length: 12}},
		},
		Segments: map[string]*Segment{
			"segment-a": {
				ID:       "segment-a",
				VolumeID: "source-volume",
				Key:      "segments/a.bin",
				Length:   13,
				SHA256:   "abcd",
				Encryption: &SegmentEncryption{
					Version:        1,
					Algorithm:      objectstore.EncryptionAlgoAES256GCMRSA,
					ChunkSize:      1024,
					PlaintextSize:  13,
					CiphertextSize: 29,
					WrappedKey:     []byte("wrapped"),
					NoncePrefix:    []byte("nonce"),
				},
			},
		},
	}
}

func benchmarkStateV2Fixture(files int) *SnapshotState {
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	state := &SnapshotState{
		NextSeq:   2,
		NextInode: uint64(files + 2),
		Nodes:     make(map[uint64]*Node, files+1),
		Children:  map[uint64]map[string]uint64{RootInode: {}},
		Data:      map[uint64][]byte{},
		ColdFiles: map[uint64][]FileExtent{},
		Segments:  map[string]*Segment{},
	}
	state.Nodes[RootInode] = &Node{Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 1, Atime: now, Mtime: now, Ctime: now}
	for i := 0; i < files; i++ {
		inode := uint64(i + 2)
		state.Nodes[inode] = &Node{Inode: inode, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 128, Atime: now, Mtime: now, Ctime: now}
		state.Children[RootInode][fmt.Sprintf("file-%08d.txt", i)] = inode
	}
	return state
}
