package s0fs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestRequestReplaySurvivesCrashAfterApplyBeforeReply(t *testing.T) {
	if walPath := os.Getenv("S0FS_REQUEST_CRASH_WAL"); walPath != "" {
		engine, err := Open(context.Background(), Config{VolumeID: "vol-crash", WALPath: walPath})
		if err != nil {
			os.Exit(2)
		}
		_, _, err = engine.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 41}, RequestMutation{
			Kind: RequestCreate, Parent: RootInode, Name: "once.txt", Type: TypeFile, Mode: 0o640,
		})
		if err != nil {
			os.Exit(3)
		}
		// Deliberately bypass Close and the reply acknowledgement.
		os.Exit(0)
	}

	walPath := filepath.Join(t.TempDir(), "engine.wal")
	cmd := exec.Command(os.Args[0], "-test.run=^TestRequestReplaySurvivesCrashAfterApplyBeforeReply$")
	cmd.Env = append(os.Environ(), "S0FS_REQUEST_CRASH_WAL="+walPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper error = %v, output = %s", err, output)
	}

	engine, err := Open(context.Background(), Config{VolumeID: "vol-crash", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(recovered) error = %v", err)
	}
	defer engine.Close()
	result, replayed, err := engine.ApplyRecoverableRequestMutation(RequestKey{Scope: "shard-0", Unique: 41}, RequestMutation{
		Kind: RequestCreate, Parent: RootInode, Name: "once.txt", Type: TypeFile, Mode: 0o640,
	}, true)
	if err != nil || !replayed {
		t.Fatalf("ApplyRequestMutation(resend) result=%+v replayed=%v error=%v", result, replayed, err)
	}
	entries, err := engine.ReadDir(RootInode)
	if err != nil || len(entries) != 1 || entries[0].Name != "once.txt" {
		t.Fatalf("ReadDir() entries=%+v error=%v, want one once.txt", entries, err)
	}
}

func TestRequestReplayFailedAckPersistenceStopsLaterMutations(t *testing.T) {
	engine := openRequestReplayTestEngine(t, Config{RequestReplayCapacity: 2})
	key := RequestKey{Scope: "shard-0", Unique: 1}
	if _, _, err := engine.ApplyRequestMutation(key, RequestMutation{
		Kind: RequestCreate, Parent: RootInode, Name: "first", Type: TypeFile, Mode: 0o600,
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.wal.close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.AcknowledgeRequest(key); !errors.Is(err, ErrClosed) {
		t.Fatalf("AcknowledgeRequest() error=%v, want %v", err, ErrClosed)
	}
	if _, _, err := engine.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 2}, RequestMutation{
		Kind: RequestCreate, Parent: RootInode, Name: "must-not-exist", Type: TypeFile, Mode: 0o600,
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("ApplyRequestMutation() error=%v, want %v", err, ErrClosed)
	}
	if _, err := engine.Lookup(RootInode, "must-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup() error=%v, want %v", err, ErrNotFound)
	}
}

func TestWALResetKeepsSyncGenerationMonotonic(t *testing.T) {
	syncs := 0
	w, _, err := openWAL(filepath.Join(t.TempDir(), "engine.wal"), "vol-sync-generation", nil, func() {
		syncs++
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	payload, err := w.prepare(walRecord{Seq: 1, Op: "create", TimeUnix: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.appendPrepared(payload); err != nil {
		t.Fatal(err)
	}
	firstWait, err := w.beginSyncCurrent()
	if err != nil || firstWait == nil {
		t.Fatalf("beginSyncCurrent(first) wait=%v error=%v", firstWait != nil, err)
	}
	delayedWait, err := w.beginSyncCurrent()
	if err != nil || delayedWait == nil {
		t.Fatalf("beginSyncCurrent(delayed) wait=%v error=%v", delayedWait != nil, err)
	}
	if err := firstWait(); err != nil {
		t.Fatal(err)
	}
	if err := w.reset(); err != nil {
		t.Fatal(err)
	}
	if err := delayedWait(); err != nil {
		t.Fatal(err)
	}
	if syncs != 1 {
		t.Fatalf("syncs after delayed waiter=%d, want 1", syncs)
	}
	if err := w.appendPrepared(payload); err != nil {
		t.Fatal(err)
	}
	newWait, err := w.beginSyncCurrent()
	if err != nil || newWait == nil {
		t.Fatalf("beginSyncCurrent(after reset) wait=%v error=%v", newWait != nil, err)
	}
	if err := newWait(); err != nil {
		t.Fatal(err)
	}
	if syncs != 2 {
		t.Fatalf("syncs after new append=%d, want 2", syncs)
	}
}

func TestRequestReplayNamespaceAndDeterministicResponses(t *testing.T) {
	engine := openRequestReplayTestEngine(t, Config{})
	defer engine.Close()

	createKey := RequestKey{Scope: "shard-0", Unique: 1}
	create := RequestMutation{Kind: RequestCreate, Parent: RootInode, Name: "a", Type: TypeFile, Mode: 0o644}
	created, replayed, err := engine.ApplyRequestMutation(createKey, create)
	if err != nil || replayed || created.Node == nil {
		t.Fatalf("create result=%+v replayed=%v error=%v", created, replayed, err)
	}
	original := cloneNode(created.Node)
	if err := engine.SetMode(created.Node.Inode, 0o600); err != nil {
		t.Fatalf("SetMode() error = %v", err)
	}
	replayedCreate, replayed, err := engine.ApplyRequestMutation(createKey, create)
	if err != nil || !replayed || replayedCreate.Node.Mode != original.Mode || replayedCreate.Node.Ctime != original.Ctime {
		t.Fatalf("replayed create=%+v replayed=%v error=%v, want original=%+v", replayedCreate, replayed, err, original)
	}

	mutations := []struct {
		key      RequestKey
		mutation RequestMutation
	}{
		{RequestKey{Scope: "shard-0", Unique: 2}, RequestMutation{Kind: RequestMkdir, Parent: RootInode, Name: "dir", Type: TypeDirectory, Mode: 0o750, UID: 10, GID: 20}},
		{RequestKey{Scope: "shard-0", Unique: 3}, RequestMutation{Kind: RequestLink, Inode: created.Node.Inode, NewParent: RootInode, NewName: "linked"}},
		{RequestKey{Scope: "shard-0", Unique: 4}, RequestMutation{Kind: RequestSymlink, Parent: RootInode, Name: "sym", Type: TypeSymlink, Mode: 0o777, Target: "a"}},
		{RequestKey{Scope: "shard-0", Unique: 5}, RequestMutation{Kind: RequestRename, Parent: RootInode, Name: "linked", NewParent: RootInode, NewName: "renamed"}},
		{RequestKey{Scope: "shard-0", Unique: 6}, RequestMutation{Kind: RequestUnlink, Parent: RootInode, Name: "renamed"}},
		{RequestKey{Scope: "shard-0", Unique: 7}, RequestMutation{Kind: RequestRmdir, Parent: RootInode, Name: "dir"}},
	}
	for _, tc := range mutations {
		first, replayed, err := engine.ApplyRequestMutation(tc.key, tc.mutation)
		if err != nil || replayed {
			t.Fatalf("ApplyRequestMutation(%s) first=%+v replayed=%v error=%v", tc.mutation.Kind, first, replayed, err)
		}
		second, replayed, err := engine.ApplyRequestMutation(tc.key, tc.mutation)
		if err != nil || !replayed || !requestResultsEqual(first, second) {
			t.Fatalf("ApplyRequestMutation(%s resend) first=%+v second=%+v replayed=%v error=%v", tc.mutation.Kind, first, second, replayed, err)
		}
	}
}

func TestRequestReplayWriteAndCompositeSetAttr(t *testing.T) {
	engine := openRequestReplayTestEngine(t, Config{})
	defer engine.Close()
	node, err := engine.CreateFileWithOwner(RootInode, "data", 0o644, 1, 2)
	if err != nil {
		t.Fatalf("CreateFileWithOwner() error = %v", err)
	}

	writeKey := RequestKey{Scope: "shard-0", Unique: 10}
	write := RequestMutation{Kind: RequestWrite, Inode: node.Inode, Offset: 0, Data: []byte("payload")}
	before := engine.SnapshotState().NextSeq
	firstWrite, replayed, err := engine.ApplyRequestMutation(writeKey, write)
	if err != nil || replayed || firstWrite.BytesWritten != 7 {
		t.Fatalf("write result=%+v replayed=%v error=%v", firstWrite, replayed, err)
	}
	after := engine.SnapshotState().NextSeq
	if _, replayed, err := engine.ApplyRequestMutation(writeKey, write); err != nil || !replayed {
		t.Fatalf("write resend replayed=%v error=%v", replayed, err)
	}
	if got := engine.SnapshotState().NextSeq; got != after || after != before+1 {
		t.Fatalf("next seq before=%d after=%d resend=%d, want one WAL mutation", before, after, got)
	}

	setAttrKey := RequestKey{Scope: "shard-0", Unique: 11}
	setAttr := RequestMutation{
		Kind: RequestSetAttr, Inode: node.Inode, SetAttrValid: 1 | 1<<1 | 1<<2 | 1<<3,
		Mode: 0o600, UID: 1000, GID: 2000, Offset: 3,
	}
	firstAttr, replayed, err := engine.ApplyRequestMutation(setAttrKey, setAttr)
	if err != nil || replayed || firstAttr.Node == nil {
		t.Fatalf("setattr result=%+v replayed=%v error=%v", firstAttr, replayed, err)
	}
	if firstAttr.Node.Mode != 0o600 || firstAttr.Node.UID != 1000 || firstAttr.Node.GID != 2000 || firstAttr.Node.Size != 3 {
		t.Fatalf("setattr node=%+v", firstAttr.Node)
	}
	if err := engine.SetMode(node.Inode, 0o400); err != nil {
		t.Fatalf("SetMode(after setattr) error = %v", err)
	}
	replayedAttr, replayed, err := engine.ApplyRequestMutation(setAttrKey, setAttr)
	if err != nil || !replayed || replayedAttr.Node.Mode != 0o600 {
		t.Fatalf("setattr resend result=%+v replayed=%v error=%v", replayedAttr, replayed, err)
	}
	data, err := engine.Read(node.Inode, 0, 16)
	if err != nil || !bytes.Equal(data, []byte("pay")) {
		t.Fatalf("Read() data=%q error=%v, want pay", data, err)
	}
}

func TestRequestReplayXattrAndRegularMknod(t *testing.T) {
	engine := openRequestReplayTestEngine(t, Config{})
	defer engine.Close()

	mknod := RequestMutation{Kind: RequestMknod, Parent: RootInode, Name: "node", Type: TypeFile, Mode: 0o620, UID: 3, GID: 4}
	created, replayed, err := engine.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 20}, mknod)
	if err != nil || replayed || created.Node == nil || created.Node.Type != TypeFile {
		t.Fatalf("mknod result=%+v replayed=%v error=%v", created, replayed, err)
	}

	set := RequestMutation{Kind: RequestSetXattr, Inode: created.Node.Inode, Name: "user.test", Data: []byte("value")}
	key := RequestKey{Scope: "shard-0", Unique: 21}
	if _, replayed, err := engine.ApplyRequestMutation(key, set); err != nil || replayed {
		t.Fatalf("setxattr replayed=%v error=%v", replayed, err)
	}
	if _, replayed, err := engine.ApplyRequestMutation(key, set); err != nil || !replayed {
		t.Fatalf("setxattr resend replayed=%v error=%v", replayed, err)
	}
	value, err := engine.GetXattr(created.Node.Inode, "user.test")
	if err != nil || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("GetXattr() value=%q error=%v", value, err)
	}

	remove := RequestMutation{Kind: RequestRemoveXattr, Inode: created.Node.Inode, Name: "user.test"}
	removeKey := RequestKey{Scope: "shard-0", Unique: 22}
	if _, _, err := engine.ApplyRequestMutation(removeKey, remove); err != nil {
		t.Fatalf("removexattr error = %v", err)
	}
	if _, replayed, err := engine.ApplyRequestMutation(removeKey, remove); err != nil || !replayed {
		t.Fatalf("removexattr resend replayed=%v error=%v", replayed, err)
	}
	if _, err := engine.GetXattr(created.Node.Inode, "user.test"); !errors.Is(err, ErrXattrNotFound) {
		t.Fatalf("GetXattr(removed) error=%v, want %v", err, ErrXattrNotFound)
	}
}

func TestRequestReplayRetainsUnackedAcrossMaterializeAndReset(t *testing.T) {
	dir := t.TempDir()
	store := newPrefixedRecordingStore(t, "vol-materialize-request")
	heads := newMemoryHeadStore()
	cfg := Config{
		VolumeID: "vol-materialize-request", WALPath: filepath.Join(dir, "engine.wal"),
		ObjectStore: store, HeadStore: heads,
	}
	engine, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	key := RequestKey{Scope: "shard-0", Unique: 30}
	mutation := RequestMutation{Kind: RequestCreate, Parent: RootInode, Name: "retained", Type: TypeFile, Mode: 0o600}
	created, _, err := engine.ApplyRequestMutation(key, mutation)
	if err != nil {
		t.Fatalf("ApplyRequestMutation() error = %v", err)
	}
	xattrKey := RequestKey{Scope: "shard-0", Unique: 31}
	xattrMutation := RequestMutation{Kind: RequestSetXattr, Inode: created.Node.Inode, Name: "user.retained", Data: []byte("metadata")}
	if _, _, err := engine.ApplyRequestMutation(xattrKey, xattrMutation); err != nil {
		t.Fatalf("ApplyRequestMutation(setxattr) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(context.Background()); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(reopened) error = %v", err)
	}
	if _, replayed, err := reopened.ApplyRequestMutation(key, mutation); err != nil || !replayed {
		t.Fatalf("ApplyRequestMutation(after materialize) replayed=%v error=%v", replayed, err)
	}
	if _, replayed, err := reopened.ApplyRequestMutation(xattrKey, xattrMutation); err != nil || !replayed {
		t.Fatalf("ApplyRequestMutation(setxattr after materialize) replayed=%v error=%v", replayed, err)
	}
	if value, err := reopened.GetXattr(created.Node.Inode, "user.retained"); err != nil || !bytes.Equal(value, []byte("metadata")) {
		t.Fatalf("GetXattr(after materialize) value=%q error=%v", value, err)
	}
	if err := reopened.AcknowledgeRequest(key); err != nil {
		t.Fatalf("AcknowledgeRequest() error = %v", err)
	}
	if err := reopened.AcknowledgeRequest(xattrKey); err != nil {
		t.Fatalf("AcknowledgeRequest(setxattr) error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close(reopened) error = %v", err)
	}

	final, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(final) error = %v", err)
	}
	defer final.Close()
	if unacked, acked := final.RequestReplayCounts(); unacked != 0 || acked != 0 {
		t.Fatalf("RequestReplayCounts()=(%d,%d), want (0,0)", unacked, acked)
	}
}

func TestRequestReplayCapacityEvictsOnlyAcknowledged(t *testing.T) {
	engine := openRequestReplayTestEngine(t, Config{RequestReplayCapacity: 2})
	defer engine.Close()
	mutation := func(name string) RequestMutation {
		return RequestMutation{Kind: RequestCreate, Parent: RootInode, Name: name, Type: TypeFile, Mode: 0o600}
	}
	key1 := RequestKey{Scope: "shard-0", Unique: 1}
	if _, _, err := engine.ApplyRequestMutation(key1, mutation("one")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 2}, mutation("two")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 3}, mutation("three")); !errors.Is(err, ErrRequestCapacity) {
		t.Fatalf("third unacked error=%v, want %v", err, ErrRequestCapacity)
	}
	if err := engine.AcknowledgeRequest(key1); err != nil {
		t.Fatalf("AcknowledgeRequest() error = %v", err)
	}
	if _, _, err := engine.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 3}, mutation("three")); err != nil {
		t.Fatalf("third after ack error = %v", err)
	}
	if unacked, acked := engine.RequestReplayCounts(); unacked != 2 || acked != 0 {
		t.Fatalf("RequestReplayCounts()=(%d,%d), want (2,0) after acked eviction", unacked, acked)
	}
}

func TestRequestReplayScopeReuseAndIdentityDominatesPayload(t *testing.T) {
	engine := openRequestReplayTestEngine(t, Config{})
	defer engine.Close()
	first := RequestMutation{Kind: RequestCreate, Parent: RootInode, Name: "first", Type: TypeFile, Mode: 0o600}
	second := RequestMutation{Kind: RequestCreate, Parent: RootInode, Name: "second", Type: TypeFile, Mode: 0o600}
	if _, _, err := engine.ApplyRequestMutation(RequestKey{Scope: "connection-a", Unique: 1}, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.ApplyRequestMutation(RequestKey{Scope: "connection-b", Unique: 1}, second); err != nil {
		t.Fatalf("same unique in another scope error = %v", err)
	}
	replayed, wasReplay, err := engine.ApplyRequestMutation(RequestKey{Scope: "connection-a", Unique: 1}, second)
	if err != nil || !wasReplay || replayed.Node == nil {
		t.Fatalf("same identity replay result=%+v replayed=%v error=%v", replayed, wasReplay, err)
	}
	if _, err := engine.Lookup(RootInode, "second"); err != nil {
		// The second scope created this name once; the reused identity above must
		// not create another namespace mutation.
		t.Fatalf("Lookup(second) error = %v", err)
	}
}

func TestRequestReplayMissingResendFailsClosed(t *testing.T) {
	engine := openRequestReplayTestEngine(t, Config{})
	defer engine.Close()
	_, _, err := engine.ApplyRecoverableRequestMutation(RequestKey{Scope: "shard-0", Unique: 99}, RequestMutation{
		Kind: RequestCreate, Parent: RootInode, Name: "must-not-exist", Type: TypeFile, Mode: 0o600,
	}, true)
	if !errors.Is(err, ErrRequestResultMissing) {
		t.Fatalf("missing resend error=%v, want %v", err, ErrRequestResultMissing)
	}
	if _, err := engine.Lookup(RootInode, "must-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(missing resend) error=%v, want %v", err, ErrNotFound)
	}
}

func TestRequestReplayMutationUsesOneWALRecordAndNoSync(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "engine.wal")
	syncs := 0
	engine := openRequestReplayTestEngine(t, Config{
		WALPath: walPath,
		WALSyncHook: func() {
			syncs++
		},
	})
	defer engine.Close()
	key := RequestKey{Scope: "shard-0", Unique: 100}
	if _, _, err := engine.ApplyRequestMutation(key, RequestMutation{
		Kind: RequestCreate, Parent: RootInode, Name: "single-record", Type: TypeFile, Mode: 0o600,
	}); err != nil {
		t.Fatalf("ApplyRequestMutation() error = %v", err)
	}
	records, err := readWAL(walPath, "vol-request-test", nil)
	if err != nil {
		t.Fatalf("readWAL() error = %v", err)
	}
	if len(records) != 1 || records[0].RequestScope != ledgerKey(RequestKey{Scope: "shard-0"}).Scope || records[0].Op != "create" {
		t.Fatalf("wal records=%+v, want one atomic request mutation", records)
	}
	if syncs != 0 {
		t.Fatalf("WAL sync count after mutation = %d, want 0", syncs)
	}
	if err := engine.AcknowledgeRequest(key); err != nil {
		t.Fatalf("AcknowledgeRequest() error = %v", err)
	}
	if syncs != 0 {
		t.Fatalf("WAL sync count after reply ack = %d, want 0", syncs)
	}
}

func TestRequestReplayAcknowledgementSurvivesProcessRestart(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "engine.wal")
	cfg := Config{VolumeID: "vol-ack-restart", WALPath: walPath, RequestReplayCapacity: 2}
	engine, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	mutation := func(name string) RequestMutation {
		return RequestMutation{Kind: RequestCreate, Parent: RootInode, Name: name, Type: TypeFile, Mode: 0o600}
	}
	key1 := RequestKey{Scope: "shard-0", Unique: 1}
	if _, _, err := engine.ApplyRequestMutation(key1, mutation("one")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 2}, mutation("two")); err != nil {
		t.Fatal(err)
	}
	if err := engine.AcknowledgeRequest(key1); err != nil {
		t.Fatal(err)
	}
	if err := engine.Fsync(RootInode); err != nil {
		t.Fatal(err)
	}
	// Closing only the WAL models process fd teardown without Engine.Close's
	// checkpoint/reset path.
	if err := engine.wal.close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(restarted) error = %v", err)
	}
	defer reopened.Close()
	if unacked, acked := reopened.RequestReplayCounts(); unacked != 1 || acked != 0 {
		t.Fatalf("RequestReplayCounts()=(%d,%d), want (1,0)", unacked, acked)
	}
	if _, _, err := reopened.ApplyRequestMutation(RequestKey{Scope: "shard-0", Unique: 3}, mutation("three")); err != nil {
		t.Fatalf("new request after durable ack error = %v", err)
	}
}

func openRequestReplayTestEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	if cfg.VolumeID == "" {
		cfg.VolumeID = "vol-request-test"
	}
	if cfg.WALPath == "" {
		cfg.WALPath = filepath.Join(t.TempDir(), "engine.wal")
	}
	engine, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return engine
}

func requestResultsEqual(a, b RequestMutationResult) bool {
	aJSON, _ := json.Marshal(a)
	bJSON, _ := json.Marshal(b)
	return bytes.Equal(aJSON, bJSON)
}

func BenchmarkSmallFileCreateWALPath(b *testing.B) {
	b.Run("existing", func(b *testing.B) {
		engine := openRequestReplayBenchmarkEngine(b)
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := engine.CreateFile(RootInode, strconv.Itoa(index), 0o600); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
	})
	b.Run("recoverable", func(b *testing.B) {
		engine := openRequestReplayBenchmarkEngine(b)
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := RequestKey{Scope: "benchmark-shard", Unique: uint64(index + 1)}
			if _, _, err := engine.ApplyRequestMutation(key, RequestMutation{
				Kind: RequestCreate, Parent: RootInode, Name: strconv.Itoa(index), Type: TypeFile, Mode: 0o600,
			}); err != nil {
				b.Fatal(err)
			}
			if err := engine.AcknowledgeRequest(key); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
	})
}

func BenchmarkSmallFileWriteWALPath(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 4096)
	b.Run("existing", func(b *testing.B) {
		engine := openRequestReplayBenchmarkEngine(b)
		node, err := engine.CreateFile(RootInode, "data", 0o600)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := engine.Write(node.Inode, 0, payload); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
	})
	b.Run("recoverable", func(b *testing.B) {
		engine := openRequestReplayBenchmarkEngine(b)
		node, err := engine.CreateFile(RootInode, "data", 0o600)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := RequestKey{Scope: "benchmark-shard", Unique: uint64(index + 1)}
			if _, _, err := engine.ApplyRequestMutation(key, RequestMutation{
				Kind: RequestWrite, Inode: node.Inode, Data: payload,
			}); err != nil {
				b.Fatal(err)
			}
			if err := engine.AcknowledgeRequest(key); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
	})
	b.Run("recoverable-reply-critical-path", func(b *testing.B) {
		engine := openRequestReplayBenchmarkEngine(b)
		node, err := engine.CreateFile(RootInode, "data", 0o600)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			key := RequestKey{Scope: "benchmark-shard", Unique: uint64(index + 1)}
			if _, _, err := engine.ApplyRequestMutation(key, RequestMutation{
				Kind: RequestWrite, Inode: node.Inode, Data: payload,
			}); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := engine.AcknowledgeRequest(key); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		b.StopTimer()
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
	})
}

func openRequestReplayBenchmarkEngine(b *testing.B) *Engine {
	b.Helper()
	engine, err := Open(context.Background(), Config{
		VolumeID: "benchmark-volume",
		WALPath:  filepath.Join(b.TempDir(), "engine.wal"),
	})
	if err != nil {
		b.Fatal(err)
	}
	return engine
}
