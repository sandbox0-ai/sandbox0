package portal

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type fakeNodeFUSEServer struct {
	state     fuse.ConnectionState
	detached  bool
	waitErr   error
	started   chan struct{}
	startOnce sync.Once
}

func (s *fakeNodeFUSEServer) Serve() { s.startOnce.Do(func() { close(s.started) }) }
func (s *fakeNodeFUSEServer) WaitMount() error {
	<-s.started
	return s.waitErr
}
func (s *fakeNodeFUSEServer) Detach() error                         { s.detached = true; return nil }
func (s *fakeNodeFUSEServer) ConnectionState() fuse.ConnectionState { return s.state }

func newFakeNodeFUSEServer(state fuse.ConnectionState) *fakeNodeFUSEServer {
	return &fakeNodeFUSEServer{state: state, started: make(chan struct{})}
}

func (s *fakeNodeFUSEServer) didServe() bool {
	select {
	case <-s.started:
		return true
	default:
		return false
	}
}

type fakeNodeFSConnectionFactory struct {
	newServer    *fakeNodeFUSEServer
	resumeServer *fakeNodeFUSEServer
	newCalls     int
	resumeCalls  int
	recoverCalls int
	cleanCalls   int
	recoverErr   error
	lastOptions  *fuse.MountOptions
}

func (f *fakeNodeFSConnectionFactory) New(_ fuse.RawFileSystem, _ string, opts *fuse.MountOptions) (nodeFUSEServer, error) {
	f.newCalls++
	f.lastOptions = opts
	return f.newServer, nil
}

func (f *fakeNodeFSConnectionFactory) Resume(_ fuse.RawFileSystem, _ string, _ int, _ fuse.ConnectionState, opts *fuse.MountOptions) (nodeFUSEServer, error) {
	f.resumeCalls++
	f.lastOptions = opts
	return f.resumeServer, nil
}

func (f *fakeNodeFSConnectionFactory) Recover(string) (int, uint64, error) {
	f.recoverCalls++
	if f.recoverErr != nil {
		return -1, 0, f.recoverErr
	}
	return 42, 7, nil
}

func (f *fakeNodeFSConnectionFactory) CleanMount(string) error {
	f.cleanCalls++
	return nil
}

func recoveryConnectionState() fuse.ConnectionState {
	flags2 := uint32(nodeFSRequiredKernelCapabilities >> 32)
	return fuse.ConnectionState{
		KernelSettings: fuse.InitIn{Major: 7, Minor: 38, Flags2: flags2},
		InitResponse:   fuse.InitOut{Major: 7, Minor: 38, Flags2: flags2},
	}
}

func preparedNodeFSJournal(t *testing.T, recovery bool) (*nodeFSJournalStore, nodeFSJournal, nodeFSShardState) {
	t.Helper()
	root := t.TempDir()
	store, err := openNodeFSJournal(root, "node-a", 1)
	if err != nil {
		t.Fatalf("openNodeFSJournal() error = %v", err)
	}
	if err := store.ConfigureRecovery(recovery); err != nil {
		t.Fatalf("ConfigureRecovery() error = %v", err)
	}
	if err := store.PrepareShards(root); err != nil {
		t.Fatalf("PrepareShards() error = %v", err)
	}
	state := store.Snapshot()
	return store, state, state.Shards[0]
}

func TestStartNodeFSConnectionCommitsStateBeforeServing(t *testing.T) {
	store, state, shard := preparedNodeFSJournal(t, true)
	server := newFakeNodeFUSEServer(recoveryConnectionState())
	factory := &fakeNodeFSConnectionFactory{newServer: server}

	connection, recovered, err := startNodeFSConnection(store, state, shard, 0, fuse.NewDefaultRawFileSystem(), factory)
	if err != nil {
		t.Fatalf("startNodeFSConnection() error = %v", err)
	}
	if connection == nil || recovered || !server.didServe() || factory.newCalls != 1 || factory.cleanCalls != 1 {
		t.Fatalf("connection=%v recovered=%v server=%+v factory=%+v", connection, recovered, server, factory)
	}
	if len(store.Snapshot().Shards[0].SessionState) == 0 {
		t.Fatal("connection state was not committed")
	}
	if factory.lastOptions.ExtraCapabilities&nodeFSRequiredKernelCapabilities != nodeFSRequiredKernelCapabilities {
		t.Fatalf("extra capabilities = %#x", factory.lastOptions.ExtraCapabilities)
	}
	if len(factory.lastOptions.Options) != 2 {
		t.Fatalf("mount recovery options = %v", factory.lastOptions.Options)
	}
}

func TestStartNodeFSConnectionResumesCommittedConnection(t *testing.T) {
	store, state, shard := preparedNodeFSJournal(t, true)
	encoded, err := json.Marshal(recoveryConnectionState())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitShardSession(0, encoded); err != nil {
		t.Fatal(err)
	}
	state = store.Snapshot()
	shard = state.Shards[0]
	server := newFakeNodeFUSEServer(recoveryConnectionState())
	factory := &fakeNodeFSConnectionFactory{resumeServer: server}

	_, recovered, err := startNodeFSConnection(store, state, shard, 1, fuse.NewDefaultRawFileSystem(), factory)
	if err != nil {
		t.Fatalf("startNodeFSConnection() error = %v", err)
	}
	if !recovered || !server.didServe() || factory.recoverCalls != 1 || factory.resumeCalls != 1 || factory.newCalls != 0 || factory.cleanCalls != 0 {
		t.Fatalf("recovered=%v server=%+v factory=%+v", recovered, server, factory)
	}
}

func TestStartNodeFSConnectionFailsClosedWithActivePortals(t *testing.T) {
	store, state, shard := preparedNodeFSJournal(t, true)
	encoded, _ := json.Marshal(recoveryConnectionState())
	if err := store.CommitShardSession(0, encoded); err != nil {
		t.Fatal(err)
	}
	state = store.Snapshot()
	shard = state.Shards[0]
	factory := &fakeNodeFSConnectionFactory{recoverErr: errors.New("connection missing")}

	if _, _, err := startNodeFSConnection(store, state, shard, 1, fuse.NewDefaultRawFileSystem(), factory); err == nil {
		t.Fatal("startNodeFSConnection() error = nil")
	}
	if factory.cleanCalls != 0 || factory.newCalls != 0 {
		t.Fatalf("failed recovery mutated mount: %+v", factory)
	}
	if len(store.Snapshot().Shards[0].SessionState) == 0 {
		t.Fatal("failed recovery cleared committed connection state")
	}
}

func TestStartNodeFSConnectionRejectsKernelWithoutRecovery(t *testing.T) {
	store, state, shard := preparedNodeFSJournal(t, true)
	server := newFakeNodeFUSEServer(fuse.ConnectionState{InitResponse: fuse.InitOut{Major: 7, Minor: 38}})
	factory := &fakeNodeFSConnectionFactory{newServer: server}

	if _, _, err := startNodeFSConnection(store, state, shard, 0, fuse.NewDefaultRawFileSystem(), factory); err == nil {
		t.Fatal("startNodeFSConnection() error = nil")
	}
	if !server.detached || factory.cleanCalls != 2 || len(store.Snapshot().Shards[0].SessionState) != 0 {
		t.Fatalf("server=%+v journal=%+v", server, store.Snapshot().Shards[0])
	}
}
