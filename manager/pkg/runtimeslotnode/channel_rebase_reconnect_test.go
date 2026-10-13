package runtimeslotnode

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type retainedChannelRebaseExecutor struct {
	channelTestExecutor
	started      chan struct{}
	release      chan struct{}
	canceled     chan struct{}
	calls        atomic.Int32
	canceledOnce sync.Once
}

func (e *retainedChannelRebaseExecutor) PausedRebase(ctx context.Context, _ protocol.NodeChannelTarget, _ protocol.NodePausedRebaseControlRequest) (rootfsrebase.WorkerResult, error) {
	if e.calls.Add(1) == 1 {
		close(e.started)
	}
	select {
	case <-e.release:
		return e.pausedRebase, nil
	case <-ctx.Done():
		e.canceledOnce.Do(func() { close(e.canceled) })
		return rootfsrebase.WorkerResult{}, ctx.Err()
	}
}

func TestNodeChannelHubReplaysRebaseAcrossAuthenticatedReconnectWithoutCancelingWork(t *testing.T) {
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	require.NoError(t, err)
	defer hub.Close()
	server, files := newNodeChannelTLSServer(t, hub)
	defer server.Close()
	request, expected := testChannelPausedRebase(t)
	executor := &retainedChannelRebaseExecutor{channelTestExecutor: channelTestExecutor{pausedRebase: expected}, started: make(chan struct{}), release: make(chan struct{}), canceled: make(chan struct{})}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert, ClientKeyFile: files.clientKey, TokenFile: files.token,
		PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
		Executor: executor, PlannedRetireExecutor: executor, PausedRebaseExecutor: executor, Capacity: channelTestCapacity(),
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond, AgentInstanceID: "agent-1", MaxConcurrent: 1,
	})
	require.NoError(t, err)
	owner, cancelOwner := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(owner) }()
	defer func() {
		cancelOwner()
		select {
		case err := <-agentDone:
			require.True(t, errors.Is(err, context.Canceled))
		case <-time.After(3 * time.Second):
			t.Error("agent did not join retained work")
		}
	}()
	key := nodeChannelKey{clusterID: "cluster-1", nodeID: "node-1", nodeUID: "node-uid-1", nodeBootID: "boot-1"}
	waitNodeChannelConnected(t, hub, key.clusterID, key.nodeID, key.nodeUID, key.nodeBootID)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	type outcome struct {
		result rootfsrebase.WorkerResult
		err    error
	}
	completed := make(chan outcome, 1)
	go func() {
		r, err := hub.PausedRebase(ctx, protocol.NodeChannelTarget{ClusterID: key.clusterID, NodeID: key.nodeID, NodeUID: key.nodeUID, NodeBootID: key.nodeBootID}, request)
		completed <- outcome{r, err}
	}()
	select {
	case <-executor.started:
	case <-ctx.Done():
		t.Fatal("rebase did not reach its first stream")
	}
	connection, _, _, err := hub.connectionForCommand(key, protocol.NodeChannelCommandPausedRebase)
	require.NoError(t, err)
	require.NotNil(t, connection)
	connection.close(errdefs.ErrUnavailable)
	require.Eventually(t, func() bool {
		next, _, _, err := hub.connectionForCommand(key, protocol.NodeChannelCommandPausedRebase)
		return err == nil && next != nil && next != connection && !next.isClosed()
	}, time.Second, time.Millisecond)
	select {
	case <-executor.canceled:
		t.Fatal("transport rotation canceled the rebase")
	default:
	}
	close(executor.release)
	select {
	case out := <-completed:
		require.NoError(t, out.err)
		require.Equal(t, expected.ProofDigest, out.result.ProofDigest)
	case <-ctx.Done():
		t.Fatal("reconnected stream did not deliver retained result")
	}
	require.EqualValues(t, 1, executor.calls.Load(), "exact reconnect retry must not repeat private work")
}
