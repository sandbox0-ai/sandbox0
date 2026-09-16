package runtimeslotnode

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// A stream cannot reconnect until its canceled node operations have unwound.
// Hold that unwind explicitly so the test exercises a real transport gap.
type drainingChannelExecutor struct {
	channelTestExecutor
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (e *drainingChannelExecutor) Claim(
	ctx context.Context,
	_ protocol.NodeChannelTarget,
	request protocol.NodeClaimControlRequest,
) (protocol.NodeControlResponse, error) {
	e.mu.Lock()
	e.claims = append(e.claims, request)
	first := len(e.claims) == 1
	e.mu.Unlock()
	if first {
		close(e.entered)
		<-ctx.Done()
		close(e.canceled)
		<-e.release
		return protocol.NodeControlResponse{}, ctx.Err()
	}
	return protocol.NodeControlResponse{Phase: string(protocol.StateActive)}, nil
}

func TestNodeChannelHubRetriesExactClaimAfterSlowStreamDrain(t *testing.T) {
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	server, files := newNodeChannelTLSServer(t, hub)
	defer server.Close()
	executor := &drainingChannelExecutor{
		entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}),
	}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token,
		PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot,
		ClusterID: "cluster-1", NodeID: "node-1", Executor: executor,
		PlannedRetireExecutor: executor, Capacity: channelTestCapacity(),
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		AgentInstanceID: "agent-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	agentCtx, cancelAgent := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(agentCtx) }()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(executor.release) }) }
	defer func() {
		release()
		cancelAgent()
		select {
		case err := <-agentDone:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("agent stop = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("agent did not stop")
		}
	}()
	key := nodeChannelKey{clusterID: "cluster-1", nodeID: "node-1", nodeUID: "node-uid-1", nodeBootID: "boot-1"}
	waitNodeChannelConnected(t, hub, key.clusterID, key.nodeID, key.nodeUID, key.nodeBootID)
	type outcome struct {
		response protocol.NodeControlResponse
		err      error
	}
	result := make(chan outcome, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	request := testChannelClaimRequest()
	go func() {
		response, err := hub.Claim(ctx, runtimeslotclaim.NodeTarget{
			SlotID: "slot-1", ClusterID: key.clusterID, AllocationID: "allocation-1",
			NodeID: key.nodeID, NodeUID: key.nodeUID, NodeBootID: key.nodeBootID,
			ControlEndpoint: "unix:///var/run/sandbox0/nomad-slots/task.sock",
		}, request)
		result <- outcome{response: response, err: err}
	}()
	select {
	case <-executor.entered:
	case <-ctx.Done():
		t.Fatal("claim did not reach the first authenticated stream")
	}
	connection, _, _, err := hub.connectionForCommand(key, protocol.NodeChannelCommandClaim)
	if err != nil || connection == nil {
		t.Fatalf("get exact stream = %v, %v", connection, err)
	}
	connection.close(errdefs.ErrUnavailable)
	select {
	case <-executor.canceled:
	case <-ctx.Done():
		t.Fatal("disconnected stream did not cancel its operation")
	}
	select {
	case early := <-result:
		t.Fatalf("claim returned while the authenticated stream was draining: %+v, %v", early.response, early.err)
	case <-time.After(1100 * time.Millisecond):
	}
	release()
	select {
	case completed := <-result:
		if completed.err != nil || completed.response.Phase != string(protocol.StateActive) {
			t.Fatalf("reconnected claim = %+v, %v", completed.response, completed.err)
		}
	case <-ctx.Done():
		t.Fatal("claim did not finish after authenticated reconnect")
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if len(executor.claims) != 2 || !reflect.DeepEqual(executor.claims[0], request) ||
		!reflect.DeepEqual(executor.claims[1], request) {
		t.Fatal("transport retry did not preserve the exact claim payload")
	}
}
