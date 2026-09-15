package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/stretchr/testify/require"
)

func installFlowPolicy(t *testing.T, store *policy.Store, incarnation, revision string) *policy.CompiledPolicy {
	t.Helper()
	raw, err := json.Marshal(v1alpha1.NetworkPolicySpec{Mode: v1alpha1.NetworkModeAllowAll, SandboxID: "sandbox-" + incarnation, TeamID: "team"})
	require.NoError(t, err)
	store.ReconcileSandboxes([]*model.SandboxInfo{{
		Scope: "runtime-slot", Name: "slot", SourceIP: "10.0.0.2", OwnerKind: "runtime-slot",
		IncarnationID: incarnation, Revision: revision, NetworkPolicyHash: "hash-" + revision, NetworkPolicy: string(raw),
	}})
	return store.GetByIP("10.0.0.2")
}

func TestFlowRetirementUnblocksBothDirectionsOfIdleTCPRelay(t *testing.T) {
	store := policy.NewStore(nil)
	server := &Server{store: store}
	compiled := installFlowPolicy(t, store, "first", "1")
	downstream, client := net.Pipe()
	upstream, peer := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
	flow, err := server.beginTCPFlow("10.0.0.2", compiled, downstream)
	require.NoError(t, err)
	require.NoError(t, flow.retain(upstream))
	done := make(chan error, 1)
	go func() { done <- server.pipeWithReader(downstream, upstream, downstream, compiled, nil) }()

	store.ReconcileSandboxes(nil)
	server.ReconcileActiveFlows()
	require.ErrorIs(t, flow.ctx.Err(), context.Canceled)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retired relay still waits on an upstream or downstream socket")
	}
	require.Empty(t, server.tcpFlows)
	server.endTCPFlow(flow)
}

func TestFlowReconciliationPreservesNewIPIncarnationAndRejectsLateOldDials(t *testing.T) {
	for _, change := range []string{"incarnation", "revision"} {
		t.Run(change, func(t *testing.T) {
			store := policy.NewStore(nil)
			server := &Server{store: store}
			oldPolicy := installFlowPolicy(t, store, "first", "1")
			oldConn, oldPeer := net.Pipe()
			defer oldPeer.Close()
			old, err := server.beginTCPFlow("10.0.0.2", oldPolicy, oldConn)
			require.NoError(t, err)
			defer server.endTCPFlow(old)
			incarnation, revision := "second", "1"
			if change == "revision" {
				incarnation, revision = "first", "2"
			}
			currentPolicy := installFlowPolicy(t, store, incarnation, revision)
			currentConn, currentPeer := net.Pipe()
			defer currentPeer.Close()
			current, err := server.beginTCPFlow("10.0.0.2", currentPolicy, currentConn)
			require.NoError(t, err)
			defer server.endTCPFlow(current)
			server.ReconcileActiveFlows()
			require.ErrorIs(t, old.ctx.Err(), context.Canceled)
			require.NoError(t, current.ctx.Err())
			_, err = server.beginTCPFlow("10.0.0.2", oldPolicy, oldConn)
			require.ErrorIs(t, err, errFlowRetired)
			late, latePeer := net.Pipe()
			defer latePeer.Close()
			require.ErrorIs(t, old.retain(late), errFlowRetired)
			_, err = latePeer.Write([]byte("must not reach a reused IP"))
			require.Error(t, err)
		})
	}
}

func TestUDPSessionsNeverReuseAnUpstreamAcrossPolicyIncarnations(t *testing.T) {
	store := policy.NewStore(nil)
	server := &Server{store: store}
	request := &adapterRequest{Server: server, SrcIP: "10.0.0.2", DestIP: net.ParseIP("127.0.0.1"), DestPort: 19000,
		UDPSource: &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 12345}}
	request.Compiled = installFlowPolicy(t, store, "first", "1")
	old, err := server.ensureUDPSession(request)
	require.NoError(t, err)
	request.Compiled = installFlowPolicy(t, store, "second", "1")
	current, err := server.ensureUDPSession(request)
	require.NoError(t, err)
	require.NotSame(t, old, current)
	defer current.close()
	server.ReconcileActiveFlows()
	require.True(t, old.isClosed())
	require.False(t, current.isClosed())
	require.Len(t, server.udpSessions, 1)
	store.ReconcileSandboxes(nil)
	server.ReconcileActiveFlows()
	require.True(t, current.isClosed())
	require.Empty(t, server.udpSessions)
}

func TestShutdownFencesConcurrentTCPAdmissionAndIsIdempotent(t *testing.T) {
	store := policy.NewStore(nil)
	server := &Server{store: store}
	compiled := installFlowPolicy(t, store, "first", "1")
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			flow, err := server.beginTCPFlow("10.0.0.2", compiled, conn)
			if err == nil {
				defer server.endTCPFlow(flow)
			} else if !errors.Is(err, errFlowRetired) {
				t.Error(err)
			}
		})
	}
	require.NoError(t, server.Shutdown(t.Context()))
	wg.Wait()
	require.NoError(t, server.Shutdown(t.Context()))
	require.Empty(t, server.tcpFlows)
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	_, err := server.beginTCPFlow("10.0.0.2", compiled, conn)
	require.ErrorIs(t, err, errFlowRetired)
}

func TestLateDNSResponseCannotSupplyHostHintsToReusedIP(t *testing.T) {
	store := policy.NewStore(nil)
	server := &Server{store: store, dnsCache: newDNSHostCache()}
	old := installFlowPolicy(t, store, "first", "1")
	current := installFlowPolicy(t, store, "second", "1")
	server.ForgetSandboxDNS("10.0.0.2")
	server.observePolicyDNSResponse("10.0.0.2", old, buildDNSAResponse(t, "old.example.", "1.1.1.1", 60))
	require.Empty(t, server.dnsCache.Lookup(dnsPolicyCacheKey("10.0.0.2", current), net.ParseIP("1.1.1.1")))
	server.observePolicyDNSResponse("10.0.0.2", current, buildDNSAResponse(t, "current.example.", "1.1.1.1", 60))
	require.Len(t, server.dnsCache.Lookup(dnsPolicyCacheKey("10.0.0.2", current), net.ParseIP("1.1.1.1")), 1)
	server.ForgetSandboxDNS("10.0.0.2")
	require.Empty(t, server.dnsCache.bySandbox)
}
