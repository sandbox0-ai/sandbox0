package runtimeslotnode

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type replicaChannelResolver struct {
	mu        sync.Mutex
	addresses []net.IPAddr
	hosts     []string
	err       error
}

func (r *replicaChannelResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hosts = append(r.hosts, host)
	return append([]net.IPAddr(nil), r.addresses...), r.err
}

func (r *replicaChannelResolver) set(addresses ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addresses = make([]net.IPAddr, 0, len(addresses))
	r.err = nil
	for _, address := range addresses {
		r.addresses = append(r.addresses, net.IPAddr{IP: net.ParseIP(address)})
	}
}

func (r *replicaChannelResolver) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func TestNodeChannelAgentSetRoutesThroughEveryManagerReplica(t *testing.T) {
	hubA, err := NewChannelHub(channelTestVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	defer hubA.Close()
	hubB, err := NewChannelHub(channelTestVerifier{})
	if err != nil {
		t.Fatal(err)
	}
	defer hubB.Close()

	const authorityHost = "manager-nodes.sandbox0-system.svc"
	serverA, serverB, port, files := newReplicaNodeChannelServers(t, authorityHost, hubA, hubB)
	defer serverA.Close()
	defer serverB.Close()

	executor := &channelTestExecutor{}
	resolver := &replicaChannelResolver{}
	// Duplicate and unstable DNS ordering cannot create duplicate streams or
	// leave one manager replica without an exact node route.
	resolver.set("127.0.0.2", "127.0.0.1", "127.0.0.1")
	agentSet, err := protocol.NewNodeChannelAgentSet(protocol.NodeChannelAgentSetConfig{
		Agent: protocol.NodeChannelAgentConfig{
			BaseURL: "https://" + net.JoinHostPort(authorityHost, strconv.Itoa(port)),
			CAFile:  files.ca, ClientCertFile: files.clientCert,
			ClientKeyFile: files.clientKey, TokenFile: files.token,
			PeerURISAN: testNodeChannelServerURI,
			ClusterID:  "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1",
			NodeBootIDFile: files.boot, Executor: executor, NetworkExecutor: executor,
			ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
			AgentInstanceID: "agent-1",
		},
		Resolver: resolver, ResolveInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- agentSet.Run(ctx) }()
	waitNodeChannelConnected(t, hubA, "cluster-1", "node-1", "node-uid-1", "boot-1")
	waitNodeChannelConnected(t, hubB, "cluster-1", "node-1", "node-uid-1", "boot-1")

	target := runtimeslotclaim.NodeTarget{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		ControlEndpoint: "unix:///var/run/sandbox0/nomad-slots/task.sock",
	}
	if response, err := hubA.Claim(t.Context(), target, testChannelClaimRequest()); err != nil ||
		response.Phase != string(protocol.StateActive) {
		t.Fatalf("claim through manager A = %+v, %v", response, err)
	}
	request := testChannelCleanupRequest()
	if proof, err := hubB.CleanupRuntimeSlot(t.Context(), Target{
		ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
	}, request); err != nil || proof.Request() != request {
		t.Fatalf("cleanup through manager B = %+v, %v", proof, err)
	}

	resolver.fail(errors.New("temporary DNS outage"))
	time.Sleep(150 * time.Millisecond)
	if !hubA.Connected("cluster-1", "node-1", "node-uid-1", "boot-1") ||
		!hubB.Connected("cluster-1", "node-1", "node-uid-1", "boot-1") {
		t.Fatal("transient DNS failure discarded the last exact membership")
	}
	resolver.set("127.0.0.1")
	waitNodeChannelDisconnected(t, hubB, "cluster-1", "node-1", "node-uid-1", "boot-1")
	if !hubA.Connected("cluster-1", "node-1", "node-uid-1", "boot-1") {
		t.Fatal("retained manager replica lost its exact channel")
	}
	resolver.set("127.0.0.2", "127.0.0.1")
	waitNodeChannelConnected(t, hubB, "cluster-1", "node-1", "node-uid-1", "boot-1")
	resolver.set()
	waitNodeChannelDisconnected(t, hubA, "cluster-1", "node-1", "node-uid-1", "boot-1")
	waitNodeChannelDisconnected(t, hubB, "cluster-1", "node-1", "node-uid-1", "boot-1")
	resolver.set("127.0.0.1", "127.0.0.2")
	waitNodeChannelConnected(t, hubA, "cluster-1", "node-1", "node-uid-1", "boot-1")
	waitNodeChannelConnected(t, hubB, "cluster-1", "node-1", "node-uid-1", "boot-1")

	resolver.mu.Lock()
	hosts := append([]string(nil), resolver.hosts...)
	resolver.mu.Unlock()
	for _, host := range hosts {
		if host != authorityHost {
			t.Fatalf("resolver host = %q, want TLS authority %q", host, authorityHost)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("agent set Run() error = %v", err)
	}
}

func waitNodeChannelDisconnected(t *testing.T, hub *ChannelHub, clusterID, nodeID, nodeUID, bootID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for hub.Connected(clusterID, nodeID, nodeUID, bootID) {
		if time.Now().After(deadline) {
			t.Fatal("node channel did not disconnect")
		}
		time.Sleep(time.Millisecond)
	}
}

func newReplicaNodeChannelServers(
	t *testing.T,
	authorityHost string,
	hubA, hubB *ChannelHub,
) (*httptest.Server, *httptest.Server, int, nodeChannelTLSFiles) {
	t.Helper()
	directory := t.TempDir()
	ca, caKey, caPEM := newNodeChannelTestCA(t)
	serverCertificate := newNodeChannelTestCertificate(t, ca, caKey, nodeChannelCertificateRequest{
		commonName: authorityHost, server: true, dnsNames: []string{authorityHost},
		uris: []string{testNodeChannelServerURI},
	})
	clientPEM, clientKeyPEM := newNodeChannelTestCertificatePEM(t, ca, caKey, nodeChannelCertificateRequest{
		commonName: "node-agent", client: true, uris: []string{"spiffe://sandbox0.test/node/node-uid-1"},
	})
	files := nodeChannelTLSFiles{
		ca: filepath.Join(directory, "ca.pem"), clientCert: filepath.Join(directory, "client.pem"),
		clientKey: filepath.Join(directory, "client-key.pem"), token: filepath.Join(directory, "token"),
		boot: filepath.Join(directory, "boot-id"),
	}
	for path, payload := range map[string][]byte{
		files.ca: caPEM, files.clientCert: clientPEM, files.clientKey: clientKeyPEM,
		files.token: []byte("node-token\n"), files.boot: []byte("boot-1\n"),
	} {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	clientRoots := x509.NewCertPool()
	if !clientRoots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append client CA")
	}
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listenerA.Addr().(*net.TCPAddr).Port
	listenerB, err := net.Listen("tcp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		listenerA.Close()
		t.Fatal(err)
	}
	start := func(listener net.Listener, hub *ChannelHub) *httptest.Server {
		server := httptest.NewUnstartedServer(hub)
		_ = server.Listener.Close()
		server.Listener = listener
		server.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
		}
		server.StartTLS()
		return server
	}
	return start(listenerA, hubA), start(listenerB, hubB), port, files
}
