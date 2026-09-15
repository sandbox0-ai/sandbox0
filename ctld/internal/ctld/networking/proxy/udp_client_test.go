package proxy

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type gatedUDPClassifier struct {
	entered chan struct{}
	release chan struct{}
}

func (*gatedUDPClassifier) Name() string { return "test-gated-datagram" }
func (c *gatedUDPClassifier) Classify(ctx *udpClassifyContext) (*classificationResult, bool) {
	close(c.entered)
	<-c.release
	return (&udpGenericClassifier{}).Classify(ctx)
}

type queuedUDPClient struct {
	packet []byte
	closed chan struct{}
	once   sync.Once
}

func (c *queuedUDPClient) Read(buffer []byte) (int, error) {
	if c.packet != nil {
		n := copy(buffer, c.packet)
		c.packet = nil
		return n, nil
	}
	<-c.closed
	return 0, net.ErrClosed
}
func (*queuedUDPClient) Write(packet []byte) (int, error) { return len(packet), nil }
func (c *queuedUDPClient) Close() error                   { c.once.Do(func() { close(c.closed) }); return nil }

// A packet may already be classifying when policy reconciliation closes its
// established transparent socket. It must never enter a replacement session,
// even when an idle close preserves the same policy and five-tuple.
func TestUDPClientPacketCannotEnterReplacementSession(t *testing.T) {
	for _, change := range []string{"idle_close", "policy_revision", "IP_incarnation"} {
		t.Run(change, func(t *testing.T) {
			upstream, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			require.NoError(t, err)
			defer upstream.Close()
			store := policy.NewStore(nil)
			gate := &gatedUDPClassifier{entered: make(chan struct{}), release: make(chan struct{})}
			registry, err := newAdapterRegistry([]proxyAdapter{&udpAdapter{}}, []proxyAdapter{&udpPassThroughAdapter{}})
			require.NoError(t, err)
			server := &Server{store: store, logger: zap.NewNop(), adapters: registry, udpClassifiers: []udpClassifier{gate}}
			request := &adapterRequest{Server: server, Compiled: installFlowPolicy(t, store, "first", "1"),
				SrcIP: "10.0.0.2", UDPSource: &net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 40000},
				DestIP: upstream.LocalAddr().(*net.UDPAddr).IP, DestPort: upstream.LocalAddr().(*net.UDPAddr).Port, UDPConn: upstream}
			old, err := server.ensureUDPSession(request)
			require.NoError(t, err)
			client := &queuedUDPClient{packet: []byte("accepted-before-retirement"), closed: make(chan struct{})}
			old.downstream = client
			done := make(chan struct{})
			go func() { old.readClientLoop(client); close(done) }()
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("established client datagram was not classified")
			}
			switch change {
			case "policy_revision":
				request.Compiled = installFlowPolicy(t, store, "first", "2")
			case "IP_incarnation":
				request.Compiled = installFlowPolicy(t, store, "second", "1")
			}
			old.close()
			current, err := server.ensureUDPSession(request)
			require.NoError(t, err)
			defer current.close()
			close(gate.release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("retired client receive loop did not exit")
			}
			require.NoError(t, upstream.SetReadDeadline(time.Now().Add(30*time.Millisecond)))
			_, _, err = upstream.ReadFromUDP(make([]byte, 256))
			require.Error(t, err, "old packet escaped through a replacement session")
			require.True(t, err.(net.Error).Timeout())
			require.Len(t, server.udpSessions, 1)
			require.Same(t, current, server.udpSessions[current.key])
			require.Nil(t, current.upstream)
		})
	}
}
