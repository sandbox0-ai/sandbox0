package proxy

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type trackingAdapter struct {
	name       string
	transport  string
	protocol   string
	capability adapterCapability
	called     bool
}

func (a *trackingAdapter) Name() string                  { return a.name }
func (a *trackingAdapter) Transport() string             { return a.transport }
func (a *trackingAdapter) Protocol() string              { return a.protocol }
func (a *trackingAdapter) Capability() adapterCapability { return a.capability }
func (a *trackingAdapter) Handle(*adapterRequest) error {
	a.called = true
	return nil
}

func TestRunAdapterPassThroughUsesGenericTCPRelay(t *testing.T) {
	prefix := []byte("prefix:")
	payload := []byte("payload")
	want := append(append([]byte(nil), prefix...), payload...)
	upstreamAddr, upstreamDone := startTCPEchoServer(t, len(want))

	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
	}
	adapter := &trackingAdapter{
		name:       "tcp-pass",
		transport:  "tcp",
		protocol:   "ssh",
		capability: adapterCapabilityPassThrough,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := proxyListener.Accept()
		if acceptErr != nil {
			t.Errorf("accept proxy conn: %v", acceptErr)
			return
		}
		defer conn.Close()
		req := &adapterRequest{
			Server:   server,
			SrcIP:    "10.0.0.2",
			DestIP:   upstreamAddr.IP,
			DestPort: upstreamAddr.Port,
			Conn:     conn,
			Prefix:   bytes.NewReader(prefix),
		}
		if runErr := server.runAdapter(adapter, req); runErr != nil {
			t.Errorf("runAdapter: %v", runErr)
		}
	}()

	clientConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()
	if _, err := clientConn.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		if err := tcpConn.CloseWrite(); err != nil {
			t.Fatalf("close write: %v", err)
		}
	}
	echoed, err := io.ReadAll(clientConn)
	if err != nil {
		t.Fatalf("read echoed bytes: %v", err)
	}
	<-done

	if !bytes.Equal(echoed, want) {
		t.Fatalf("echoed = %q, want %q", echoed, want)
	}
	if got := <-upstreamDone; !bytes.Equal(got, want) {
		t.Fatalf("upstream received = %q, want %q", got, want)
	}
	if adapter.called {
		t.Fatalf("pass-through adapter Handle should not be called")
	}
}

func TestRunAdapterInspectInvokesAdapterHandle(t *testing.T) {
	server := &Server{}
	adapter := &trackingAdapter{
		name:       "http-inspect",
		transport:  "tcp",
		protocol:   "http",
		capability: adapterCapabilityInspect,
	}

	if err := server.runAdapter(adapter, &adapterRequest{Server: server}); err != nil {
		t.Fatalf("runAdapter: %v", err)
	}
	if !adapter.called {
		t.Fatalf("inspect adapter Handle should be called")
	}
}

func TestRunAdapterTerminateInvokesAdapterHandle(t *testing.T) {
	server := &Server{}
	adapter := &trackingAdapter{
		name:       "tls-terminate",
		transport:  "tcp",
		protocol:   "tls",
		capability: adapterCapabilityTerminate,
	}

	if err := server.runAdapter(adapter, &adapterRequest{Server: server}); err != nil {
		t.Fatalf("runAdapter: %v", err)
	}
	if !adapter.called {
		t.Fatalf("terminate adapter Handle should be called")
	}
}
