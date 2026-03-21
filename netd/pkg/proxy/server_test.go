package proxy

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
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

func TestProbeServerFirstSSHReclassifiesUnknownTraffic(t *testing.T) {
	upstreamAddr, upstreamDone := startTCPBannerServer(t, "SSH-2.0-TestServer\r\n", []byte("SSH-2.0-TestClient\r\n"), nil)

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
			ProxyHeaderLimit:     1024,
		},
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: policy.CompiledRuleSet{
				TrafficRules: []policy.CompiledTrafficRule{{
					Name:         "allow-ssh",
					Action:       v1alpha1.TrafficRuleActionAllow,
					AppProtocols: []string{"ssh"},
				}},
			},
		},
		DestIP:   upstreamAddr.IP,
		DestPort: upstreamAddr.Port,
	}
	classification := server.probeServerFirstSSH(req, classifyUnknownTraffic("tcp", "unknown", upstreamAddr.IP, upstreamAddr.Port, "client_idle"), &tcpClassifyContext{
		OrigIP:      upstreamAddr.IP,
		OrigPort:    upstreamAddr.Port,
		HeaderLimit: 1024,
	})
	if classification.Protocol != "ssh" {
		t.Fatalf("protocol = %q, want ssh", classification.Protocol)
	}
	if req.UpstreamConn == nil {
		t.Fatalf("expected probed upstream connection to be preserved")
	}
	if req.UpstreamPrefix == nil {
		t.Fatalf("expected upstream prefix to be preserved")
	}
	data, err := io.ReadAll(req.UpstreamPrefix)
	if err != nil {
		t.Fatalf("read upstream prefix: %v", err)
	}
	if string(data) != "SSH-2.0-TestServer\r\n" {
		t.Fatalf("upstream prefix = %q", string(data))
	}
	req.UpstreamPrefix = bytes.NewReader(data)

	relayListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen relay: %v", err)
	}
	defer relayListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, acceptErr := relayListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		req.Conn = conn
		done <- server.runPassThrough(&trackingAdapter{
			name:       "ssh",
			transport:  "tcp",
			protocol:   "ssh",
			capability: adapterCapabilityPassThrough,
		}, req)
	}()

	clientConn, err := net.Dial("tcp4", relayListener.Addr().String())
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	defer clientConn.Close()
	got := make([]byte, len("SSH-2.0-TestServer\r\n"))
	if _, err := io.ReadFull(clientConn, got); err != nil {
		t.Fatalf("read replayed upstream banner: %v", err)
	}
	if string(got) != "SSH-2.0-TestServer\r\n" {
		t.Fatalf("replayed upstream banner = %q", string(got))
	}
	if _, err := clientConn.Write([]byte("SSH-2.0-TestClient\r\n")); err != nil {
		t.Fatalf("write client banner: %v", err)
	}
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		if err := tcpConn.CloseWrite(); err != nil {
			t.Fatalf("close write: %v", err)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("runPassThrough: %v", err)
	}
	if got := <-upstreamDone; !bytes.Equal(got, []byte("SSH-2.0-TestClient\r\n")) {
		t.Fatalf("upstream received = %q", got)
	}
}

func TestRunPassThroughUsesPreconnectedUpstreamPrefix(t *testing.T) {
	upstreamAddr, upstreamDone := startTCPBannerServer(t, "SSH-2.0-TestServer\r\n", []byte("hello"), []byte("world"))

	upstreamConn, err := net.Dial("tcp4", upstreamAddr.String())
	if err != nil {
		t.Fatalf("dial upstream: %v", err)
	}

	prefix, err := readTCPPrefix(upstreamConn, 1024, 50*time.Millisecond, 50*time.Millisecond, parseSSHBannerClassification)
	if err != nil {
		t.Fatalf("readTCPPrefix: %v", err)
	}

	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: time.Second},
		},
	}
	adapter := &trackingAdapter{
		name:       "tcp-pass",
		transport:  "tcp",
		protocol:   "unknown",
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
			Server:         server,
			SrcIP:          "10.0.0.2",
			DestIP:         upstreamAddr.IP,
			DestPort:       upstreamAddr.Port,
			Conn:           conn,
			Prefix:         bytes.NewReader([]byte("hello")),
			UpstreamConn:   upstreamConn,
			UpstreamPrefix: bytes.NewReader(prefix),
		}
		if runErr := server.runPassThrough(adapter, req); runErr != nil {
			t.Errorf("runPassThrough: %v", runErr)
		}
	}()

	clientConn, err := net.Dial("tcp4", proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer clientConn.Close()

	reply := make([]byte, len("SSH-2.0-TestServer\r\n"))
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatalf("read upstream banner: %v", err)
	}
	if string(reply) != "SSH-2.0-TestServer\r\n" {
		t.Fatalf("banner = %q", string(reply))
	}
	if _, err := clientConn.Write([]byte("hello")); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		if err := tcpConn.CloseWrite(); err != nil {
			t.Fatalf("close write: %v", err)
		}
	}
	ack := make([]byte, len("world"))
	if _, err := io.ReadFull(clientConn, ack); err != nil {
		t.Fatalf("read ack: %v", err)
	}
	<-done

	if string(ack) != "world" {
		t.Fatalf("ack = %q", string(ack))
	}
	if got := <-upstreamDone; !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("upstream received = %q, want hello", got)
	}
	if adapter.called {
		t.Fatalf("pass-through adapter Handle should not be called")
	}
}

func TestPipeWithReaderPropagatesCopyErrors(t *testing.T) {
	server := &Server{}
	wantErr := errors.New("write failed")

	err := server.pipeWithReader(
		&stubConn{reader: bytes.NewReader(nil), writer: io.Discard},
		&stubConn{reader: bytes.NewReader(nil), writer: errWriter{err: wantErr}},
		bytes.NewReader([]byte("payload")),
		nil,
		nil,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("pipeWithReader error = %v, want %v", err, wantErr)
	}
}

type stubConn struct {
	reader io.Reader
	writer io.Writer
}

func (c *stubConn) Read(p []byte) (int, error) {
	if c.reader == nil {
		return 0, io.EOF
	}
	return c.reader.Read(p)
}

func (c *stubConn) Write(p []byte) (int, error) {
	if c.writer == nil {
		return len(p), nil
	}
	return c.writer.Write(p)
}

func (c *stubConn) Close() error                     { return nil }
func (c *stubConn) LocalAddr() net.Addr              { return stubNetAddr("local") }
func (c *stubConn) RemoteAddr() net.Addr             { return stubNetAddr("remote") }
func (c *stubConn) SetDeadline(time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(time.Time) error { return nil }

type stubNetAddr string

func (a stubNetAddr) Network() string { return "tcp" }
func (a stubNetAddr) String() string  { return string(a) }

type errWriter struct {
	err error
}

func (w errWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

func startTCPBannerServer(t *testing.T, banner string, expectedBytes []byte, response []byte) (*net.TCPAddr, <-chan []byte) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream tcp: %v", err)
	}
	done := make(chan []byte, 1)
	go func() {
		defer listener.Close()
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			t.Errorf("accept upstream tcp: %v", acceptErr)
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, writeErr := io.WriteString(conn, banner); writeErr != nil {
			t.Errorf("write upstream banner: %v", writeErr)
			return
		}
		if expectedBytes == nil {
			done <- nil
			return
		}
		data := make([]byte, len(expectedBytes))
		if _, readErr := io.ReadFull(conn, data); readErr != nil {
			t.Errorf("read upstream tcp: %v", readErr)
			return
		}
		done <- data
		if len(response) > 0 {
			if _, writeErr := conn.Write(response); writeErr != nil {
				t.Errorf("write upstream response: %v", writeErr)
			}
		}
	}()
	return listener.Addr().(*net.TCPAddr), done
}
