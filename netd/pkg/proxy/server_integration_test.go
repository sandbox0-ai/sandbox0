package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHandleTCPDecisionPassThroughRelaysAndAudits(t *testing.T) {
	prefix := []byte("opaque-prefix:")
	payload := []byte("hello-from-client")
	want := append(append([]byte(nil), prefix...), payload...)
	upstreamAddr, upstreamDone := startTCPEchoServer(t, len(want))
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer proxyListener.Close()

	var auditBuf bytes.Buffer
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
		logger:      zap.NewNop(),
		tcpFallback: &tcpPassThroughAdapter{},
		auditor:     newAuditLoggerFromWriter(nopWriteCloser{Writer: &auditBuf}),
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
		compiled := &policy.CompiledPolicy{Mode: v1alpha1.NetworkModeAllowAll}
		req := &adapterRequest{
			Server:   server,
			Compiled: compiled,
			SrcIP:    "10.0.0.2",
			DestIP:   upstreamAddr.IP,
			DestPort: upstreamAddr.Port,
			Conn:     conn,
			Prefix:   bytes.NewReader(prefix),
		}
		decision := decideTraffic(compiled, classifyUnknownTraffic("tcp", "tls", upstreamAddr.IP, upstreamAddr.Port, "parse_failed"))
		server.handleTCPDecision(req, decision, "")
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

	event := decodeSingleAuditEvent(t, auditBuf.Bytes())
	if event.Action != string(decisionActionPassThrough) || event.ClassifierResult != "unknown" {
		t.Fatalf("unexpected audit decision: %+v", event)
	}
	if event.Adapter != "tcp-pass-through" || event.Outcome != "completed" {
		t.Fatalf("unexpected audit adapter/outcome: %+v", event)
	}
	if event.EgressBytes != int64(len(want)) || event.IngressBytes != int64(len(want)) {
		t.Fatalf("unexpected audit byte counts: %+v", event)
	}
	if event.FlowID == "" || event.DurationMS < 0 {
		t.Fatalf("expected flow metadata: %+v", event)
	}
}

func TestHandleUDPDecisionPassThroughRelaysAndAudits(t *testing.T) {
	upstreamConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen udp upstream: %v", err)
	}
	defer upstreamConn.Close()

	upstreamDone := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 1024)
		_ = upstreamConn.SetDeadline(time.Now().Add(2 * time.Second))
		n, addr, readErr := upstreamConn.ReadFromUDP(buf)
		if readErr != nil {
			t.Errorf("read upstream udp: %v", readErr)
			return
		}
		packet := append([]byte(nil), buf[:n]...)
		upstreamDone <- packet
		if _, writeErr := upstreamConn.WriteToUDP(packet, addr); writeErr != nil {
			t.Errorf("write upstream udp: %v", writeErr)
		}
	}()

	proxyConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen udp proxy: %v", err)
	}
	defer proxyConn.Close()

	clientConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen udp client: %v", err)
	}
	defer clientConn.Close()

	var auditBuf bytes.Buffer
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
		logger:      zap.NewNop(),
		udpConn:     proxyConn,
		udpFallback: &udpPassThroughAdapter{},
		auditor:     newAuditLoggerFromWriter(nopWriteCloser{Writer: &auditBuf}),
	}

	payload := []byte("udp-payload")
	compiled := &policy.CompiledPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	req := &adapterRequest{
		Server:     server,
		Compiled:   compiled,
		SrcIP:      clientConn.LocalAddr().(*net.UDPAddr).IP.String(),
		DestIP:     upstreamConn.LocalAddr().(*net.UDPAddr).IP,
		DestPort:   upstreamConn.LocalAddr().(*net.UDPAddr).Port,
		UDPSource:  clientConn.LocalAddr().(*net.UDPAddr),
		UDPPayload: payload,
	}
	decision := decideTraffic(compiled, classifyUnknownTraffic("udp", "udp", req.DestIP, req.DestPort, "missing_sni"))
	server.handleUDPDecision(req, decision, "")

	reply := make([]byte, 1024)
	_ = clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	n, _, err := clientConn.ReadFromUDP(reply)
	if err != nil {
		t.Fatalf("read udp reply: %v", err)
	}
	if got := reply[:n]; !bytes.Equal(got, payload) {
		t.Fatalf("udp reply = %q, want %q", got, payload)
	}
	if got := <-upstreamDone; !bytes.Equal(got, payload) {
		t.Fatalf("upstream received = %q, want %q", got, payload)
	}

	event := decodeSingleAuditEvent(t, auditBuf.Bytes())
	if event.Action != string(decisionActionPassThrough) || event.Adapter != "udp-pass-through" {
		t.Fatalf("unexpected udp audit event: %+v", event)
	}
	if event.Outcome != "completed" || event.EgressBytes != int64(len(payload)) || event.IngressBytes != int64(len(payload)) {
		t.Fatalf("unexpected udp audit bytes/outcome: %+v", event)
	}
}

func startTCPEchoServer(t *testing.T, expectedBytes int) (*net.TCPAddr, <-chan []byte) {
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
		data := make([]byte, expectedBytes)
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, readErr := io.ReadFull(conn, data)
		if readErr != nil {
			t.Errorf("read upstream tcp: %v", readErr)
			return
		}
		done <- data
		if _, writeErr := conn.Write(data); writeErr != nil {
			t.Errorf("write upstream tcp: %v", writeErr)
		}
	}()
	return listener.Addr().(*net.TCPAddr), done
}

func decodeSingleAuditEvent(t *testing.T, data []byte) auditEvent {
	t.Helper()
	var event auditEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("decode audit event: %v", err)
	}
	return event
}
