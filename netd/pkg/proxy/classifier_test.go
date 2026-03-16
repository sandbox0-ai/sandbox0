package proxy

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type stubTCPClassifier struct {
	name     string
	result   *classificationResult
	decision tcpClassifierDecision
}

func (c stubTCPClassifier) Name() string { return c.name }

func (c stubTCPClassifier) Classify(_ *tcpClassifyContext) (*classificationResult, tcpClassifierDecision) {
	return c.result, c.decision
}

type scriptedConn struct {
	fragments [][]byte
	readIndex int
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if c.readIndex >= len(c.fragments) {
		return 0, io.EOF
	}
	fragment := c.fragments[c.readIndex]
	c.readIndex++
	n := copy(p, fragment)
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *scriptedConn) Close() error                { return nil }
func (c *scriptedConn) LocalAddr() net.Addr         { return stubAddr("local") }
func (c *scriptedConn) RemoteAddr() net.Addr        { return stubAddr("remote") }
func (c *scriptedConn) SetDeadline(time.Time) error { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *scriptedConn) SetWriteDeadline(time.Time) error {
	return nil
}

type timeoutConn struct{}

func (c *timeoutConn) Read(_ []byte) (int, error)  { return 0, stubTimeoutError{} }
func (c *timeoutConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *timeoutConn) Close() error                { return nil }
func (c *timeoutConn) LocalAddr() net.Addr         { return stubAddr("local") }
func (c *timeoutConn) RemoteAddr() net.Addr        { return stubAddr("remote") }
func (c *timeoutConn) SetDeadline(time.Time) error { return nil }
func (c *timeoutConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *timeoutConn) SetWriteDeadline(time.Time) error {
	return nil
}

type stubTimeoutError struct{}

func (stubTimeoutError) Error() string   { return "timeout" }
func (stubTimeoutError) Timeout() bool   { return true }
func (stubTimeoutError) Temporary() bool { return true }

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

type captureConn struct {
	written []byte
}

func (c *captureConn) Read(p []byte) (int, error) { return 0, io.EOF }
func (c *captureConn) Write(p []byte) (int, error) {
	c.written = append(c.written, p...)
	return len(p), nil
}
func (c *captureConn) Close() error                { return nil }
func (c *captureConn) LocalAddr() net.Addr         { return stubAddr("local") }
func (c *captureConn) RemoteAddr() net.Addr        { return stubAddr("remote") }
func (c *captureConn) SetDeadline(time.Time) error { return nil }
func (c *captureConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *captureConn) SetWriteDeadline(time.Time) error {
	return nil
}

func TestClassifyTCPUsesRegistryOrder(t *testing.T) {
	want := &classificationResult{
		Classification: classifyKnownTraffic("tcp", "ssh", net.ParseIP("8.8.8.8"), 22, ""),
	}
	result, err := classifyTCP([]tcpClassifier{
		stubTCPClassifier{name: "miss", decision: tcpClassifierNoMatch},
		stubTCPClassifier{name: "hit", result: want, decision: tcpClassifierMatched},
	}, &tcpClassifyContext{})
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result != want {
		t.Fatalf("classifyTCP returned %+v, want %+v", result, want)
	}
}

func TestClassifyTCPReadsFragmentedHTTPRequest(t *testing.T) {
	ctx := &tcpClassifyContext{
		OrigIP:      net.ParseIP("8.8.8.8"),
		OrigPort:    80,
		Conn:        &scriptedConn{fragments: [][]byte{[]byte("GET / HTTP/1.1\r\nHo"), []byte("st: Example.COM\r\n"), []byte("\r\n")}},
		HeaderLimit: 1024,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "http" {
		t.Fatalf("protocol = %q, want http", result.Classification.Protocol)
	}
	if result.Host != "example.com" {
		t.Fatalf("host = %q, want example.com", result.Host)
	}
	req := &adapterRequest{}
	result.Apply(req)
	data, readErr := io.ReadAll(req.Prefix)
	if readErr != nil {
		t.Fatalf("failed to read replay prefix: %v", readErr)
	}
	if string(data) != "GET / HTTP/1.1\r\nHost: Example.COM\r\n\r\n" {
		t.Fatalf("replay prefix = %q", string(data))
	}
}

func TestDefaultHTTPClassifiersClassifyHTTPRequest(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = io.WriteString(client, "GET / HTTP/1.1\r\nHost: Example.COM\r\n\r\n")
		_ = client.Close()
	}()

	ctx := &tcpClassifyContext{
		OrigIP:      net.ParseIP("8.8.8.8"),
		OrigPort:    80,
		Conn:        server,
		HeaderLimit: 1024,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "http" {
		t.Fatalf("protocol = %q, want http", result.Classification.Protocol)
	}
	if result.Host != "example.com" {
		t.Fatalf("host = %q, want example.com", result.Host)
	}
	req := &adapterRequest{}
	result.Apply(req)
	data, readErr := io.ReadAll(req.Prefix)
	if readErr != nil {
		t.Fatalf("failed to read replay prefix: %v", readErr)
	}
	if string(data) != "GET / HTTP/1.1\r\nHost: Example.COM\r\n\r\n" {
		t.Fatalf("replay prefix = %q", string(data))
	}
}

func TestClassifyTCPReadsFragmentedTLSClientHello(t *testing.T) {
	hello := buildTLSClientHello(t)
	if len(hello) < 32 {
		t.Fatalf("unexpected client hello length: %d", len(hello))
	}
	ctx := &tcpClassifyContext{
		OrigIP:   net.ParseIP("8.8.8.8"),
		OrigPort: 443,
		Conn: &scriptedConn{fragments: [][]byte{
			append([]byte(nil), hello[:9]...),
			append([]byte(nil), hello[9:24]...),
			append([]byte(nil), hello[24:]...),
		}},
		HeaderLimit: 4096,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "tls" {
		t.Fatalf("protocol = %q, want tls", result.Classification.Protocol)
	}
	if result.Host != "example.com" {
		t.Fatalf("host = %q, want example.com", result.Host)
	}
}

func TestDefaultHTTPClassifiersFallbackToSSHBanner(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = io.WriteString(client, "SSH-2.0-OpenSSH_9.0\r\n")
		_ = client.Close()
	}()

	ctx := &tcpClassifyContext{
		OrigIP:      net.ParseIP("8.8.8.8"),
		OrigPort:    22,
		Conn:        server,
		HeaderLimit: 1024,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "ssh" {
		t.Fatalf("protocol = %q, want ssh", result.Classification.Protocol)
	}
	req := &adapterRequest{}
	result.Apply(req)
	data, readErr := io.ReadAll(req.Prefix)
	if readErr != nil {
		t.Fatalf("failed to read replay prefix: %v", readErr)
	}
	if string(data) != "SSH-2.0-OpenSSH_9.0\r\n" {
		t.Fatalf("replay prefix = %q", string(data))
	}
}

func TestClassifyTCPReadsFragmentedPostgresStartup(t *testing.T) {
	packet := make([]byte, 8)
	binary.BigEndian.PutUint32(packet[:4], 8)
	binary.BigEndian.PutUint32(packet[4:8], 80877103)
	ctx := &tcpClassifyContext{
		OrigIP:      net.ParseIP("8.8.8.8"),
		OrigPort:    5432,
		Conn:        &scriptedConn{fragments: [][]byte{packet[:4], packet[4:]}},
		HeaderLimit: 1024,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "postgres" {
		t.Fatalf("protocol = %q, want postgres", result.Classification.Protocol)
	}
}

func TestUDPSNIClassifierEngagesForPlatformDomainRules(t *testing.T) {
	classifier := &udpSNIClassifier{}
	result, matched := classifier.Classify(&udpClassifyContext{
		Compiled: &policy.CompiledPolicy{
			Mode: v1alpha1.NetworkModeAllowAll,
			Platform: &policy.PlatformPolicy{
				DeniedDomains: []policy.DomainRule{{Pattern: "blocked.example.com", Type: policy.DomainMatchExact}},
			},
		},
		SrcIP:    "10.0.0.2",
		DestIP:   net.ParseIP("8.8.8.8"),
		DestPort: 443,
	})
	if !matched {
		t.Fatalf("expected udp sni classifier to engage when platform domain rules exist")
	}
	if result == nil || result.Classification.UnknownReason != "missing_sni" {
		t.Fatalf("unexpected classifier result: %+v", result)
	}
}

func TestDefaultTCPClassifiersClassifyPostgresStartup(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	packet := make([]byte, 8)
	binary.BigEndian.PutUint32(packet[:4], 8)
	binary.BigEndian.PutUint32(packet[4:8], 80877103)

	go func() {
		_, _ = client.Write(packet)
		_ = client.Close()
	}()

	ctx := &tcpClassifyContext{
		OrigIP:      net.ParseIP("8.8.8.8"),
		OrigPort:    5432,
		Conn:        server,
		HeaderLimit: 1024,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "postgres" {
		t.Fatalf("protocol = %q, want postgres", result.Classification.Protocol)
	}
	req := &adapterRequest{}
	result.Apply(req)
	data, readErr := io.ReadAll(req.Prefix)
	if readErr != nil {
		t.Fatalf("failed to read replay prefix: %v", readErr)
	}
	if string(data) != string(packet) {
		t.Fatalf("replay prefix = %v, want %v", data, packet)
	}
}

func TestDefaultTCPClassifiersClassifySSHBanner(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = io.WriteString(client, "SSH-2.0-OpenSSH_9.0\r\n")
		_ = client.Close()
	}()

	ctx := &tcpClassifyContext{
		OrigIP:      net.ParseIP("8.8.8.8"),
		OrigPort:    443,
		Conn:        server,
		HeaderLimit: 1024,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "ssh" {
		t.Fatalf("protocol = %q, want ssh", result.Classification.Protocol)
	}
	req := &adapterRequest{}
	result.Apply(req)
	data, readErr := io.ReadAll(req.Prefix)
	if readErr != nil {
		t.Fatalf("failed to read replay prefix: %v", readErr)
	}
	if string(data) != "SSH-2.0-OpenSSH_9.0\r\n" {
		t.Fatalf("replay prefix = %q", string(data))
	}
}

func TestDefaultTCPClassifiersFallbackToUnknown(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = io.WriteString(client, "opaque-binary-prefix")
		_ = client.Close()
	}()

	ctx := &tcpClassifyContext{
		OrigIP:      net.ParseIP("8.8.8.8"),
		OrigPort:    1234,
		Conn:        server,
		HeaderLimit: 1024,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.UnknownReason != "unclassified" {
		t.Fatalf("unknown reason = %q, want unclassified", result.Classification.UnknownReason)
	}
	if result.Classification.Protocol != "unknown" {
		t.Fatalf("protocol = %q, want unknown", result.Classification.Protocol)
	}
}

func TestDefaultTCPClassifiersFallbackToUnknownWhenClientIsIdle(t *testing.T) {
	ctx := &tcpClassifyContext{
		OrigIP:           net.ParseIP("8.8.8.8"),
		OrigPort:         1234,
		Conn:             &timeoutConn{},
		HeaderLimit:      1024,
		FirstByteTimeout: 10 * time.Millisecond,
	}
	result, err := classifyTCP(defaultTCPClassifiers(), ctx)
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result.Classification.Protocol != "unknown" {
		t.Fatalf("protocol = %q, want unknown", result.Classification.Protocol)
	}
	if result.Classification.UnknownReason != "client_idle" {
		t.Fatalf("unknown reason = %q, want client_idle", result.Classification.UnknownReason)
	}
	if result.Error != nil {
		t.Fatalf("unexpected error = %v", result.Error)
	}
}

func TestDefaultUDPClassifiersFallbackToGenericUDP(t *testing.T) {
	result, err := classifyUDP(defaultUDPClassifiers(), &udpClassifyContext{
		Compiled: &policy.CompiledPolicy{Mode: v1alpha1.NetworkModeAllowAll},
		SrcIP:    "10.0.0.2",
		DestIP:   net.ParseIP("8.8.8.8"),
		DestPort: 443,
	})
	if err != nil {
		t.Fatalf("classifyUDP returned error: %v", err)
	}
	if result.Classification.Protocol != "udp" {
		t.Fatalf("protocol = %q, want udp", result.Classification.Protocol)
	}
	if result.Classification.UnknownReason != "" {
		t.Fatalf("unknown reason = %q, want empty", result.Classification.UnknownReason)
	}
}

func buildTLSClientHello(t *testing.T) []byte {
	t.Helper()
	conn := &captureConn{}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         "example.com",
		InsecureSkipVerify: true,
	})
	_ = tlsConn.SetDeadline(time.Now().Add(250 * time.Millisecond))
	_ = tlsConn.Handshake()
	_ = tlsConn.Close()
	if len(conn.written) == 0 {
		t.Fatalf("expected tls client hello bytes")
	}
	return append([]byte(nil), conn.written...)
}
