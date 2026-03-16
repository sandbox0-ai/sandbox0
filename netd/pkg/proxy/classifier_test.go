package proxy

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type stubTCPClassifier struct {
	name   string
	result *classificationResult
	match  bool
}

func (c stubTCPClassifier) Name() string { return c.name }

func (c stubTCPClassifier) Classify(_ *tcpClassifyContext) (*classificationResult, bool) {
	return c.result, c.match
}

func TestClassifyTCPUsesRegistryOrder(t *testing.T) {
	want := &classificationResult{
		Classification: classifyKnownTraffic("tcp", "ssh", net.ParseIP("8.8.8.8"), 22, ""),
	}
	result, err := classifyTCP([]tcpClassifier{
		stubTCPClassifier{name: "miss", match: false},
		stubTCPClassifier{name: "hit", result: want, match: true},
	}, &tcpClassifyContext{})
	if err != nil {
		t.Fatalf("classifyTCP returned error: %v", err)
	}
	if result != want {
		t.Fatalf("classifyTCP returned %+v, want %+v", result, want)
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
		OrigIP:       net.ParseIP("8.8.8.8"),
		OrigPort:     80,
		Conn:         server,
		ObservedConn: &recordingConn{Conn: server},
	}
	result, err := classifyTCP(defaultHTTPClassifiers(), ctx)
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
	if req.HTTPRequest == nil || req.HTTPReader == nil {
		t.Fatalf("expected HTTP request and reader to be populated")
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
		OrigIP:       net.ParseIP("8.8.8.8"),
		OrigPort:     22,
		Conn:         server,
		ObservedConn: &recordingConn{Conn: server},
	}
	result, err := classifyTCP(defaultHTTPClassifiers(), ctx)
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

func TestDefaultHTTPSClassifiersClassifyPostgresStartup(t *testing.T) {
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
	result, err := classifyTCP(defaultHTTPSClassifiers(), ctx)
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
