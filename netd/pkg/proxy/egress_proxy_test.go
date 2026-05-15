package proxy

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDialTCPUpstreamForRequestUsesSOCKS5Proxy(t *testing.T) {
	allowLocalEgressProxyEndpointsForTest = true
	t.Cleanup(func() { allowLocalEgressProxyEndpointsForTest = false })
	proxyAddr, proxySeen := startTestSOCKS5Proxy(t, "", "")

	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			SandboxID: "sbx_123",
			Egress: policy.CompiledRuleSet{
				Proxy: &policy.CompiledEgressProxy{
					Type:    "socks5",
					Address: proxyAddr.String(),
					Host:    proxyAddr.IP.String(),
					Port:    proxyAddr.Port,
				},
			},
		},
		DestIP:   net.ParseIP("203.0.113.10"),
		DestPort: 443,
		Host:     "example.internal",
	}

	conn, err := server.dialTCPUpstreamForRequest(req)
	if err != nil {
		t.Fatalf("dial via socks5: %v", err)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write target: %v", err)
	}
	body := make([]byte, len("pong"))
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("read target: %v", err)
	}
	_ = conn.Close()

	if string(body) != "pong" {
		t.Fatalf("body = %q, want pong", body)
	}
	seen := <-proxySeen
	if seen.host != "example.internal" || seen.port != 443 {
		t.Fatalf("proxy target = %s:%d, want example.internal:443", seen.host, seen.port)
	}
	if string(seen.payload) != "ping" {
		t.Fatalf("proxy payload = %q, want ping", seen.payload)
	}
}

func TestDialTCPUpstreamForRequestUsesSOCKS5Credentials(t *testing.T) {
	allowLocalEgressProxyEndpointsForTest = true
	t.Cleanup(func() { allowLocalEgressProxyEndpointsForTest = false })
	proxyAddr, proxySeen := startTestSOCKS5Proxy(t, "alice", "secret")
	expiresAt := time.Now().Add(time.Minute).UTC()
	resolver := &stubEgressAuthResolver{
		resp: egressauth.NewUsernamePasswordResolveResponse("corp-proxy", &egressauth.UsernamePasswordDirective{
			Username: "alice",
			Password: "secret",
		}, &expiresAt),
	}
	server := &Server{
		cfg: &config.NetdConfig{
			ProxyUpstreamTimeout: metav1.Duration{Duration: 2 * time.Second},
		},
		authResolver: resolver,
		authCache:    newMemoryEgressAuthCache(),
	}
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			SandboxID: "sbx_123",
			TeamID:    "team_123",
			Egress: policy.CompiledRuleSet{
				Proxy: &policy.CompiledEgressProxy{
					Type:          "socks5",
					Address:       proxyAddr.String(),
					Host:          proxyAddr.IP.String(),
					Port:          proxyAddr.Port,
					CredentialRef: "corp-proxy",
				},
			},
		},
		DestIP:   net.ParseIP("203.0.113.20"),
		DestPort: 443,
	}

	conn, err := server.dialTCPUpstreamForRequest(req)
	if err != nil {
		t.Fatalf("dial via authenticated socks5: %v", err)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write target: %v", err)
	}
	_ = conn.Close()
	seen := <-proxySeen
	if seen.username != "alice" || seen.password != "secret" {
		t.Fatalf("proxy credentials = %q/%q, want alice/secret", seen.username, seen.password)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
}

func TestValidateSOCKS5ProxyEndpointRejectsLoopback(t *testing.T) {
	err := validateSOCKS5ProxyEndpoint(context.Background(), nil, "127.0.0.1")
	if err == nil {
		t.Fatal("expected loopback endpoint rejection")
	}
}

type socks5Connect struct {
	host     string
	port     int
	username string
	password string
	payload  []byte
}

func startTestSOCKS5Proxy(t *testing.T, username, password string) (*net.TCPAddr, <-chan socks5Connect) {
	t.Helper()
	listenIP := net.ParseIP("127.0.0.1")
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: listenIP})
	if err != nil {
		t.Fatalf("listen socks5 proxy: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	seen := make(chan socks5Connect, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		methods, _, err := readSOCKS5Greeting(reader)
		if err != nil {
			return
		}
		selection := byte(socks5MethodNoAuth)
		if username != "" || password != "" {
			if !socks5MethodsContain(methods, socks5MethodUsernamePassword) {
				_, _ = conn.Write([]byte{socks5Version, socks5MethodNoAcceptable})
				return
			}
			selection = socks5MethodUsernamePassword
		}
		if _, err := conn.Write([]byte{socks5Version, selection}); err != nil {
			return
		}
		got := socks5Connect{}
		if selection == socks5MethodUsernamePassword {
			packet, err := readSOCKS5UsernamePasswordRequest(reader)
			if err != nil {
				return
			}
			got.username, got.password, err = parseSOCKS5UsernamePasswordRequest(packet)
			if err != nil || got.username != username || got.password != password {
				_, _ = conn.Write([]byte{socks5UserPassAuthVersion, 0x01})
				return
			}
			if _, err := conn.Write([]byte{socks5UserPassAuthVersion, 0x00}); err != nil {
				return
			}
		}
		host, port, err := readSOCKS5ConnectRequest(reader)
		if err != nil {
			return
		}
		got.host = host
		got.port = port
		_, _ = conn.Write([]byte{socks5Version, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		payload := make([]byte, 4)
		if _, err := io.ReadFull(reader, payload); err == nil {
			got.payload = payload
			_, _ = conn.Write([]byte("pong"))
		}
		seen <- got
	}()
	return ln.Addr().(*net.TCPAddr), seen
}

func readSOCKS5ConnectRequest(reader *bufio.Reader) (string, int, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return "", 0, err
	}
	var host string
	switch header[3] {
	case 0x01:
		ip := make([]byte, 4)
		if _, err := io.ReadFull(reader, ip); err != nil {
			return "", 0, err
		}
		host = net.IP(ip).String()
	case 0x03:
		length, err := reader.ReadByte()
		if err != nil {
			return "", 0, err
		}
		name := make([]byte, int(length))
		if _, err := io.ReadFull(reader, name); err != nil {
			return "", 0, err
		}
		host = string(name)
	default:
		return "", 0, io.ErrUnexpectedEOF
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(reader, portBytes); err != nil {
		return "", 0, err
	}
	return host, int(binary.BigEndian.Uint16(portBytes)), nil
}

func firstNonLoopbackIPv4(t *testing.T) net.IP {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("list interface addresses: %v", err)
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipNet.IP.To4()
		if ip != nil && !ip.IsLoopback() {
			return ip
		}
	}
	t.Fatal("no non-loopback IPv4 address found")
	return nil
}
